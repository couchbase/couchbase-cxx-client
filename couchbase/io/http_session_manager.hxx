/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <couchbase/io/http_session.hxx>
#include <couchbase/service_type.hxx>
#include <couchbase/io/http_context.hxx>
#include <couchbase/operations/http_noop.hxx>
#include <couchbase/io/http_command.hxx>

#include <couchbase/tracing/noop_tracer.hxx>
#include <couchbase/metrics/meter.hxx>

#include <random>

namespace couchbase::io
{

class http_session_manager : public std::enable_shared_from_this<http_session_manager>
{
  public:
    http_session_manager(std::string client_id, asio::io_context& ctx, asio::ssl::context& tls)
      : client_id_(std::move(client_id))
      , ctx_(ctx)
      , tls_(tls)
    {
    }

    void set_tracer(tracing::request_tracer* tracer)
    {
        tracer_ = tracer;
    }

    void set_meter(metrics::meter* meter)
    {
        meter_ = meter;
    }

    void set_configuration(const topology::configuration& config, const cluster_options& options)
    {
        options_ = options;
        config_ = config;
        next_index_ = 0;
        if (config_.nodes.size() > 1) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<std::size_t> dis(0, config_.nodes.size() - 1);
            next_index_ = dis(gen);
        }
    }

    void export_diag_info(diag::diagnostics_result& res)
    {
        std::scoped_lock lock(sessions_mutex_);

        for (const auto& [type, sessions] : busy_sessions_) {
            for (const auto& session : sessions) {
                if (session) {
                    res.services[type].emplace_back(session->diag_info());
                }
            }
        }
        for (const auto& [type, sessions] : idle_sessions_) {
            for (const auto& session : sessions) {
                if (session) {
                    res.services[type].emplace_back(session->diag_info());
                }
            }
        }
    }

    template<typename Collector>
    void ping(std::set<service_type> services, std::shared_ptr<Collector> collector, const couchbase::cluster_credentials& credentials)
    {
        std::array<service_type, 4> known_types{ service_type::query, service_type::analytics, service_type::search, service_type::view };
        for (const auto& node : config_.nodes) {
            for (auto type : known_types) {
                if (services.find(type) == services.end()) {
                    continue;
                }
                std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
                if (port != 0) {
                    std::scoped_lock lock(sessions_mutex_);
                    std::shared_ptr<http_session> session;
                    session = options_.enable_tls ? std::make_shared<http_session>(type,
                                                                                   client_id_,
                                                                                   ctx_,
                                                                                   tls_,
                                                                                   credentials,
                                                                                   node.hostname_for(options_.network),
                                                                                   std::to_string(port),
                                                                                   http_context{ config_, options_, query_cache_ })
                                                  : std::make_shared<http_session>(type,
                                                                                   client_id_,
                                                                                   ctx_,
                                                                                   credentials,
                                                                                   node.hostname_for(options_.network),
                                                                                   std::to_string(port),
                                                                                   http_context{ config_, options_, query_cache_ });
                    session->start();
                    session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                        for (auto& s : self->busy_sessions_[type]) {
                            if (s && s->id() == id) {
                                s.reset();
                            }
                        }
                        for (auto& s : self->idle_sessions_[type]) {
                            if (s && s->id() == id) {
                                s.reset();
                            }
                        }
                    });
                    busy_sessions_[type].push_back(session);
                    operations::http_noop_request request{};
                    request.type = type;
                    auto cmd = std::make_shared<operations::http_command<operations::http_noop_request>>(ctx_, request, tracer_, meter_);
                    cmd->start([start = std::chrono::steady_clock::now(),
                                self = shared_from_this(),
                                type,
                                cmd,
                                handler = std::move(collector->build_reporter())](std::error_code ec, io::http_response&& msg) {
                        diag::ping_state state = diag::ping_state::ok;
                        std::optional<std::string> error{};
                        if (ec) {
                            state = diag::ping_state::error;
                            error.emplace(fmt::format("code={}, message={}, http_code={}", ec.value(), ec.message(), msg.status_code));
                        }
                        handler(diag::endpoint_ping_info{
                          type,
                          cmd->session_->id(),
                          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start),
                          cmd->session_->remote_address(),
                          cmd->session_->local_address(),
                          state,
                          {},
                          error });
                        self->check_in(type, std::move(cmd->session_));
                    });
                    cmd->send_to(session);
                }
            }
        }
    }

    std::shared_ptr<http_session> check_out(service_type type, const couchbase::cluster_credentials& credentials)
    {
        std::scoped_lock lock(sessions_mutex_);
        idle_sessions_[type].remove_if([](const auto& s) -> bool { return !s; });
        busy_sessions_[type].remove_if([](const auto& s) -> bool { return !s; });
        if (idle_sessions_[type].empty()) {
            auto [hostname, port] = next_node(type);
            if (port == 0) {
                return nullptr;
            }
            config_.nodes.size();
            std::shared_ptr<http_session> session;
            if (options_.enable_tls) {
                session = std::make_shared<http_session>(type,
                                                         client_id_,
                                                         ctx_,
                                                         tls_,
                                                         credentials,
                                                         hostname,
                                                         std::to_string(port),
                                                         http_context{ config_, options_, query_cache_ });
            } else {
                session = std::make_shared<http_session>(
                  type, client_id_, ctx_, credentials, hostname, std::to_string(port), http_context{ config_, options_, query_cache_ });
            }
            session->start();

            session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                std::scoped_lock inner_lock(self->sessions_mutex_);
                for (auto& s : self->busy_sessions_[type]) {
                    if (s && s->id() == id) {
                        s.reset();
                    }
                }
                for (auto& s : self->idle_sessions_[type]) {
                    if (s && s->id() == id) {
                        s.reset();
                    }
                }
            });
            busy_sessions_[type].push_back(session);
            return session;
        }
        auto session = idle_sessions_[type].front();
        idle_sessions_[type].pop_front();
        session->reset_idle();
        busy_sessions_[type].push_back(session);
        return session;
    }

    void check_in(service_type type, std::shared_ptr<http_session> session)
    {
        if (!session->keep_alive()) {
            return session->stop();
        }
        if (!session->is_stopped()) {
            session->set_idle(options_.idle_http_connection_timeout);
            std::scoped_lock lock(sessions_mutex_);
            LOG_DEBUG("{} put HTTP session back to idle connections", session->log_prefix());
            idle_sessions_[type].push_back(session);
            busy_sessions_[type].remove_if([id = session->id()](const auto& s) -> bool { return !s || s->id() == id; });
        }
    }

    void close()
    {
        for (auto& [type, sessions] : idle_sessions_) {
            for (auto& s : sessions) {
                if (s) {
                    s->reset_idle();
                    s.reset();
                }
            }
        }
        for (auto& [type, sessions] : busy_sessions_) {
            for (auto& s : sessions) {
                s.reset();
            }
        }
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler, const couchbase::cluster_credentials& credentials)
    {
        auto session = check_out(Request::type, credentials);
        if (!session) {
            typename Request::error_context_type ctx{};
            ctx.ec = error::common_errc::service_not_available;
            using response_type = typename Request::encoded_response_type;
            return handler(request.make_response(std::move(ctx), response_type{}));
        }

        auto cmd = std::make_shared<operations::http_command<Request>>(ctx_, request, tracer_, meter_);
        cmd->start(
          [self = shared_from_this(), cmd, handler = std::forward<Handler>(handler)](std::error_code ec, io::http_response&& msg) mutable {
              using command_type = typename decltype(cmd)::element_type;
              using encoded_response_type = typename command_type::encoded_response_type;
              using error_context_type = typename command_type::error_context_type;
              encoded_response_type resp{ std::move(msg) };
              error_context_type ctx{};
              ctx.ec = ec;
              ctx.client_context_id = cmd->request.client_context_id;
              ctx.method = cmd->encoded.method;
              ctx.path = cmd->encoded.path;
              ctx.last_dispatched_from = cmd->session_->local_address();
              ctx.last_dispatched_to = cmd->session_->remote_address();
              ctx.http_status = resp.status_code;
              ctx.http_body = resp.body;
              handler(cmd->request.make_response(std::move(ctx), std::move(resp)));
              self->check_in(Request::type, std::move(cmd->session_));
          });
        cmd->send_to(session);
    }

  private:
    std::pair<std::string, std::uint16_t> next_node(service_type type)
    {
        auto candidates = config_.nodes.size();
        while (candidates > 0) {
            --candidates;
            const auto& node = config_.nodes[next_index_];
            next_index_ = (next_index_ + 1) % config_.nodes.size();
            std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
            if (port != 0) {
                return { node.hostname_for(options_.network), port };
            }
        }
        return { "", 0 };
    }

    std::string client_id_;
    asio::io_context& ctx_;
    asio::ssl::context& tls_;
    tracing::request_tracer* tracer_{ nullptr };
    metrics::meter* meter_{ nullptr };
    cluster_options options_{};

    topology::configuration config_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> idle_sessions_{};
    std::size_t next_index_{ 0 };
    std::mutex sessions_mutex_{};
    query_cache query_cache_{};
};
} // namespace couchbase::io
