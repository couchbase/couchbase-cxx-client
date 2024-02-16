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

#include "core/config_listener.hxx"
#include "core/operations/http_noop.hxx"
#include "core/service_type.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "couchbase/metrics/meter.hxx"
#include "http_command.hxx"
#include "http_context.hxx"
#include "http_session.hxx"
#include "http_traits.hxx"

#include <gsl/narrow>

#include <chrono>
#include <optional>
#include <random>

namespace couchbase::core::io
{

class http_session_manager
  : public std::enable_shared_from_this<http_session_manager>
  , public config_listener
{
  public:
    http_session_manager(std::string client_id, asio::io_context& ctx, asio::ssl::context& tls)
      : client_id_(std::move(client_id))
      , ctx_(ctx)
      , tls_(tls)
    {
    }

    void set_tracer(std::shared_ptr<couchbase::tracing::request_tracer> tracer)
    {
        tracer_ = std::move(tracer);
    }

    void set_meter(std::shared_ptr<couchbase::metrics::meter> meter)
    {
        meter_ = std::move(meter);
    }

    auto configuration_capabilities() const -> configuration_capabilities
    {
        std::scoped_lock config_lock(config_mutex_);
        return config_.capabilities;
    }

    void update_config(topology::configuration config) override
    {
        std::scoped_lock config_lock(config_mutex_, sessions_mutex_);
        config_ = std::move(config);
        for (auto& [type, sessions] : idle_sessions_) {
            sessions.remove_if([&opts = options_, &cfg = config_](const auto& session) {
                return session && !cfg.has_node(opts.network, session->type(), opts.enable_tls, session->hostname(), session->port());
            });
        }
    }

    void set_configuration(const topology::configuration& config, const cluster_options& options)
    {
        std::size_t next_index = 0;
        if (config.nodes.size() > 1) {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<std::size_t> dis(0, config.nodes.size() - 1);
            next_index = dis(gen);
        }
        std::scoped_lock lock(config_mutex_, next_index_mutex_);
        options_ = options;
        next_index_ = next_index;
        config_ = config;
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
    void ping(std::set<service_type> services,
              std::optional<std::chrono::milliseconds> timeout,
              std::shared_ptr<Collector> collector,
              const couchbase::core::cluster_credentials& credentials)
    {
        std::array known_types{
            service_type::query, service_type::analytics, service_type::search,
            service_type::view,  service_type::eventing,  service_type::management,
        };
        std::vector<topology::configuration::node> nodes{};
        {
            std::scoped_lock lock(config_mutex_);
            nodes = config_.nodes;
        }
        for (const auto& node : nodes) {
            for (auto type : known_types) {
                if (services.find(type) == services.end()) {
                    continue;
                }
                std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
                if (port != 0) {
                    std::shared_ptr<http_session> session;
                    const auto& hostname = node.hostname_for(options_.network);
                    session = options_.enable_tls
                                ? std::make_shared<http_session>(type,
                                                                 client_id_,
                                                                 ctx_,
                                                                 tls_,
                                                                 credentials,
                                                                 hostname,
                                                                 std::to_string(port),
                                                                 http_context{ config_, options_, query_cache_, hostname, port })
                                : std::make_shared<http_session>(type,
                                                                 client_id_,
                                                                 ctx_,
                                                                 credentials,
                                                                 hostname,
                                                                 std::to_string(port),
                                                                 http_context{ config_, options_, query_cache_, hostname, port });
                    session->start();
                    session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
                        std::scoped_lock lock(self->sessions_mutex_);
                        self->busy_sessions_[type].remove_if([&id](const auto& s) { return !s || s->id() == id; });
                        self->idle_sessions_[type].remove_if([&id](const auto& s) { return !s || s->id() == id; });
                    });
                    {
                        std::scoped_lock lock(sessions_mutex_);
                        busy_sessions_[type].push_back(session);
                    }
                    operations::http_noop_request request{};
                    request.type = type;
                    request.timeout = timeout;
                    auto cmd = std::make_shared<operations::http_command<operations::http_noop_request>>(
                      ctx_, request, tracer_, meter_, options_.default_timeout_for(request.type));
                    cmd->start([start = std::chrono::steady_clock::now(),
                                self = shared_from_this(),
                                type,
                                cmd,
                                handler = collector->build_reporter()](std::error_code ec, io::http_response&& msg) {
                        diag::ping_state state = diag::ping_state::ok;
                        std::optional<std::string> error{};
                        if (ec) {
                            if (ec == errc::common::unambiguous_timeout || ec == errc::common::ambiguous_timeout) {
                                state = diag::ping_state::timeout;
                            } else {
                                state = diag::ping_state::error;
                            }
                            error.emplace(fmt::format("code={}, message={}, http_code={}", ec.value(), ec.message(), msg.status_code));
                        }
                        handler->report(diag::endpoint_ping_info{
                          type,
                          cmd->session_->id(),
                          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start),
                          cmd->session_->remote_address(),
                          cmd->session_->local_address(),
                          state,
                          {},
                          error });
                        self->check_in(type, cmd->session_);
                    });
                    cmd->send_to(session);
                }
            }
        }
    }

    std::pair<std::error_code, std::shared_ptr<http_session>> check_out(service_type type,
                                                                        const couchbase::core::cluster_credentials& credentials,
                                                                        const std::string& preferred_node)
    {
        std::scoped_lock lock(sessions_mutex_);
        idle_sessions_[type].remove_if([](const auto& s) { return !s; });
        busy_sessions_[type].remove_if([](const auto& s) { return !s; });
        if (idle_sessions_[type].empty()) {
            auto [hostname, port] = preferred_node.empty() ? next_node(type) : lookup_node(type, preferred_node);
            if (port == 0) {
                return { errc::common::service_not_available, nullptr };
            }
            auto session = bootstrap_session(type, credentials, hostname, port);
            busy_sessions_[type].push_back(session);
            return { {}, session };
        }
        std::shared_ptr<http_session> session{};
        if (preferred_node.empty()) {
            session = idle_sessions_[type].front();
            idle_sessions_[type].pop_front();
            session->reset_idle();
        } else {
            auto ptr = std::find_if(idle_sessions_[type].begin(), idle_sessions_[type].end(), [preferred_node](const auto& s) {
                return s->remote_address() == preferred_node;
            });
            if (ptr != idle_sessions_[type].end()) {
                session = *ptr;
                idle_sessions_[type].erase(ptr);
                session->reset_idle();
            } else {
                auto [hostname, port] = split_host_port(preferred_node);
                session = bootstrap_session(type, credentials, hostname, port);
            }
        }
        busy_sessions_[type].push_back(session);
        return { {}, session };
    }

    void check_in(service_type type, std::shared_ptr<http_session> session)
    {
        {
            std::scoped_lock lock(config_mutex_);
            if (!session->keep_alive() ||
                !config_.has_node(options_.network, session->type(), options_.enable_tls, session->hostname(), session->port())) {
                return asio::post(session->get_executor(), [session]() { session->stop(); });
            }
        }
        if (!session->is_stopped()) {
            session->set_idle(options_.idle_http_connection_timeout);
            CB_LOG_DEBUG("{} put HTTP session back to idle connections", session->log_prefix());
            std::scoped_lock lock(sessions_mutex_);
            idle_sessions_[type].push_back(session);
            busy_sessions_[type].remove_if([id = session->id()](const auto& s) -> bool { return !s || s->id() == id; });
        }
    }

    void close()
    {
        std::scoped_lock lock(sessions_mutex_);
        for (auto& [type, sessions] : idle_sessions_) {
            for (auto& s : sessions) {
                if (s) {
                    s->reset_idle();
                    s.reset();
                }
            }
        }
        busy_sessions_.clear();
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler, const couchbase::core::cluster_credentials& credentials)
    {
        std::string preferred_node;
        if constexpr (http_traits::supports_sticky_node_v<Request>) {
            if (request.send_to_node) {
                preferred_node = *request.send_to_node;
            }
        }
        auto [error, session] = check_out(request.type, credentials, preferred_node);
        if (error) {
            typename Request::error_context_type ctx{};
            ctx.ec = error;
            using response_type = typename Request::encoded_response_type;
            return handler(request.make_response(std::move(ctx), response_type{}));
        }
        const auto& http_ctx = session->http_context();

        auto cmd =
          std::make_shared<operations::http_command<Request>>(ctx_, request, tracer_, meter_, options_.default_timeout_for(request.type));
        cmd->start([self = shared_from_this(), cmd, http_ctx, handler = std::forward<Handler>(handler)](std::error_code ec,
                                                                                                        io::http_response&& msg) mutable {
            using command_type = typename decltype(cmd)::element_type;
            using encoded_response_type = typename command_type::encoded_response_type;
            using error_context_type = typename command_type::error_context_type;
            encoded_response_type resp{ std::move(msg) };
            error_context_type ctx{};
            ctx.ec = ec;
            ctx.client_context_id = cmd->client_context_id_;
            ctx.method = cmd->encoded.method;
            ctx.path = cmd->encoded.path;
            ctx.last_dispatched_from = cmd->session_->local_address();
            ctx.last_dispatched_to = cmd->session_->remote_address();
            ctx.http_status = resp.status_code;
            ctx.http_body = resp.body.data();
            ctx.hostname = http_ctx.hostname;
            ctx.port = http_ctx.port;
            handler(cmd->request.make_response(std::move(ctx), std::move(resp)));
            self->check_in(cmd->request.type, cmd->session_);
        });
        cmd->send_to(session);
    }

  private:
    std::shared_ptr<http_session> bootstrap_session(service_type type,
                                                    const couchbase::core::cluster_credentials& credentials,
                                                    const std::string& hostname,
                                                    std::uint16_t port)
    {
        std::shared_ptr<http_session> session;
        if (options_.enable_tls) {
            session = std::make_shared<http_session>(type,
                                                     client_id_,
                                                     ctx_,
                                                     tls_,
                                                     credentials,
                                                     hostname,
                                                     std::to_string(port),
                                                     http_context{ config_, options_, query_cache_, hostname, port });
        } else {
            session = std::make_shared<http_session>(type,
                                                     client_id_,
                                                     ctx_,
                                                     credentials,
                                                     hostname,
                                                     std::to_string(port),
                                                     http_context{ config_, options_, query_cache_, hostname, port });
        }
        session->start();

        session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
            std::scoped_lock inner_lock(self->sessions_mutex_);
            self->busy_sessions_[type].remove_if([&id](const auto& s) { return !s || s->id() == id; });
            self->idle_sessions_[type].remove_if([&id](const auto& s) { return !s || s->id() == id; });
        });
        return session;
    }

    std::pair<std::string, std::uint16_t> next_node(service_type type)
    {
        std::scoped_lock lock(config_mutex_);
        auto candidates = config_.nodes.size();
        while (candidates > 0) {
            --candidates;
            std::scoped_lock index_lock(next_index_mutex_);
            const auto& node = config_.nodes[next_index_];
            next_index_ = (next_index_ + 1) % config_.nodes.size();
            std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
            if (port != 0) {
                return { node.hostname_for(options_.network), port };
            }
        }
        return { "", static_cast<std::uint16_t>(0U) };
    }

    std::pair<std::string, std::uint16_t> split_host_port(const std::string& address)
    {
        auto last_colon = address.find_last_of(':');
        if (last_colon == std::string::npos || address.size() - 1 == last_colon) {
            return { "", static_cast<std::uint16_t>(0U) };
        }
        auto hostname = address.substr(0, last_colon);
        auto port = gsl::narrow_cast<std::uint16_t>(std::stoul(address.substr(last_colon + 1)));
        return { hostname, port };
    }

    std::pair<std::string, std::uint16_t> lookup_node(service_type type, const std::string& preferred_node)
    {
        std::scoped_lock lock(config_mutex_);
        auto [hostname, port] = split_host_port(preferred_node);
        if (std::none_of(config_.nodes.begin(), config_.nodes.end(), [this, type, &h = hostname, &p = port](const auto& node) {
                return node.hostname == h && node.port_or(options_.network, type, options_.enable_tls, 0) == p;
            })) {
            return { "", static_cast<std::uint16_t>(0U) };
        }
        return { hostname, port };
    }

    std::string client_id_;
    asio::io_context& ctx_;
    asio::ssl::context& tls_;
    std::shared_ptr<couchbase::tracing::request_tracer> tracer_{ nullptr };
    std::shared_ptr<couchbase::metrics::meter> meter_{ nullptr };
    cluster_options options_{};

    topology::configuration config_{};
    mutable std::mutex config_mutex_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions_{};
    std::map<service_type, std::list<std::shared_ptr<http_session>>> idle_sessions_{};
    std::size_t next_index_{ 0 };
    std::mutex next_index_mutex_{};
    std::mutex sessions_mutex_{};
    query_cache query_cache_{};
};
} // namespace couchbase::core::io
