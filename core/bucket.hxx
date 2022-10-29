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

#include "config_listener.hxx"
#include "core/io/mcbp_command.hxx"
#include "couchbase/metrics/meter.hxx"
#include "couchbase/tracing/request_tracer.hxx"
#include "operations.hxx"
#include "origin.hxx"

#include <asio/io_context.hpp>
#include <asio/ssl.hpp>
#include <queue>
#include <utility>
#include <vector>

namespace couchbase::core
{
class bucket
  : public std::enable_shared_from_this<bucket>
  , public config_listener
{
  public:
    explicit bucket(const std::string& client_id,
                    asio::io_context& ctx,
                    asio::ssl::context& tls,
                    std::shared_ptr<couchbase::tracing::request_tracer> tracer,
                    std::shared_ptr<couchbase::metrics::meter> meter,
                    std::string name,
                    couchbase::core::origin origin,
                    const std::vector<protocol::hello_feature>& known_features,
                    std::shared_ptr<impl::bootstrap_state_listener> state_listener)

      : client_id_(client_id)
      , ctx_(ctx)
      , tls_(tls)
      , tracer_(std::move(tracer))
      , meter_(std::move(meter))
      , name_(std::move(name))
      , origin_(std::move(origin))
      , known_features_(known_features)
      , state_listener_(std::move(state_listener))
    {
        log_prefix_ = fmt::format("[{}/{}]", client_id_, name_);
    }

    ~bucket()
    {
        close();
    }

    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    /**
     * copies nodes from rhs that are not in lhs to output vector
     */
    static void diff_nodes(const std::vector<topology::configuration::node>& lhs,
                           const std::vector<topology::configuration::node>& rhs,
                           std::vector<topology::configuration::node>& output)
    {
        for (const auto& re : rhs) {
            bool known = false;
            for (const auto& le : lhs) {
                if (le.hostname == re.hostname && le.services_plain.management.value_or(0) == re.services_plain.management.value_or(0)) {
                    known = true;
                    break;
                }
            }
            if (!known) {
                output.push_back(re);
            }
        }
    }

    void update_config(topology::configuration config) override
    {
        bool forced_config = false;
        std::vector<topology::configuration::node> added{};
        std::vector<topology::configuration::node> removed{};
        {
            std::scoped_lock lock(config_mutex_);
            if (!config_) {
                LOG_DEBUG("{} initialize configuration rev={}", log_prefix_, config.rev_str());
            } else if (config.force) {
                LOG_DEBUG("{} forced to accept configuration rev={}", log_prefix_, config.rev_str());
                forced_config = true;
            } else if (!config.vbmap) {
                LOG_DEBUG("{} will not update the configuration old={} -> new={}, because new config does not have partition map",
                          log_prefix_,
                          config_->rev_str(),
                          config.rev_str());
                return;
            } else if (config_ < config) {
                LOG_DEBUG("{} will update the configuration old={} -> new={}", log_prefix_, config_->rev_str(), config.rev_str());
            } else {
                return;
            }

            if (config_) {
                diff_nodes(config_->nodes, config.nodes, added);
                diff_nodes(config.nodes, config_->nodes, removed);
            } else {
                added = config.nodes;
            }
            config_ = config;
            configured_ = true;

            {
                std::scoped_lock listeners_lock(config_listeners_mutex_);
                for (const auto& listener : config_listeners_) {
                    listener->update_config(*config_);
                }
            }
        }
        if (!added.empty() || !removed.empty()) {
            std::scoped_lock lock(sessions_mutex_);
            std::map<size_t, std::shared_ptr<io::mcbp_session>> new_sessions{};

            for (auto& [index, session] : sessions_) {
                std::size_t new_index = config.nodes.size() + 1;
                for (const auto& node : config.nodes) {
                    if (session->bootstrap_hostname() == node.hostname_for(origin_.options().network) &&
                        session->bootstrap_port() ==
                          std::to_string(
                            node.port_or(origin_.options().network, service_type::key_value, origin_.options().enable_tls, 0))) {
                        new_index = node.index;
                        break;
                    }
                }
                if (new_index < config.nodes.size()) {
                    LOG_DEBUG(R"({} rev={}, preserve session="{}", address="{}:{}")",
                              log_prefix_,
                              config.rev_str(),
                              session->id(),
                              session->bootstrap_hostname(),
                              session->bootstrap_port());
                    new_sessions[new_index] = std::move(session);
                } else {
                    LOG_DEBUG(R"({} rev={}, drop session="{}", address="{}:{}")",
                              log_prefix_,
                              config.rev_str(),
                              session->id(),
                              session->bootstrap_hostname(),
                              session->bootstrap_port());
                    asio::post(
                      asio::bind_executor(ctx_, [session = std::move(session)]() { return session->stop(retry_reason::do_not_retry); }));
                }
            }

            for (const auto& node : config.nodes) {
                if (new_sessions.find(node.index) != new_sessions.end()) {
                    continue;
                }

                const auto& hostname = node.hostname_for(origin_.options().network);
                auto port = node.port_or(origin_.options().network, service_type::key_value, origin_.options().enable_tls, 0);
                if (port == 0) {
                    continue;
                }
                couchbase::core::origin origin(origin_.credentials(), hostname, port, origin_.options());
                std::shared_ptr<io::mcbp_session> session;
                if (origin_.options().enable_tls) {
                    session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin, state_listener_, name_, known_features_);
                } else {
                    session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin, state_listener_, name_, known_features_);
                }
                LOG_DEBUG(R"({} rev={}, add session="{}", address="{}:{}")", log_prefix_, config.rev_str(), session->id(), hostname, port);
                session->bootstrap(
                  [self = shared_from_this(), session, forced_config, idx = node.index](std::error_code err, topology::configuration cfg) {
                      if (!err) {
                          self->update_config(std::move(cfg));
                          session->on_configuration_update(self);
                          session->on_stop(
                            [index = session->index(), hostname = session->bootstrap_hostname(), port = session->bootstrap_port(), self](
                              retry_reason reason) {
                                if (reason == retry_reason::socket_closed_while_in_flight) {
                                    self->restart_node(index, hostname, port);
                                }
                            });
                          self->drain_deferred_queue();
                      } else if (err == errc::common::unambiguous_timeout && forced_config) {
                          self->restart_node(idx, session->bootstrap_hostname(), session->bootstrap_port());
                      }
                  },
                  true);
                new_sessions[node.index] = std::move(session);
            }
            sessions_ = new_sessions;
        }
    }

    void restart_node(std::size_t index, const std::string& hostname, const std::string& port)
    {
        if (closed_) {
            LOG_DEBUG(R"({} requested to restart session, but the bucket has been closed already. idx={}, address="{}:{}")",
                      log_prefix_,
                      index,
                      hostname,
                      port);
            return;
        }
        {
            std::scoped_lock lock(config_mutex_);
            if (!config_->has_node_with_hostname(hostname)) {
                LOG_TRACE(
                  R"({} requested to restart session, but the node has been ejected from current configuration already. idx={}, address="{}:{}")",
                  log_prefix_,
                  index,
                  hostname,
                  port);
                return;
            }
        }
        couchbase::core::origin origin(origin_.credentials(), hostname, port, origin_.options());

        std::shared_ptr<io::mcbp_session> session{};
        if (origin_.options().enable_tls) {
            session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin, state_listener_, name_, known_features_);
        } else {
            session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin, state_listener_, name_, known_features_);
        }

        std::scoped_lock lock(sessions_mutex_);
        if (auto ptr = sessions_.find(index); ptr == sessions_.end() || ptr->second == nullptr) {
            LOG_DEBUG(R"({} requested to restart session idx={}, which does not exist yet, initiate new one id="{}", address="{}:{}")",
                      log_prefix_,
                      index,
                      session->id(),
                      hostname,
                      port);
        } else {
            const auto& old_session = ptr->second;
            auto old_id = old_session->id();
            sessions_.erase(ptr);
            Expects(sessions_[index] == nullptr);
            LOG_DEBUG(R"({} restarting session idx={}, id=("{}" -> "{}"), address="{}:{}")",
                      log_prefix_,
                      index,
                      old_id,
                      session->id(),
                      hostname,
                      port);
        }

        session->bootstrap(
          [self = shared_from_this(), session, this_index = index, hostname, port](std::error_code ec,
                                                                                   const topology::configuration& config) {
              if (self->closed_) {
                  asio::post(asio::bind_executor(self->ctx_, [session]() { return session->stop(retry_reason::do_not_retry); }));
                  return;
              }
              if (ec) {
                  LOG_WARNING(R"({} failed to restart session idx={}, ec={})", session->log_prefix(), this_index, ec.message());
                  self->restart_node(this_index, hostname, port);
                  return;
              }
              session->on_configuration_update(self);
              session->on_stop([this_index, hostname, port, self](retry_reason reason) {
                  if (reason == retry_reason::socket_closed_while_in_flight) {
                      self->restart_node(this_index, hostname, port);
                  }
              });

              self->update_config(config);
              self->drain_deferred_queue();
          },
          true);
        sessions_[index] = std::move(session);
    }

    template<typename Handler>
    void bootstrap(Handler&& handler)
    {
        if (state_listener_) {
            state_listener_->register_config_listener(shared_from_this());
        }
        std::shared_ptr<io::mcbp_session> new_session{};
        if (origin_.options().enable_tls) {
            new_session = std::make_shared<io::mcbp_session>(client_id_, ctx_, tls_, origin_, state_listener_, name_, known_features_);
        } else {
            new_session = std::make_shared<io::mcbp_session>(client_id_, ctx_, origin_, state_listener_, name_, known_features_);
        }
        new_session->bootstrap([self = shared_from_this(), new_session, h = std::forward<Handler>(handler)](
                                 std::error_code ec, const topology::configuration& cfg) mutable {
            if (ec) {
                LOG_WARNING(R"({} failed to bootstrap session ec={}, bucket="{}")", new_session->log_prefix(), ec.message(), self->name_);
            } else {
                size_t this_index = new_session->index();
                new_session->on_configuration_update(self);
                new_session->on_stop([this_index, hostname = new_session->bootstrap_hostname(), port = new_session->bootstrap_port(), self](
                                       retry_reason reason) {
                    if (reason == retry_reason::socket_closed_while_in_flight) {
                        self->restart_node(this_index, hostname, port);
                    }
                });

                {
                    std::scoped_lock lock(self->sessions_mutex_);
                    self->sessions_[this_index] = std::move(new_session);
                }
                self->update_config(cfg);
                self->drain_deferred_queue();
            }
            h(ec, cfg);
        });
    }

    void on_configuration_update(std::shared_ptr<config_listener> handler)
    {
        std::scoped_lock lock(config_listeners_mutex_);
        config_listeners_.emplace_back(std::move(handler));
    }

    void drain_deferred_queue()
    {
        std::queue<utils::movable_function<void()>> commands{};
        {
            std::scoped_lock lock(deferred_commands_mutex_);
            std::swap(deferred_commands_, commands);
        }
        while (!commands.empty()) {
            commands.front()();
            commands.pop();
        }
    }

    template<typename Handler>
    void with_configuration(Handler&& handler)
    {
        if (closed_) {
            return handler(errc::network::configuration_not_available, topology::configuration{});
        }
        if (configured_) {
            std::optional<topology::configuration> config{};
            {
                std::scoped_lock config_lock(config_mutex_);
                config = config_;
            }
            if (config) {
                return handler({}, config.value());
            }
            return handler(errc::network::configuration_not_available, topology::configuration{});
        }
        std::scoped_lock lock(deferred_commands_mutex_);
        deferred_commands_.emplace([self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
            if (self->closed_ || !self->configured_) {
                return handler(errc::network::configuration_not_available, topology::configuration{});
            }

            std::optional<topology::configuration> config{};
            {
                std::scoped_lock config_lock(self->config_mutex_);
                config = self->config_;
            }
            if (config) {
                return handler({}, config.value());
            }
            return handler(errc::network::configuration_not_available, topology::configuration{});
        });
    }

    template<typename Request, typename Handler>
    void execute(Request request, Handler&& handler)
    {
        if (closed_) {
            return;
        }
        auto cmd = std::make_shared<operations::mcbp_command<bucket, Request>>(
          ctx_, shared_from_this(), request, origin_.options().default_timeout_for(service_type::key_value));
        cmd->start([cmd, handler = std::forward<Handler>(handler)](std::error_code ec, std::optional<io::mcbp_message> msg) mutable {
            using encoded_response_type = typename Request::encoded_response_type;
            std::uint16_t status_code = msg ? msg->header.status() : 0U;
            auto resp = msg ? encoded_response_type(std::move(*msg)) : encoded_response_type{};
            auto ctx = make_key_value_error_context(ec, status_code, cmd, resp);
            handler(cmd->request.make_response(std::move(ctx), std::move(resp)));
        });
        if (configured_) {
            map_and_send(cmd);
        } else {
            std::scoped_lock lock(deferred_commands_mutex_);
            deferred_commands_.emplace([self = shared_from_this(), cmd]() { self->map_and_send(cmd); });
        }
    }

    void close()
    {
        if (closed_) {
            return;
        }
        closed_ = true;

        drain_deferred_queue();

        if (state_listener_ != nullptr) {
            state_listener_->unregister_config_listener(shared_from_this());
        }

        {
            std::scoped_lock lock(config_listeners_mutex_);
            config_listeners_.clear();
        }

        std::map<size_t, std::shared_ptr<io::mcbp_session>> old_sessions;
        {
            std::scoped_lock lock(sessions_mutex_);
            std::swap(old_sessions, sessions_);
        }
        for (auto& [index, session] : old_sessions) {
            if (session) {
                LOG_DEBUG(R"({} shutdown session session="{}", idx={})", log_prefix_, session->id(), index);
                session->stop(retry_reason::do_not_retry);
            }
        }
    }

    std::pair<std::uint16_t, std::int16_t> map_id(const document_id& id)
    {
        std::scoped_lock lock(config_mutex_);
        return config_->map_key(id.key(), id.node_index());
    }

    template<typename Request>
    void map_and_send(std::shared_ptr<operations::mcbp_command<bucket, Request>> cmd)
    {
        if (closed_) {
            return cmd->cancel(retry_reason::do_not_retry);
        }
        std::int16_t index = 0;
        if (cmd->request.id.use_any_session()) {
            index = round_robin_next_.fetch_add(1);
            std::size_t number_of_sessions{ 0 };
            {
                std::scoped_lock lock(sessions_mutex_);
                number_of_sessions = sessions_.size();
            }
            if (static_cast<std::size_t>(round_robin_next_) >= number_of_sessions) {
                round_robin_next_ = 0;
            }
        } else {
            std::tie(cmd->request.partition, index) = map_id(cmd->request.id);
            if (index < 0) {
                return io::retry_orchestrator::maybe_retry(
                  cmd->manager_, cmd, retry_reason::node_not_available, errc::common::request_canceled);
            }
        }
        std::shared_ptr<io::mcbp_session> session{};
        bool found{ false };
        {
            std::scoped_lock lock(sessions_mutex_);
            auto ptr = sessions_.find(static_cast<std::size_t>(index));
            found = ptr != sessions_.end();
            if (found) {
                session = ptr->second;
            }
        }
        if (!found || session == nullptr || !session->has_config()) {
            std::scoped_lock lock_for_deferred_commands(deferred_commands_mutex_);
            deferred_commands_.emplace([self = shared_from_this(), cmd]() { self->map_and_send(cmd); });
            return;
        }
        if (session->is_stopped()) {
            return io::retry_orchestrator::maybe_retry(
              cmd->manager_, cmd, retry_reason::node_not_available, errc::common::request_canceled);
        }
        cmd->send_to(session);
    }

    template<typename Request>
    void schedule_for_retry(std::shared_ptr<operations::mcbp_command<bucket, Request>> cmd, std::chrono::milliseconds duration)
    {
        if (closed_) {
            return cmd->cancel(retry_reason::do_not_retry);
        }
        cmd->retry_backoff.expires_after(duration);
        cmd->retry_backoff.async_wait([self = shared_from_this(), cmd](std::error_code ec) mutable {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->map_and_send(cmd);
        });
    }

    [[nodiscard]] const std::string& log_prefix() const
    {
        return log_prefix_;
    }

    void export_diag_info(diag::diagnostics_result& res) const
    {
        std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions;
        {
            std::scoped_lock lock(sessions_mutex_);
            sessions = sessions_;
        }
        for (const auto& [index, session] : sessions) {
            res.services[service_type::key_value].emplace_back(session->diag_info());
        }
    }

    template<typename Collector>
    void ping(std::shared_ptr<Collector> collector)
    {
        std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions;
        {
            std::scoped_lock lock(sessions_mutex_);
            sessions = sessions_;
        }
        for (const auto& [index, session] : sessions) {
            session->ping(collector->build_reporter());
        }
    }

    auto tracer() const
    {
        return tracer_;
    }

    auto meter() const
    {
        return meter_;
    }

    auto default_retry_strategy() const
    {
        return origin_.options().default_retry_strategy_;
    }

  private:
    std::string client_id_;
    asio::io_context& ctx_;
    asio::ssl::context& tls_;
    std::shared_ptr<couchbase::tracing::request_tracer> tracer_;
    std::shared_ptr<couchbase::metrics::meter> meter_;
    std::string name_;
    origin origin_;

    std::optional<topology::configuration> config_{};
    mutable std::mutex config_mutex_{};
    std::vector<protocol::hello_feature> known_features_;

    std::shared_ptr<impl::bootstrap_state_listener> state_listener_{ nullptr };

    std::queue<utils::movable_function<void()>> deferred_commands_{};
    std::mutex deferred_commands_mutex_{};

    std::atomic_bool closed_{ false };
    std::atomic_bool configured_{ false };
    std::map<size_t, std::shared_ptr<io::mcbp_session>> sessions_{};
    mutable std::mutex sessions_mutex_{};
    std::atomic_int16_t round_robin_next_{ 0 };

    std::vector<std::shared_ptr<config_listener>> config_listeners_{};
    std::mutex config_listeners_mutex_{};

    std::string log_prefix_{};
};
} // namespace couchbase::core
