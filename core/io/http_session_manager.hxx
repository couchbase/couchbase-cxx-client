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

#include "core/app_telemetry_meter.hxx"
#include "core/config_listener.hxx"
#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/http_noop.hxx"
#include "core/service_type.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/movable_function.hxx"
#include "http_command.hxx"
#include "http_context.hxx"
#include "http_session.hxx"
#include "http_traits.hxx"

#include <gsl/narrow>

#include <atomic>
#include <chrono>
#include <exception>
#include <optional>
#include <queue>
#include <random>
#include <utility>
#include <vector>

namespace couchbase::core::io
{

class http_session_manager
  : public std::enable_shared_from_this<http_session_manager>
  , public config_listener
{
public:
  http_session_manager(std::string client_id,
                       asio::io_context& ctx,
                       tls_context_provider& tls,
                       origin& origin)
    : client_id_(std::move(client_id))
    , ctx_(ctx)
    , tls_(tls)
    , origin_(origin)
  {
  }

  void set_tracer(std::shared_ptr<tracing::tracer_wrapper> tracer)
  {
    tracer_ = std::move(tracer);
  }

  [[nodiscard]] auto tracer() const -> std::shared_ptr<tracing::tracer_wrapper>
  {
    return tracer_;
  }

  void set_meter(std::shared_ptr<metrics::meter_wrapper> meter)
  {
    meter_ = std::move(meter);
  }

  void set_app_telemetry_meter(std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter)
  {
    app_telemetry_meter_ = std::move(app_telemetry_meter);
  }

  auto configuration_capabilities() const -> configuration_capabilities
  {
    std::scoped_lock config_lock(config_mutex_);
    return config_.capabilities;
  }

  void update_config(topology::configuration config) override
  {
    // Idle sessions to nodes that have left the cluster, collected under the lock
    // and stopped afterwards (stop() re-enters sessions_mutex_ via on_stop).
    std::vector<std::shared_ptr<http_session>> evicted;
    {
      std::scoped_lock config_lock(config_mutex_, sessions_mutex_, next_index_mutex_);
      config_ = std::move(config);
      if (!config_.nodes.empty() && next_index_ >= config_.nodes.size()) {
        next_index_ = 0;
      }
      for (auto& [type, sessions] : idle_sessions_) {
        sessions.remove_if([&opts = options_, &cfg = config_, &evicted](const auto& session) {
          if (session && !cfg.has_node(opts.network,
                                       session->type(),
                                       opts.enable_tls,
                                       session->hostname(),
                                       session->port())) {
            evicted.push_back(session);
            return true;
          }
          return false;
        });
      }
    }
    // Tear the evicted connections down now instead of leaving them open until
    // the idle timer fires; the node is gone, so the socket is dead weight (and,
    // against a proxy that has already dropped the far side, a stale handle).
    for (const auto& session : evicted) {
      asio::post(session->get_executor(), [session]() {
        session->stop();
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
    {
      std::scoped_lock lock(config_mutex_, next_index_mutex_);
      options_ = options;
      next_index_ = next_index;
      config_ = config;
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
  void ping(std::set<service_type> services,
            std::optional<std::chrono::milliseconds> timeout,
            std::shared_ptr<Collector> collector)
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
          const auto& hostname = node.hostname_for(options_.network);
          const auto& canonical_hostname = node.hostname;
          std::uint16_t canonical_port = node.port_or(type, options_.enable_tls, 0);
          // Created and listed pending in one section, as check_out does, so a close() either
          // stops it or precedes it. Unlisted while its connect is pending, it would be invisible
          // to a close() whose completion lets the io_context stop.
          std::shared_ptr<http_session> session;
          {
            const std::scoped_lock lock(sessions_mutex_);
            session = create_session(type,
                                     node_details{
                                       hostname,
                                       port,
                                       node.node_uuid,
                                       canonical_hostname,
                                       canonical_port,
                                     },
                                     generation_);
            pending_sessions_[type].push_back(session);
          }
          operations::http_noop_request request{};
          request.type = type;
          request.timeout = timeout;
          auto cmd = std::make_shared<operations::http_command<operations::http_noop_request>>(
            ctx_,
            request,
            tracer_,
            meter_,
            app_telemetry_meter_,
            options_.default_timeout_for(request.type));
          cmd->set_command_session(session);
          cmd->start(
            [start = std::chrono::steady_clock::now(),
             self = shared_from_this(),
             type,
             session,
             weak_cmd = std::weak_ptr(cmd),
             handler = collector->build_reporter()](operations::http_noop_response&& resp) {
              diag::ping_state state = diag::ping_state::ok;
              std::optional<std::string> error{};
              if (auto ec = resp.ctx.ec; ec) {
                if (ec == errc::common::unambiguous_timeout ||
                    ec == errc::common::ambiguous_timeout) {
                  state = diag::ping_state::timeout;
                } else {
                  state = diag::ping_state::error;
                }
                error.emplace(fmt::format("code={}, message={}, http_code={}",
                                          ec.value(),
                                          ec.message(),
                                          resp.ctx.http_status));
              }
              auto remote_address = session->remote_address();
              // If not connected, the remote address will be empty.  Better to
              // give the user some context on the "attempted" remote address.
              if (remote_address.empty()) {
                remote_address = fmt::format("{}:{}", session->hostname(), session->port());
              }
              handler->report(
                diag::endpoint_ping_info{ type,
                                          session->id(),
                                          std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - start),
                                          remote_address,
                                          session->local_address(),
                                          state,
                                          {},
                                          error });
              if (const auto cmd = weak_cmd.lock(); cmd) {
                self->check_in(type, cmd->session_for_check_in());
              }
            });

          if (!session->is_connected()) {
            connect_then_send(session, cmd, {}, true);
          } else {
            cmd->send_to();
          }
        }
      }
    }
  }

  struct node_details {
    std::string hostname{};
    std::uint16_t port{};
    std::string node_uuid{};
    std::string canonical_hostname{};
    std::uint16_t canonical_port{};
  };

  auto check_out(service_type type,
                 std::string preferred_node_address,
                 const std::string& undesired_node_address = {})
    -> std::pair<std::error_code, std::shared_ptr<http_session>>
  {
    node_details preferred_node{};

    if (preferred_node_address.empty() && !undesired_node_address.empty()) {
      // No sticky node, but one to avoid. Retrying elsewhere requires a different node chosen
      // at random, not merely any node this manager happens to hold an idle session to.
      if (auto n = pick_random_node(type, undesired_node_address); n.port != 0) {
        preferred_node_address = fmt::format("{}:{}", n.hostname, n.port);
        preferred_node = std::move(n);
      }
    } else if (!preferred_node_address.empty()) {
      // A 'sticky' node was specified. We should populate the node details.
      preferred_node = lookup_node(type, preferred_node_address);
      if (preferred_node.port == 0) {
        return { errc::common::service_not_available, nullptr };
      }
    }

    const std::scoped_lock lock(sessions_mutex_);
    idle_sessions_[type].remove_if([](const auto& s) {
      return !s;
    });
    busy_sessions_[type].remove_if([](const auto& s) {
      return !s;
    });
    pending_sessions_[type].remove_if([](const auto& s) {
      return !s;
    });
    std::shared_ptr<http_session> session{};
    // Only a reusable session leaves the idle list. One that is not has a stop queued, by its idle
    // timer or by retire_sessions(), and stays listed until on_stop unlists it, so a close() before
    // then still stops it and waits for it.
    auto& idle = idle_sessions_[type];
    const auto taken = std::find_if(
      idle.begin(),
      idle.end(),
      [&preferred_node_address, &h = preferred_node.hostname, &p = preferred_node.port](
        const auto& s) {
        // Check for a match using both the unresolved hostname & IP address
        const bool matches = preferred_node_address.empty() ||
                             s->remote_address() == preferred_node_address ||
                             (s->hostname() == h && s->port() == std::to_string(p));
        return matches && is_reusable(*s);
      });
    if (taken != idle.end()) {
      session = *taken;
      idle.erase(taken);
    } else if (!preferred_node_address.empty()) {
      session = create_session(type, preferred_node, generation_);
    }
    if (!session) {
      auto node = preferred_node_address.empty() ? next_node(type)
                                                 : lookup_node(type, preferred_node_address);
      if (node.port == 0) {
        return { errc::common::service_not_available, nullptr };
      }
      session = create_session(type, node, generation_);
    }
    if (session->is_connected()) {
      busy_sessions_[type].push_back(session);
    } else {
      pending_sessions_[type].push_back(session);
    }
    return { {}, session };
  }

  void check_in(service_type type, std::shared_ptr<http_session> session)
  {
    if (!session) {
      return;
    }
    if (!session->is_connected()) {
      {
        std::scoped_lock lock(sessions_mutex_);
        if (auto pend_it = pending_sessions_.find(type); pend_it != pending_sessions_.end()) {
          pend_it->second.remove_if([id = session->id()](const auto& s) {
            return !s || s->id() == id;
          });
        }
      }
      CB_LOG_DEBUG("{} HTTP session never connected.  Ensured session is not in pending.",
                   session->log_prefix());
      return;
    }
    bool should_stop = false;
    {
      // config_mutex_ guards config_, which set_configuration() writes without sessions_mutex_.
      // sessions_mutex_ in the same section keeps update_config()'s eviction, which holds both,
      // from landing between the has_node test and the publication: that would pool a connection
      // to a node that has left the cluster. check_in must therefore never be called with
      // sessions_mutex_ held.
      const std::scoped_lock lock(config_mutex_, sessions_mutex_);
      // stopped_ is set before stop()'s teardown runs on_stop, whose cleanup takes
      // sessions_mutex_. Read under that lock, either the stop is seen here, or its cleanup runs
      // after the publication below and removes the session again.
      //
      // stopping_ is a stop the session's owner has decided and not yet executed, claimed before
      // this check_in can run: by a streaming body under its own mutex while reading_complete_ is
      // false (http_session::read_some runs the body's completion, which sets it, before the
      // stream-end handler).
      if (session->is_stopped() || session->is_stopping()) {
        return;
      }
      if (session->pool_generation() != generation_ || session->tls_epoch() != tls_epoch_ ||
          !session->keep_alive() ||
          !config_.has_node(options_.network,
                            session->type(),
                            options_.enable_tls,
                            session->hostname(),
                            session->port())) {
        should_stop = true;
      } else {
        // Under sessions_mutex_, as check_out's reset_idle() is. set_idle() binds the timer
        // completion to the strand and never calls stop() inline, so on_stop cannot re-enter
        // sessions_mutex_ here.
        session->set_idle(options_.idle_http_connection_timeout);
        idle_sessions_[type].push_back(session);
        if (auto busy_it = busy_sessions_.find(type); busy_it != busy_sessions_.end()) {
          busy_it->second.remove_if([id = session->id()](const auto& s) -> bool {
            return !s || s->id() == id;
          });
        }
        if (auto pend_it = pending_sessions_.find(type); pend_it != pending_sessions_.end()) {
          pend_it->second.remove_if([id = session->id()](const auto& s) -> bool {
            return !s || s->id() == id;
          });
        }
      }
    }
    if (should_stop) {
      return asio::post(session->get_executor(), [session]() {
        session->stop();
      });
    }
    CB_LOG_DEBUG("{} put HTTP session back to idle connections", session->log_prefix());
  }

  // Stops the reuse of every session created before the call, for a credential that the connection
  // carries, such as a client certificate. Idle sessions stop now. A busy or connecting session
  // completes its request first; check_in then stops it, because its TLS epoch is stale.
  void retire_sessions()
  {
    std::vector<std::shared_ptr<http_session>> retired;
    {
      const std::scoped_lock lock(sessions_mutex_);
      ++tls_epoch_;
      // Claimed but left listed until their stop runs, so a close() before then stops them and
      // waits for them. check_out skips a claimed session, and on_stop unlists it.
      for (const auto& [type, sessions] : idle_sessions_) {
        for (const auto& s : sessions) {
          if (s) {
            s->mark_stopping();
            retired.push_back(s);
          }
        }
      }
    }
    // Stopped on its own strand, as close() does.
    for (auto& s : retired) {
      asio::post(s->get_executor(), [s]() {
        s->stop();
      });
    }
  }

  // on_stopped runs once every session listed at the call has been stopped and holds no
  // read_some() call, on the strand of the last one, or before close() returns when none is listed.
  // Until then the read_some() calls queued on a session hold the body that holds the session, and
  // its on_stop cleanup holds this manager. Whoever stops the io_context must wait for it: a
  // handler still queued then never runs, and the session, its body and this manager are never
  // released. A do_read() the stop aborts is not waited for: its completion holds only the session,
  // which destroying the io_context releases.
  void close(utils::movable_function<void()> on_stopped = {})
  {
    std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions, idle_sessions,
      pending_sessions;
    {
      // A new generation starts with the lists moved out. check_in and publish_busy refuse a
      // session from an earlier one, so none is listed again; a connect callback stops its session
      // instead. check_out creates sessions in the new generation.
      const std::scoped_lock lock(sessions_mutex_);
      ++generation_;
      // Exchanged rather than moved: the manager goes on serving, and a moved-from map is only
      // valid, not empty.
      busy_sessions = std::exchange(busy_sessions_, {});
      idle_sessions = std::exchange(idle_sessions_, {});
      pending_sessions = std::exchange(pending_sessions_, {});
    }
    // Every session is stopped on its own strand, as check_in and update_config() do. stop()
    // closes the stream and cancels the response, which the strand's read and write handlers use.
    // Run on this thread, it races those handlers, and a queued do_write() finds no socket. An
    // idle session is stopped rather than only reset_idle()d, so the read armed by set_idle() is
    // torn down and its shared_ptr does not keep the io_context from draining.
    std::vector<std::shared_ptr<http_session>> stopping;
    for (auto* lists : { &idle_sessions, &busy_sessions, &pending_sessions }) {
      for (auto& [type, sessions] : *lists) {
        for (auto& s : sessions) {
          if (s) {
            stopping.emplace_back(std::move(s));
          }
        }
      }
    }
    if (stopping.empty()) {
      if (on_stopped) {
        on_stopped();
      }
      return;
    }
    auto remaining = std::make_shared<std::atomic_size_t>(stopping.size());
    auto done = std::make_shared<utils::movable_function<void()>>(std::move(on_stopped));
    for (auto& s : stopping) {
      asio::post(s->get_executor(), [s, remaining, done]() {
        // A stop() that throws has still torn the session down, so its reads still drain.
        std::exception_ptr error{};
        try {
          s->stop();
        } catch (...) {
          error = std::current_exception();
        }
        s->on_reads_drained([remaining, done]() {
          if (remaining->fetch_sub(1) == 1 && *done) {
            (*done)();
          }
        });
        if (error) {
          std::rethrow_exception(error);
        }
      });
    }
  }

  template<typename Request, typename Handler>
  void execute(Request request, Handler&& handler)
  {
    std::string preferred_node;
    if constexpr (http_traits::supports_sticky_node_v<Request>) {
      if (request.send_to_node) {
        preferred_node = *request.send_to_node;
      }
    }
    auto [error, session] = check_out(request.type, preferred_node);
    if (error) {
      typename Request::error_context_type ctx{};
      ctx.ec = error;
      using encoded_response_type = typename Request::encoded_response_type;
      return handler(request.make_response(std::move(ctx), encoded_response_type{}));
    }

    auto cmd = std::make_shared<operations::http_command<Request>>(
      ctx_,
      request,
      tracer_,
      meter_,
      app_telemetry_meter_,
      options_.default_timeout_for(request.type));

    using response_type = typename Request::response_type;
    // Before start(), so a deadline that fires at once still reports the session's endpoint.
    cmd->set_command_session(session);
    cmd->start([self = shared_from_this(), cmd, handler = std::forward<Handler>(handler)](
                 response_type&& resp) mutable {
      handler(std::move(resp));
      self->check_in(cmd->request.type, cmd->session_for_check_in());
    });
    if (!session->is_connected()) {
      connect_then_send(session, cmd, preferred_node);
    } else {
      cmd->send_to();
    }
  }

  void connect_then_send_pending_op(
    std::shared_ptr<http_session> session,
    const std::string& preferred_node,
    std::chrono::time_point<std::chrono::steady_clock> deadline,
    utils::movable_function<void(std::error_code, std::shared_ptr<http_session>)> callback)
  {
    session->connect([self = shared_from_this(),
                      session,
                      preferred_node,
                      deadline,
                      cb = std::move(callback)]() mutable {
      // stop() runs this callback, from close() among others. A stopped session is terminal: a
      // replacement would open a socket for an operation the stop has ended.
      if (session->is_stopped()) {
        return cb(errc::common::request_canceled, {});
      }
      // A session from before a close() gets no replacement and no reconnect: close() may not reach
      // it, so it is stopped here.
      if (self->is_retired(*session)) {
        session->stop();
        return cb(errc::common::request_canceled, {});
      }
      if (!session->is_connected()) {
        if (deadline < std::chrono::steady_clock::now()) {
          session->stop();
          return cb(errc::common::unambiguous_timeout, {});
          return;
        }

        // stop this session and create a new one w/ new hostname + port
        session->stop();
        const auto node = preferred_node.empty()
                            ? self->next_node(session->type())
                            : self->lookup_node(session->type(), preferred_node);
        if (node.port == 0) {
          cb(errc::common::service_not_available, {});
          return;
        }
        auto new_session = self->create_replacement(session->type(), node, *session);
        if (!new_session) {
          return cb(errc::common::request_canceled, {});
        }
        if (new_session->is_connected()) {
          if (!self->publish_busy(new_session, /* leaving_pending */ true)) {
            new_session->stop();
            return cb(errc::common::request_canceled, {});
          }
          cb({}, new_session);
        } else {
          self->connect_then_send_pending_op(new_session, preferred_node, deadline, std::move(cb));
        }
      } else {
        if (deadline < std::chrono::steady_clock::now()) {
          session->stop();
          cb(errc::common::unambiguous_timeout, {});
          return;
        }
        if (!self->publish_busy(session, /* leaving_pending */ true)) {
          session->stop();
          return cb(errc::common::request_canceled, {});
        }
        cb({}, session);
      }
    });
  }

private:
  template<typename Request>
  void connect_then_send(std::shared_ptr<http_session> session,
                         std::shared_ptr<operations::http_command<Request>> cmd,
                         const std::string& preferred_node,
                         bool reuse_session = false)
  {
    session->connect([self = shared_from_this(),
                      session,
                      cmd,
                      preferred_node = std::move(preferred_node),
                      reuse_session]() mutable {
      // stop() runs this callback, from close() among others. A stopped session is terminal: a
      // replacement would open a socket and send a request the stop has ended, and with
      // reuse_session the stopped session's connect() would run this callback again inline, an
      // unbounded recursion.
      if (session->is_stopped()) {
        return cmd->invoke_handler(errc::common::request_canceled, {});
      }
      // A session from before a close() gets no replacement and no reconnect: close() may not reach
      // it, so it is stopped here.
      if (self->is_retired(*session)) {
        session->stop();
        return cmd->invoke_handler(errc::common::request_canceled, {});
      }
      if (!session->is_connected()) {
        if (cmd->deadline_expiry() < std::chrono::steady_clock::now()) {
          // The command's deadline completes it, but stops only the session it held when it
          // fired, which may be the one this replaced. Stopped here, a replacement does not stay
          // listed pending until close().
          session->stop();
          return;
        }
        if (reuse_session) {
          return self->connect_then_send(session, cmd, preferred_node, reuse_session);
        }
        // stop this session and create a new one w/ new hostname + port
        session->stop();
        const auto node = preferred_node.empty()
                            ? self->next_node(session->type())
                            : self->lookup_node(session->type(), preferred_node);
        if (node.port == 0) {
          cmd->invoke_handler(errc::common::service_not_available, {});
          return;
        }
        auto new_session = self->create_replacement(session->type(), node, *session);
        if (!new_session) {
          return cmd->invoke_handler(errc::common::request_canceled, {});
        }
        cmd->set_command_session(new_session);
        if (new_session->is_connected()) {
          if (!self->publish_busy(new_session, /* leaving_pending */ true)) {
            new_session->stop();
            return cmd->invoke_handler(errc::common::request_canceled, {});
          }
          cmd->send_to();
        } else {
          self->connect_then_send(new_session, cmd, preferred_node);
        }
      } else {
        if (!self->publish_busy(session, /* leaving_pending */ true)) {
          session->stop();
          return cmd->invoke_handler(errc::common::request_canceled, {});
        }
        // Outside sessions_mutex_: send_to() can complete the command inline (an encode_to
        // failure, or a session already stopped), and the completion calls check_in, which takes
        // it.
        cmd->send_to();
      }
    });
  }

  // Lists a connected session as busy unless its stop() has begun or it is from before a close(),
  // and reports whether it did. stop() sets stopped_ before on_stop's cleanup takes
  // sessions_mutex_, so a stop is either seen here or removes the session after it is listed.
  // Tested outside the lock, a stop landing in between would list a session whose cleanup has
  // already run. A refused session is the caller's to stop: close() does not reach one it never had
  // in a list.
  auto publish_busy(const std::shared_ptr<http_session>& session, bool leaving_pending = false)
    -> bool
  {
    const std::scoped_lock lock(sessions_mutex_);
    if (session->pool_generation() != generation_ || session->is_stopped()) {
      return false;
    }
    busy_sessions_[session->type()].push_back(session);
    if (leaving_pending) {
      pending_sessions_[session->type()].remove_if([id = session->id()](const auto& s) -> bool {
        return !s || s->id() == id;
      });
    }
    return true;
  }

  auto current_generation() -> std::uint64_t
  {
    const std::scoped_lock lock(sessions_mutex_);
    return generation_;
  }

  // Serves a generation that a close() has ended.
  auto is_retired(const http_session& session) -> bool
  {
    return session.pool_generation() != current_generation();
  }

  // A popped idle session goes back into service only if nothing has claimed it and reset_idle()
  // accepts it, which it refuses once the idle timer has fired or a stop() has begun.
  static auto is_reusable(http_session& session) -> bool
  {
    return !session.is_stopping() && session.reset_idle();
  }

  // A failover replacement for `failed`. It serves the same operation, so it belongs to the same
  // generation, and it is listed pending from creation: close() either stops it or has already
  // retired that generation, and then the replacement is discarded and nullptr returned. Unlisted
  // until its connect completes, it would be invisible to a close() whose completion lets the
  // io_context stop.
  auto create_replacement(service_type type, const node_details& node, const http_session& failed)
    -> std::shared_ptr<http_session>
  {
    auto session = create_session(type, node, failed.pool_generation());
    const std::scoped_lock lock(sessions_mutex_);
    if (session->pool_generation() != generation_) {
      return nullptr;
    }
    pending_sessions_[type].push_back(session);
    return session;
  }

  auto create_session(service_type type, const node_details& node, std::uint64_t generation)
    -> std::shared_ptr<http_session>
  {
    // Read before the session takes the TLS context. retire_sessions() runs after the context is
    // replaced, so this epoch is never newer than the context.
    const std::uint64_t tls_epoch = tls_epoch_;
    std::shared_ptr<http_session> session;
    if (options_.enable_tls) {
      session = std::make_shared<http_session>(type,
                                               client_id_,
                                               node.node_uuid,
                                               ctx_,
                                               tls_,
                                               origin_,
                                               node.hostname,
                                               std::to_string(node.port),
                                               http_context{
                                                 config_,
                                                 options_,
                                                 query_cache_,
                                                 node.hostname,
                                                 node.port,
                                                 node.canonical_hostname,
                                                 node.canonical_port,
                                               },
                                               generation);
    } else {
      session = std::make_shared<http_session>(type,
                                               client_id_,
                                               node.node_uuid,
                                               ctx_,
                                               origin_,
                                               node.hostname,
                                               std::to_string(node.port),
                                               http_context{
                                                 config_,
                                                 options_,
                                                 query_cache_,
                                                 node.hostname,
                                                 node.port,
                                                 node.canonical_hostname,
                                                 node.canonical_port,
                                               },
                                               generation);
    }

    session->on_stop([type, id = session->id(), self = this->shared_from_this()]() {
      // Declared outside the lock so destructors run after sessions_mutex_ is released.
      // cppcheck-suppress variableScope
      std::vector<std::shared_ptr<http_session>> dropped;
      {
        const std::scoped_lock inner_lock(self->sessions_mutex_);
        for (auto* map :
             { &self->busy_sessions_, &self->idle_sessions_, &self->pending_sessions_ }) {
          auto map_it = map->find(type);
          if (map_it == map->end()) {
            continue;
          }
          auto& list = map_it->second;
          for (auto it = list.begin(); it != list.end();) {
            if (!*it || (*it)->id() == id) {
              if (*it) {
                dropped.push_back(std::move(*it));
              }
              it = list.erase(it);
            } else {
              ++it;
            }
          }
        }
      }
    });
    session->set_tls_epoch(tls_epoch);
    return session;
  }

  auto next_node(service_type type) -> node_details
  {
    std::scoped_lock lock(config_mutex_);
    auto candidates = config_.nodes.size();
    while (candidates > 0) {
      --candidates;
      const std::scoped_lock index_lock(next_index_mutex_);
      const auto& node = config_.nodes[next_index_];
      next_index_ = (next_index_ + 1) % config_.nodes.size();
      const std::uint16_t port = node.port_or(options_.network, type, options_.enable_tls, 0);
      if (port != 0) {
        return {
          node.hostname_for(options_.network),        port, node.node_uuid, node.hostname,
          node.port_or(type, options_.enable_tls, 0),
        };
      }
    }
    return {};
  }

  auto split_host_port(const std::string& address) -> std::pair<std::string, std::uint16_t>
  {
    const auto last_colon = address.find_last_of(':');
    if (last_colon == std::string::npos || address.size() - 1 == last_colon) {
      return { "", static_cast<std::uint16_t>(0U) };
    }
    auto hostname = address.substr(0, last_colon);
    auto port = gsl::narrow_cast<std::uint16_t>(std::stoul(address.substr(last_colon + 1)));
    return { hostname, port };
  }

  auto lookup_node(service_type type, const std::string& preferred_node_address) -> node_details
  {
    const std::scoped_lock lock(config_mutex_);
    auto [hostname, port] = split_host_port(preferred_node_address);
    for (const auto& node : config_.nodes) {
      if (node.hostname_for(options_.network) == hostname &&
          node.port_or(options_.network, type, options_.enable_tls, 0) == port) {
        return {
          hostname, port, node.node_uuid, node.hostname, node.port_or(type, options_.enable_tls, 0),
        };
      }
    }
    return {};
  }

  auto pick_random_node(service_type type, const std::string& undesired_node) -> node_details
  {
    std::vector<topology::configuration::node> candidate_nodes{};
    {
      const std::scoped_lock lock(config_mutex_);
      std::copy_if(config_.nodes.begin(),
                   config_.nodes.end(),
                   std::back_inserter(candidate_nodes),
                   [this, type, &undesired_node](const topology::configuration::node& node) {
                     auto endpoint = node.endpoint(options_.network, type, options_.enable_tls);
                     return endpoint.has_value() && (endpoint.value() != undesired_node);
                   });
    }

    if (candidate_nodes.empty()) {
      // Could not find any other nodes
      return {};
    }

    std::vector<topology::configuration::node> selected{};
    std::sample(candidate_nodes.begin(),
                candidate_nodes.end(),
                std::back_inserter(selected),
                1,
                std::mt19937{ std::random_device{}() });
    const auto& first_selected = selected.at(0);
    return {
      first_selected.hostname_for(options_.network),
      first_selected.port_or(options_.network, type, options_.enable_tls, 0),
      first_selected.node_uuid,
      first_selected.hostname,
      first_selected.port_or(type, options_.enable_tls, 0),
    };
  }

  std::string client_id_;
  asio::io_context& ctx_;
  tls_context_provider& tls_;
  origin& origin_;
  std::shared_ptr<tracing::tracer_wrapper> tracer_{ nullptr };
  std::shared_ptr<metrics::meter_wrapper> meter_{ nullptr };
  std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter_{ nullptr };
  cluster_options options_{};

  topology::configuration config_{};
  mutable std::mutex config_mutex_{};
  std::map<service_type, std::list<std::shared_ptr<http_session>>> busy_sessions_{};
  std::map<service_type, std::list<std::shared_ptr<http_session>>> idle_sessions_{};
  std::map<service_type, std::list<std::shared_ptr<http_session>>> pending_sessions_{};
  std::size_t next_index_{ 0 };
  std::mutex next_index_mutex_{};
  std::mutex sessions_mutex_{};
  // Advanced by close(); guarded by sessions_mutex_. A session carries the generation it serves,
  // and check_in and publish_busy refuse one from an earlier generation.
  std::uint64_t generation_{ 0 };
  // Advanced by retire_sessions(). Unlike generation_, it ends no in-flight request.
  std::atomic<std::uint64_t> tls_epoch_{ 0 };
  query_cache query_cache_{};
};
} // namespace couchbase::core::io
