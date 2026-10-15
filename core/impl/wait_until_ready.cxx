/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "wait_until_ready.hxx"

#include "core/diagnostics.hxx"
#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/topology/configuration.hxx"
#include "with_bucket_config_or_timeout.hxx"

#include <couchbase/error.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

namespace couchbase::core::impl
{
namespace
{
// The SDK refreshes the cluster map on its own interval (2.5s by default), so a freshly created
// bucket's vbucket map will not change faster than that regardless; a short poll just keeps latency
// low once it does.
constexpr std::chrono::milliseconds poll_interval{ 100 };

auto
data_node_count(const topology::configuration& config) -> std::uint32_t
{
  return static_cast<std::uint32_t>(
    std::count_if(config.nodes.begin(), config.nodes.end(), [](const auto& node) {
      return node.services_plain.key_value.has_value() || node.services_tls.key_value.has_value();
    }));
}
} // namespace

// online   : every requested service has at least one endpoint and ALL of them are ok.
// degraded : every requested service has at least one endpoint that is ok.
// When no services were requested we consider whatever the report contains; an empty report
// (nothing pinged back yet) is treated as not-ready.
auto
ping_predicate_satisfied(const diag::ping_result& report,
                         couchbase::cluster_state state,
                         const std::set<service_type>& requested) -> bool
{
  const auto& endpoints = report.services;

  std::set<service_type> services = requested;
  if (services.empty()) {
    for (const auto& [service, reports] : endpoints) {
      services.insert(service);
    }
    if (services.empty()) {
      return false;
    }
  }

  for (const auto service : services) {
    const auto it = endpoints.find(service);
    if (it == endpoints.end() || it->second.empty()) {
      return false;
    }
    const auto& reports = it->second;
    const auto is_ok = [](const diag::endpoint_ping_info& r) {
      return r.state == diag::ping_state::ok;
    };
    const bool ok = (state == couchbase::cluster_state::online)
                      ? std::all_of(reports.begin(), reports.end(), is_ok)
                      : std::any_of(reports.begin(), reports.end(), is_ok);
    if (!ok) {
      return false;
    }
  }
  return true;
}

// One replica per data node besides the one holding the active, up to num_replicas. The server
// leaves a replica slot at -1 for as long as there is no node to place it on. A configuration
// listing no data node is not capped.
auto
placeable_replicas(const topology::configuration& config) -> std::uint32_t
{
  const auto data_nodes = data_node_count(config);
  if (data_nodes == 0) {
    return config.num_replicas.value_or(0); // no node list to cap against
  }
  return std::min(config.num_replicas.value_or(0), data_nodes - 1);
}

auto
unplaceable_replicas(const topology::configuration& config) -> std::uint32_t
{
  return config.num_replicas.value_or(0) - placeable_replicas(config);
}

void
warn_if_replicas_unplaceable(const topology::configuration& config,
                             std::string_view bucket_name,
                             std::atomic_bool& logged)
{
  if (unplaceable_replicas(config) == 0 || logged.exchange(true)) {
    return;
  }
  CB_LOG_WARNING("bucket \"{}\" is configured with {} replica(s), but {} data node(s) can hold "
                 "only {} replica(s)",
                 logger::metadata(bucket_name),
                 config.num_replicas.value_or(0),
                 data_node_count(config),
                 placeable_replicas(config));
}

// True once every vbucket has its active and the placeable replica copies assigned to a node. A
// freshly created bucket reports an empty/partial vbucket map (replica slots set to -1) until the
// server finishes placing replicas; durable (MAJORITY) writes are ambiguous until then.
auto
vbucket_map_ready(const topology::configuration& config) -> bool
{
  if (!config.vbmap.has_value()) {
    return false;
  }
  const auto& vbmap = config.vbmap.value();
  if (vbmap.empty()) {
    return false;
  }
  // active + replicas
  const auto copies = static_cast<std::size_t>(placeable_replicas(config)) + 1;
  for (const auto& chain : vbmap) {
    if (chain.size() < copies) {
      return false;
    }
    for (std::size_t i = 0; i < copies; ++i) {
      if (chain[i] < 0) { // -1 => copy not yet assigned to a node
        return false;
      }
    }
  }
  return true;
}

namespace
{
// Self-owning async poll loop. Exactly one continuation (config fetch, ping, or the poll timer) is
// in flight at a time, each holding a shared_ptr to keep the operation alive; every continuation
// returns to poll()/schedule_retry(), which re-check the deadline, so the whole operation is
// bounded by `timeout` without a separate deadline timer.
class wait_until_ready_operation : public std::enable_shared_from_this<wait_until_ready_operation>
{
public:
  wait_until_ready_operation(core::cluster core,
                             std::optional<std::string> bucket_name,
                             std::chrono::milliseconds timeout,
                             couchbase::cluster_state desired_state,
                             std::set<service_type> services,
                             utils::movable_function<void(std::error_code)> handler)
    : core_{ std::move(core) }
    , bucket_name_{ std::move(bucket_name) }
    , deadline_{ std::chrono::steady_clock::now() + timeout }
    , desired_state_{ desired_state }
    , services_{ std::move(services) }
    , handler_{ std::move(handler) }
    , timer_{ core_.io_context() }
  {
  }

  wait_until_ready_operation(const wait_until_ready_operation&) = delete;
  wait_until_ready_operation(wait_until_ready_operation&&) = delete;
  auto operator=(const wait_until_ready_operation&) -> wait_until_ready_operation& = delete;
  auto operator=(wait_until_ready_operation&&) -> wait_until_ready_operation& = delete;

  ~wait_until_ready_operation()
  {
    // Fail closed. If the io_context is torn down while the wait is in flight, it drops every
    // pending continuation (the poll timer, the config wait, the ping) without invoking their
    // handlers; the last shared_ptr then expires here with the operation never completed. Deliver
    // an error so a std::future caller never observes broken_promise and a callback caller never
    // hangs. By the time the destructor runs the last reference is gone, so no other thread can be
    // in complete() concurrently. The handler is user code, and a destructor must not let an
    // exception escape.
    if (!completed_) {
      try {
        handler_(errc::network::cluster_closed);
      } catch (...) { // NOLINT(bugprone-empty-catch)
      }
    }
  }

  void run()
  {
    // close() completes a wait still in flight: the public cluster stops its io_context once
    // close()'s handler runs, so no timer or posted continuation of this wait would run after
    // that. A cluster already closed completes here, inline, as open_bucket() does.
    const std::weak_ptr<wait_until_ready_operation> weak_for_close = shared_from_this();
    if (!core_.add_close_hook(weak_for_close, [weak_for_close]() {
          if (auto self = weak_for_close.lock(); self) {
            self->complete(errc::network::cluster_closed);
          }
        })) {
      return complete(errc::network::cluster_closed);
    }
    if (desired_state_ == couchbase::cluster_state::offline) {
      // Reject on the io_context, not inline, so the handler runs on the io thread as on the poll
      // path. A closed cluster is answered inline above, because its io_context may be stopped.
      auto self = shared_from_this();
      return asio::post(core_.io_context(), [self]() {
        self->complete(errc::common::invalid_argument);
      });
    }
    if (core_.is_protostellar()) {
      // couchbase2 reaches every service through the gateway's gRPC channel, so a READY channel is
      // both the online and the degraded state, whatever the bucket and services. There is no
      // bucket to open and no session to ping. Posted for the same reason as the rejection above.
      auto self = shared_from_this();
      return asio::post(core_.io_context(), [self]() {
        self->poll();
      });
    }
    if (bucket_name_) {
      // Opening the bucket is what makes its configuration (and therefore the vbucket map we poll)
      // available on the core cluster. It is idempotent for an already-open bucket.
      //
      // Capture a weak_ptr, not self: open_bucket parks the callback in the core's deferred queue
      // until the bucket opens, and the operation holds a strong core_ member -- a strong capture
      // would form an op<->core reference cycle that survives cluster close, so the operation would
      // never be destroyed and its handler never fires. anchor() keeps the operation alive on the
      // io_context while the open is outstanding instead.
      const auto& bucket_name = *bucket_name_;
      anchor();
      const std::weak_ptr<wait_until_ready_operation> weak = shared_from_this();
      core_.open_bucket(bucket_name, [weak](std::error_code ec) {
        auto self = weak.lock();
        if (!self) {
          return;
        }
        self->timer_.cancel();
        // A closed cluster will never open the bucket; fail fast with the accurate error instead of
        // polling until the deadline (ping() returns an empty report once stopped, so the poll loop
        // would otherwise surface unambiguous_timeout). Other open errors (e.g. a missing bucket)
        // are transient from the caller's point of view and still resolve through the poll loop.
        if (ec == errc::network::cluster_closed) {
          return self->complete(ec);
        }
        self->poll();
      });
    } else {
      poll();
    }
  }

private:
  auto remaining() const -> std::chrono::milliseconds
  {
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline_ - std::chrono::steady_clock::now());
    return std::max(left, std::chrono::milliseconds::zero());
  }

  // The close hook can run on another io thread than a continuation of this wait.
  void complete(std::error_code ec)
  {
    if (completed_.exchange(true)) {
      return;
    }
    // Moved out, so that the handler's captures are released when it returns: a completed wait can
    // stay referenced by a continuation queued on a stopped io_context.
    auto handler = std::move(handler_);
    handler(ec);
  }

  void schedule_retry()
  {
    if (std::chrono::steady_clock::now() >= deadline_) {
      return complete(errc::common::unambiguous_timeout);
    }
    // Never sleep past the deadline: when less than a full interval remains, a fixed poll_interval
    // wait would overshoot the caller's timeout by up to one interval before poll() notices.
    timer_.expires_after(std::min(poll_interval, remaining()));
    auto self = shared_from_this();
    timer_.async_wait([self](std::error_code timer_ec) {
      if (timer_ec) {
        return; // cancelled / io_context shutting down
      }
      self->poll();
    });
  }

  // Keep the operation alive on the io_context (via timer_, holding a strong self) while a core
  // call that captures only a weak_ptr is outstanding -- open_bucket and ping park their callbacks
  // in the core's queues, so anchoring here rather than through those callbacks is what avoids the
  // op<->core reference cycle. The anchor doubles as the deadline guard for the outstanding call:
  // its continuation cancels the anchor, and if the deadline is reached first the wait completes
  // with a timeout instead of stalling until the call happens to return.
  void anchor()
  {
    timer_.expires_after(remaining());
    timer_.async_wait([self = shared_from_this()](std::error_code timer_ec) {
      if (timer_ec) {
        return; // cancelled by the core call's continuation, or io_context shutting down
      }
      self->complete(errc::common::unambiguous_timeout);
    });
  }

  void poll()
  {
    if (completed_) {
      return;
    }
    if (remaining() == std::chrono::milliseconds::zero()) {
      return complete(errc::common::unambiguous_timeout);
    }
    if (core_.is_protostellar()) {
      const auto [ec, ready] = core_.protostellar_channel_ready();
      if (ec) {
        return complete(ec);
      }
      if (ready) {
        return complete({});
      }
      return schedule_retry();
    }
    if (bucket_name_ && desired_state_ == couchbase::cluster_state::online) {
      // KV readiness (replica placement) first -- it is the condition durable writes need and the
      // one a fresh bucket fails; then confirm service health with a ping. Only gate on it for the
      // `online` target: `degraded` deliberately tolerates partial availability, so requiring full
      // replica placement there would contradict the state's meaning and could never be satisfied
      // by a genuinely degraded bucket.
      const auto& bucket_name = *bucket_name_;
      auto self = shared_from_this();
      // Bound each waiter to one poll interval, not the whole remaining budget: when a config never
      // arrives, a per-poll waiter armed for `remaining()` would leave one pending config callback
      // and steady_timer stacked up for every 100ms tick until the deadline. A short inner timeout
      // clears each waiter before the next poll (a cached config still fires its callback promptly,
      // so this adds no latency to the common ready path). The overall deadline is still enforced
      // by remaining() at the top of poll()/schedule_retry().
      with_bucket_config_or_timeout<bool>(
        core_,
        bucket_name,
        std::min(remaining(), poll_interval),
        [self](couchbase::error err, bool ready) {
          // A closed cluster is terminal: complete with the accurate error rather than retrying
          // until the deadline turns it into unambiguous_timeout. Any other error (including the
          // helper's own inner unambiguous_timeout when no config has arrived yet) just means "not
          // ready yet" and is retried.
          if (err.ec() == errc::network::cluster_closed) {
            return self->complete(err.ec());
          }
          if (err.ec() || !ready) {
            return self->schedule_retry();
          }
          self->do_ping();
        },
        // Weak: the helper parks this resolver in the bucket's deferred queue, which must not pin
        // the operation and its handler.
        [weak = weak_from_this()](const std::shared_ptr<topology::configuration>& config)
          -> std::pair<std::error_code, bool> {
          auto self = weak.lock();
          if (!self || !config) {
            return { {}, false };
          }
          warn_if_replicas_unplaceable(
            *config, self->bucket_name_.value_or(""), self->unplaceable_replicas_logged_);
          return { {}, vbucket_map_ready(*config) };
        });
    } else {
      do_ping();
    }
  }

  void do_ping()
  {
    // Weak capture + anchor, for the same reason as open_bucket: core_.ping parks the callback in
    // the core, so a strong self-capture would cycle with the core_ member and leak on close.
    anchor();
    const std::weak_ptr<wait_until_ready_operation> weak = shared_from_this();
    core_.ping(
      std::nullopt, bucket_name_, services_, remaining(), [weak](const diag::ping_result& report) {
        auto self = weak.lock();
        if (!self) {
          return;
        }
        self->timer_.cancel();
        if (ping_predicate_satisfied(report, self->desired_state_, self->services_)) {
          return self->complete({});
        }
        self->schedule_retry();
      });
  }

  core::cluster core_;
  std::optional<std::string> bucket_name_;
  std::chrono::steady_clock::time_point deadline_;
  couchbase::cluster_state desired_state_;
  std::set<service_type> services_;
  utils::movable_function<void(std::error_code)> handler_;
  asio::steady_timer timer_;
  std::atomic_bool completed_{ false };
  // Every poll sees the same configuration until a node is added, so the warning is logged once.
  std::atomic_bool unplaceable_replicas_logged_{ false };
};
} // namespace

void
wait_until_ready(core::cluster core,
                 std::optional<std::string> bucket_name,
                 std::chrono::milliseconds timeout,
                 couchbase::cluster_state desired_state,
                 std::set<service_type> services,
                 utils::movable_function<void(std::error_code)> handler)
{
  auto op = std::make_shared<wait_until_ready_operation>(std::move(core),
                                                         std::move(bucket_name),
                                                         timeout,
                                                         desired_state,
                                                         std::move(services),
                                                         std::move(handler));
  op->run();
}
} // namespace couchbase::core::impl
