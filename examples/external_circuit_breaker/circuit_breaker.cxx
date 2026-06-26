/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include "circuit_breaker.hxx"

#include <limits>
#include <sstream>
#include <thread>
#include <utility>

namespace example::cb
{

auto
to_string(circuit_state s) -> const char*
{
  switch (s) {
    case circuit_state::closed:
      return "CLOSED";
    case circuit_state::open:
      return "OPEN";
    case circuit_state::half_open:
      return "HALF_OPEN";
    case circuit_state::forced_open:
      return "FORCED_OPEN";
    case circuit_state::disabled:
      return "DISABLED";
  }
  return "UNKNOWN";
}

auto
breaker_category() -> const std::error_category&
{
  struct category : std::error_category {
    auto name() const noexcept -> const char* override
    {
      return "example::cb::circuit_breaker";
    }
    auto message(int ev) const -> std::string override
    {
      switch (static_cast<breaker_errc>(ev)) {
        case breaker_errc::call_short_circuited:
          return "circuit breaker short-circuited the call before reaching the SDK";
      }
      return "unknown circuit breaker error";
    }
  };
  static const category instance;
  return instance;
}

// ---------------------------------------------------------------------------
// sliding_window_counters
// ---------------------------------------------------------------------------

namespace
{
auto
clamp_bucket_count(std::chrono::milliseconds window, std::uint32_t buckets) -> std::uint32_t
{
  // Each bucket needs at least 1ms of window to itself; otherwise integer
  // division below would produce bucket_size_ms == 0 and we'd have to clamp
  // it up to 1, silently inflating the effective window length beyond what
  // the caller configured.  Reduce the bucket count instead so the sampled
  // window still matches the configured one.
  auto window_ms = std::max<std::int64_t>(1, window.count());
  // Saturate rather than narrow: window_ms above UINT32_MAX (~49.7 days) would
  // wrap modulo 2^32 and could clamp the bucket count to a small/garbage value.
  constexpr auto kU32Max = std::numeric_limits<std::uint32_t>::max();
  auto max_buckets = window_ms > static_cast<std::int64_t>(kU32Max)
                       ? kU32Max
                       : static_cast<std::uint32_t>(window_ms);
  return std::max<std::uint32_t>(1, std::min(buckets, max_buckets));
}
} // namespace

sliding_window_counters::sliding_window_counters(std::chrono::milliseconds window,
                                                 std::uint32_t buckets)
  : bucket_count_{ clamp_bucket_count(window, buckets) }
  , bucket_size_ms_{ std::max<std::int64_t>(1, window.count()) / bucket_count_ }
{
  buckets_.reserve(bucket_count_);
  for (std::uint32_t i = 0; i < bucket_count_; ++i) {
    buckets_.emplace_back(std::make_unique<bucket>());
  }
}

auto
sliding_window_counters::record(call_outcome o, bool slow) -> void
{
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count();
  auto epoch = now_ms / bucket_size_ms_;
  auto& b = *buckets_[bucket_index_for(epoch)];
  auto stored = b.epoch.load(std::memory_order_acquire);
  while (stored != epoch) {
    if (stored == kRotatingEpoch) {
      // Another thread is mid-rotation; back off and re-read.
      std::this_thread::yield();
      stored = b.epoch.load(std::memory_order_acquire);
      continue;
    }
    if (b.epoch.compare_exchange_weak(
          stored, kRotatingEpoch, std::memory_order_acq_rel, std::memory_order_acquire)) {
      // We own the rotation: zero the counters before publishing the new
      // epoch so concurrent recorders never see a partially-reset bucket.
      b.successes.store(0, std::memory_order_relaxed);
      b.failures.store(0, std::memory_order_relaxed);
      b.slow.store(0, std::memory_order_relaxed);
      b.epoch.store(epoch, std::memory_order_release);
      break;
    }
  }
  if (o == call_outcome::success) {
    b.successes.fetch_add(1, std::memory_order_relaxed);
  } else {
    b.failures.fetch_add(1, std::memory_order_relaxed);
  }
  if (slow) {
    b.slow.fetch_add(1, std::memory_order_relaxed);
  }
}

auto
sliding_window_counters::sample() const -> snapshot
{
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count();
  auto current_epoch = now_ms / bucket_size_ms_;
  auto min_epoch = current_epoch - static_cast<std::int64_t>(bucket_count_) + 1;
  snapshot s{};
  for (std::uint32_t i = 0; i < bucket_count_; ++i) {
    const auto& b = *buckets_[i];
    // Explicit sentinel exclusion: when steady_clock is small (early after
    // process boot) min_epoch can go negative, in which case the sentinels
    // (kUninitializedEpoch = -1, kRotatingEpoch = -2) would otherwise pass
    // the in-range check below.
    auto ep = b.epoch.load(std::memory_order_acquire);
    if (ep == kRotatingEpoch || ep == kUninitializedEpoch) {
      continue;
    }
    if (ep < min_epoch || ep > current_epoch) {
      continue;
    }
    auto succ = b.successes.load(std::memory_order_relaxed);
    auto fail = b.failures.load(std::memory_order_relaxed);
    auto slow = b.slow.load(std::memory_order_relaxed);
    // Re-check the epoch: if the bucket rotated between the epoch load and
    // the counter loads we may have read freshly-zeroed counters that
    // belong to the new epoch, not the one we admitted.  Skip the bucket
    // rather than mix epochs in the snapshot.
    if (b.epoch.load(std::memory_order_acquire) != ep) {
      continue;
    }
    s.successes += succ;
    s.failures += fail;
    s.slow += slow;
  }
  s.total = s.successes + s.failures;
  return s;
}

auto
sliding_window_counters::reset() -> void
{
  for (std::uint32_t i = 0; i < bucket_count_; ++i) {
    buckets_[i]->epoch.store(kUninitializedEpoch, std::memory_order_relaxed);
    buckets_[i]->successes.store(0, std::memory_order_relaxed);
    buckets_[i]->failures.store(0, std::memory_order_relaxed);
    buckets_[i]->slow.store(0, std::memory_order_relaxed);
  }
}

auto
sliding_window_counters::bucket_index_for(std::int64_t epoch) const -> std::size_t
{
  return static_cast<std::size_t>(((epoch % bucket_count_) + bucket_count_) % bucket_count_);
}

// ---------------------------------------------------------------------------
// node_breaker
// ---------------------------------------------------------------------------

node_breaker::node_breaker(circuit_breaker_config config, transition_callback on_transition)
  : config_{ std::move(config) }
  , on_transition_{ std::move(on_transition) }
  , window_{ config_.sliding_window(), config_.number_of_buckets() }
  , state_{ circuit_state::closed }
  , half_open_permits_{ 0 }
  , half_open_successes_{ 0 }
  , half_open_failures_{ 0 }
  , rejected_{ 0 }
  , opened_at_ms_{ kNotOpenedYet }
  , half_open_at_ms_{ kNotOpenedYet }
{
}

auto
node_breaker::try_acquire() -> bool
{
  switch (state_.load(std::memory_order_acquire)) {
    case circuit_state::disabled:
      return true;
    case circuit_state::forced_open:
      rejected_.fetch_add(1, std::memory_order_relaxed);
      return false;
    case circuit_state::closed:
      return true;
    case circuit_state::open:
      if (cool_off_elapsed()) {
        try_transition_to_half_open("cool-off elapsed");
        if (try_acquire_half_open_permit()) {
          return true;
        }
      }
      rejected_.fetch_add(1, std::memory_order_relaxed);
      return false;
    case circuit_state::half_open:
      if (try_acquire_half_open_permit()) {
        return true;
      }
      // No permit available. In the normal case this means the configured
      // number of probes are already in flight and we are simply waiting
      // for their outcomes — reject and let them resolve. But if those
      // probes never report back (a permit was leaked), re-open after the
      // cool-off window so the breaker re-arms instead of wedging shut.
      if (half_open_stalled()) {
        try_transition_to_open("half-open stalled: no probe resolved within the cool-off window");
      }
      rejected_.fetch_add(1, std::memory_order_relaxed);
      return false;
  }
  return true;
}

auto
node_breaker::record(call_outcome o, std::chrono::milliseconds duration) -> void
{
  auto s = state_.load(std::memory_order_acquire);
  if (s == circuit_state::disabled || s == circuit_state::forced_open) {
    return;
  }
  auto slow = duration >= config_.slow_call_duration_threshold();
  window_.record(o, slow);

  if (s == circuit_state::half_open) {
    // Note: the state_.load() at record() entry is a snapshot. Between the
    // snapshot and the transition call below, the breaker may have moved on
    // — typically because a concurrent failed probe re-opened it, or
    // because a different success raced ahead and already closed it.
    // expected_from=half_open below makes transition() abandon the change
    // when the snapshot is stale, so a late success can never flip a
    // freshly-re-opened breaker back to closed, and a late failure can
    // never re-open a freshly-closed one.
    if (o == call_outcome::success) {
      auto succ = half_open_successes_.fetch_add(1, std::memory_order_acq_rel) + 1;
      if (succ >= config_.permitted_calls_in_half_open_state()) {
        transition("half-open probes succeeded", circuit_state::closed, circuit_state::half_open);
      }
    } else {
      half_open_failures_.fetch_add(1, std::memory_order_acq_rel);
      transition("half-open probe failed", circuit_state::open, circuit_state::half_open);
    }
    return;
  }

  if (s == circuit_state::closed) {
    evaluate_and_maybe_open();
  }
}

auto
node_breaker::force_open() -> void
{
  transition("administrative force_open", circuit_state::forced_open);
}

auto
node_breaker::disable() -> void
{
  transition("administrative disable", circuit_state::disabled);
}

auto
node_breaker::reset() -> void
{
  std::lock_guard guard{ state_mutex_ };
  // Mirror transition()'s aux-fields-before-state_ ordering (see the
  // comment there).  Otherwise a reader could observe stale state_=open
  // and the freshly-zeroed opened_at_ms_=kNotOpenedYet, which makes
  // cool_off_elapsed() return false and produces a spurious rejected_++
  // for a breaker that has just been reset to CLOSED.
  auto prev = state_.load(std::memory_order_acquire);
  window_.reset();
  half_open_permits_.store(0, std::memory_order_relaxed);
  half_open_successes_.store(0, std::memory_order_relaxed);
  half_open_failures_.store(0, std::memory_order_relaxed);
  rejected_.store(0, std::memory_order_relaxed);
  opened_at_ms_.store(kNotOpenedYet, std::memory_order_relaxed);
  half_open_at_ms_.store(kNotOpenedYet, std::memory_order_relaxed);
  state_.store(circuit_state::closed, std::memory_order_release);
  if (prev != circuit_state::closed && on_transition_) {
    on_transition_(prev, circuit_state::closed, "reset");
  }
}

auto
node_breaker::snapshot_metrics() const -> circuit_breaker_metrics
{
  auto snap = window_.sample();
  circuit_breaker_metrics m{};
  m.state = state_.load(std::memory_order_acquire);
  m.successful_calls = snap.successes;
  m.failed_calls = snap.failures;
  m.slow_calls = snap.slow;
  m.total_calls = snap.total;
  m.failure_rate_percent =
    snap.total == 0 ? 0 : static_cast<std::uint32_t>((snap.failures * 100) / snap.total);
  m.slow_call_rate_percent =
    snap.total == 0 ? 0 : static_cast<std::uint32_t>((snap.slow * 100) / snap.total);
  m.rejected_calls = rejected_.load(std::memory_order_relaxed);
  return m;
}

auto
node_breaker::cool_off_elapsed() const -> bool
{
  // transition() always stores a real opened_at_ms_ before publishing
  // state_=open, and the caller only reaches this method after observing
  // state_=open via an acquire-load of state_, so by C++17 [intro.races]
  // we are guaranteed to see a real timestamp here.  The kNotOpenedYet
  // branch is a defensive guard: if the invariant is ever broken, fail
  // safe by refusing to elapse rather than spuriously promoting the
  // breaker to HALF_OPEN.
  auto opened_ms = opened_at_ms_.load(std::memory_order_acquire);
  if (opened_ms == kNotOpenedYet) {
    return false;
  }
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count();
  return (now_ms - opened_ms) >= config_.wait_duration_in_open_state().count();
}

auto
node_breaker::half_open_stalled() const -> bool
{
  // Mirrors cool_off_elapsed(): half_open_at_ms_ is published before
  // state_=half_open, and the caller only reaches here after observing
  // state_=half_open, so we are guaranteed to read a real timestamp. The
  // kNotOpenedYet branch fails safe (not stalled) if that invariant is
  // ever broken. We reuse the OPEN cool-off as the stall window — a probe
  // that has not resolved within a full cool-off is treated as lost.
  auto entered_ms = half_open_at_ms_.load(std::memory_order_acquire);
  if (entered_ms == kNotOpenedYet) {
    return false;
  }
  auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now().time_since_epoch())
                  .count();
  return (now_ms - entered_ms) >= config_.wait_duration_in_open_state().count();
}

auto
node_breaker::try_acquire_half_open_permit() -> bool
{
  auto current = half_open_permits_.load(std::memory_order_acquire);
  while (current > 0) {
    if (half_open_permits_.compare_exchange_weak(
          current, current - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

auto
node_breaker::evaluate_and_maybe_open() -> void
{
  auto snap = window_.sample();
  if (snap.total < config_.minimum_number_of_calls()) {
    return;
  }
  auto fail_pct = static_cast<std::uint32_t>((snap.failures * 100) / snap.total);
  auto slow_pct = static_cast<std::uint32_t>((snap.slow * 100) / snap.total);
  if (fail_pct >= config_.failure_rate_threshold_percent()) {
    std::ostringstream why;
    why << "failure rate " << fail_pct << "% >= threshold "
        << config_.failure_rate_threshold_percent() << "% over " << snap.total << " calls";
    try_transition_to_open(why.str());
  } else if (slow_pct >= config_.slow_call_rate_threshold_percent()) {
    std::ostringstream why;
    why << "slow-call rate " << slow_pct << "% >= threshold "
        << config_.slow_call_rate_threshold_percent() << "% over " << snap.total << " calls";
    try_transition_to_open(why.str());
  }
}

auto
node_breaker::try_transition_to_open(const std::string& why) -> void
{
  transition(why, circuit_state::open);
}

auto
node_breaker::try_transition_to_half_open(const std::string& why) -> void
{
  transition(why, circuit_state::half_open);
}

auto
node_breaker::try_transition_to_closed(const std::string& why) -> void
{
  transition(why, circuit_state::closed);
}

auto
node_breaker::transition(const std::string& why,
                         circuit_state target,
                         std::optional<circuit_state> expected_from) -> void
{
  circuit_state prev;
  {
    std::lock_guard guard{ state_mutex_ };
    prev = state_.load(std::memory_order_acquire);
    if (prev == target) {
      return;
    }
    // Expected-from gate: callers that depend on the source state being
    // unchanged since their snapshot pass expected_from. Used by the
    // half-open record path to avoid flipping a freshly-re-opened breaker
    // back to closed on a late success (or vice versa).
    if (expected_from.has_value() && prev != *expected_from) {
      return;
    }
    // An administrative override (forced_open / disabled) must not be undone
    // by a concurrent automatic transition.  force_open() / disable() can
    // still swap one admin state for another; reset() bypasses transition()
    // entirely and is the only path back to closed.
    auto is_admin_state = [](circuit_state s) -> bool {
      return s == circuit_state::forced_open || s == circuit_state::disabled;
    };
    if (is_admin_state(prev) && !is_admin_state(target)) {
      return;
    }
    // Publish the auxiliary fields (opened_at_ms_, half-open counters, etc.)
    // *before* the state_ store below.  state_ is the single synchronization
    // point on the read path: a reader doing state_.load(acquire) that
    // observes a value written by the state_.store(release) below is also
    // guaranteed to see every store sequenced-before that release in this
    // thread (C++17 [intro.races] synchronizes-with).  That covers all of
    // the auxiliary fields here, so the read path can use plain atomic
    // loads on each field without an additional fence.
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();
    if (target == circuit_state::open) {
      opened_at_ms_.store(now_ms, std::memory_order_release);
      half_open_at_ms_.store(kNotOpenedYet, std::memory_order_release);
      half_open_permits_.store(0, std::memory_order_release);
      half_open_successes_.store(0, std::memory_order_release);
      half_open_failures_.store(0, std::memory_order_release);
    } else if (target == circuit_state::half_open) {
      half_open_at_ms_.store(now_ms, std::memory_order_release);
      half_open_permits_.store(config_.permitted_calls_in_half_open_state(),
                               std::memory_order_release);
      half_open_successes_.store(0, std::memory_order_release);
      half_open_failures_.store(0, std::memory_order_release);
    } else if (target == circuit_state::closed) {
      window_.reset();
      half_open_permits_.store(0, std::memory_order_release);
      half_open_successes_.store(0, std::memory_order_release);
      half_open_failures_.store(0, std::memory_order_release);
      opened_at_ms_.store(kNotOpenedYet, std::memory_order_release);
      half_open_at_ms_.store(kNotOpenedYet, std::memory_order_release);
    }
    state_.store(target, std::memory_order_release);
  }
  if (on_transition_) {
    on_transition_(prev, target, why);
  }
}

// ---------------------------------------------------------------------------
// circuit_breaker
// ---------------------------------------------------------------------------

circuit_breaker::circuit_breaker(circuit_breaker_config config)
  : config_{ std::move(config) }
{
}

auto
circuit_breaker::set_transition_callback(transition_callback cb) -> void
{
  std::unique_lock guard{ mutex_ };
  on_transition_ = std::move(cb);
}

auto
circuit_breaker::dispatch_transition(const std::string& node_id_str,
                                     circuit_state from,
                                     circuit_state to,
                                     const std::string& why) const -> void
{
  transition_callback cb;
  {
    std::shared_lock guard{ mutex_ };
    cb = on_transition_;
  }
  if (cb) {
    cb(from, to, "[node " + node_id_str + "] " + why);
  }
}

auto
circuit_breaker::allow(const couchbase::node_id& nid) -> bool
{
  if (!nid) {
    return true;
  }
  auto b = get_or_create(nid);
  return b->try_acquire();
}

auto
circuit_breaker::record_success(const couchbase::node_id& nid, std::chrono::milliseconds duration)
  -> void
{
  if (!nid) {
    return;
  }
  get_or_create(nid)->record(call_outcome::success, duration);
}

auto
circuit_breaker::record_failure(const couchbase::node_id& nid, std::chrono::milliseconds duration)
  -> void
{
  if (!nid) {
    return;
  }
  get_or_create(nid)->record(call_outcome::failure, duration);
}

auto
circuit_breaker::state_of(const couchbase::node_id& nid) const -> circuit_state
{
  if (auto b = find(nid)) {
    return b->state();
  }
  return circuit_state::closed;
}

auto
circuit_breaker::metrics_of(const couchbase::node_id& nid) const
  -> std::optional<circuit_breaker_metrics>
{
  if (auto b = find(nid)) {
    return b->snapshot_metrics();
  }
  return std::nullopt;
}

auto
circuit_breaker::all_metrics() const
  -> std::vector<std::pair<couchbase::node_id, circuit_breaker_metrics>>
{
  // Copy the (id, breaker) pairs out from under the registry lock first, so
  // sampling each node — which involves steady_clock and a handful of
  // atomic loads per bucket — does not stall concurrent get_or_create() or
  // set_transition_callback() callers.  shared_ptr keeps each node_breaker
  // alive while we sample.
  std::vector<std::pair<couchbase::node_id, std::shared_ptr<node_breaker>>> snapshot;
  {
    std::shared_lock guard{ mutex_ };
    snapshot.reserve(nodes_.size());
    for (const auto& [k, v] : nodes_) {
      snapshot.emplace_back(k, v);
    }
  }
  std::vector<std::pair<couchbase::node_id, circuit_breaker_metrics>> out;
  out.reserve(snapshot.size());
  for (const auto& [k, v] : snapshot) {
    out.emplace_back(k, v->snapshot_metrics());
  }
  return out;
}

auto
circuit_breaker::force_open(const couchbase::node_id& nid) -> void
{
  if (nid) {
    get_or_create(nid)->force_open();
  }
}

auto
circuit_breaker::disable(const couchbase::node_id& nid) -> void
{
  if (nid) {
    get_or_create(nid)->disable();
  }
}

auto
circuit_breaker::reset(const couchbase::node_id& nid) -> void
{
  if (auto b = find(nid)) {
    b->reset();
  }
}

auto
circuit_breaker::forget(const couchbase::node_id& nid) -> bool
{
  if (!nid) {
    return false;
  }
  std::unique_lock guard{ mutex_ };
  return nodes_.erase(nid) != 0;
}

auto
circuit_breaker::find(const couchbase::node_id& nid) const -> std::shared_ptr<node_breaker>
{
  if (!nid) {
    return nullptr;
  }
  std::shared_lock guard{ mutex_ };
  if (auto it = nodes_.find(nid); it != nodes_.end()) {
    return it->second;
  }
  return nullptr;
}

auto
circuit_breaker::get_or_create(const couchbase::node_id& nid) -> std::shared_ptr<node_breaker>
{
  {
    std::shared_lock guard{ mutex_ };
    if (auto it = nodes_.find(nid); it != nodes_.end()) {
      return it->second;
    }
  }
  std::unique_lock guard{ mutex_ };
  if (auto it = nodes_.find(nid); it != nodes_.end()) {
    return it->second;
  }
  // self = this is a non-owning raw capture by design. See the lifetime
  // contract on the @c circuit_breaker class doc-comment: the caller is
  // responsible for keeping the @c circuit_breaker alive across every
  // thread that may invoke record_success / record_failure / execute. A
  // refactor to enable_shared_from_this + weak_ptr would make this
  // automatic but would force every application to construct a
  // shared_ptr<circuit_breaker>, which the example deliberately avoids.
  auto wrapped = [self = this, id = nid.id()](
                   circuit_state from, circuit_state to, const std::string& why) -> void {
    self->dispatch_transition(id, from, to, why);
  };
  auto breaker = std::make_shared<node_breaker>(config_, std::move(wrapped));
  nodes_.emplace(nid, breaker);
  return breaker;
}

} // namespace example::cb
