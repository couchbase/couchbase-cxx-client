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

/**
 * @file
 *
 * Standalone, production-quality circuit breaker layered on top of the
 * Couchbase C++ SDK's node_id API.  The implementation lives in the
 * ::example::cb namespace so applications can copy this pair of files
 * (circuit_breaker.hxx / circuit_breaker.cxx) verbatim, rename the
 * namespace, and drop them into their codebase.
 *
 * Design references: Netflix Hystrix, Resilience4j.
 *
 * Features:
 *   * Per-node isolation — a degraded node does not poison the others.
 *   * Time-based sliding window with fixed bucket ring; recording is
 *     lock-free in the steady state with a brief spin during epoch
 *     rotation (see sliding_window_counters::record() for the
 *     synchronisation discipline).
 *   * Failure-rate and slow-call-rate thresholds.
 *   * Minimum-calls gate so a cold cache does not flap the breaker.
 *   * Five states: closed, open, half_open, disabled, forced_open.
 *   * Bounded probe concurrency in half_open (permits).
 *   * State-transition listener for logging and telemetry hooks.
 *   * Administrative disable / force-open overrides.
 *   * No dependencies beyond the C++17 standard library and the SDK.
 *
 * The breaker sits outside the SDK — it decides whether to attempt an
 * operation, and records the outcome.  It does not modify SDK retry
 * behavior.  Integration with a call site is illustrated by
 * circuit_breaker::execute().
 */

#pragma once

#include <couchbase/error.hxx>
#include <couchbase/node_id.hxx>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace example::cb
{

/**
 * Lifecycle states of a per-node breaker.
 *
 *   closed       — requests flow through; outcomes update the sliding window.
 *   open         — requests are short-circuited until the sleep window elapses.
 *   half_open    — a bounded number of probe requests are allowed; their
 *                  outcome decides whether to close or re-open.
 *   forced_open  — administrative: always rejects, never auto-transitions.
 *   disabled     — administrative: always allows, does not record metrics.
 */
enum class circuit_state : std::uint8_t {
  closed,
  open,
  half_open,
  forced_open,
  disabled,
};

[[nodiscard]] auto
to_string(circuit_state s) -> const char*;

/**
 * Classification of a single call outcome, supplied by the caller.
 *
 * The breaker does not try to be clever about which std::error_code values
 * indicate a server-side problem versus a client-side user error — the
 * caller decides, because only the caller knows the business context.
 */
enum class call_outcome : std::uint8_t {
  success,
  failure,
};

/**
 * Error codes synthesized by the breaker itself (never by the SDK).
 *
 * When circuit_breaker::execute() short-circuits a call it returns a
 * couchbase::error whose error_code lives in this dedicated category, so a
 * caller can tell a breaker rejection apart from a real SDK error and choose
 * not to feed it back into the breaker's failure rate.  The SDK's own
 * couchbase::errc::common::request_canceled is reserved for actual
 * SDK-emitted cancellations (e.g. socket close mid-flight) and is therefore
 * still a legitimate breaker-trippable signal.
 */
enum class breaker_errc {
  // Renamed from user_canceled — the breaker, not the user, is the entity
  // that decides to short-circuit the call. The dedicated value lives in
  // this enum's own category so a caller can pattern-match on it without
  // confusing it with couchbase::errc::common::request_canceled.
  call_short_circuited = 1,
};

[[nodiscard]] auto
breaker_category() -> const std::error_category&;

[[nodiscard]] inline auto
make_error_code(breaker_errc e) -> std::error_code
{
  return { static_cast<int>(e), breaker_category() };
}

/**
 * Tunables for the breaker.  The defaults are conservative and match the
 * common recommendation (50% error rate over a 10-second window with at
 * least 20 calls, 5-second cool-off).  Every setter validates and clamps
 * its argument and returns *this so calls can be chained:
 *
 * @code
 * auto cfg = example::cb::circuit_breaker_config{}
 *              .with_failure_rate_threshold_percent(60)
 *              .with_sliding_window(std::chrono::seconds{ 30 })
 *              .with_minimum_number_of_calls(50);
 * @endcode
 */
class circuit_breaker_config
{
public:
  /**
   * Failure rate (in percent, 1..100) that opens the breaker.  Evaluated
   * over the sliding window once at least minimum_number_of_calls() calls
   * have accumulated.
   *
   * @par Example
   * With the default of 50, the breaker trips when 50% or more of the
   * recent calls failed.
   *
   * @par Tuning
   * Lower values (e.g. 25) make the breaker more aggressive and trip
   * sooner; higher values tolerate more transient failure but delay
   * isolating a misbehaving node.
   *
   * @return the configured failure-rate threshold, in percent.
   */
  [[nodiscard]] auto failure_rate_threshold_percent() const -> std::uint32_t
  {
    return failure_rate_threshold_percent_;
  }

  /**
   * Set the failure-rate trip threshold.  See
   * failure_rate_threshold_percent() for semantics.
   *
   * @param value percent of failed calls that opens the breaker; clamped
   * to the range [1, 100].
   * @return *this, for chaining.
   */
  auto with_failure_rate_threshold_percent(std::uint32_t value) -> circuit_breaker_config&
  {
    failure_rate_threshold_percent_ = std::clamp<std::uint32_t>(value, 1, 100);
    return *this;
  }

  /**
   * Slow-call rate (in percent, 1..100) that opens the breaker.  A call is
   * "slow" when its measured duration is at least
   * slow_call_duration_threshold().  This is independent of the failure
   * rate: a node that answers every request but answers them slowly will
   * still be tripped by this rule.
   *
   * @par Example
   * With the default of 100, the breaker only trips on slow-rate when
   * *every* call in the window exceeded the duration threshold — a very
   * conservative setting that effectively disables the slow-call rule
   * unless every observed call is degraded.
   *
   * @par Tuning
   * Set this lower (e.g. 50) if you want a node that answers slowly for
   * half its traffic to be considered unhealthy.
   *
   * @return the configured slow-call-rate threshold, in percent.
   */
  [[nodiscard]] auto slow_call_rate_threshold_percent() const -> std::uint32_t
  {
    return slow_call_rate_threshold_percent_;
  }

  /**
   * Set the slow-call-rate trip threshold.  See
   * slow_call_rate_threshold_percent() for semantics.
   *
   * @param value percent of slow calls that opens the breaker; clamped to
   * the range [1, 100].
   * @return *this, for chaining.
   */
  auto with_slow_call_rate_threshold_percent(std::uint32_t value) -> circuit_breaker_config&
  {
    slow_call_rate_threshold_percent_ = std::clamp<std::uint32_t>(value, 1, 100);
    return *this;
  }

  /**
   * Duration above which a successful call is still classified as "slow"
   * for the slow-call-rate calculation.  This is the latency above which
   * you stop considering a response useful even if it eventually arrived.
   *
   * @par Example
   * With the default of 2000ms, every call that takes 2s or more counts
   * as slow regardless of whether it ultimately succeeded.
   *
   * @par Tuning
   * Should be set comfortably above the p99 of healthy latency for your
   * workload but well under any user-facing timeout.
   *
   * @return the slow-call duration cut-off.
   */
  [[nodiscard]] auto slow_call_duration_threshold() const -> std::chrono::milliseconds
  {
    return slow_call_duration_threshold_;
  }

  /**
   * Set the slow-call duration cut-off.  See
   * slow_call_duration_threshold() for semantics.
   *
   * @param value the latency above which a call is treated as slow;
   * clamped to be non-negative (a negative threshold would classify
   * every call as slow and continually trip the breaker).
   * @return *this, for chaining.
   */
  auto with_slow_call_duration_threshold(std::chrono::milliseconds value) -> circuit_breaker_config&
  {
    slow_call_duration_threshold_ = std::max(value, std::chrono::milliseconds{ 0 });
    return *this;
  }

  /**
   * Minimum number of recorded calls in the sliding window before the
   * breaker is allowed to trip.  This is the cold-start gate — without it
   * a single failed call (one failure out of one) would register as 100%
   * failure rate and trip the breaker on the first hiccup.
   *
   * @par Example
   * With the default of 20, the breaker waits until at least 20 calls
   * have occurred in the current window before checking the failure-rate
   * or slow-call-rate thresholds.
   *
   * @par Tuning
   * Raise this for high-throughput services where a couple of bad calls
   * are statistical noise; lower it for low-traffic paths where waiting
   * for 20 samples would mean tolerating a long stretch of failures.
   *
   * @return the minimum call volume gate.
   */
  [[nodiscard]] auto minimum_number_of_calls() const -> std::uint32_t
  {
    return minimum_number_of_calls_;
  }

  /**
   * Set the minimum-call gate.  See minimum_number_of_calls() for
   * semantics.
   *
   * @param value the cold-start gate; clamped to be at least 1.
   * @return *this, for chaining.
   */
  auto with_minimum_number_of_calls(std::uint32_t value) -> circuit_breaker_config&
  {
    minimum_number_of_calls_ = std::max<std::uint32_t>(1, value);
    return *this;
  }

  /**
   * Length of the time-based sliding window over which call outcomes are
   * aggregated.  Every threshold (failure rate, slow-call rate, minimum
   * calls) is evaluated against the calls that fall inside this rolling
   * window.
   *
   * @par Example
   * With the default of 10s, the breaker reasons about "the last ten
   * seconds of traffic" — a brief outage is detected quickly and a
   * recovered node is forgiven quickly.
   *
   * @par Tuning
   * A longer window smooths over transient blips but reacts more slowly
   * to a real outage; a shorter window is twitchier.  The window is
   * divided into number_of_buckets() equal slices.
   *
   * @return the sliding window length.
   */
  [[nodiscard]] auto sliding_window() const -> std::chrono::milliseconds
  {
    return sliding_window_;
  }

  /**
   * Set the sliding window length.  See sliding_window() for semantics.
   * If the requested window is shorter than the configured bucket count
   * (in ms), the bucket count will be reduced at construction time so
   * each bucket still covers at least 1ms.
   *
   * @param value the rolling window length; clamped to be at least 1ms
   * so the stored value matches what the breaker actually applies.
   * @return *this, for chaining.
   */
  auto with_sliding_window(std::chrono::milliseconds value) -> circuit_breaker_config&
  {
    sliding_window_ = std::max(value, std::chrono::milliseconds{ 1 });
    return *this;
  }

  /**
   * Number of equal-size buckets the sliding window is divided into.
   * Buckets are the granularity at which old data ages out: as time
   * advances, the oldest bucket falls out of the window and a fresh one
   * is rotated in.  More buckets give finer time resolution at the cost
   * of memory and a small bit of work per record() call.
   *
   * @par Example
   * With the default of 10 buckets and a 10s window, each bucket covers
   * 1s of traffic.
   *
   * @par Tuning
   * 10 is a sensible default.  Going much higher rarely changes the
   * decisions the breaker makes; going much lower (e.g. 2) makes the
   * window jerk forward in coarse steps.
   *
   * @return the bucket count.
   */
  [[nodiscard]] auto number_of_buckets() const -> std::uint32_t
  {
    return number_of_buckets_;
  }

  /**
   * Set the bucket count.  See number_of_buckets() for semantics.  The
   * value is clamped to be at least 1 and is further capped at
   * construction time if it would force a sub-millisecond bucket size.
   *
   * @param value the number of buckets in the sliding window.
   * @return *this, for chaining.
   */
  auto with_number_of_buckets(std::uint32_t value) -> circuit_breaker_config&
  {
    number_of_buckets_ = std::max<std::uint32_t>(1, value);
    return *this;
  }

  /**
   * Cool-off duration the breaker stays in the OPEN state before allowing
   * probe traffic.  While OPEN the breaker short-circuits every request
   * to spare the failing node and the calling threads; after this
   * duration elapses the next call promotes the breaker to half_open and
   * a small number of probes are admitted.
   *
   * @par Example
   * With the default of 5s, a tripped node is given five seconds to
   * settle before any client traffic is sent its way again.
   *
   * @par Tuning
   * Too short and you stampede a node that hasn't recovered; too long
   * and you keep rejecting traffic after the node is healthy again.  Set
   * roughly to the time you'd expect a transient incident (failover,
   * GC pause, restart) to clear.
   *
   * @return the cool-off duration.
   */
  [[nodiscard]] auto wait_duration_in_open_state() const -> std::chrono::milliseconds
  {
    return wait_duration_in_open_state_;
  }

  /**
   * Set the OPEN-state cool-off.  See wait_duration_in_open_state() for
   * semantics.
   *
   * @param value the time the breaker stays OPEN before probing;
   * clamped to be at least 1ms.  A zero or negative cool-off would let
   * the breaker flap directly back to half_open on the very next call,
   * which defeats the purpose.
   * @return *this, for chaining.
   */
  auto with_wait_duration_in_open_state(std::chrono::milliseconds value) -> circuit_breaker_config&
  {
    wait_duration_in_open_state_ = std::max(value, std::chrono::milliseconds{ 1 });
    return *this;
  }

  /**
   * Number of probe slots issued when the breaker enters half_open.
   * This value plays two roles:
   *
   *  -# It is the total number of probe attempts admitted while
   *     half_open.  Each call to allow() consumes one slot; slots are
   *     **not** returned when a probe completes, so once N probes have
   *     been admitted, further calls are rejected even if some of the
   *     in-flight probes have already finished.
   *  -# It is also the success threshold that closes the breaker: when
   *     the count of successful probes reaches N the breaker closes.
   *     A single failed probe immediately re-opens the breaker without
   *     waiting for the others.
   *
   * @par Example
   * With the default of 3, when a tripped breaker enters half_open it
   * admits up to three probes; if all three succeed the breaker closes
   * and full traffic resumes, but if any one fails the breaker reverts
   * to OPEN for another cool-off period.
   *
   * @par Tuning
   * Higher values demand more evidence of recovery before closing,
   * which is safer but takes longer; lower values close the breaker
   * sooner but risk closing on a flaky node.
   *
   * @return the number of probe attempts admitted in half_open.
   */
  [[nodiscard]] auto permitted_calls_in_half_open_state() const -> std::uint32_t
  {
    return permitted_calls_in_half_open_state_;
  }

  /**
   * Set the half_open probe count.  See
   * permitted_calls_in_half_open_state() for semantics.
   *
   * @param value the probe count; clamped to be at least 1.
   * @return *this, for chaining.
   */
  auto with_permitted_calls_in_half_open_state(std::uint32_t value) -> circuit_breaker_config&
  {
    permitted_calls_in_half_open_state_ = std::max<std::uint32_t>(1, value);
    return *this;
  }

private:
  std::uint32_t failure_rate_threshold_percent_{ 50 };
  std::uint32_t slow_call_rate_threshold_percent_{ 100 };
  std::chrono::milliseconds slow_call_duration_threshold_{ 2000 };
  std::uint32_t minimum_number_of_calls_{ 20 };
  std::chrono::milliseconds sliding_window_{ 10000 };
  std::uint32_t number_of_buckets_{ 10 };
  std::chrono::milliseconds wait_duration_in_open_state_{ 5000 };
  std::uint32_t permitted_calls_in_half_open_state_{ 3 };
};

/**
 * Observable state of a single per-node breaker, as viewed at the time of
 * sampling.  successful_calls, failed_calls, slow_calls, total_calls, and
 * the two rate fields refer to the current sliding window.  rejected_calls
 * is a lifetime counter — it tracks every call short-circuited since the
 * breaker was constructed and is only cleared by reset().
 */
struct circuit_breaker_metrics {
  circuit_state state{ circuit_state::closed };
  std::uint64_t successful_calls{ 0 };
  std::uint64_t failed_calls{ 0 };
  std::uint64_t slow_calls{ 0 };
  std::uint64_t total_calls{ 0 };
  std::uint32_t failure_rate_percent{ 0 };
  std::uint32_t slow_call_rate_percent{ 0 };
  std::uint64_t rejected_calls{ 0 };
};

// ---------------------------------------------------------------------------
// Time-based sliding window of fixed bucket ring.
//
// Each bucket covers sliding_window / number_of_buckets milliseconds.  A
// bucket holds atomic counters and an atomic "epoch" that identifies which
// time slice the bucket currently represents.  Recording is lock-free; a
// recording thread lazily resets a bucket whose epoch has advanced.
// ---------------------------------------------------------------------------

class sliding_window_counters
{
public:
  struct snapshot {
    std::uint64_t successes{ 0 };
    std::uint64_t failures{ 0 };
    std::uint64_t slow{ 0 };
    std::uint64_t total{ 0 };
  };

  sliding_window_counters(std::chrono::milliseconds window, std::uint32_t buckets);

  auto record(call_outcome o, bool slow) -> void;
  [[nodiscard]] auto sample() const -> snapshot;
  auto reset() -> void;

private:
  static constexpr std::int64_t kUninitializedEpoch = -1;
  // Sentinel published while a thread is mid-rotation: zero counters, then
  // publish the real epoch.  Other threads observing the sentinel spin until
  // the rotation completes; samplers treat it as a momentarily skipped
  // bucket.
  static constexpr std::int64_t kRotatingEpoch = -2;

  // One bucket of the ring. The four counters are atomic so record() and
  // sample() can run concurrently without a lock.
  struct bucket {
    std::atomic<std::int64_t> epoch{ kUninitializedEpoch };
    std::atomic<std::uint64_t> successes{ 0 };
    std::atomic<std::uint64_t> failures{ 0 };
    std::atomic<std::uint64_t> slow{ 0 };
  };

  [[nodiscard]] auto bucket_index_for(std::int64_t epoch) const -> std::size_t;

  std::uint32_t bucket_count_;
  std::int64_t bucket_size_ms_;
  std::vector<std::unique_ptr<bucket>> buckets_;
};

// ---------------------------------------------------------------------------
// Per-node breaker — owns the state machine and the sliding window for one
// cluster node.  Thread-safe.  state_mutex_ is held only to serialize state
// transitions; the hot path (allow / record) is atomic.
// ---------------------------------------------------------------------------

class node_breaker
{
public:
  using transition_callback =
    std::function<void(circuit_state /*from*/, circuit_state /*to*/, const std::string& /*why*/)>;

  node_breaker(circuit_breaker_config config, transition_callback on_transition);

  [[nodiscard]] auto state() const -> circuit_state
  {
    return state_.load(std::memory_order_acquire);
  }

  /**
   * Admission decision.  Side-effects limited to the open -> half_open
   * transition triggered by the first caller observed after the cool-off
   * elapses.  Rejections increment a counter that is reported in metrics.
   */
  [[nodiscard]] auto try_acquire() -> bool;

  auto record(call_outcome o, std::chrono::milliseconds duration) -> void;

  auto force_open() -> void;
  auto disable() -> void;
  auto reset() -> void;

  [[nodiscard]] auto snapshot_metrics() const -> circuit_breaker_metrics;

private:
  // Sentinel for opened_at_ms_ meaning "no open timestamp recorded".  We use
  // a negative value so it cannot collide with a legitimate steady_clock
  // count (which is always non-negative; the standard does allow the count
  // to be 0 on a freshly-booted system).
  static constexpr std::int64_t kNotOpenedYet = -1;

  [[nodiscard]] auto cool_off_elapsed() const -> bool;
  // True once a HALF_OPEN breaker has spent longer than the OPEN cool-off
  // window without any probe resolving it. This is a liveness backstop: if
  // a probe is admitted but its outcome is never recorded (e.g. the caller
  // bypassed execute() and a thrown operation swallowed the permit), the
  // breaker would otherwise sit in HALF_OPEN with no permits and no way
  // out. When stalled, try_acquire() re-opens the breaker so the normal
  // cool-off -> probe cycle can start over.
  [[nodiscard]] auto half_open_stalled() const -> bool;
  auto try_acquire_half_open_permit() -> bool;
  auto evaluate_and_maybe_open() -> void;

  auto try_transition_to_open(const std::string& why) -> void;
  auto try_transition_to_half_open(const std::string& why) -> void;
  auto try_transition_to_closed(const std::string& why) -> void;

  // transition() is guarded against the half-open race: if @p expected_from
  // is provided and the observed previous state does not match it under the
  // state_mutex_, the transition is abandoned. Used by the half-open record
  // path where, between snapshotting state_ at record() entry and reaching
  // the transition call, a concurrent failed probe may have re-opened the
  // breaker; a late success that races past that re-open must not flip the
  // breaker back to closed.
  auto transition(const std::string& why,
                  circuit_state target,
                  std::optional<circuit_state> expected_from = std::nullopt) -> void;

  circuit_breaker_config config_;
  transition_callback on_transition_;
  sliding_window_counters window_;
  std::mutex state_mutex_;
  std::atomic<circuit_state> state_;
  std::atomic<std::uint32_t> half_open_permits_;
  std::atomic<std::uint32_t> half_open_successes_;
  std::atomic<std::uint32_t> half_open_failures_;
  std::atomic<std::uint64_t> rejected_;
  std::atomic<std::int64_t> opened_at_ms_;
  // steady_clock milliseconds at which the breaker last entered HALF_OPEN,
  // or kNotOpenedYet when it is not HALF_OPEN. Published before state_ (see
  // transition()) so a reader that observes state_=half_open also observes
  // a real timestamp here. Consulted only by half_open_stalled().
  std::atomic<std::int64_t> half_open_at_ms_;
};

// ---------------------------------------------------------------------------
// Registry of per-node breakers, keyed by couchbase::node_id.  The registry
// is the object applications interact with directly.
// ---------------------------------------------------------------------------

/**
 * @par Lifetime contract (important)
 *
 * The transition listener installed via @ref set_transition_callback is
 * dispatched from per-node breakers that hold a non-owning raw reference
 * back to this @c circuit_breaker (see @c get_or_create() in
 * circuit_breaker.cxx). It is the caller's responsibility to ensure the
 * @c circuit_breaker outlives every thread that may call into
 * @ref record_success / @ref record_failure / @ref execute — i.e. every
 * thread that issues SDK calls wrapped by the breaker. Concretely:
 *
 *   * If you scope the breaker on the stack of `main()` (as the demo
 *     `main.cxx` does), make sure every worker thread joins before
 *     `main()` returns — the cluster's `cluster.close().get()` already
 *     joins the SDK's threads, so as long as your own threads finish
 *     using the breaker first, you are safe.
 *
 *   * If your application owns the breaker via @c std::shared_ptr and
 *     wants strict lifetime safety, the cleanest path is to follow that
 *     same discipline at every call site — only call breaker methods
 *     from a thread that holds a copy of the shared_ptr. Re-architecting
 *     this class to use @c std::enable_shared_from_this with a
 *     @c std::weak_ptr capture in the transition lambda would push that
 *     guarantee into the framework, at the cost of forcing every
 *     application to construct a @c shared_ptr<circuit_breaker>. The
 *     current design intentionally trades that mandatory shared_ptr for
 *     a simpler stack-scoped value-type usage, and surfaces the
 *     trade-off here.
 */
class circuit_breaker
{
public:
  /**
   * Type of the user-supplied state-transition listener.  Receives the
   * previous state, the new state, and a human-readable reason; the reason
   * is prefixed with "[node <id>] " by the breaker so a single shared
   * callback can identify which node tripped without extra plumbing.
   */
  using transition_callback = node_breaker::transition_callback;

  /**
   * Construct a breaker with default tunables.  See circuit_breaker_config
   * for the defaults; pass a configured instance to the other constructor
   * to override them.
   */
  circuit_breaker() = default;

  /**
   * Construct a breaker with explicit tunables.  The configuration is
   * captured by value and applied to every per-node breaker created
   * afterwards.
   *
   * @param config the tunables this breaker (and all of its per-node
   * children) will use.
   */
  explicit circuit_breaker(circuit_breaker_config config);

  /**
   * Read-only access to the configuration this breaker was constructed
   * with.  Useful for callers that need to coordinate timing with the
   * breaker (e.g. a probe loop that sleeps for the configured cool-off).
   */
  [[nodiscard]] auto config() const -> const circuit_breaker_config&
  {
    return config_;
  }

  /**
   * Install or replace the listener invoked on every state transition of
   * any per-node breaker owned by this registry.  Updates take effect
   * immediately, including for breakers that were created before this
   * call.  Pass an empty std::function to disable notifications.
   *
   * @param cb the listener; safe to capture references to long-lived
   * objects but not to anything that may outlive this circuit_breaker.
   */
  auto set_transition_callback(transition_callback cb) -> void;

  /**
   * Admission decision: ask the breaker whether a call to @p nid should
   * be attempted right now.  Cheap and lock-free on the hot path.  An
   * empty node_id is treated as "always allowed" (the breaker has no
   * opinion when it can't identify the target node).
   *
   * Side-effects: the very first call observed after the OPEN cool-off
   * elapses promotes the per-node breaker to half_open.  Rejected calls
   * increment the per-node rejected counter.
   *
   * @param nid the node the call would target.
   * @return true if the call may proceed; false if the breaker
   * short-circuited it.
   */
  [[nodiscard]] auto allow(const couchbase::node_id& nid) -> bool;

  /**
   * Record a successful call against @p nid.  Should be paired with a
   * preceding allow() that returned true.  An empty node_id is silently
   * ignored.
   *
   * @param nid the node the call hit.
   * @param duration how long the call took, end-to-end; used to feed the
   * slow-call rate.
   */
  auto record_success(const couchbase::node_id& nid, std::chrono::milliseconds duration) -> void;

  /**
   * Record a failed call against @p nid.  "Failure" here means an error
   * the caller has decided counts toward the breaker (e.g. timeouts,
   * service unavailable) — *not* user errors like document_not_found.
   * Pairs with a preceding allow() that returned true.  An empty
   * node_id is silently ignored.
   *
   * @param nid the node the call hit.
   * @param duration how long the call took before failing.
   */
  auto record_failure(const couchbase::node_id& nid, std::chrono::milliseconds duration) -> void;

  /**
   * Read the current circuit state for @p nid.  If no per-node breaker
   * exists yet for the given id (no traffic has touched it), closed is
   * returned — that is the default for any unobserved node.
   */
  [[nodiscard]] auto state_of(const couchbase::node_id& nid) const -> circuit_state;

  /**
   * Snapshot the current metrics for @p nid.  Returns std::nullopt if
   * the breaker has never seen this node; otherwise returns a copy of
   * the counters and the current state at the moment of sampling.
   */
  [[nodiscard]] auto metrics_of(const couchbase::node_id& nid) const
    -> std::optional<circuit_breaker_metrics>;

  /**
   * Snapshot metrics for every node the breaker is currently tracking.
   * Useful for periodic dashboards and logging — pair each entry with
   * its node id to render a per-node view.
   *
   * @return one (node_id, metrics) pair per known node.
   */
  [[nodiscard]] auto all_metrics() const
    -> std::vector<std::pair<couchbase::node_id, circuit_breaker_metrics>>;

  /**
   * Operator override: force the breaker for @p nid into forced_open.
   * All future calls to that node short-circuit and the breaker will not
   * auto-recover until reset().  Use during incident response or
   * planned maintenance.
   */
  auto force_open(const couchbase::node_id& nid) -> void;

  /**
   * Operator override: disable the breaker for @p nid.  All future calls
   * are admitted and outcomes are *not* recorded — effectively turning
   * the breaker off for that node.  Reverse with reset().
   */
  auto disable(const couchbase::node_id& nid) -> void;

  /**
   * Clear all state for @p nid: counters, half-open bookkeeping, and the
   * rejected-call lifetime counter all return to zero, and the state
   * machine returns to closed.  This is also the only path back to
   * closed from forced_open or disabled.  No-op if @p nid is unknown.
   */
  auto reset(const couchbase::node_id& nid) -> void;

  /**
   * Erase the per-node breaker for @p nid from the registry entirely.
   * Unlike reset(), this is the right call when the underlying cluster
   * node has *gone away* — its tracking entry is no longer reachable
   * from the topology and should not consume memory waiting for traffic
   * that will never arrive.  No-op if @p nid is unknown.
   *
   * @par Race window with concurrent traffic
   *
   * forget() takes the registry's unique_lock and erases the entry under
   * it, but the per-node breaker is held by @c std::shared_ptr — any
   * caller that has already acquired a reference to the breaker through
   * record_success(), record_failure(), or all_metrics() will keep the
   * breaker alive even after forget() returns. The shared_ptr-keyed
   * registry guarantees there is no use-after-free, but two other
   * race-window behaviours are worth knowing about:
   *
   *   * A concurrent record_success(nid, ...) call that arrives between
   *     forget() and the operator's stop-the-traffic decision will call
   *     get_or_create() and resurrect a fresh entry for the same node id.
   *     This is by design — record() is the cheap hot path and cannot
   *     consult an "are we forgetting this node?" flag without giving
   *     back the lock-free property.
   *
   *   * Transition listeners may still observe events from the
   *     forgotten-but-still-shared old breaker while another thread
   *     holds a reference to it. The events are correct (the breaker
   *     state really did change) but reference a node id the registry
   *     no longer tracks.
   *
   * Practical guidance: call forget() only *after* you have stopped
   * issuing traffic to that node. The SDK's topology removal is the
   * natural trigger — a topology sweep that diffs the live set against
   * the registry's keys is described below.
   *
   * @par Topology-sweep pattern
   *
   * Pair with couchbase::collection::node_ids() in a periodic sweep:
   *
   * @code
   * // absent_streak persists across sweeps (e.g. a member of the sweep
   * // task). A node must be missing from two consecutive snapshots before
   * // its tracker is retired — see the @note below.
   * auto [err, live] = collection.node_ids().get();
   * if (!err) {
   *   std::unordered_set<couchbase::node_id> live_set{ live.begin(), live.end() };
   *   for (const auto& [tracked, metrics] : breaker.all_metrics()) {
   *     (void)metrics; // we only care about the keys here
   *     if (live_set.find(tracked) == live_set.end()) {
   *       if (++absent_streak[tracked] >= 2) {
   *         breaker.forget(tracked);
   *         absent_streak.erase(tracked);
   *       }
   *     } else {
   *       absent_streak.erase(tracked); // reappeared — reset its streak
   *     }
   *   }
   * }
   * @endcode
   *
   * @note couchbase::collection::node_ids() returns a point-in-time
   * topology snapshot: during a rebalance a node can transiently vanish
   * from one snapshot and reappear in the next. Requiring two consecutive
   * misses before calling forget() (as above) avoids discarding a live
   * node's accumulated breaker state over a transient blip. See the @note
   * on node_ids() for the underlying guarantee.
   *
   * @return true if an entry was removed, false if the node was unknown.
   */
  auto forget(const couchbase::node_id& nid) -> bool;

  /**
   * Synchronous execute helper that wraps an SDK call with breaker
   * admission control, latency timing, and outcome reporting.  Use this
   * when you want a one-liner that does the right thing; if you need
   * finer control over the flow (e.g. retries, fan-out, custom timing)
   * call allow() / record_success() / record_failure() directly.
   *
   * Behaviour:
   *  -# Asks the breaker whether @p target may be called via allow().
   *  -# If admission is denied, returns immediately with
   *     example::cb::breaker_errc::call_short_circuited and a value-initialized
   *     placeholder for the response — the SDK is never invoked.  The
   *     dedicated category lets callers distinguish a breaker rejection
   *     from a real SDK couchbase::errc::common::request_canceled, which
   *     remains a valid breaker-trippable failure signal.
   *  -# Otherwise invokes @p op, blocks on the returned future, measures
   *     wall-clock duration, and records success or failure.
   *  -# The recorded outcome is attributed to the node_id reported by
   *     the SDK (err.node_id() or resp.node_id()).  This may differ from
   *     @p target if the SDK retried internally and ultimately landed on
   *     a different node; if the SDK reported no node, @p target is used
   *     as a fallback.
   *
   * @tparam Operation a nullary callable returning a future-like object
   * whose `get()` yields `std::pair<couchbase::error, R>`.  Typically a
   * lambda that initiates an SDK request, e.g.
   * `[&]{ return collection.upsert(key, doc, {}); }`.
   * @tparam Classifier a callable invocable as `bool(const
   * couchbase::error&)` that decides whether an error counts as a
   * breaker-trippable failure.  A typical implementation maps
   * timeouts, temporary_failure, and service_not_available to true and
   * everything else (especially application errors like
   * document_not_found) to false — the demo provides
   * example::demo::is_breaker_failure as a starting point.
   *
   * @param target the node the operation is meant for; used both as the
   * admission key and as the fallback attribution when the SDK does not
   * report a node id on the response/error.
   * @param op the SDK operation to invoke; called at most once per
   * execute() invocation, only after admission is granted.
   * @param is_breaker_failure the error classifier described above.
   *
   * @return the SDK's `(error, response)` pair on a real call, or a
   * synthesized `(breaker_errc::call_short_circuited, R{})` pair if the breaker
   * short-circuited the call.
   *
   * @note R must be default-constructible; this is enforced by a
   * static_assert.  Most SDK response types satisfy this; if yours
   * doesn't, perform admission and recording yourself or have your
   * Operation wrap the response in std::optional.
   */
  template<typename Operation, typename Classifier>
  auto execute(const couchbase::node_id& target, Operation op, Classifier is_breaker_failure)
    // The return type names a few intermediate type aliases — pulled out
    // here for readability and so the type chain doesn't synthetically
    // evaluate @c op() in unevaluated context for the SFINAE expression.
    // @c future_t is what @p op returns; @c pair_t is what calling
    // @c .get() on that future yields; @c result_type is the second
    // element of that pair, which is what the breaker has to synthesize
    // when it short-circuits the call.
    -> std::pair<couchbase::error,
                 std::decay_t<typename decltype(std::declval<std::invoke_result_t<Operation&>&>()
                                                  .get())::second_type>>
  {
    using future_t = std::invoke_result_t<Operation&>;
    using pair_t = decltype(std::declval<future_t&>().get());
    using result_type = std::decay_t<typename pair_t::second_type>;
    static_assert(std::is_default_constructible_v<result_type>,
                  "circuit_breaker::execute() requires a default-constructible result type so it "
                  "can synthesize a placeholder when the breaker rejects the call");
    if (!allow(target)) {
      auto state = state_of(target);
      auto message = std::string{ "circuit breaker rejected call to node " } + target.id() +
                     " (state=" + to_string(state) + ")";
      return { couchbase::error{ make_error_code(breaker_errc::call_short_circuited),
                                 std::move(message),
                                 couchbase::error_context{},
                                 target },
               result_type{} };
    }
    auto start = std::chrono::steady_clock::now();
    // Note: @p op is invoked exactly once. The synchronous .get() is a
    // deliberate teaching choice — it makes the demo readable but blocks
    // the caller's thread. In production code prefer composing the
    // breaker into your existing async flow via allow() +
    // record_success() / record_failure() directly.
    //
    // allow() above already consumed a half-open probe permit when the
    // breaker is HALF_OPEN. Every admitted call must therefore reach
    // exactly one record_*() so that permit is accounted for. op() or its
    // .get() can throw (e.g. std::future_error, or an exception from a
    // user-supplied operation), which would unwind past the recording
    // below and leak the permit — and a HALF_OPEN breaker that loses all
    // its permits has no probe outcome left to act on, so it would wedge
    // shut forever. Guard the call so a thrown operation is recorded as a
    // breaker failure (re-opening the breaker and re-arming the cool-off)
    // before the exception propagates.
    try {
      auto [err, resp] = op().get();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
      auto effective_nid = err ? err.node_id() : resp.node_id();
      if (!effective_nid) {
        effective_nid = target;
      }
      if (err && is_breaker_failure(err)) {
        record_failure(effective_nid, duration);
      } else {
        record_success(effective_nid, duration);
      }
      return { std::move(err), std::move(resp) };
    } catch (...) {
      // No SDK node id is available on the exceptional path, so attribute
      // the failure to @p target — the node the call was admitted against.
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
      record_failure(target, duration);
      throw;
    }
  }

private:
  [[nodiscard]] auto find(const couchbase::node_id& nid) const -> std::shared_ptr<node_breaker>;
  [[nodiscard]] auto get_or_create(const couchbase::node_id& nid) -> std::shared_ptr<node_breaker>;
  auto dispatch_transition(const std::string& node_id_str,
                           circuit_state from,
                           circuit_state to,
                           const std::string& why) const -> void;

  mutable std::shared_mutex mutex_;
  circuit_breaker_config config_{};
  std::unordered_map<couchbase::node_id, std::shared_ptr<node_breaker>> nodes_{};
  transition_callback on_transition_{};
};

} // namespace example::cb

namespace std
{
// Fully qualify std::true_type — we are inside namespace std but it's good
// hygiene to qualify so the specialisation reads correctly out of context.
template<>
struct is_error_code_enum<example::cb::breaker_errc> : std::true_type {
};
} // namespace std
