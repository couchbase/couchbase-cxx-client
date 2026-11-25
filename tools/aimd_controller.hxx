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

#pragma once

#include "overload_detector.hxx"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>

namespace cbc
{
enum class throttle_state : std::uint8_t {
  unlimited,   // no ceiling and no overload since the last recovery
  at_ceiling,  // the configured ceiling is in effect
  backing_off, // the rate was cut and has not yet recovered
};

struct aimd_decision {
  double rate{};          // the rate to apply until the next window
  throttle_state state{}; // the state after this window
  bool cut{};             // this window halved the rate
  bool recovered{};       // this window ended backing off
  // The rate before this window. On a cut, the rate the cut started from: the peak admitted rate
  // when the rate was unlimited.
  double previous_rate{};
};

inline constexpr std::uint64_t aimd_minimum_window_completions{ 100 };
inline constexpr double aimd_overload_retry_share{ 0.05 };
inline constexpr double aimd_overload_slow_share{ 0.20 };
inline constexpr double aimd_decrease_factor{ 0.5 };
inline constexpr double aimd_slow_start_factor{ 2 };
inline constexpr double aimd_uncapped_recovery_share{ 0.9 };

/**
 * Multiplicative-decrease control of an admission rate with slow-start recovery, one decision per
 * window. Pure: no clock, no I/O, no locking. Call it from one thread.
 *
 * - Overloaded (temporary-failure retries per completion above aimd_overload_retry_share, or slow
 *   completions per completion above aimd_overload_slow_share): the rate halves, never below the
 *   floor. An unlimited rate halves from the peak admitted rate of the judged windows, so a
 *   stalled tick cannot lower it. A cut that leaves the rate unchanged, at the floor, is not a cut.
 * - The rate before the first cut of an overload episode is the recovery target; later cuts keep
 *   it and every recovery clears it. When that rate is not above the cut rate, as after a stalled
 *   tick cut to the floor, the target is unknown and held as unlimited.
 * - The window after a cut cannot cut again: retries in flight at the cut land in it. If that
 *   window is overloaded it changes nothing, so overload never grows the rate or recovers it.
 * - Healthy while backing off: the rate doubles. Reaching the ceiling recovers at the ceiling.
 *   With no ceiling, reaching the target, or an admitted rate below aimd_uncapped_recovery_share
 *   of the bucket rate, returns the rate to unlimited.
 * - Windows with fewer than aimd_minimum_window_completions completions accumulate until they
 *   reach it, so a low rate is judged less often rather than never.
 */
class aimd_controller
{
public:
  /**
   * Starts at `ceiling`, or unlimited without one. `floor` is the lowest rate a cut can reach.
   *
   * @throws std::invalid_argument unless `floor > 0` and, when given, `ceiling >= floor`.
   */
  aimd_controller(std::optional<double> ceiling, double floor)
    : ceiling_{ ceiling }
    , floor_{ floor }
    , rate_{ ceiling.value_or(std::numeric_limits<double>::infinity()) }
    , state_{ ceiling ? throttle_state::at_ceiling : throttle_state::unlimited }
  {
    if (!(floor > 0) || (ceiling && !(*ceiling >= floor))) {
      throw std::invalid_argument("aimd_controller: requires floor > 0 and ceiling >= floor");
    }
  }

  /// `admitted_per_second` is the rate the bucket granted during `window`.
  auto on_window(const overload_window& window, double admitted_per_second) -> aimd_decision
  {
    const auto before = rate_;
    pending_.completions += window.completions;
    pending_.slow += window.slow;
    pending_.temporary_failure_retries += window.temporary_failure_retries;
    pending_peak_admitted_ = std::max(pending_peak_admitted_, admitted_per_second);
    if (pending_.completions < aimd_minimum_window_completions) {
      return { rate_, state_, false, false, before };
    }

    const auto completions = static_cast<double>(pending_.completions);
    const bool overloaded =
      static_cast<double>(pending_.temporary_failure_retries) / completions >
        aimd_overload_retry_share ||
      static_cast<double>(pending_.slow) / completions > aimd_overload_slow_share;
    const auto admitted = pending_peak_admitted_;
    pending_ = {};
    pending_peak_admitted_ = 0;

    if (overloaded && !hold_off_) {
      const auto base = std::isinf(rate_) ? admitted : rate_;
      const auto next = std::max(floor_, base * aimd_decrease_factor);
      if (next >= rate_) { // already at the floor
        return { rate_, state_, false, false, before };
      }
      if (!target_) { // the first cut of an episode
        target_ = base > next ? base : std::numeric_limits<double>::infinity();
      }
      rate_ = next;
      hold_off_ = true;
      state_ = throttle_state::backing_off;
      return { rate_, state_, true, false, base };
    }

    if (overloaded) { // in hold-off
      hold_off_ = false;
      return { rate_, state_, false, false, before };
    }

    hold_off_ = false;
    if (state_ != throttle_state::backing_off) {
      return { rate_, state_, false, false, before };
    }
    if (!ceiling_ && admitted < aimd_uncapped_recovery_share * rate_) {
      return recover(std::numeric_limits<double>::infinity(), throttle_state::unlimited, before);
    }
    rate_ *= aimd_slow_start_factor;
    if (ceiling_ && rate_ >= *ceiling_) {
      return recover(*ceiling_, throttle_state::at_ceiling, before);
    }
    assert(target_); // backing_off implies a target
    if (!ceiling_ && rate_ >= *target_) {
      return recover(std::numeric_limits<double>::infinity(), throttle_state::unlimited, before);
    }
    return { rate_, state_, false, false, before };
  }

  [[nodiscard]] auto rate() const -> double
  {
    return rate_;
  }

  /// Returns to the constructor's state: the ceiling or unlimited, with no pending window, target
  /// or hold-off.
  void restart()
  {
    *this = aimd_controller{ ceiling_, floor_ };
  }

private:
  auto recover(double rate, throttle_state state, double before) -> aimd_decision
  {
    rate_ = rate;
    state_ = state;
    target_.reset();
    return { rate_, state_, false, true, before };
  }

  std::optional<double> ceiling_;
  double floor_;
  double rate_;
  throttle_state state_;
  std::optional<double> target_{}; // the rate before the first cut of the current episode
  bool hold_off_{ false };
  overload_window pending_{};
  double pending_peak_admitted_{ 0 };
};
} // namespace cbc
