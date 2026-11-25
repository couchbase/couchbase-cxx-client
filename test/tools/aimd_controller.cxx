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

#include "test/framework/test_framework.hxx"

#include "tools/aimd_controller.hxx"

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>

namespace couchbase::test
{
namespace
{
constexpr double ceiling{ 1000 };
constexpr double floor_rate{ 10 };
constexpr cbc::overload_window healthy{ 100, 0, 0 };
constexpr cbc::overload_window overloaded{ 100, 0, 6 };

void
cuts_in_half_on_retry_share([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  const auto decision = controller.on_window(overloaded, 1000);
  assert_eq(decision.rate, 500.0, "a 6% retry share halves the rate");
  assert_true(decision.cut, "the halving is reported as a cut");
  assert_eq(decision.previous_rate, 1000.0, "the cut reports the rate it started from");
  assert_true(decision.state == cbc::throttle_state::backing_off, "a cut starts backing off");
}

void
exact_threshold_is_healthy([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  const auto decision = controller.on_window({ 100, 0, 5 }, 1000);
  assert_false(decision.cut, "a retry share of exactly 5% is not overload");
  assert_eq(decision.rate, 1000.0, "a healthy window keeps the ceiling");
}

void
cuts_on_slow_share([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  assert_false(controller.on_window({ 100, 20, 0 }, 1000).cut,
               "a slow share of exactly 20% is not overload");
  assert_true(controller.on_window({ 100, 21, 0 }, 1000).cut, "a 21% slow share is overload");
}

void
never_below_floor([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  for (int i = 0; i < 20 && controller.rate() > floor_rate; ++i) {
    const auto cut = controller.on_window(overloaded, controller.rate());
    assert_true(cut.cut, "an overloaded window outside hold-off cuts above the floor");
    assert_true(cut.rate >= floor_rate, "a cut never goes below the floor");
    assert_false(controller.on_window(overloaded, controller.rate()).cut,
                 "the hold-off window does not cut");
  }
  assert_eq(controller.rate(), floor_rate, "repeated cuts settle at the floor");
  const auto at_floor = controller.on_window(overloaded, floor_rate);
  assert_false(at_floor.cut, "an overloaded window at the floor is not a cut");
  assert_eq(at_floor.rate, floor_rate, "the rate stays at the floor");
  assert_false(controller.on_window(overloaded, floor_rate).cut,
               "the next overloaded window at the floor is not a cut either");
}

void
slow_start_doubles_to_the_target([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  assert_eq(controller.on_window(overloaded, 1000).rate, 500.0, "the cut halves the ceiling");
  const auto decision = controller.on_window(healthy, controller.rate());
  assert_eq(decision.rate, ceiling, "one healthy window doubles back to the target");
  assert_true(decision.state == cbc::throttle_state::at_ceiling, "the target is the ceiling");
  assert_true(decision.recovered, "reaching the ceiling is a recovery");
  const auto after = controller.on_window(healthy, controller.rate());
  assert_false(after.recovered, "recovery is reported once");
  assert_eq(after.rate, ceiling, "the rate stays at the ceiling");
}

void
recovery_from_the_floor_doubles([[maybe_unused]] context& ctx)
{
  constexpr double high_ceiling{ 102400 };
  constexpr double high_floor{ 100 };
  cbc::aimd_controller controller{ high_ceiling, high_floor };
  while (controller.rate() > high_floor) {
    assert_true(controller.on_window(overloaded, controller.rate()).cut, "a cut above the floor");
    assert_false(controller.on_window(overloaded, controller.rate()).cut, "the hold-off window");
  }
  double expected{ high_floor };
  for (int window = 1; window <= 10; ++window) {
    expected *= 2;
    const auto decision = controller.on_window(healthy, controller.rate());
    assert_eq(decision.rate, expected, "slow start doubles toward the episode target");
    assert_eq(decision.recovered, window == 10, "only reaching the ceiling is a recovery");
    assert_eq(decision.state == cbc::throttle_state::at_ceiling,
              window == 10,
              "the tenth window reaches the ceiling");
  }
}

void
a_second_cut_keeps_the_episode_target([[maybe_unused]] context& ctx)
{
  constexpr double high_ceiling{ 10000 };
  cbc::aimd_controller controller{ high_ceiling, floor_rate };
  assert_eq(controller.on_window(overloaded, high_ceiling).rate, 5000.0, "the first cut");
  const auto hold_off = controller.on_window(overloaded, 5000);
  assert_false(hold_off.cut, "the hold-off window does not cut");
  assert_eq(hold_off.rate, 5000.0, "the overloaded hold-off window changes nothing");
  const auto second = controller.on_window(overloaded, 5000);
  assert_true(second.cut, "an overloaded window after the hold-off cuts again");
  assert_eq(second.rate, 2500.0, "the second cut halves the rate");
  const auto first_healthy = controller.on_window(healthy, controller.rate());
  assert_eq(first_healthy.rate, 5000.0, "slow start doubles");
  assert_false(first_healthy.recovered, "5000 is below the episode target");
  const auto second_healthy = controller.on_window(healthy, controller.rate());
  assert_eq(second_healthy.rate, high_ceiling, "slow start continues to the episode target");
  assert_true(second_healthy.state == cbc::throttle_state::at_ceiling, "the target is the ceiling");
  assert_true(second_healthy.recovered, "reaching the ceiling is a recovery");
}

void
uncapped_recovers_when_the_bucket_stops_binding([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  assert_eq(
    controller.on_window(overloaded, 8000).rate, 4000.0, "the cut halves the admitted rate");
  assert_false(controller.on_window(overloaded, 4000).cut, "the hold-off window does not cut");
  assert_eq(controller.on_window(overloaded, 4000).rate, 2000.0, "a second cut");
  assert_false(controller.on_window(overloaded, 2000).cut, "the hold-off window does not cut");
  assert_eq(controller.on_window(overloaded, 2000).rate, 1000.0, "a third cut");
  const auto decision = controller.on_window(healthy, 800);
  assert_true(decision.recovered, "admitted below 90% of the rate recovers without doubling");
  assert_true(decision.state == cbc::throttle_state::unlimited, "recovery returns to unlimited");
  assert_true(std::isinf(decision.rate), "an unlimited state has an unlimited rate");

  // The recovery cleared the episode target: the next episode's target is its own pre-cut rate.
  assert_eq(controller.on_window(overloaded, 2000).rate, 1000.0, "a new episode starts");
  const auto next_episode = controller.on_window(healthy, 1000);
  assert_true(next_episode.recovered, "doubling to the new episode's target recovers");
}

void
uncapped_admitted_at_ninety_percent_still_binds([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  for (const double admitted : { 8000.0, 4000.0, 2000.0 }) {
    assert_true(controller.on_window(overloaded, admitted).cut, "a cut in one episode");
    assert_false(controller.on_window(overloaded, admitted / 2).cut, "the hold-off window");
  }
  const auto binding = controller.on_window(healthy, 900);
  assert_false(binding.recovered, "an admitted rate of exactly 90% still binds");
  assert_eq(binding.rate, 2000.0, "a binding bucket doubles toward the episode target");
}

void
cut_base_is_the_peak_admitted_of_the_span([[maybe_unused]] context& ctx)
{
  const cbc::overload_window short_window{ 60, 0, 6 };
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  assert_true(std::isinf(controller.rate()), "with no ceiling the rate starts unlimited");
  assert_false(controller.on_window(short_window, 1000).cut, "60 completions are not judged");
  const auto decision = controller.on_window(short_window, 3000);
  assert_true(decision.cut, "120 completions with 12 retries are judged together");
  assert_true(decision.state == cbc::throttle_state::backing_off, "a cut starts backing off");
  assert_eq(decision.rate, 1500.0, "the cut halves the peak admitted rate of both ticks");
  assert_eq(decision.previous_rate, 3000.0, "an unlimited rate reports the peak base it cut from");

  cbc::aimd_controller falling{ std::nullopt, floor_rate };
  assert_false(falling.on_window(short_window, 3000).cut, "60 completions are not judged");
  assert_eq(falling.on_window(short_window, 1000).rate,
            1500.0,
            "a lower last tick does not lower the cut base");
}

void
a_stall_does_not_skip_slow_start([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  const auto cut = controller.on_window(overloaded, 0);
  assert_true(cut.cut, "an unlimited rate is cut");
  assert_eq(cut.rate, floor_rate, "a stalled tick cuts to the floor");
  const auto hold_off = controller.on_window(overloaded, 0);
  assert_false(hold_off.cut, "the hold-off window does not cut");
  assert_eq(hold_off.rate, floor_rate, "the overloaded hold-off window changes nothing");
  for (const double expected : { 20.0, 40.0, 80.0 }) {
    const auto decision = controller.on_window(healthy, controller.rate());
    assert_eq(decision.rate, expected, "with an unknown target slow start keeps doubling");
    assert_true(decision.state == cbc::throttle_state::backing_off, "still backing off");
    assert_false(decision.recovered, "a binding bucket has not recovered");
  }
  const auto decision = controller.on_window(healthy, 70);
  assert_true(decision.recovered, "admitted below 90% of the rate recovers");
  assert_true(decision.state == cbc::throttle_state::unlimited, "recovery returns to unlimited");
}

void
uncapped_slow_start_recovers_at_the_target([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  assert_eq(
    controller.on_window(overloaded, 4000).rate, 2000.0, "the cut halves the admitted rate");
  const auto decision = controller.on_window(healthy, 2000);
  assert_true(decision.recovered, "doubling back to the admitted rate before the cut recovers");
  assert_true(decision.state == cbc::throttle_state::unlimited, "recovery returns to unlimited");
  assert_true(std::isinf(decision.rate), "an unlimited state has an unlimited rate");
}

void
overload_during_hold_off_changes_nothing([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ std::nullopt, floor_rate };
  assert_eq(
    controller.on_window(overloaded, 4000).rate, 2000.0, "the cut halves the admitted rate");
  const auto decision = controller.on_window(overloaded, 1000);
  assert_eq(decision.rate, 2000.0, "overload during hold-off leaves the rate unchanged");
  assert_true(decision.state == cbc::throttle_state::backing_off,
              "overload during hold-off keeps backing off");
  assert_false(decision.recovered, "a depressed admitted rate under overload is not a recovery");
  assert_false(decision.cut, "the hold-off window does not cut");
}

void
rejects_invalid_bounds([[maybe_unused]] context& ctx)
{
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  for (const double floor : { 0.0, -1.0, nan }) {
    assert_throws<std::invalid_argument>(
      [&] {
        const cbc::aimd_controller rejected{ std::nullopt, floor };
      },
      "the floor must be greater than zero");
  }
  for (const double bad_ceiling : { 0.0, 5.0, nan }) {
    assert_throws<std::invalid_argument>(
      [&] {
        const cbc::aimd_controller rejected{ bad_ceiling, floor_rate };
      },
      "the ceiling must be at least the floor");
  }
  assert_no_throw(
    [] {
      const cbc::aimd_controller accepted{ floor_rate, floor_rate };
    },
    "a ceiling equal to the floor is valid");
}

void
a_window_is_judged_at_one_hundred_completions([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  assert_false(controller.on_window({ 99, 0, 99 }, 1000).cut, "99 completions are not judged");
  assert_true(controller.on_window({ 1, 0, 0 }, 1000).cut,
              "the 100th completion is judged with the 99 before it");
}

void
short_windows_accumulate_until_judged([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  const cbc::overload_window short_window{ 40, 0, 3 };
  assert_false(controller.on_window(short_window, 1000).cut, "40 completions are not judged");
  assert_false(controller.on_window(short_window, 1000).cut, "80 completions are not judged");
  assert_true(controller.on_window(short_window, 1000).cut,
              "120 completions with 9 retries are judged together");
}

void
restart_returns_to_the_ceiling([[maybe_unused]] context& ctx)
{
  cbc::aimd_controller controller{ ceiling, floor_rate };
  assert_true(controller.on_window(overloaded, 1000).cut, "the first window cuts");
  controller.restart();
  assert_eq(controller.rate(), ceiling, "a restart returns to the ceiling");
  // Without the restart this window is the hold-off window and cannot cut.
  assert_true(controller.on_window(overloaded, 1000).cut, "a restart clears the hold-off");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_aimd_controller",
    {
      { "cuts_in_half_on_retry_share", cuts_in_half_on_retry_share, {}, timeout::instant },
      { "exact_threshold_is_healthy", exact_threshold_is_healthy, {}, timeout::instant },
      { "cuts_on_slow_share", cuts_on_slow_share, {}, timeout::instant },
      { "never_below_floor", never_below_floor, {}, timeout::instant },
      { "slow_start_doubles_to_the_target",
        slow_start_doubles_to_the_target,
        {},
        timeout::instant },
      { "recovery_from_the_floor_doubles", recovery_from_the_floor_doubles, {}, timeout::instant },
      { "a_second_cut_keeps_the_episode_target",
        a_second_cut_keeps_the_episode_target,
        {},
        timeout::instant },
      { "uncapped_recovers_when_the_bucket_stops_binding",
        uncapped_recovers_when_the_bucket_stops_binding,
        {},
        timeout::instant },
      { "cut_base_is_the_peak_admitted_of_the_span",
        cut_base_is_the_peak_admitted_of_the_span,
        {},
        timeout::instant },
      { "a_stall_does_not_skip_slow_start",
        a_stall_does_not_skip_slow_start,
        {},
        timeout::instant },
      { "uncapped_admitted_at_ninety_percent_still_binds",
        uncapped_admitted_at_ninety_percent_still_binds,
        {},
        timeout::instant },
      { "uncapped_slow_start_recovers_at_the_target",
        uncapped_slow_start_recovers_at_the_target,
        {},
        timeout::instant },
      { "overload_during_hold_off_changes_nothing",
        overload_during_hold_off_changes_nothing,
        {},
        timeout::instant },
      { "rejects_invalid_bounds", rejects_invalid_bounds, {}, timeout::instant },
      { "a_window_is_judged_at_one_hundred_completions",
        a_window_is_judged_at_one_hundred_completions,
        {},
        timeout::instant },
      { "short_windows_accumulate_until_judged",
        short_windows_accumulate_until_judged,
        {},
        timeout::instant },
      { "restart_returns_to_the_ceiling", restart_returns_to_the_ceiling, {}, timeout::instant },
    },
  };
}
} // namespace couchbase::test
