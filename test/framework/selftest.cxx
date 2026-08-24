/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Self-test for the test framework. It exercises the runner's pass / fail / skip / timeout /
// env-gating / filter behaviour by driving run() with in-memory sub-suites and asserting on the
// returned run_result. Because it asserts on the runner's *return value* (rather than letting an
// inner failure propagate), every case here passes and this binary exits 0.

#include "test_runner.hxx"

#include <chrono>
#include <cstddef>
#include <ios>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

namespace couchbase::test
{
namespace
{
// Case bodies used to build the in-memory sub-suites the runner is tested against.
void
body_pass()
{
  assert_true(true);
}
void
body_fail()
{
  assert_true(false, "intentional failure");
}
void
body_skip()
{
  skip("intentional skip");
}
void
body_sleep()
{
  std::this_thread::sleep_for(std::chrono::milliseconds{ 200 });
}

// assert_throws asked for a base of the framework's own control-flow types. Both bodies exist to be
// run through the runner rather than called directly: what is under test is the outcome the runner
// reports, and a case body cannot observe its own verdict.
void
body_skip_inside_assert_throws()
{
  assert_throws<std::exception>([]() {
    skip("intentional skip from inside a callable");
  });
}
void
body_failed_assertion_inside_assert_throws()
{
  assert_throws<std::exception>([]() {
    assert_true(false, "intentional failure from inside a callable");
  });
}

// A sink for the runner's progress output so the self-test stays quiet.
auto
run_quiet(const test_suite& suite, const std::set<std::string>& filter, bool real_cluster)
  -> run_result
{
  std::ostringstream sink;
  return run(suite, filter, real_cluster, sink);
}

// Comparable but not formattable, which is what selects assert_eq's value-free branch. It has no
// operator<< either. That is not what makes it unformattable today -- fmt only consults operator<<
// when fmt/ostream.h is included, and nothing here includes it -- but leaving one out keeps the
// type unformattable if that support is ever switched on, rather than silently moving this case to
// the other branch.
struct opaque {
  int value;
};

inline auto
operator==(const opaque& lhs, const opaque& rhs) -> bool
{
  return lhs.value == rhs.value;
}

// The message an assertion produced, or empty if it did not fail. assert_throws cannot be used to
// observe a test_assertion_failure: naming that type as the expected one leaves its own handler
// unreachable, so the framework's own tests catch it directly.
//
// Only that type is caught, and the omission is the contract rather than an oversight. Anything
// else
// -- a skip(), or an exception from the code under test -- propagates to the runner, which is what
// decides the case. A catch-all here would turn both into an empty string, and a caller reading
// empty as "the assertion did not fail" would report passed for a case that skipped or threw.
// message_of_passes_everything_else_to_the_runner holds that.
template<typename Fn>
auto
message_of(Fn&& fn) -> std::string
{
  try {
    std::forward<Fn>(fn)();
  } catch (const test_assertion_failure& e) {
    return e.what();
  }
  return {};
}

void
body_skip_inside_message_of()
{
  static_cast<void>(message_of([]() {
    skip("intentional skip from inside a callable");
  }));
  // Reached only if message_of swallowed the skip. Under the contract above this line is dead.
  assert_true(false, "the skip did not reach the runner");
}
void
body_throw_inside_message_of()
{
  static_cast<void>(message_of([]() {
    throw std::runtime_error("not an assertion failure");
  }));
  assert_true(false, "the exception did not reach the runner");
}

// ── the self-test cases ──────────────────────────────────────────────────────

void
should_run_gating_is_correct()
{
  assert_true(should_run(test_env::agnostic, false), "agnostic runs in mock mode");
  assert_true(should_run(test_env::agnostic, true), "agnostic runs in real mode");
  assert_true(should_run(test_env::any_server, false), "any_server runs in mock mode");
  assert_true(should_run(test_env::any_server, true), "any_server runs in real mode");
  assert_true(should_run(test_env::mock_only, false), "mock_only runs in mock mode");
  assert_false(should_run(test_env::mock_only, true), "mock_only skipped in real mode");
  assert_false(should_run(test_env::cluster_only, false), "cluster_only skipped in mock mode");
  assert_true(should_run(test_env::cluster_only, true), "cluster_only runs in real mode");
}

void
runner_reports_a_passing_case()
{
  const test_suite s{ "inner", { { "p", body_pass } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.passed, std::size_t{ 1 }, "one pass counted");
  assert_eq(r.failed, std::size_t{ 0 }, "no failures");
  assert_eq(exit_code(r), 0, "exit code 0 on success");
}

void
runner_detects_a_failing_case()
{
  const test_suite s{ "inner", { { "f", body_fail } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.failed, std::size_t{ 1 }, "one failure counted");
  assert_eq(exit_code(r), 1, "exit code 1 on failure");
}

void
runner_reports_a_skipped_case()
{
  const test_suite s{ "inner", { { "s", body_skip } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.skipped, std::size_t{ 1 }, "one skip counted");
  assert_eq(r.failed, std::size_t{ 0 }, "skip is not a failure");
  // The other half of the contract couchbase_add_test() wires up as SKIP_RETURN_CODE: a binary that
  // ran nothing but skips must report ctest Skipped, not success.
  assert_eq(exit_code(r), 77, "an all-skipped binary reports 77");
}

void
runner_detects_a_timeout()
{
  // 200ms body against a 20ms budget must be reported as a failure.
  const test_suite s{ "inner", { { "t", body_sleep, std::chrono::milliseconds{ 20 } } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.failed, std::size_t{ 1 }, "timeout counted as a failure");
}

void
env_gating_skips_cluster_only_without_a_cluster()
{
  const test_suite s{ "inner", { { "c", body_pass, default_timeout, test_env::cluster_only } } };
  const auto without = run_quiet(s, {}, /*real_cluster=*/false);
  assert_eq(without.skipped, std::size_t{ 1 }, "cluster_only skipped without a cluster");
  assert_eq(without.passed, std::size_t{ 0 }, "cluster_only did not run without a cluster");

  const auto with = run_quiet(s, {}, /*real_cluster=*/true);
  assert_eq(with.passed, std::size_t{ 1 }, "cluster_only runs with a cluster");
}

void
case_output_is_flushed_as_it_is_written()
{
  // ctest gives these binaries a pipe for stdout, where the stream is block-buffered, and a case
  // that aborts the process discards whatever is still buffered -- so the log names neither the
  // aborting case nor any that passed before it. The cases that crash are the ones whose output is
  // most needed, so the runner must not leave its stream block-buffered.
  std::ostringstream sink;
  const test_suite s{ "inner", { { "p", body_pass } } };
  static_cast<void>(run(s, {}, false, sink));
  assert_true((sink.flags() & std::ios::unitbuf) != 0,
              "the runner flushes its stream after every write");
}

void
filter_selects_cases_by_name()
{
  const test_suite s{ "inner", { { "a", body_pass }, { "b", body_pass } } };
  const auto r = run_quiet(s, { "a" }, false);
  assert_eq(r.passed, std::size_t{ 1 }, "only the filtered case runs");
}

void
a_failing_case_does_not_suppress_later_cases()
{
  // One ctest test is registered per executable, so abandoning the suite on the first failure
  // would hide every later regression in the same binary until the first one is fixed.
  const test_suite s{ "inner", { { "f", body_fail }, { "p", body_pass }, { "s", body_skip } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.failed, std::size_t{ 1 }, "the failure is counted");
  assert_eq(r.passed, std::size_t{ 1 }, "a later case still runs after a failure");
  assert_eq(r.skipped, std::size_t{ 1 }, "a later skip is still reported after a failure");
}

void
slow_cases_run_after_a_failure()
{
  test_suite s{ "inner", { { "f", body_fail } } };
  s.slow_test_cases = { { "p", body_pass } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.passed, std::size_t{ 1 }, "slow cases are not abandoned by an earlier failure");
}

void
a_filter_name_matching_nothing_fails()
{
  const test_suite s{ "inner", { { "a", body_pass } } };
  const auto r = run_quiet(s, { "typo" }, false);
  assert_eq(r.passed, std::size_t{ 0 }, "nothing ran");
  assert_eq(r.failed, std::size_t{ 1 }, "an unmatched filter name is a failure");
  assert_eq(exit_code(r), 1, "exit code 1, not a silent pass");
}

void
a_suite_that_runs_nothing_does_not_pass()
{
  const test_suite empty{ "inner", {} };
  const auto r = run_quiet(empty, {}, false);
  assert_eq(r.passed, std::size_t{ 0 }, "nothing ran");
  assert_eq(r.skipped, std::size_t{ 0 }, "nothing skipped");
  assert_eq(exit_code(r), 1, "an empty suite must not report success");
}

void
a_timeout_is_reported_as_such()
{
  // main() needs to distinguish a timeout from an ordinary failure: only a timeout leaves a
  // detached worker thread, which dictates how the process exits.
  const test_suite s{ "inner", { { "t", body_sleep, std::chrono::milliseconds{ 20 } } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.timed_out, std::size_t{ 1 }, "the timeout is flagged separately");
  assert_eq(r.failed, std::size_t{ 1 }, "and still counted as a failure");
}

void
case_names_covers_slow_cases_and_ignores_the_environment()
{
  const test_suite suite{
    "listing",
    { { "regular", body_pass, timeout::instant, test_env::cluster_only } },
    { { "deliberately_slow", body_pass, timeout::slow } },
  };

  const auto names = case_names(suite);
  assert_eq(names.size(), std::size_t{ 2 }, "both lists are enumerated");
  assert_eq(names[0], std::string{ "regular" }, "in registration order");
  assert_eq(names[1], std::string{ "deliberately_slow" }, "slow cases come last");
}

void
timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number()
{
  assert_eq(timeout_multiplier(std::nullopt), 1.0, "unset means unscaled");
  assert_eq(timeout_multiplier(std::optional<std::string>{ "10" }), 10.0, "an integer is read");
  assert_eq(timeout_multiplier(std::optional<std::string>{ "2.5" }), 2.5, "so is a fraction");

  // A trailing suffix must not be read as a bare number: silently running with a budget nobody
  // asked for is how a leg goes green on timing rather than on behaviour.
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "10x" });
    },
    "a trailing suffix is rejected");
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "0" });
    },
    "zero would kill every case immediately");
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "-1" });
    },
    "a negative factor is rejected");
  // "inf" parses, and satisfies a bare positivity test. Accepting it makes every scaled budget
  // undefined at the narrowing cast in scale_timeouts.
  assert_throws<std::invalid_argument>(
    []() {
      (void)timeout_multiplier(std::optional<std::string>{ "inf" });
    },
    "a non-finite factor is rejected");
}

void
scale_timeouts_multiplies_every_budget()
{
  test_suite suite{
    "budgets",
    { { "fast", body_pass, std::chrono::milliseconds{ 100 } } },
    { { "slow", body_pass, std::chrono::milliseconds{ 1'000 } } },
  };

  scale_timeouts(suite, 10.0);
  assert_eq(suite.test_cases[0].timeout.count(), 1'000, "the regular list is scaled");
  assert_eq(suite.slow_test_cases[0].timeout.count(), 10'000, "and so is the slow list");

  // Ceiling, not nearest and not truncation. Only a product whose fraction falls below .5 tells
  // the three apart: 100 x 1.004 is 101 under ceil and 100 under both floor and llround.
  test_suite fractional{ "fractional",
                         { { "case", body_pass, std::chrono::milliseconds{ 100 } } } };
  scale_timeouts(fractional, 1.004);
  assert_eq(fractional.test_cases[0].timeout.count(), 101, "a fraction below .5 still rounds up");

  // Rounding must not produce a zero budget, which would fail every case before it started.
  scale_timeouts(suite, 0.0001);
  assert_true(suite.test_cases[0].timeout.count() >= 1, "a budget never rounds down to nothing");
}

void
a_scaled_budget_lets_a_case_outlive_its_original_one()
{
  // body_sleep runs for 200ms; 50ms is not enough and 50ms x 10 is.
  test_suite suite{ "scaled", { { "sleeper", body_sleep, std::chrono::milliseconds{ 50 } } } };

  const auto unscaled = run_quiet(suite, {}, false);
  assert_eq(unscaled.timed_out, std::size_t{ 1 }, "the original budget kills the case");

  scale_timeouts(suite, 10.0);
  const auto scaled = run_quiet(suite, {}, false);
  assert_eq(scaled.timed_out, std::size_t{ 0 }, "the scaled budget does not");
  assert_eq(scaled.passed, std::size_t{ 1 }, "and the case reports its own outcome");
}

} // namespace

void
a_skip_inside_assert_throws_skips_its_case()
{
  // std::exception is a base of test_skip_exception, so an expected-exception handler matched ahead
  // of the control-flow ones claims the skip. The body still runs to completion; what is lost is
  // the skip itself -- assert_throws returns normally, nothing propagates, and the runner counts
  // the case as passed. A case that declared it does not apply then reads as one that verified
  // something.
  //
  // This guards it twice over. Under the default build the wrong order does not compile at all,
  // because instantiating assert_throws<std::exception> leaves a handler unreachable and
  // -Wexceptions is an error; with those warnings demoted it builds, and the counts below catch it.
  const test_suite s{ "inner", { { "s", body_skip_inside_assert_throws } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.skipped, std::size_t{ 1 }, "the skip reached the runner");
  assert_eq(r.passed, std::size_t{ 0 }, "and was not counted as the expected exception");
  assert_eq(r.failed, std::size_t{ 0 }, "a skip is not a failure either");
}

void
a_failed_assertion_inside_assert_throws_fails_its_case()
{
  // Same ordering, the other control-flow type. A nested failure carries its own location and
  // message, and being counted as the expected exception discards both.
  const test_suite s{ "inner", { { "s", body_failed_assertion_inside_assert_throws } } };
  const auto r = run_quiet(s, {}, false);
  assert_eq(r.failed, std::size_t{ 1 }, "the nested failure reached the runner");
  assert_eq(r.passed, std::size_t{ 0 }, "and did not satisfy the expectation");
}

void
assert_throws_accepts_a_base_of_the_thrown_type()
{
  // The legitimate use of a base type, which the ordering above must not have cost: asking for
  // std::exception and getting something derived from it is a match.
  assert_throws<std::exception>([]() {
    throw std::invalid_argument("derived from std::exception");
  });
  assert_throws<std::logic_error>([]() {
    throw std::invalid_argument("derived from std::logic_error");
  });
}

void
assert_throws_rejects_an_unrelated_type_and_says_so()
{
  const auto message = message_of([]() {
    assert_throws<std::logic_error>([]() {
      throw std::runtime_error("unrelated");
    });
  });
  assert_false(message.empty(), "an unrelated type is not accepted");
  assert_true(message.find("a different exception type was thrown") != std::string::npos,
              "and the report says the type was wrong rather than that nothing was thrown");
}

void
assert_throws_rejects_a_callable_that_throws_nothing()
{
  const auto message = message_of([]() {
    assert_throws<std::exception>([]() {
    });
  });
  assert_false(message.empty(), "a callable that throws nothing fails the assertion");
  assert_true(message.find("nothing was thrown") != std::string::npos,
              "and is distinguished from having thrown the wrong type");
}

void
assert_eq_reports_both_values_when_it_can_format_them()
{
  const auto message = message_of([]() {
    assert_eq(2 + 2, 5, "arithmetic");
  });
  assert_true(message.find("actual: 4") != std::string::npos, "the value it saw");
  assert_true(message.find("expected: 5") != std::string::npos, "and the value it wanted");
  // A report naming only the assertion sends the reader back to reproduce it before they can start.
  assert_true(message.find("arithmetic") != std::string::npos, "alongside the caller's message");
}

void
assert_eq_omits_values_it_cannot_format()
{
  // The fallback is correct -- there is nothing to print -- but which branch a type takes is a
  // compile-time decision, so without this a change to the condition would silently move every
  // comparison to the value-free message.
  const auto message = message_of([]() {
    assert_eq(opaque{ 1 }, opaque{ 2 }, "opaque values differ");
  });
  assert_true(message.find("opaque values differ") != std::string::npos, "the message survives");
  assert_true(message.find("actual:") == std::string::npos,
              "and no value is printed for a type fmt cannot format");
}

void
assert_true_and_assert_false_report_what_was_asked()
{
  assert_true(message_of([]() {
                assert_true(true);
              }).empty(),
              "a satisfied assertion says nothing");
  assert_true(message_of([]() {
                assert_false(false);
              }).empty(),
              "and so does its negation");

  assert_true(message_of([]() {
                assert_true(false, "the caller's words");
              }).find("the caller's words") != std::string::npos,
              "assert_true reports the message it was given");
  assert_true(message_of([]() {
                assert_false(true, "the caller's other words");
              }).find("the caller's other words") != std::string::npos,
              "and so does assert_false");

  // The defaults have to differ, or a failure with no message cannot say which way it went.
  assert_true(message_of([]() {
                assert_true(false);
              }).find("expected true") != std::string::npos,
              "assert_true's default names what it wanted");
  assert_true(message_of([]() {
                assert_false(true);
              }).find("expected false") != std::string::npos,
              "and assert_false's default is not the same string");
}

void
an_assertion_reports_the_location_of_its_call_site()
{
  // Every assertion defaults its source_location at the call site. Lose that -- a helper passing
  // its own location, or the default dropped -- and every failure in the suite names the framework
  // header instead of the test that failed, which reads as a framework bug in every report.
  //
  // The expected name is taken from a location captured here rather than written out. Where the
  // toolchain has no location builtins the shim degrades to "unknown:0" by design, and a hard-coded
  // "selftest.cxx" would report that documented behaviour as a defect.
  const auto here = source_location::current();
  const auto message = message_of([]() {
    assert_true(false, "located");
  });
  assert_true(message.find(here.file_name()) != std::string::npos,
              "the failure names the file that asserted");
  assert_true(message.find("test_framework.hxx") == std::string::npos,
              "and not the header the assertion is defined in");
}

void
message_of_passes_everything_else_to_the_runner()
{
  // message_of catches test_assertion_failure and nothing else. A skip is the case that matters:
  // swallowed, it becomes an empty string, the body carries on, and a case that declared it does
  // not apply reports passed. Each body below asserts false after its message_of call, so reaching
  // that line is itself the failure.
  const test_suite skipping_suite{ "inner", { { "s", body_skip_inside_message_of } } };
  const auto skipped = run_quiet(skipping_suite, {}, false);
  assert_eq(skipped.skipped, std::size_t{ 1 }, "a skip reaches the runner");
  assert_eq(skipped.passed, std::size_t{ 0 }, "and is not turned into an empty message");
  assert_eq(skipped.failed, std::size_t{ 0 }, "so the line after the call never ran");

  const test_suite throwing_suite{ "inner", { { "t", body_throw_inside_message_of } } };
  const auto threw = run_quiet(throwing_suite, {}, false);
  assert_eq(threw.failed, std::size_t{ 1 }, "an unrelated exception reaches the runner too");
  assert_eq(threw.passed, std::size_t{ 0 }, "and does not read as a satisfied assertion");
}

auto
tests() -> test_suite
{
  return {
    "framework_selftest",
    {
      { "should_run_gating_is_correct", should_run_gating_is_correct },
      { "runner_reports_a_passing_case", runner_reports_a_passing_case },
      { "runner_detects_a_failing_case", runner_detects_a_failing_case },
      { "runner_reports_a_skipped_case", runner_reports_a_skipped_case },
      { "runner_detects_a_timeout", runner_detects_a_timeout, timeout::fast },
      { "env_gating_skips_cluster_only_without_a_cluster",
        env_gating_skips_cluster_only_without_a_cluster },
      { "case_output_is_flushed_as_it_is_written", case_output_is_flushed_as_it_is_written },
      { "filter_selects_cases_by_name", filter_selects_cases_by_name },
      { "a_failing_case_does_not_suppress_later_cases",
        a_failing_case_does_not_suppress_later_cases },
      { "slow_cases_run_after_a_failure", slow_cases_run_after_a_failure },
      { "a_filter_name_matching_nothing_fails", a_filter_name_matching_nothing_fails },
      { "a_suite_that_runs_nothing_does_not_pass", a_suite_that_runs_nothing_does_not_pass },
      { "a_timeout_is_reported_as_such", a_timeout_is_reported_as_such, timeout::fast },
      { "case_names_covers_slow_cases_and_ignores_the_environment",
        case_names_covers_slow_cases_and_ignores_the_environment },
      { "timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number",
        timeout_multiplier_defaults_to_one_and_rejects_anything_but_a_number },
      { "scale_timeouts_multiplies_every_budget", scale_timeouts_multiplies_every_budget },
      { "a_skip_inside_assert_throws_skips_its_case", a_skip_inside_assert_throws_skips_its_case },
      { "a_failed_assertion_inside_assert_throws_fails_its_case",
        a_failed_assertion_inside_assert_throws_fails_its_case },
      { "assert_throws_accepts_a_base_of_the_thrown_type",
        assert_throws_accepts_a_base_of_the_thrown_type },
      { "assert_throws_rejects_an_unrelated_type_and_says_so",
        assert_throws_rejects_an_unrelated_type_and_says_so },
      { "assert_throws_rejects_a_callable_that_throws_nothing",
        assert_throws_rejects_a_callable_that_throws_nothing },
      { "assert_eq_reports_both_values_when_it_can_format_them",
        assert_eq_reports_both_values_when_it_can_format_them },
      { "assert_eq_omits_values_it_cannot_format", assert_eq_omits_values_it_cannot_format },
      { "assert_true_and_assert_false_report_what_was_asked",
        assert_true_and_assert_false_report_what_was_asked },
      { "an_assertion_reports_the_location_of_its_call_site",
        an_assertion_reports_the_location_of_its_call_site },
      { "message_of_passes_everything_else_to_the_runner",
        message_of_passes_everything_else_to_the_runner },
      { "a_scaled_budget_lets_a_case_outlive_its_original_one",
        a_scaled_budget_lets_a_case_outlive_its_original_one,
        timeout::fast },
    },
  };
}

} // namespace couchbase::test
