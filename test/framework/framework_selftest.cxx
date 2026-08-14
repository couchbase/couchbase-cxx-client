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
#include <set>
#include <sstream>
#include <thread>

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

// A sink for the runner's progress output so the self-test stays quiet.
auto
run_quiet(const test_suite& suite, const std::set<std::string>& filter, bool real_cluster)
  -> run_result
{
  std::ostringstream sink;
  return run(suite, filter, real_cluster, sink);
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

} // namespace

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
      { "filter_selects_cases_by_name", filter_selects_cases_by_name },
      { "a_failing_case_does_not_suppress_later_cases",
        a_failing_case_does_not_suppress_later_cases },
      { "slow_cases_run_after_a_failure", slow_cases_run_after_a_failure },
      { "a_filter_name_matching_nothing_fails", a_filter_name_matching_nothing_fails },
      { "a_suite_that_runs_nothing_does_not_pass", a_suite_that_runs_nothing_does_not_pass },
      { "a_timeout_is_reported_as_such", a_timeout_is_reported_as_such, timeout::fast },
    },
  };
}

} // namespace couchbase::test
