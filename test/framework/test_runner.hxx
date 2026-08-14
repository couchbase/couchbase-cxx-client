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

#pragma once

#include "test_framework.hxx"

#include <cstddef>
#include <map>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <vector>

namespace couchbase::test
{

struct run_result {
  std::size_t passed{ 0 };
  std::size_t skipped{ 0 };
  std::size_t failed{ 0 };
  // Why cases were skipped: requirement description -> how many cases it turned away. Printed at
  // the end of a run, because "37 skipped" without the reasons is how a permanently-false
  // predicate stays invisible for a year. Every key is a requirement an environment could be given,
  // so a skip the requirement phase itself decided -- a check() that called skip() -- appears in
  // `skipped` alone; it has no requirement to name.
  std::map<std::string, std::size_t> skipped_by_requirement{};
  // Cases that exceeded their budget. Counted in `failed` too; tracked separately because a
  // timeout leaves a detached worker thread running, which changes how the process must exit
  // (see main()).
  std::size_t timed_out{ 0 };
};

// Run a suite. `filter` empty => run every case; otherwise only cases whose name is in `filter`;
// a filter name matching no case is itself a failure. Each case's requirements are checked
// against `ctx` first: unsatisfied skips it, undetermined fails it. Progress lines go to `out`.
// Every selected case runs even after one fails, so a single CI round-trip reports every
// regression in the binary. Exposed (rather than buried in main) so a self-test can drive it with
// in-memory suites.
auto
run(const test_suite& suite, const std::set<std::string>& filter, context& ctx, std::ostream& out)
  -> run_result;

// Process exit code for a result: any failure => 1; nothing ran but something skipped => 77
// (the GNU/ctest "skipped" convention); otherwise 0.
[[nodiscard]] auto
exit_code(const run_result& result) -> int;

// Every case name in `suite`, in registration order, including cases this environment would not
// run. CMake enumerates the ctest entries from this list at build time, so a name must be present
// whether or not a cluster is configured -- otherwise the set of registered tests would depend on
// the machine that configured the build.
[[nodiscard]] auto
case_names(const test_suite& suite) -> std::vector<std::string>;

// What --list-tests prints: the case name, then a tab, then what the case requires. The tab is
// load-bearing -- cmake/TestFrameworkAddTests.cmake registers the part before it, so the same
// output serves the build and a person trying to find out why a case never runs.
[[nodiscard]] auto
describe_cases(const test_suite& suite) -> std::vector<std::string>;

// Environment variable holding a factor applied to every case's timeout budget.
inline constexpr auto timeout_multiplier_variable = "CB_TEST_TIMEOUT_MULTIPLIER";

// Interpret the value of timeout_multiplier_variable; std::nullopt yields 1.0. Budgets are
// absolute milliseconds and a run under valgrind or a sanitizer is an order of magnitude slower,
// so without a multiplier such a leg reports timeouts rather than behaviour. A value that is not
// wholly a positive number throws std::invalid_argument: it is a broken invocation, not a request
// for the default.
[[nodiscard]] auto
timeout_multiplier(const std::optional<std::string>& raw) -> double;

// Multiply every budget in `suite` by `factor`, rounding up, to at least one millisecond.
void
scale_timeouts(test_suite& suite, double factor);

// The same, for the budget the requirement phase runs under. Separate because the configuration is
// resolved after the suite, and both have to be scaled by the same factor.
void
scale_timeouts(configuration& config, double factor);

} // namespace couchbase::test
