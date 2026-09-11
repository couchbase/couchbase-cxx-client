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
#include <memory>
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
  // Cases that exceeded their budget. Counted in `failed` too, and tracked separately for two
  // reasons. Whether it is zero decides how the process exits, because a timeout leaves a
  // detached worker thread running and main() must not destroy anything underneath it. How
  // large it is goes into the report, because that many workers were abandoned still holding
  // whatever their cases held. Nothing joins them, so an abandoned worker may or may not still
  // be running by the time the report is produced. The second reason is why this is a count
  // rather than a flag.
  std::size_t timed_out{ 0 };
};

// Run a suite. `filter` empty => run every case; otherwise only cases whose name is in `filter`;
// a filter name matching no case is itself a failure. Each case's requirements are checked
// against `ctx` first: unsatisfied skips it, undetermined fails it. Progress lines go to `out`.
// Every selected case runs even after one fails, so a single CI round-trip reports every
// regression in the binary. Exposed (rather than buried in main) so a self-test can drive it with
// in-memory suites.
auto
run(const test_suite& suite,
    const std::set<std::string>& filter,
    std::shared_ptr<context> ctx,
    std::ostream& out) -> run_result;

// Close a run down: release the context, then call the suite's teardown hook if it has one. The
// order is the whole of it. The context owns the probe backend and, through it, whatever connection
// the probes opened, while the hook is where a suite unloads a library it used -- the use it
// documents is the OPENSSL_cleanup() that test/main.cxx performs for the Catch2 suites -- so
// anything whose destructor calls into such a library has to be gone before the hook runs.
// Ownership transfers here: the share passed in has to be the last one, and a caller that kept
// another -- or an abandoned worker still holding one -- gets a std::logic_error instead. This
// lives here rather than inline in main() so a self-test can observe the order.
//
// Not for the timeout path: a case that exceeded its budget leaves a worker detached, possibly
// still inside the context, so main() leaves through _Exit there and destroys nothing.
void
tear_down(std::shared_ptr<context> ctx, void (*teardown)());

// Process exit code for a result: any failure => 1; nothing ran but something skipped => 77
// (the GNU/ctest "skipped" convention); otherwise 0.
[[nodiscard]] auto
exit_code(const run_result& result) -> int;

// What to print about workers a run abandoned, or empty when it abandoned none. A timed-out case
// leaves its worker detached rather than killing it, so for the rest of the binary it may still be
// running and competing for whatever the body held -- and that is invisible in a report that says
// only "FAILED". Separate from main() so a test can assert on it.
[[nodiscard]] auto
abandoned_workers_note(const run_result& result) -> std::string;

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
