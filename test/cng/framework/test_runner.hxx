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
#include <optional>
#include <ostream>
#include <set>
#include <string>

namespace couchbase::cng::test
{

struct run_result {
  std::size_t passed{ 0 };
  std::size_t skipped{ 0 };
  std::size_t failed{ 0 };
  // Cases that exceeded their budget. Counted in `failed` too; tracked separately because a
  // timeout leaves a detached worker thread running, which changes how the process must exit
  // (see main()).
  std::size_t timed_out{ 0 };
};

// Run a suite. `filter` empty => run every case; otherwise only cases whose name is in `filter`;
// a filter name matching no case is itself a failure. `real_cluster` selects env-gated cases (see
// should_run). Progress lines go to `out`. Every selected case runs even after one fails, so a
// single CI round-trip reports every regression in the binary. Exposed (rather than buried in
// main) so a self-test can drive it with in-memory suites.
auto
run(const test_suite& suite,
    const std::set<std::string>& filter,
    bool real_cluster,
    std::ostream& out) -> run_result;

// Process exit code for a result: any failure => 1; nothing ran but something skipped => 77
// (the GNU/ctest "skipped" convention); otherwise 0.
[[nodiscard]] auto
exit_code(const run_result& result) -> int;

// Portable std::getenv wrapper returning std::nullopt for unset *or* empty values, matching the
// wrappers in tools/utils.cxx and examples/external_circuit_breaker. Needed because MSVC treats
// plain getenv() as deprecated, and the CNG tree builds with /W4 /WX. Lives here so every CNG
// test executable shares one implementation.
[[nodiscard]] auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>;

} // namespace couchbase::cng::test
