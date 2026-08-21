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

// Shared entry point for every test executable built on the framework. A test file provides
// tests(); this main gathers a name filter from argv, resolves the configuration from the
// environment, runs the suite, and maps the outcome to a process exit code
// (0 pass / 1 fail / 77 all-skipped).
//
// `--list-tests` prints each case name and what it requires, then exits. cmake/TestFramework.cmake
// runs it after linking to register one ctest entry per case.
//
// `--no-share-clusters` gives every case a cluster of its own. Sharing is the runner's decision,
// not the test's; this is the switch that takes the optimisation away when a failure looks like
// one case standing on another's state.

#include "test_runner.hxx"

#include <cstdlib> // std::_Exit
#include <exception>
#include <iostream>
#include <set>
#include <string>
#include <string_view>

#include <spdlog/fmt/fmt.h>

auto
main(int argc, char* argv[]) -> int
{
  using namespace couchbase::test;

  bool list_only{ false };
  std::set<std::string> filter;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{ argv[i] };
    if (arg == "--list-tests") {
      list_only = true;
    } else {
      filter.emplace(arg);
    }
  }

  auto suite = tests();

  if (list_only) {
    for (const auto& line : describe_cases(suite)) {
      std::cout << line << '\n';
    }
    return 0;
  }

  auto config = configuration::from_environment();
  try {
    const auto factor = timeout_multiplier(safe_getenv(timeout_multiplier_variable));
    scale_timeouts(suite, factor);
    // The requirement phase runs under its own budget on the configuration, not on any case, so it
    // has to be scaled by the same factor or the multiplier misses the phase that opens the
    // cluster.
    scale_timeouts(config, factor);
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }

  context ctx{ std::move(config) };

  const auto result = run(suite, filter, ctx, std::cout);

  // Grouped, because the count alone is what lets a predicate that is permanently false in CI go
  // unnoticed: 37 skipped reads the same whether the environment lacked one thing or everything.
  if (!result.skipped_by_requirement.empty()) {
    std::cout << "\nSkipped for want of:\n";
    for (const auto& [requirement, count] : result.skipped_by_requirement) {
      std::cout << fmt::format("  {} — {} case(s)\n", requirement, count);
    }
    std::cout << '\n';
  }

  if (result.failed > 0) {
    std::cout << fmt::format("Suite \"{}\": {} passed, {} skipped, {} FAILED\n",
                             suite.name,
                             result.passed,
                             result.skipped,
                             result.failed);
  } else if (result.passed == 0 && result.skipped > 0) {
    std::cout << fmt::format("Suite \"{}\": all {} case(s) skipped (not applicable to this mode)\n",
                             suite.name,
                             result.skipped);
  } else if (result.passed == 0) {
    std::cout << fmt::format("Suite \"{}\": no cases executed\n", suite.name);
  } else {
    std::cout << fmt::format(
      "Suite \"{}\": {} passed, {} skipped\n", suite.name, result.passed, result.skipped);
  }

  // A timed-out case leaves its worker detached and still running. Returning from main would run
  // static destructors underneath it -- tearing down spdlog's registry, std::cout and any gRPC
  // channel the body still holds -- which surfaces as a nondeterministic abort at exit, exactly
  // what the timeout runner exists to prevent. Leave immediately instead. Conditional on purpose:
  // an unconditional _Exit would also suppress LeakSanitizer's atexit report.
  if (result.timed_out > 0) {
    std::cout.flush();
    std::_Exit(exit_code(result));
  }

  return exit_code(result);
}
