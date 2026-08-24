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
// tests(); this main gathers a name filter from argv, decides mock-vs-real mode from
// TEST_CONNECTION_STRING, runs the suite, and maps the outcome to a process exit code
// (0 pass / 1 fail / 77 all-skipped).
//
// `--list-tests` prints the case names one per line and exits. cmake/TestFramework.cmake runs it
// after linking to register one ctest entry per case.

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
    if (const std::string_view arg{ argv[i] }; arg == "--list-tests") {
      list_only = true;
    } else {
      filter.emplace(arg);
    }
  }

  auto suite = tests();

  if (list_only) {
    for (const auto& name : case_names(suite)) {
      std::cout << name << '\n';
    }
    return 0;
  }

  try {
    scale_timeouts(suite, timeout_multiplier(safe_getenv(timeout_multiplier_variable)));
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    return 1;
  }

  // Real-cluster mode iff TEST_CONNECTION_STRING is set and non-empty (the couchbase-cxx-client
  // integration convention).
  const bool real_cluster = safe_getenv("TEST_CONNECTION_STRING").has_value();

  const auto result = run(suite, filter, real_cluster, std::cout);

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
