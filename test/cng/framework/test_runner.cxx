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

#include "test_runner.hxx"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <thread>

#include <spdlog/fmt/fmt.h>

namespace couchbase::cng::test
{
namespace
{
struct case_result {
  enum class status : std::uint8_t {
    passed,
    skipped,
    failed
  };
  status outcome{ status::passed };
  std::chrono::nanoseconds duration{ 0 };
  std::string message{};
  bool timed_out{ false };
};

// One decimal place: truncating to whole milliseconds reports every timeout::instant-class case as
// "0ms", which hides a case that got an order of magnitude slower.
auto
human(std::chrono::nanoseconds ns) -> std::string
{
  return fmt::format("{:.1f}ms", std::chrono::duration<double, std::milli>(ns).count());
}

// Run one case on a worker thread, enforcing `timeout`. On timeout the worker is detached: the
// packaged_task moved into it co-owns the future's shared state, so the detached thread writes
// only to that heap state (never to this stack frame) and the result is simply never read.
auto
run_case(void (*func)(), std::chrono::milliseconds timeout) -> case_result
{
  std::packaged_task<void()> task(func);
  auto future = task.get_future();
  const auto start = std::chrono::steady_clock::now();
  std::thread worker(std::move(task));

  if (future.wait_for(timeout) == std::future_status::ready) {
    const auto duration = std::chrono::steady_clock::now() - start;
    worker.join();
    try {
      future.get();
      return { case_result::status::passed, duration, {} };
    } catch (const test_skip_exception& e) {
      return { case_result::status::skipped, duration, e.reason() };
    } catch (const std::exception& e) {
      return { case_result::status::failed, duration, e.what() };
    } catch (...) {
      return { case_result::status::failed, duration, "threw an unknown exception" };
    }
  }

  worker.detach();
  const auto duration = std::chrono::steady_clock::now() - start;
  return { case_result::status::failed,
           duration,
           fmt::format("timed out after {}", human(timeout)),
           /*timed_out=*/true };
}
} // namespace

auto
run(const test_suite& suite,
    const std::set<std::string>& filter,
    bool real_cluster,
    std::ostream& out) -> run_result
{
  run_result result;
  // Names the filter asked for that actually exist, so an unmatched name can be reported below.
  std::set<std::string> matched;

  const auto run_cases = [&](const std::vector<test_case>& cases) {
    for (const auto& tc : cases) {
      if (!filter.empty()) {
        if (filter.find(tc.name) == filter.end()) {
          continue;
        }
        matched.insert(tc.name);
      }
      if (!should_run(tc.env, real_cluster)) {
        out << fmt::format("Skipping \"{}\" (environment not applicable)\n", tc.name);
        ++result.skipped;
        continue;
      }
      out << fmt::format("Running \"{}\" (budget: {})...\n", tc.name, human(tc.timeout));
      const auto r = run_case(tc.func, tc.timeout);
      switch (r.outcome) {
        case case_result::status::passed:
          out << fmt::format("\"{}\" passed ({})\n", tc.name, human(r.duration));
          ++result.passed;
          break;
        case case_result::status::skipped:
          out << fmt::format("\"{}\" skipped ({})\n", tc.name, r.message);
          ++result.skipped;
          break;
        case case_result::status::failed:
          out << fmt::format("\"{}\" FAILED: {}\n", tc.name, r.message);
          ++result.failed;
          if (r.timed_out) {
            ++result.timed_out;
          }
          break;
      }
    }
  };

  run_cases(suite.test_cases);
  if (!suite.slow_test_cases.empty()) {
    out << "\nRunning slow tests...\n";
    run_cases(suite.slow_test_cases);
  }

  // A filter name that matches nothing is a typo, not a request to run nothing: without this the
  // binary would exit 0 having tested nothing at all.
  for (const auto& name : filter) {
    if (matched.find(name) == matched.end()) {
      out << fmt::format("No test case named \"{}\" in suite \"{}\"\n", name, suite.name);
      ++result.failed;
    }
  }
  return result;
}

auto
exit_code(const run_result& result) -> int
{
  if (result.failed > 0) {
    return 1;
  }
  if (result.passed == 0 && result.skipped > 0) {
    return 77; // GNU/ctest "skipped" convention
  }
  if (result.passed == 0) {
    // Nothing ran, nothing skipped, nothing failed. An empty tests(), or a suite whose every case
    // was filtered out, must not report PASS -- otherwise ctest stays green on a binary that
    // verified nothing, indefinitely and silently.
    return 1;
  }
  return 0;
}

auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>
{
  if (name.empty()) {
    return std::nullopt;
  }

#if defined(_WIN32)
  char* buf = nullptr;
  std::size_t len = 0;
  if (_dupenv_s(&buf, &len, name.c_str()) == 0 && buf != nullptr) {
    std::string value(buf);
    free(buf); // NOLINT(cppcoreguidelines-no-malloc) — _dupenv_s allocates with malloc
    if (!value.empty()) {
      return value;
    }
  }
  return std::nullopt;
#else
  if (const char* value = std::getenv(name.c_str()); // NOLINT(concurrency-mt-unsafe)
      value != nullptr && value[0] != '\0') {
    return std::string{ value };
  }
  return std::nullopt;
#endif
}

} // namespace couchbase::cng::test
