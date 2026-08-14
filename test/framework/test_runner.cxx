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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <thread>

#include <spdlog/fmt/fmt.h>

namespace couchbase::test
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

// Run `body` on a worker thread, enforcing `timeout`. On timeout the worker is detached: the
// packaged_task moved into it co-owns the future's shared state, so the detached thread writes
// only to that heap state (never to this stack frame) and the result is simply never read.
auto
run_bounded(std::function<void()> body, std::chrono::milliseconds timeout) -> case_result
{
  std::packaged_task<void()> task(std::move(body));
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

// What the requirement phase concluded about one case.
struct gate_result {
  enum class verdict : std::uint8_t {
    run,
    skip,
    fail
  };
  verdict outcome{ verdict::run };
  std::string requirement{}; // which one decided, for the grouped skip report
  std::string detail{};
  // A requirement that blew its budget leaves a worker detached, exactly as a case body does, and
  // the process must leave the same way (see main()).
  bool timed_out{ false };
};

// Check every requirement of a case, in declaration order, stopping at the first that does not
// hold. The whole phase runs on the bounded worker: a requirement that blocks must cost this case
// its budget, not the run.
auto
check_requirements(const test_case& tc, context& ctx) -> gate_result
{
  if (tc.requirements.empty()) {
    return {};
  }

  // On the heap, and captured by value: run_bounded detaches the worker when the budget is blown,
  // so a stack local here would be written to by that worker after this function returned. `tc` and
  // `ctx` are safe by reference -- both outlive the whole run.
  auto gate = std::make_shared<gate_result>();
  const auto bounded = run_bounded(
    [&tc, &ctx, gate]() {
      for (const auto& req : tc.requirements) {
        if (req == nullptr) {
          *gate = { gate_result::verdict::fail, "(null)", "a null requirement was registered" };
          return;
        }
        check_result checked;
        try {
          checked = req->check(ctx);
        } catch (const probe_failure& e) {
          // The probe could not answer. Not knowing whether a case applies is not the same as
          // knowing it does not, and only one of the two may pass silently.
          checked = check_result::unknown(e.what());
        }
        switch (checked.value) {
          case check_result::status::satisfied:
            break;
          case check_result::status::not_satisfied:
            *gate = { gate_result::verdict::skip, req->describe(), std::move(checked.detail) };
            return;
          case check_result::status::undetermined:
            *gate = { gate_result::verdict::fail, req->describe(), std::move(checked.detail) };
            return;
        }
      }
    },
    ctx.config().requirement_budget);

  if (bounded.outcome == case_result::status::failed) {
    return { gate_result::verdict::fail,
             "(requirement phase)",
             bounded.timed_out ? fmt::format("checking requirements exceeded {}",
                                             human(ctx.config().requirement_budget))
                               : bounded.message,
             bounded.timed_out };
  }
  // A requirement whose check() called skip() rather than returning a status. Falling through to
  // the default-constructed gate would say "run", so the body would execute and report passed
  // behind a precondition that declared itself unmet. Skip, not fail: skip() is a statement that
  // the case does not apply, which is what not_satisfied means; undetermined keeps its own fail
  // path.
  if (bounded.outcome == case_result::status::skipped) {
    return { gate_result::verdict::skip, "(requirement phase)", bounded.message };
  }
  return *gate;
}
} // namespace

auto
run(const test_suite& suite, const std::set<std::string>& filter, context& ctx, std::ostream& out)
  -> run_result
{
  // Flush each line as it is written. ctest runs these binaries with stdout on a pipe, where the
  // stream is block-buffered, and a case that aborts the process discards the whole buffer: the log
  // then shows neither the case that was running nor any that had already passed, only ctest's
  // "Subprocess aborted". The cases that crash are exactly the ones whose output is needed.
  out << std::unitbuf;

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

      const auto gate = check_requirements(tc, ctx);
      if (gate.outcome == gate_result::verdict::skip) {
        out << fmt::format(
          "Skipping \"{}\": needs {} ({})\n", tc.name, gate.requirement, gate.detail);
        ++result.skipped;
        ++result.skipped_by_requirement[gate.requirement];
        continue;
      }
      if (gate.outcome == gate_result::verdict::fail) {
        out << fmt::format("\"{}\" FAILED: could not establish whether it needs {}: {}\n",
                           tc.name,
                           gate.requirement,
                           gate.detail);
        ++result.failed;
        if (gate.timed_out) {
          ++result.timed_out;
        }
        continue;
      }

      out << fmt::format("Running \"{}\" (budget: {})...\n", tc.name, human(tc.timeout));
      const auto r = run_bounded(
        [&tc, &ctx]() {
          tc.func(ctx);
        },
        tc.timeout);
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
case_names(const test_suite& suite) -> std::vector<std::string>
{
  std::vector<std::string> names;
  names.reserve(suite.test_cases.size() + suite.slow_test_cases.size());
  for (const auto& tc : suite.test_cases) {
    names.push_back(tc.name);
  }
  for (const auto& tc : suite.slow_test_cases) {
    names.push_back(tc.name);
  }
  return names;
}

auto
describe_cases(const test_suite& suite) -> std::vector<std::string>
{
  std::vector<std::string> lines;
  const auto describe = [&lines](const std::vector<test_case>& cases) {
    for (const auto& tc : cases) {
      std::string requirements;
      for (const auto& req : tc.requirements) {
        if (!requirements.empty()) {
          requirements += "; ";
        }
        requirements += req == nullptr ? "(null)" : req->describe();
      }
      lines.push_back(
        fmt::format("{}\t{}", tc.name, requirements.empty() ? "no requirements" : requirements));
    }
  };
  describe(suite.test_cases);
  describe(suite.slow_test_cases);
  return lines;
}

auto
timeout_multiplier(const std::optional<std::string>& raw) -> double
{
  if (!raw.has_value()) {
    return 1.0;
  }
  std::size_t consumed{ 0 };
  double factor{ 0.0 };
  try {
    factor = std::stod(*raw, &consumed);
  } catch (const std::exception&) {
    consumed = 0;
  }
  // The whole value has to be a number: "10x" must not be read as 10, because the run would then
  // silently use a budget nobody asked for.
  //
  // isfinite rejects "inf", which passes a bare `> 0.0` test and then makes every scaled budget
  // undefined at the cast in scale_timeouts. NaN is already rejected: any comparison against it is
  // false.
  if (consumed != raw->size() || !std::isfinite(factor) || !(factor > 0.0)) {
    throw std::invalid_argument(
      fmt::format("{} must be a positive number, got \"{}\"", timeout_multiplier_variable, *raw));
  }
  return factor;
}

namespace
{
auto
scale_budget(std::chrono::milliseconds budget, double factor) -> std::chrono::milliseconds
{
  // Ceiling, not nearest: this exists to give a case more room on a slower runner, and rounding
  // a budget down -- which std::llround does for any product with a fraction below .5 -- would
  // quietly do the opposite of what the caller asked for.
  const auto product = std::ceil(static_cast<double>(budget.count()) * factor);
  // Saturate before narrowing. A product past the integer range is undefined at the cast, and on
  // a signed overflow the budget would come back negative and clamp to 1ms -- a multiplier asking
  // for more room would give every case the least possible.
  constexpr auto ceiling = static_cast<double>(std::numeric_limits<long long>::max());
  const auto scaled =
    product >= ceiling ? std::numeric_limits<long long>::max() : static_cast<long long>(product);
  return std::chrono::milliseconds{ std::max<long long>(scaled, 1) };
}
} // namespace

void
scale_timeouts(test_suite& suite, double factor)
{
  const auto scale = [factor](test_case& tc) {
    tc.timeout = scale_budget(tc.timeout, factor);
  };
  for (auto& tc : suite.test_cases) {
    scale(tc);
  }
  for (auto& tc : suite.slow_test_cases) {
    scale(tc);
  }
}

void
scale_timeouts(configuration& config, double factor)
{
  // The requirement phase is where the cluster is opened, so it is the phase a slow runner is most
  // likely to blow -- and exceeding it is reported as a failure, not a skip. Leaving it unscaled
  // made the multiplier that exists for slow legs unable to reach the one budget they need most.
  config.requirement_budget = scale_budget(config.requirement_budget, factor);
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

} // namespace couchbase::test
