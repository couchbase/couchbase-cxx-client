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

// The benchmark runner's stopping rules, which assert on wall-clock durations.

#include "benchmark/bench_runner.hxx"

#include "framework/test_registry.hxx"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace bench;
using namespace std::chrono_literals;

auto
measure_one(const bench_suite& suite, context& ctx, const options& opts) -> case_result
{
  std::ostringstream progress;
  const auto results = run(suite, {}, ctx, opts, progress);
  assert_eq(results.size(), 1U);
  return results[0];
}

void
ok_op(state& s)
{
  s.per_op("t1", 1, [](std::size_t) {
    return true;
  });
}

void
jittered_sleep(state& s)
{
  std::atomic<std::uint64_t> n{ 0 };
  s.per_op("t1", 1, [&n](std::size_t) {
    std::this_thread::sleep_for(std::chrono::microseconds{ 50 + ((n++ * 37) % 200) });
    return true;
  });
}

void
slow_op(state& s)
{
  s.per_op("t1", 1, [](std::size_t) {
    std::this_thread::sleep_for(60ms);
    return true;
  });
}

void
stops_at_max_time(context& ctx)
{
  const auto r =
    measure_one({ "s", { { CASE(jittered_sleep) } } }, ctx, { 10ms, 50ms, 200ms, 0.0001 });
  assert_eq(r.key, std::string{ "jittered_sleep/t1" });
  assert_true(r.status == outcome::measured, "measured");
  assert_true(r.measured_for >= 200ms && r.measured_for <= 300ms,
              "stopped at max_time, measured for " + std::to_string(r.measured_for.count()) +
                " ms");
}

void
converges_before_max_time(context& ctx)
{
  const auto r = measure_one({ "s", { { CASE(ok_op) } } }, ctx, { 10ms, 50ms, 2000ms, 5.0 });
  assert_eq(r.key, std::string{ "ok_op/t1" });
  assert_true(r.status == outcome::measured, "measured");
  assert_true(r.measured_for >= 50ms && r.measured_for < 2000ms,
              "stopped once converged, measured for " + std::to_string(r.measured_for.count()) +
                " ms");
}

// Fewer than 6 samples have no 95% interval, so however wide the target, the key runs to max_time.
void
too_few_samples_never_converge(context& ctx)
{
  const auto r = measure_one({ "s", { { CASE(slow_op) } } }, ctx, { 0ms, 0ms, 250ms, 1000.0 });
  assert_true(r.status == outcome::measured, "measured");
  assert_true(r.measured_for >= 250ms,
              "stopped at max_time, measured for " + std::to_string(r.measured_for.count()) +
                " ms");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(stops_at_max_time) },
      { CASE(converges_before_max_time) },
      { CASE(too_few_samples_never_converge) },
    },
  };
}
} // namespace couchbase::test
