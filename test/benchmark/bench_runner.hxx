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

#include "bench_framework.hxx"
#include "resource_usage.hxx"
#include "statistics.hxx"

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <vector>

namespace couchbase::test::bench
{
enum class outcome : std::uint8_t {
  measured,
  // Measured, but the number cannot be what the case meant to measure: a batched median under
  // 0.5 ns per iteration, which indicates that the compiler removed the work.
  suspect,
  failed,
  skipped,
};

struct case_result {
  // "case" or "case/variant". A case skipped or failed before it reached a key reports its name.
  std::string key{};
  outcome status{ outcome::measured };
  // The skip reason, the failure message, or the suspect note; empty when measured.
  std::string detail{};
  // Per operation (per iteration for batched), with the 95% confidence interval of the median.
  median_ci latency_ns{ 0, 0, 0 };
  // per_op only.
  std::optional<std::uint64_t> min_ns{}, p50_ns{}, p90_ns{}, p99_ns{}, p999_ns{}, max_ns{};
  // per_op only: successful operations per second of measurement.
  std::optional<double> ops_per_second{};
  // Over successful operations for per_op, over batch samples for batched.
  double mean_ns{ 0 };
  double stddev_ns{ 0 };
  // Operations attempted (iterations for batched), and how many of them failed.
  std::uint64_t operations{ 0 };
  std::uint64_t errors{ 0 };
  std::chrono::milliseconds measured_for{ 0 };
  // Process-wide, over the measurement window that the latencies come from: every thread of the
  // process counts, including the SDK's I/O threads and an in-process fake server. Per operation
  // attempted (per iteration for batched, counting the batches that the latency leaves out). Empty
  // when the platform does not provide the counter.
  std::optional<double> cpu_ns_per_op{};
  std::optional<double> context_switches_per_op{};
  // The process's peak resident set size when the key finished. A high-water mark, so it never
  // decreases from one key to the next in one binary.
  std::optional<std::uint64_t> max_rss_bytes{};
};

// Reads process resource usage at the ends of a key's measurement window.
using resource_sampler = std::function<resource_usage()>;

namespace detail
{
// The measurement phase of a key: how long it ran, and the resource usage at its ends.
struct window {
  std::chrono::steady_clock::duration elapsed;
  resource_usage before;
  resource_usage after;
};

// Warmup rounds, then measurement rounds until the stopping rule holds. round(deadline,
// measuring) runs every thread until its first operation that ends at or after `deadline`.
// If provided, settle() runs before the opening resource sample (outside `elapsed`) and before the
// closing resource sample (inside `elapsed`).
auto
drive(const options& opts,
      const std::function<void(std::chrono::steady_clock::time_point, bool)>& round,
      const std::function<median_ci()>& current,
      const resource_sampler& sample,
      const std::function<void()>& settle = {}) -> window;
} // namespace detail

// Run the cases `filter` selects, in registration order. An empty filter selects everything; an
// entry selects a case by its name (all its keys) or one key in full. An entry that selects
// nothing yields a failed result keyed by the entry.
//
// Requirements are checked first: not_satisfied, or a check() that calls skip(), skips the case
// with the requirement's describe() as the detail; undetermined, or any other exception from
// check(), fails it. An exception escaping the case function fails the case, keeps the keys it
// already measured, and moves on to the next case. Each key warms up for `opts.warmup`. It then
// measures until both hold: `opts.min_time` has passed, and the relative half-width of the
// median's confidence interval is at most `opts.ci_target_percent`. `opts.max_time` ends it
// regardless. The rule is checked every 100 ms. A line naming each key goes to `progress` before it
// is measured. `sample` reads process resource usage at the start and end of each measurement
// window.
auto
run(const bench_suite& suite,
    const std::set<std::string>& filter,
    context& ctx,
    const options& opts,
    std::ostream& progress,
    const resource_sampler& sample = current_resource_usage) -> std::vector<case_result>;

// 1 when any result failed, 77 when there are results and every one was skipped, 0 otherwise.
[[nodiscard]] auto
exit_code(const std::vector<case_result>& results) -> int;
} // namespace couchbase::test::bench
