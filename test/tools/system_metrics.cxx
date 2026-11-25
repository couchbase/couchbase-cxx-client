/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "framework/test_registry.hxx"

#include "linux_proc_stat_parser.hxx"
#include "system_metrics.hxx"
#include "system_metrics_reporter.hxx"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>

#if defined(_WIN32)
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#endif

namespace couchbase::test
{
namespace
{

void
proc_stat_parser_reads_aggregate_and_per_core_cpu_without_double_counting_guest(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view sample = "intr 12345 0 0\n"
                                      "cpu  1000 200 300 4000 100 50 150 200 80 20\n"
                                      "cpu0 600 100 150 500 20 20 60 50 40 10\n"
                                      "cpu1 400 100 150 3500 80 30 90 150 40 10\n"
                                      "ctxt 99999\n"
                                      "btime 1700000000\n";

  const auto cpu = system_metrics::parse_linux_proc_stat_cpu(sample, 100);

  assert_eq(cpu.total, std::uint64_t{ 60'000 }, "aggregate total ms");
  assert_eq(cpu.busy, std::uint64_t{ 19'000 }, "aggregate busy ms");
  assert_true(cpu.iowait.has_value(), "Linux /proc/stat exposes iowait");
  assert_true(cpu.steal.has_value(), "Linux /proc/stat exposes steal");
  assert_eq(*cpu.iowait, std::uint64_t{ 1'000 }, "aggregate iowait ms");
  assert_eq(*cpu.steal, std::uint64_t{ 2'000 }, "aggregate steal ms");

  assert_eq(cpu.cores.size(), std::size_t{ 2 }, "parsed core count");
  assert_eq(cpu.cores[0].core_id, std::uint32_t{ 0 }, "cpu0 id");
  assert_eq(cpu.cores[0].total, std::uint64_t{ 15'000 }, "cpu0 total ms");
  assert_eq(cpu.cores[0].busy, std::uint64_t{ 9'800 }, "cpu0 busy ms");
  assert_eq(cpu.cores[1].core_id, std::uint32_t{ 1 }, "cpu1 id");
  assert_eq(cpu.cores[1].total, std::uint64_t{ 45'000 }, "cpu1 total ms");
  assert_eq(cpu.cores[1].busy, std::uint64_t{ 9'200 }, "cpu1 busy ms");
}

void
proc_stat_parser_preserves_core_ids_when_offline_cpus_are_omitted([[maybe_unused]] context& ctx)
{
  constexpr std::string_view before_offline = "cpu  600 0 0 0 0 0 0 0\n"
                                              "cpu0 100 0 0 0 0 0 0 0\n"
                                              "cpu1 200 0 0 0 0 0 0 0\n"
                                              "cpu2 300 0 0 0 0 0 0 0\n";
  constexpr std::string_view after_offline = "cpu  620 0 0 0 0 0 0 0\n"
                                             "cpu0 110 0 0 0 0 0 0 0\n"
                                             "cpu2 310 0 0 0 0 0 0 0\n";

  const auto t0 = system_metrics::parse_linux_proc_stat_cpu(before_offline, 100);
  const auto t1 = system_metrics::parse_linux_proc_stat_cpu(after_offline, 100);

  assert_eq(t0.cores.size(), std::size_t{ 3 }, "three online CPUs initially");
  assert_eq(t1.cores.size(), std::size_t{ 2 }, "two online CPUs after cpu1 goes offline");
  assert_eq(t1.cores[0].core_id, std::uint32_t{ 0 }, "slot 0 keeps core_id 0");
  assert_eq(t1.cores[1].core_id, std::uint32_t{ 2 }, "slot 1 keeps core_id 2");

  const auto prev_cpu2 = std::find_if(t0.cores.begin(), t0.cores.end(), [](const auto& c) {
    return c.core_id == 2;
  });
  assert_true(prev_cpu2 != t0.cores.end(), "cpu2 present in baseline sample");
  assert_eq(t1.cores[1].busy - prev_cpu2->busy, std::uint64_t{ 100 }, "cpu2 busy delta is 100 ms");
}

void
proc_stat_parser_handles_empty_truncated_and_high_uptime_ticks([[maybe_unused]] context& ctx)
{
  const auto empty = system_metrics::parse_linux_proc_stat_cpu("", 100);
  assert_eq(empty.total, std::uint64_t{ 0 }, "empty total");
  assert_eq(empty.busy, std::uint64_t{ 0 }, "empty busy");
  assert_false(empty.iowait.has_value(), "empty input carries no iowait");
  assert_false(empty.steal.has_value(), "empty input carries no steal");
  assert_eq(empty.cores.size(), std::size_t{ 0 }, "empty cores");

  const auto zero_hz = system_metrics::parse_linux_proc_stat_cpu("cpu 100 0 0 100 0 0 0 0\n", 0);
  assert_eq(zero_hz.total, std::uint64_t{ 0 }, "zero ticks_per_sec returns zero");

  const auto truncated = system_metrics::parse_linux_proc_stat_cpu("cpu 10 20 30\ncpu0 1 2\n", 100);
  assert_eq(truncated.total, std::uint64_t{ 0 }, "truncated aggregate ignored");
  assert_eq(truncated.cores.size(), std::size_t{ 0 }, "truncated core ignored");

  // Tick count above UINT64_MAX / 1000 overflows unsplit `ticks * 1000ULL`.
  const auto large = system_metrics::parse_linux_proc_stat_cpu(
    "cpu 20000000000000000 0 0 20000000000000000 0 0 0 0\n", 1'000);
  assert_eq(large.busy, std::uint64_t{ 20'000'000'000'000'000ULL }, "high-uptime busy ms");
  assert_eq(large.total, std::uint64_t{ 40'000'000'000'000'000ULL }, "high-uptime total ms");
}

void
native_system_metrics_reports_logical_cpus_and_system_cpu([[maybe_unused]] context& ctx)
{
  const auto reporter = system_metrics::system_metrics_reporter::create();
  const auto logical_cpus = reporter->get_logical_cpu_count();
  assert_true(logical_cpus > 0 && logical_cpus < std::numeric_limits<std::uint32_t>::max(),
              "logical CPU count must be positive");

  const auto sys_cpu = reporter->get_system_cpu();
#if defined(__linux__)
  assert_true(sys_cpu.total > 0, "system total CPU time must be positive");
  assert_true(sys_cpu.busy <= sys_cpu.total, "busy CPU time cannot exceed total CPU time");
  // Under taskset / cpuset containers on glibc >= 2.34, _SC_NPROCESSORS_ONLN reflects the
  // affinity mask while /proc/stat lists all online host CPUs.
  assert_true(sys_cpu.cores.size() >= static_cast<std::size_t>(logical_cpus),
              "/proc/stat core count is at least the affinity-allowed logical CPU count");
  for (std::size_t i = 0; i < sys_cpu.cores.size(); ++i) {
    assert_true(sys_cpu.cores[i].total > 0, "each core must have positive total time");
    assert_true(sys_cpu.cores[i].busy <= sys_cpu.cores[i].total,
                "each core's busy time cannot exceed its total time");
    if (i > 0) {
      assert_true(sys_cpu.cores[i - 1].core_id < sys_cpu.cores[i].core_id,
                  "Linux /proc/stat core_ids must be strictly ascending");
    }
  }
#elif defined(__APPLE__)
  assert_true(sys_cpu.total > 0, "system total CPU time must be positive");
  assert_true(sys_cpu.busy <= sys_cpu.total, "busy CPU time cannot exceed total CPU time");
  assert_eq(sys_cpu.cores.size(),
            static_cast<std::size_t>(logical_cpus),
            "per-core vector must match logical CPU count on macOS");
  for (std::size_t i = 0; i < sys_cpu.cores.size(); ++i) {
    assert_eq(
      sys_cpu.cores[i].core_id, static_cast<std::uint32_t>(i), "macOS core_id matches slot");
    assert_true(sys_cpu.cores[i].total > 0, "each core must have positive total time");
    assert_true(sys_cpu.cores[i].busy <= sys_cpu.cores[i].total,
                "each core's busy time cannot exceed its total time");
  }
#elif defined(_WIN32)
  assert_eq(logical_cpus,
            static_cast<std::uint32_t>(GetActiveProcessorCount(ALL_PROCESSOR_GROUPS)),
            "Windows logical CPU count covers all processor groups");
  if (GetActiveProcessorGroupCount() > 1) {
    assert_eq(sys_cpu.total, std::uint64_t{ 0 }, "multi-group Windows omits aggregate system CPU");
    assert_eq(sys_cpu.busy, std::uint64_t{ 0 }, "multi-group Windows omits aggregate busy CPU");
  } else {
    assert_true(sys_cpu.total > 0, "system total CPU time must be positive");
    assert_true(sys_cpu.busy <= sys_cpu.total, "busy CPU time cannot exceed total CPU time");
  }
#else
  assert_true(sys_cpu.busy <= sys_cpu.total, "busy CPU time cannot exceed total CPU time");
#endif
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(proc_stat_parser_reads_aggregate_and_per_core_cpu_without_double_counting_guest),
        {},
        timeout::instant },
      { CASE(proc_stat_parser_preserves_core_ids_when_offline_cpus_are_omitted),
        {},
        timeout::instant },
      { CASE(proc_stat_parser_handles_empty_truncated_and_high_uptime_ticks),
        {},
        timeout::instant },
      { CASE(native_system_metrics_reports_logical_cpus_and_system_cpu), {}, timeout::fast },
    },
  };
}
} // namespace couchbase::test
