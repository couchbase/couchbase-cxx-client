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
#include "tools/fit_performer/src/metrics/metrics_accumulator.hxx"

#include <tao/json.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <vector>

namespace couchbase::test
{
namespace
{

constexpr double k_tol = 1e-6;

auto
as_number(const tao::json::value& obj,
          const char* key,
          source_location loc = source_location::current()) -> double
{
  const auto* field = obj.find(key);
  assert_true(field != nullptr, "expected numeric JSON field is missing", loc);
  if (field->is_double()) {
    return field->get_double();
  }
  if (field->is_unsigned()) {
    return static_cast<double>(field->get_unsigned());
  }
  return static_cast<double>(field->get_signed());
}

void
accumulator_emits_java_compatible_and_extended_cpu_metrics([[maybe_unused]] context& ctx)
{
  fit_cxx::metrics_accumulator acc;
  const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 100 } };

  fit_cxx::metrics_sample_input s0{};
  s0.sampled_at = t0;
  s0.logical_cpu_count = 16;
  s0.proc_cpu = system_metrics::proc_cpu_t{ 1'000, 5'000, 1'000 };
  s0.system_cpu.busy = 100'000;
  s0.system_cpu.total = 400'000;
  s0.system_cpu.iowait = 10'000;
  s0.system_cpu.steal = 4'000;
  s0.system_cpu.cores.reserve(16);
  for (std::uint32_t i = 0; i < 16; ++i) {
    s0.system_cpu.cores.push_back(system_metrics::core_cpu_t{ i, 5'000, 25'000 });
  }
  s0.thread_cpu_usec = {
    { system_metrics::sm_tid_t{ 101 }, 2'000'000ULL },
    { system_metrics::sm_tid_t{ 102 }, 1'000'000ULL },
  };
  s0.memory = system_metrics::proc_mem_t{ 512ULL * 1024 * 1024, 32ULL * 1024 * 1024, 0, 0 };

  assert_false(acc.record(s0).has_value(), "first tick records baseline and emits no sample");

  fit_cxx::metrics_sample_input s1 = s0;
  s1.sampled_at = t0 + std::chrono::milliseconds{ 1'000 };
  s1.proc_cpu.user += 1'800;
  s1.proc_cpu.sys += 600;
  s1.system_cpu.busy += 4'000;
  s1.system_cpu.total += 16'000;
  s1.system_cpu.iowait = 10'320;
  s1.system_cpu.steal = 4'160;
  for (std::size_t i = 0; i < 16; ++i) {
    s1.system_cpu.cores[i].total += 1'000;
    if (i == 0) {
      s1.system_cpu.cores[i].busy += 940;
    } else if (i == 1) {
      s1.system_cpu.cores[i].busy += 810;
    } else {
      s1.system_cpu.cores[i].busy += 150;
    }
  }
  s1.thread_cpu_usec = {
    { system_metrics::sm_tid_t{ 101 }, 2'950'000ULL },
    { system_metrics::sm_tid_t{ 102 }, 1'400'000ULL },
    { system_metrics::sm_tid_t{ 103 }, 500'000ULL },
  };
  s1.heap_used_bytes = 2ULL * 1024 * 1024;
  s1.free_swap_bytes = 128ULL * 1024 * 1024;

  const auto out = acc.record(s1);
  assert_true(out.has_value(), "second tick must produce a sample");
  const auto& report = *out;

  assert_near(as_number(report, "processCpu"), 15.0, k_tol, "processCpu matches Java scale");
  assert_near(as_number(report, "systemCpu"), 25.0, k_tol, "systemCpu matches Java scale");
  assert_near(as_number(report, "systemCpuIoWait"), 2.0, k_tol, "systemCpuIoWait");
  assert_near(as_number(report, "systemCpuSteal"), 1.0, k_tol, "systemCpuSteal");

  assert_near(as_number(report, "processCpuTotal"), 240.0, k_tol, "processCpuTotal");
  assert_near(as_number(report, "processCpuUser"), 180.0, k_tol, "processCpuUser");
  assert_near(as_number(report, "processCpuSys"), 60.0, k_tol, "processCpuSys");
  assert_near(as_number(report, "maxThreadCpu"), 95.0, k_tol, "maxThreadCpu");

  assert_eq(report.at("logicalCpuCount").get_unsigned(), std::uint64_t{ 16 }, "logicalCpuCount");
  assert_eq(report.at("threadCount").get_unsigned(), std::uint64_t{ 3 }, "threadCount");
  assert_near(as_number(report, "maxCoreCpu"), 94.0, k_tol, "maxCoreCpu");
  const auto& cores = report.at("coresCpu").get_array();
  assert_eq(cores.size(), std::size_t{ 16 }, "coresCpu array length");
  assert_near(cores[0].get_double(), 94.0, k_tol, "core 0 pct");
  assert_near(cores[1].get_double(), 81.0, k_tol, "core 1 pct");
  assert_near(cores[2].get_double(), 15.0, k_tol, "core 2 pct");

  assert_near(as_number(report, "memRssUsedMB"), 32.0, k_tol, "memRssUsedMB");
  assert_near(as_number(report, "memVmsMB"), 512.0, k_tol, "memVmsMB");
  assert_near(as_number(report, "memHeapUsedMB"), 2.0, k_tol, "memHeapUsedMB");
  assert_near(as_number(report, "freeSwapSizeMB"), 128.0, k_tol, "freeSwapSizeMB");
}

void
accumulator_cumulative_process_cpu_is_invariant_across_core_counts([[maybe_unused]] context& ctx)
{
  const auto run_for_cores = [](std::uint32_t cores) -> tao::json::value {
    fit_cxx::metrics_accumulator acc;
    const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 10 } };

    fit_cxx::metrics_sample_input s0{};
    s0.sampled_at = t0;
    s0.logical_cpu_count = cores;
    s0.proc_cpu = system_metrics::proc_cpu_t{ 0, 1'000, 500 };
    s0.system_cpu.busy = 10'000;
    s0.system_cpu.total = 50'000;
    assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

    fit_cxx::metrics_sample_input s1 = s0;
    s1.sampled_at = t0 + std::chrono::milliseconds{ 1'000 };
    s1.proc_cpu.user += 1'900;
    s1.proc_cpu.sys += 500;
    s1.system_cpu.busy += 2'400;
    s1.system_cpu.total += std::uint64_t{ cores } * 1'000ULL;
    const auto out = acc.record(s1);
    assert_true(out.has_value(), "second tick produces sample");
    return *out;
  };

  const auto r8 = run_for_cores(8);
  const auto r16 = run_for_cores(16);
  const auto r32 = run_for_cores(32);

  assert_near(as_number(r8, "processCpu"), 30.0, k_tol, "8-core host-normalized processCpu");
  assert_near(as_number(r16, "processCpu"), 15.0, k_tol, "16-core host-normalized processCpu");
  assert_near(as_number(r32, "processCpu"), 7.5, k_tol, "32-core host-normalized processCpu");

  assert_near(as_number(r8, "processCpuTotal"), 240.0, k_tol, "8-core cumulative processCpuTotal");
  assert_near(
    as_number(r16, "processCpuTotal"), 240.0, k_tol, "16-core cumulative processCpuTotal");
  assert_near(
    as_number(r32, "processCpuTotal"), 240.0, k_tol, "32-core cumulative processCpuTotal");
}

void
accumulator_max_thread_cpu_catches_single_thread_migrated_across_cores(
  [[maybe_unused]] context& ctx)
{
  fit_cxx::metrics_accumulator acc;
  const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 50 } };

  fit_cxx::metrics_sample_input s0{};
  s0.sampled_at = t0;
  s0.logical_cpu_count = 2;
  s0.system_cpu.cores = { { 0, 0, 0 }, { 1, 0, 0 } };
  s0.thread_cpu_usec = { { system_metrics::sm_tid_t{ 42 }, 1'000'000ULL } };
  assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

  fit_cxx::metrics_sample_input s1 = s0;
  s1.sampled_at = t0 + std::chrono::seconds{ 1 };
  s1.proc_cpu.user = 1'000;
  s1.system_cpu.busy = 1'000;
  s1.system_cpu.total = 2'000;
  s1.system_cpu.cores = { { 0, 500, 1'000 }, { 1, 500, 1'000 } };
  s1.thread_cpu_usec = { { system_metrics::sm_tid_t{ 42 }, 2'000'000ULL } };

  const auto out = acc.record(s1);
  assert_true(out.has_value(), "sample emitted");
  assert_near(as_number(*out, "maxCoreCpu"), 50.0, k_tol, "each core only saw half the second");
  assert_near(as_number(*out, "maxThreadCpu"), 100.0, k_tol, "migrated single thread is 100% busy");
}

void
accumulator_rounds_to_two_decimals_and_clamps_bounded_percentages([[maybe_unused]] context& ctx)
{
  fit_cxx::metrics_accumulator acc;
  const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 75 } };

  fit_cxx::metrics_sample_input s0{};
  s0.sampled_at = t0;
  s0.logical_cpu_count = 3;
  s0.system_cpu.cores = { { 0, 0, 0 }, { 1, 0, 0 }, { 2, 0, 0 } };
  s0.thread_cpu_usec = { { system_metrics::sm_tid_t{ 7 }, 0ULL } };
  assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

  // Repeating decimals (1/3 -> 33.33, 2/3 -> 66.67) and slight tick overshoot (>100% clamped).
  fit_cxx::metrics_sample_input s1 = s0;
  s1.sampled_at = t0 + std::chrono::milliseconds{ 300 };
  s1.proc_cpu.user = 400; // 400 / 300 ms wall = 133.33% unclamped; 1000 / 900 sys = clamped 100%
  s1.proc_cpu.sys = 600;
  s1.system_cpu.busy = 300; // 300 / 900 = 33.333... -> 33.33
  s1.system_cpu.total = 900;
  s1.system_cpu.cores = { { 0, 100, 300 }, { 1, 200, 300 }, { 2, 320, 300 } };
  s1.thread_cpu_usec = { { system_metrics::sm_tid_t{ 7 },
                           350'000ULL } }; // >300,000 us -> clamped 100.0

  const auto out = acc.record(s1);
  assert_true(out.has_value(), "sample emitted");
  assert_near(as_number(*out, "processCpu"), 100.0, k_tol, "processCpu clamped at 100");
  assert_near(as_number(*out, "systemCpu"), 33.33, k_tol, "systemCpu rounded to 2 decimals");
  assert_near(
    as_number(*out, "processCpuUser"), 133.33, k_tol, "processCpuUser unclamped & rounded");
  assert_near(as_number(*out, "processCpuSys"), 200.0, k_tol, "processCpuSys unclamped");
  assert_near(
    as_number(*out, "processCpuTotal"), 333.33, k_tol, "processCpuTotal unclamped & rounded");
  assert_near(as_number(*out, "maxThreadCpu"), 100.0, k_tol, "maxThreadCpu clamped at 100");
  assert_near(as_number(*out, "maxCoreCpu"), 100.0, k_tol, "maxCoreCpu clamped at 100");

  const auto& cores = out->at("coresCpu").get_array();
  assert_near(cores[0].get_double(), 33.33, k_tol, "core 0 rounded");
  assert_near(cores[1].get_double(), 66.67, k_tol, "core 1 rounded");
  assert_near(cores[2].get_double(), 100.0, k_tol, "core 2 clamped at 100");
}

void
accumulator_omits_per_core_metrics_on_hotplug_swap_and_recovers([[maybe_unused]] context& ctx)
{
  constexpr std::string_view frame0 = "cpu  300 0 0 1700 0 0 0 0\n"
                                      "cpu0 100 0 0 900 0 0 0 0\n"
                                      "cpu1 200 0 0 800 0 0 0 0\n";
  constexpr std::string_view frame1 = "cpu  350 0 0 1850 0 0 0 0\n"
                                      "cpu0 110 0 0 990 0 0 0 0\n"
                                      "cpu2 1200 0 0 800 0 0 0 0\n";
  constexpr std::string_view frame2 = "cpu  420 0 0 1980 0 0 0 0\n"
                                      "cpu0 130 0 0 1070 0 0 0 0\n"
                                      "cpu2 1250 0 0 850 0 0 0 0\n";

  const auto reporter = system_metrics::system_metrics_reporter::create();
  const auto host_cpus = reporter->get_logical_cpu_count();

  fit_cxx::metrics_accumulator acc;
  const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 300 } };

  fit_cxx::metrics_sample_input s0{};
  s0.sampled_at = t0;
  s0.logical_cpu_count = host_cpus;
  s0.system_cpu = system_metrics::parse_linux_proc_stat_cpu(frame0, 100);
  assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

  fit_cxx::metrics_sample_input s1 = s0;
  s1.sampled_at = t0 + std::chrono::seconds{ 1 };
  s1.system_cpu = system_metrics::parse_linux_proc_stat_cpu(frame1, 100);
  const auto swap_out = acc.record(s1);
  assert_true(swap_out.has_value(), "swap tick emits aggregate metrics");
  assert_eq(
    swap_out->at("logicalCpuCount").get_unsigned(), std::uint64_t{ host_cpus }, "logicalCpuCount");
  assert_near(as_number(*swap_out, "systemCpu"), 25.0, k_tol, "aggregate systemCpu kept");
  assert_true(swap_out->find("coresCpu") == nullptr, "hotplug swap omits coresCpu");
  assert_true(swap_out->find("maxCoreCpu") == nullptr, "hotplug swap omits maxCoreCpu");

  fit_cxx::metrics_sample_input s2 = s1;
  s2.sampled_at = t0 + std::chrono::seconds{ 2 };
  s2.system_cpu = system_metrics::parse_linux_proc_stat_cpu(frame2, 100);
  const auto rec_out = acc.record(s2);
  assert_true(rec_out.has_value(), "third tick recovers per-core output");
  const auto& cores = rec_out->at("coresCpu").get_array();
  assert_eq(cores.size(), std::size_t{ 2 }, "two online cores reported");
  assert_near(cores[0].get_double(), 20.0, k_tol, "cpu0 20/100 ticks = 20%");
  assert_near(cores[1].get_double(), 50.0, k_tol, "cpu2 50/100 ticks = 50%");
  assert_near(as_number(*rec_out, "maxCoreCpu"), 50.0, k_tol, "maxCoreCpu recovers to 50%");
}

void
accumulator_omits_only_fields_whose_counters_did_not_advance([[maybe_unused]] context& ctx)
{
  const auto t0 = std::chrono::steady_clock::time_point{ std::chrono::seconds{ 200 } };

  // Case 1: system_cpu.total == 0, logical_cpu_count == 0, disjoint thread IDs.
  {
    fit_cxx::metrics_accumulator acc;
    fit_cxx::metrics_sample_input s0{};
    s0.sampled_at = t0;
    s0.logical_cpu_count = 0;
    s0.proc_cpu = system_metrics::proc_cpu_t{ 0, 100, 50 };
    s0.thread_cpu_usec = { { system_metrics::sm_tid_t{ 1 }, 500ULL } };
    assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

    fit_cxx::metrics_sample_input s1 = s0;
    s1.sampled_at = t0 + std::chrono::milliseconds{ 1'000 };
    s1.proc_cpu.user = 300;
    s1.proc_cpu.sys = 150;
    s1.thread_cpu_usec = { { system_metrics::sm_tid_t{ 2 }, 900ULL } }; // disjoint thread
    const auto out = acc.record(s1);
    assert_true(out.has_value(), "sample emitted without system_cpu");
    assert_true(out->find("processCpu") == nullptr, "processCpu omitted when sys.total == 0");
    assert_true(out->find("systemCpu") == nullptr, "systemCpu omitted when sys.total == 0");
    assert_true(out->find("logicalCpuCount") == nullptr, "zero logicalCpuCount omitted");
    assert_true(out->find("maxThreadCpu") == nullptr, "disjoint threads omit maxThreadCpu");
    assert_eq(
      out->at("threadCount").get_unsigned(), std::uint64_t{ 1 }, "threadCount still emitted");
    assert_near(as_number(*out, "processCpuTotal"), 30.0, k_tol, "wall processCpuTotal emitted");
  }

  // Case 2: wall_usec == 0 while system_cpu advances -> host-% kept, wall-% omitted.
  {
    fit_cxx::metrics_accumulator acc;
    fit_cxx::metrics_sample_input s0{};
    s0.sampled_at = t0;
    s0.logical_cpu_count = std::numeric_limits<std::uint32_t>::max();
    s0.proc_cpu = system_metrics::proc_cpu_t{ 0, 100, 50 };
    s0.system_cpu.busy = 200;
    s0.system_cpu.total = 1'000;
    assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

    fit_cxx::metrics_sample_input s1 = s0;
    s1.sampled_at = t0; // zero wall elapsed
    s1.proc_cpu.user = 200;
    s1.proc_cpu.sys = 100;
    s1.system_cpu.busy = 500;
    s1.system_cpu.total = 2'000;
    const auto out = acc.record(s1);
    assert_true(out.has_value(), "sample emitted");
    assert_near(as_number(*out, "processCpu"), 15.0, k_tol, "host processCpu kept");
    assert_near(as_number(*out, "systemCpu"), 30.0, k_tol, "host systemCpu kept");
    assert_true(out->find("logicalCpuCount") == nullptr, "sentinel logicalCpuCount omitted");
    assert_true(out->find("processCpuTotal") == nullptr, "processCpuTotal omitted on zero wall");
    assert_true(out->find("processCpuUser") == nullptr, "processCpuUser omitted on zero wall");
    assert_true(out->find("processCpuSys") == nullptr, "processCpuSys omitted on zero wall");
    assert_true(out->find("maxThreadCpu") == nullptr, "maxThreadCpu omitted on zero wall");
  }

  // Case 3: regressed iowait + core count mismatch on tick 1, then recovery on tick 2.
  {
    fit_cxx::metrics_accumulator acc;
    fit_cxx::metrics_sample_input s0{};
    s0.sampled_at = t0;
    s0.logical_cpu_count = 2;
    s0.system_cpu.busy = 1'000;
    s0.system_cpu.total = 2'000;
    s0.system_cpu.iowait = 500;
    s0.system_cpu.cores = { { 0, 500, 1'000 }, { 1, 500, 1'000 } };
    assert_false(acc.record(s0).has_value(), "baseline tick emits nothing");

    fit_cxx::metrics_sample_input s1 = s0;
    s1.sampled_at = t0 + std::chrono::seconds{ 1 };
    s1.logical_cpu_count = 1;
    s1.system_cpu.busy = 1'500;
    s1.system_cpu.total = 4'000;
    s1.system_cpu.iowait = 480; // regressed (Linux /proc/stat iowait can step backwards)
    s1.system_cpu.cores = { { 0, 800, 2'000 } }; // core count changed -> omit coresCpu
    const auto out1 = acc.record(s1);
    assert_true(out1.has_value(), "tick 1 emitted");
    assert_eq(
      out1->at("logicalCpuCount").get_unsigned(), std::uint64_t{ 1 }, "updated logicalCpuCount");
    assert_near(as_number(*out1, "systemCpu"), 25.0, k_tol, "systemCpu kept despite iowait dip");
    assert_true(out1->find("systemCpuIoWait") == nullptr, "regressed iowait omitted");
    assert_true(out1->find("coresCpu") == nullptr, "mismatched core count omits coresCpu");
    assert_true(out1->find("maxCoreCpu") == nullptr, "maxCoreCpu omitted with coresCpu");

    fit_cxx::metrics_sample_input s2 = s1;
    s2.sampled_at = t0 + std::chrono::seconds{ 2 };
    s2.system_cpu.busy = 2'000;
    s2.system_cpu.total = 5'000;
    s2.system_cpu.iowait = 530;                    // +50 / 1000 => 5.0%
    s2.system_cpu.cores = { { 0, 1'200, 3'000 } }; // 400 / 1000 => 40.0%
    const auto out2 = acc.record(s2);
    assert_true(out2.has_value(), "tick 2 recovers");
    assert_eq(
      out2->at("logicalCpuCount").get_unsigned(), std::uint64_t{ 1 }, "recovered logicalCpuCount");
    assert_near(as_number(*out2, "systemCpuIoWait"), 5.0, k_tol, "iowait recovers on next tick");
    assert_near(as_number(*out2, "maxCoreCpu"), 40.0, k_tol, "coresCpu recovers on next tick");
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(accumulator_emits_java_compatible_and_extended_cpu_metrics), {}, timeout::instant },
      { CASE(accumulator_cumulative_process_cpu_is_invariant_across_core_counts),
        {},
        timeout::instant },
      { CASE(accumulator_max_thread_cpu_catches_single_thread_migrated_across_cores),
        {},
        timeout::instant },
      { CASE(accumulator_rounds_to_two_decimals_and_clamps_bounded_percentages),
        {},
        timeout::instant },
      { CASE(accumulator_omits_per_core_metrics_on_hotplug_swap_and_recovers),
        {},
        timeout::instant },
      { CASE(accumulator_omits_only_fields_whose_counters_did_not_advance), {}, timeout::instant },
    },
  };
}
} // namespace couchbase::test
