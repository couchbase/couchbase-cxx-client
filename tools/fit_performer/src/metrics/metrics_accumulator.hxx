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

#pragma once

#include <system_metrics.hxx>

#include <tao/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <unordered_map>
#include <utility>

namespace fit_cxx
{

/// Raw OS counters for a single sample.
struct metrics_sample_input {
  std::chrono::steady_clock::time_point sampled_at{};
  std::uint32_t logical_cpu_count{ 0 };
  system_metrics::proc_cpu_t proc_cpu{};
  system_metrics::system_cpu_t system_cpu{};
  /// Cumulative (utime + stime) in microseconds, keyed by thread ID.
  std::unordered_map<system_metrics::sm_tid_t, std::uint64_t> thread_cpu_usec{};
  system_metrics::proc_mem_t memory{};
  std::optional<std::uint64_t> heap_used_bytes{};
  std::optional<std::uint64_t> free_swap_bytes{};
};

/// Delta calculator for `protocol::metrics::Result.metrics`.
/// Floating-point values are rounded to two decimal places. Megabytes are 2^20 bytes.
///
/// Whole-machine CPU fields (match Java `com.sun.management.OperatingSystemMXBean`, default in FIT
/// cross-SDK charts):
///   - `processCpu`      (0..100 %): Process (user + sys) CPU delta / whole-machine CPU delta.
///                       Matches `OperatingSystemMXBean#getProcessCpuLoad() * 100.0`:
///                       https://docs.oracle.com/en/java/javase/17/docs/api/jdk.management/com/sun/management/OperatingSystemMXBean.html#getProcessCpuLoad()
///   - `systemCpu`       (0..100 %): Whole-machine busy CPU delta / whole-machine CPU delta.
///                       Matches `OperatingSystemMXBean#getCpuLoad() * 100.0`
///                       (`#getSystemCpuLoad() * 100.0`).
///
/// Additional CPU and host fields:
///   - `systemCpuSteal`  (0..100 %): Hypervisor steal share of whole-machine CPU time (Linux).
///   - `systemCpuIoWait` (0..100 %): I/O-wait share of whole-machine CPU time (Linux).
///   - `processCpuTotal` (>= 0 %, 100 = 1 busy core): Process CPU delta / wall-clock time.
///   - `processCpuUser`  (>= 0 %, 100 = 1 busy core): User-space CPU delta / wall-clock time.
///   - `processCpuSys`   (>= 0 %, 100 = 1 busy core): Kernel-space CPU delta / wall-clock time.
///   - `maxThreadCpu`    (0..100 %, 100 = 1 busy core): Busiest thread of this process over the
///                       interval. Tracks thread IDs across OS core migrations.
///   - `coresCpu`        (double[], 0..100 % each): Per-logical-CPU non-idle share in ascending
///                       OS `core_id` order (Linux and macOS).
///   - `maxCoreCpu`      (0..100 %): Maximum element of `coresCpu`.
///   - `logicalCpuCount` (uint32): Host logical CPU count.
class metrics_accumulator
{
public:
  [[nodiscard]] auto record(metrics_sample_input sample) -> std::optional<tao::json::value>
  {
    if (!initialized_) {
      baseline_ = std::move(sample);
      initialized_ = true;
      return std::nullopt;
    }

    constexpr double k_bytes_per_mb = 1024.0 * 1024.0;

    tao::json::value report = {
      { "memRssUsedMB", round2(static_cast<double>(sample.memory.resident) / k_bytes_per_mb) },
      { "memVmsMB", round2(static_cast<double>(sample.memory.size) / k_bytes_per_mb) },
      { "threadCount", sample.thread_cpu_usec.size() },
    };

    if (sample.logical_cpu_count > 0 &&
        sample.logical_cpu_count < std::numeric_limits<std::uint32_t>::max()) {
      report["logicalCpuCount"] = sample.logical_cpu_count;
    }

    const auto proc_user_delta = non_negative_delta(sample.proc_cpu.user, baseline_.proc_cpu.user);
    const auto proc_sys_delta = non_negative_delta(sample.proc_cpu.sys, baseline_.proc_cpu.sys);
    const auto proc_total_delta =
      (proc_user_delta.has_value() && proc_sys_delta.has_value())
        ? std::optional<std::uint64_t>{ *proc_user_delta + *proc_sys_delta }
        : std::nullopt;

    // 1. Host-normalized CPU metrics (0..100%), matching Java's OperatingSystemMXBean.
    if (const auto sys_total =
          non_negative_delta(sample.system_cpu.total, baseline_.system_cpu.total);
        sys_total.has_value() && *sys_total > 0) {
      if (proc_total_delta.has_value()) {
        report["processCpu"] = clamped_percent(*proc_total_delta, *sys_total);
      }
      if (const auto sys_busy =
            non_negative_delta(sample.system_cpu.busy, baseline_.system_cpu.busy);
          sys_busy.has_value()) {
        report["systemCpu"] = clamped_percent(*sys_busy, *sys_total);
      }
      if (sample.system_cpu.iowait.has_value() && baseline_.system_cpu.iowait.has_value()) {
        if (const auto iowait_delta =
              non_negative_delta(*sample.system_cpu.iowait, *baseline_.system_cpu.iowait);
            iowait_delta.has_value()) {
          report["systemCpuIoWait"] = clamped_percent(*iowait_delta, *sys_total);
        }
      }
      if (sample.system_cpu.steal.has_value() && baseline_.system_cpu.steal.has_value()) {
        if (const auto steal_delta =
              non_negative_delta(*sample.system_cpu.steal, *baseline_.system_cpu.steal);
            steal_delta.has_value()) {
          report["systemCpuSteal"] = clamped_percent(*steal_delta, *sys_total);
        }
      }
    }

    // 2. Per-logical-CPU utilisation vector (0..100% per core) + chartable maxCoreCpu scalar.
    if (!sample.system_cpu.cores.empty() &&
        sample.system_cpu.cores.size() == baseline_.system_cpu.cores.size()) {
      tao::json::value cores_array = tao::json::empty_array;
      cores_array.get_array().reserve(sample.system_cpu.cores.size());
      double max_core_pct = 0.0;
      bool all_cores_valid = true;

      for (std::size_t i = 0; i < sample.system_cpu.cores.size(); ++i) {
        if (sample.system_cpu.cores[i].core_id != baseline_.system_cpu.cores[i].core_id) {
          all_cores_valid = false;
          break;
        }
        const auto core_total =
          non_negative_delta(sample.system_cpu.cores[i].total, baseline_.system_cpu.cores[i].total);
        const auto core_busy =
          non_negative_delta(sample.system_cpu.cores[i].busy, baseline_.system_cpu.cores[i].busy);
        if (!core_total.has_value() || *core_total == 0 || !core_busy.has_value()) {
          all_cores_valid = false;
          break;
        }
        const double pct = clamped_percent(*core_busy, *core_total);
        max_core_pct = std::max(max_core_pct, pct);
        cores_array.get_array().emplace_back(pct);
      }

      if (all_cores_valid) {
        report["coresCpu"] = std::move(cores_array);
        report["maxCoreCpu"] = max_core_pct;
      }
    }

    // 3. Wall-clock cumulative process & per-thread CPU rates (100% = 1 busy core).
    const auto wall_usec = std::chrono::duration_cast<std::chrono::microseconds>(
                             sample.sampled_at - baseline_.sampled_at)
                             .count();
    if (wall_usec > 0) {
      const double wall_ms = static_cast<double>(wall_usec) / 1'000.0;
      if (proc_total_delta.has_value()) {
        report["processCpuTotal"] =
          unclamped_percent(static_cast<double>(*proc_total_delta), wall_ms);
      }
      if (proc_user_delta.has_value()) {
        report["processCpuUser"] =
          unclamped_percent(static_cast<double>(*proc_user_delta), wall_ms);
      }
      if (proc_sys_delta.has_value()) {
        report["processCpuSys"] = unclamped_percent(static_cast<double>(*proc_sys_delta), wall_ms);
      }

      double max_thread_pct = 0.0;
      bool matched_any_thread = false;
      const auto wall_usec_u64 = static_cast<std::uint64_t>(wall_usec);
      for (const auto& [tid, cpu_usec_now] : sample.thread_cpu_usec) {
        const auto prev_it = baseline_.thread_cpu_usec.find(tid);
        if (prev_it == baseline_.thread_cpu_usec.end()) {
          continue;
        }
        if (const auto thread_delta = non_negative_delta(cpu_usec_now, prev_it->second);
            thread_delta.has_value()) {
          matched_any_thread = true;
          max_thread_pct = std::max(max_thread_pct, clamped_percent(*thread_delta, wall_usec_u64));
        }
      }
      if (matched_any_thread) {
        report["maxThreadCpu"] = max_thread_pct;
      }
    }

    if (sample.heap_used_bytes.has_value()) {
      report["memHeapUsedMB"] =
        round2(static_cast<double>(*sample.heap_used_bytes) / k_bytes_per_mb);
    }
    if (sample.free_swap_bytes.has_value()) {
      report["freeSwapSizeMB"] =
        round2(static_cast<double>(*sample.free_swap_bytes) / k_bytes_per_mb);
    }

    baseline_ = std::move(sample);
    return report;
  }

private:
  [[nodiscard]] static auto round2(double value) noexcept -> double
  {
    return std::round(value * 100.0) / 100.0;
  }

  [[nodiscard]] static auto non_negative_delta(std::uint64_t now, std::uint64_t then) noexcept
    -> std::optional<std::uint64_t>
  {
    if (now < then) {
      return std::nullopt;
    }
    return now - then;
  }

  [[nodiscard]] static auto clamped_percent(std::uint64_t part, std::uint64_t whole) noexcept
    -> double
  {
    return round2(
      std::clamp(100.0 * static_cast<double>(part) / static_cast<double>(whole), 0.0, 100.0));
  }

  [[nodiscard]] static auto unclamped_percent(double part, double whole) noexcept -> double
  {
    return round2(std::max(0.0, 100.0 * part / whole));
  }

  bool initialized_{ false };
  metrics_sample_input baseline_{};
};

} // namespace fit_cxx
