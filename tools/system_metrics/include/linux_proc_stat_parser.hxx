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

#include "system_metrics.hxx"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <system_error>

namespace system_metrics
{

/// Parses the aggregate `cpu` line and per-core `cpu<N>` lines from Linux `/proc/stat` text.
///
/// Format documented in proc_stat(5):
///   cpu  user nice system idle iowait irq softirq steal guest guest_nice
///   cpu0 user nice system idle iowait irq softirq steal guest guest_nice
/// `guest` and `guest_nice` are already included in `user` and `nice` by the kernel, so only
/// columns 1..8 (`user` through `steal`) are summed.
[[nodiscard]] inline auto
parse_linux_proc_stat_cpu(std::string_view content, std::uint64_t ticks_per_sec) -> system_cpu_t
{
  system_cpu_t result{};
  if (ticks_per_sec == 0) {
    return result;
  }

  constexpr std::size_t k_idle_col = 3;   // 0-based index in columns[0..7] -> field 4 (idle)
  constexpr std::size_t k_iowait_col = 4; // field 5 (iowait)
  constexpr std::size_t k_steal_col = 7;  // field 8 (steal)
  constexpr std::size_t k_required_cols = 8;

  const auto ticks_to_msec = [ticks_per_sec](std::uint64_t ticks) -> std::uint64_t {
    return ((ticks / ticks_per_sec) * 1'000ULL) +
           (((ticks % ticks_per_sec) * 1'000ULL) / ticks_per_sec);
  };

  while (!content.empty()) {
    const auto nl = content.find('\n');
    auto line = (nl == std::string_view::npos) ? content : content.substr(0, nl);
    if (nl == std::string_view::npos) {
      content = {};
    } else {
      content.remove_prefix(nl + 1);
    }

    while (!line.empty() && line.back() == '\r') {
      line.remove_suffix(1);
    }
    while (!line.empty() && line.front() == ' ') {
      line.remove_prefix(1);
    }
    if (line.empty()) {
      continue;
    }

    const auto sp = line.find(' ');
    if (sp == std::string_view::npos) {
      continue;
    }
    const auto label = line.substr(0, sp);
    const bool is_aggregate = (label == "cpu");
    bool is_core = false;
    std::uint32_t core_id = 0;
    if (!is_aggregate && label.size() > 3 && label.substr(0, 3) == "cpu") {
      const auto id_sv = label.substr(3);
      const auto [id_ptr, id_ec] =
        std::from_chars(id_sv.data(), id_sv.data() + id_sv.size(), core_id, 10);
      is_core = (id_ec == std::errc{} && id_ptr == id_sv.data() + id_sv.size());
    }
    if (!is_aggregate && !is_core) {
      continue;
    }

    auto rest = line.substr(sp + 1);
    std::array<std::uint64_t, k_required_cols> cols{};
    bool ok = true;
    for (std::size_t col = 0; col < k_required_cols; ++col) {
      while (!rest.empty() && rest.front() == ' ') {
        rest.remove_prefix(1);
      }
      if (rest.empty()) {
        ok = false;
        break;
      }
      const auto next_sp = rest.find(' ');
      const auto tok = (next_sp == std::string_view::npos) ? rest : rest.substr(0, next_sp);
      std::uint64_t val = 0;
      const auto [ptr, ec] = std::from_chars(tok.data(), tok.data() + tok.size(), val, 10);
      if (ec != std::errc{} || ptr != tok.data() + tok.size()) {
        ok = false;
        break;
      }
      cols[col] = val;
      if (next_sp == std::string_view::npos) {
        rest = {};
      } else {
        rest.remove_prefix(next_sp + 1);
      }
    }
    if (!ok) {
      continue;
    }

    std::uint64_t total_ticks = 0;
    for (const auto v : cols) {
      total_ticks += v;
    }
    const std::uint64_t idle_ticks = cols[k_idle_col] + cols[k_iowait_col];
    const std::uint64_t busy_ticks = total_ticks - idle_ticks;

    if (is_aggregate) {
      result.total = ticks_to_msec(total_ticks);
      result.busy = ticks_to_msec(busy_ticks);
      result.iowait = ticks_to_msec(cols[k_iowait_col]);
      result.steal = ticks_to_msec(cols[k_steal_col]);
    } else {
      result.cores.push_back(
        core_cpu_t{ core_id, ticks_to_msec(busy_ticks), ticks_to_msec(total_ticks) });
    }
  }

  return result;
}

} // namespace system_metrics
