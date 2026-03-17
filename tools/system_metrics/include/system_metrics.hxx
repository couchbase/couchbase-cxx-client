/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <cstdint>
#include <functional>

#include <sys/types.h>

namespace system_metrics
{

#ifdef _WIN32
using sm_pid_t = std::uint32_t;
#else
using sm_pid_t = pid_t;
#endif

struct proc_cpu_t {
  proc_cpu_t() = default;

  proc_cpu_t(std::uint64_t start, std::uint64_t u, std::uint64_t s)
    : start_time(start)
    , user(u)
    , sys(s)
  {
  }

  /// The time (in milliseconds) at which the process was started
  /// NOTE: The Linux backend adds boot_time to this value
  std::uint64_t start_time{ 0 };
  /// The amount (in milliseconds) the process spent running in userspace
  /// since the process was started
  std::uint64_t user{ 0 };
  /// The amount (in milliseconds) the process spent running in kernel space
  /// since the process was started
  std::uint64_t sys{ 0 };
};

struct proc_mem_t {
  std::uint64_t size{ 0 };     // virtual memory size
  std::uint64_t resident{ 0 }; // resident set size
  std::uint64_t share{ 0 };
  std::uint64_t page_faults{ 0 };
};

using sm_tid_t = sm_pid_t;

/// Callback invoked for each thread belonging to a process.
/// Parameters:
///  - sm_tid_t:      thread identifier
///  - std::uint64_t: per-thread user CPU time in microseconds
///  - std::uint64_t: per-thread system CPU time in microseconds
using IterateThreadCallback = std::function<void(sm_tid_t, std::uint64_t, std::uint64_t)>;

enum class OperatingSystem : std::uint8_t {
  /// The correct backend to use on the running platform
  Native,
  /// The backend used on MacOSX
  Apple,
  /// The backend used on Linux (based on /proc)
  Linux,
  /// The backend used on Windows
  Windows
};

class system_metrics
{
public:
  system_metrics() = default;
  system_metrics(const system_metrics&) = default;
  system_metrics(system_metrics&&) = delete;
  auto operator=(const system_metrics&) -> system_metrics& = default;
  auto operator=(system_metrics&&) -> system_metrics& = delete;
  virtual ~system_metrics() = default;

  virtual void iterate_proc_threads(sm_pid_t pid, IterateThreadCallback callback) = 0;

  [[nodiscard]] virtual auto get_system_boot_time() const -> std::uint64_t = 0;
  [[nodiscard]] virtual auto get_logical_cpu_count() const -> std::uint32_t = 0;
  [[nodiscard]] virtual auto get_proc_memory(sm_pid_t pid) const -> proc_mem_t = 0;
  [[nodiscard]] virtual auto get_proc_cpu(sm_pid_t pid) const -> proc_cpu_t = 0;
};

} // namespace system_metrics
