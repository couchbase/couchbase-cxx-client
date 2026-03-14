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

#include "windows_system_metrics.hxx"

#include <cstdint>
#include <limits>

#if defined(_WIN32)

#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <psapi.h>
#include <tlhelp32.h>

namespace system_metrics
{

namespace
{

class SystemConstants
{
public:
  [[nodiscard]] static auto instance() -> const SystemConstants&
  {
    static SystemConstants instance;
    return instance;
  }

  const std::uint64_t boot_time;

protected:
  SystemConstants()
    : boot_time(get_boot_time())
  {
  }

  static auto get_boot_time() -> std::uint64_t
  {
    ULONGLONG tick_count = GetTickCount64();
    FILETIME ft_now{};
    GetSystemTimeAsFileTime(&ft_now);

    ULARGE_INTEGER uli_now{};
    uli_now.LowPart = ft_now.dwLowDateTime;
    uli_now.HighPart = ft_now.dwHighDateTime;

    ULONGLONG now_millis = uli_now.QuadPart / 10000ULL;
    ULONGLONG boot_millis = now_millis - tick_count;

    // Convert from Windows epoch (1601-01-01) to Unix epoch (1970-01-01)
    constexpr ULONGLONG windows_to_unix_epoch_millis = 11644473600000ULL;
    return (boot_millis - windows_to_unix_epoch_millis) / 1000ULL;
  }
};

inline auto
filetime2msec(FILETIME ft) -> std::uint64_t
{
  ULARGE_INTEGER uli{};
  uli.LowPart = ft.dwLowDateTime;
  uli.HighPart = ft.dwHighDateTime;
  return uli.QuadPart / 10000ULL;
}

inline auto
filetime2unix_msec(FILETIME ft) -> std::uint64_t
{
  constexpr ULONGLONG windows_to_unix_epoch_millis = 11644473600000ULL;
  return filetime2msec(ft) - windows_to_unix_epoch_millis;
}

} // namespace

auto
WindowsSystemMetrics::get_system_boot_time() const -> std::uint64_t
{
  return SystemConstants::instance().boot_time;
}

auto
WindowsSystemMetrics::get_logical_cpu_count() const -> std::uint32_t
{
  SYSTEM_INFO sys_info{};
  GetNativeSystemInfo(&sys_info);
  return sys_info.dwNumberOfProcessors;
}

auto
WindowsSystemMetrics::get_proc_cpu(sm_pid_t pid) const -> proc_cpu_t
{
  HANDLE hProcess =
    OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, static_cast<DWORD>(pid));
  if (hProcess == NULL) {
    return proc_cpu_t{};
  }

  FILETIME CreationTime{};
  FILETIME ExitTime{};
  FILETIME KernelTime{};
  FILETIME UserTime{};

  BOOL result = GetProcessTimes(hProcess, &CreationTime, &ExitTime, &KernelTime, &UserTime);
  CloseHandle(hProcess);

  if (!result) {
    return proc_cpu_t{};
  }

  std::uint64_t start_time = filetime2unix_msec(CreationTime);
  std::uint64_t user = filetime2msec(UserTime);
  std::uint64_t sys = filetime2msec(KernelTime);

  return { start_time, user, sys };
}

auto
WindowsSystemMetrics::get_proc_memory(sm_pid_t pid) const -> proc_mem_t
{
  proc_mem_t procmem{};

  HANDLE hProcess =
    OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, static_cast<DWORD>(pid));
  if (hProcess == NULL) {
    return procmem;
  }

  PROCESS_MEMORY_COUNTERS pmc{};
  if (GetProcessMemoryInfo(hProcess, &pmc, sizeof(pmc))) {
    procmem.size = pmc.PagefileUsage;
    procmem.resident = pmc.WorkingSetSize;
    procmem.page_faults = pmc.PageFaultCount;
  }

  CloseHandle(hProcess);
  return procmem;
}

void
WindowsSystemMetrics::iterate_proc_threads(sm_pid_t pid, IterateThreadCallback callback)
{
  HANDLE hProcess =
    OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, static_cast<DWORD>(pid));
  if (hProcess == NULL) {
    return;
  }

  HANDLE hSnapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
  if (hSnapshot == INVALID_HANDLE_VALUE) {
    CloseHandle(hProcess);
    return;
  }

  THREADENTRY32 te{};
  te.dwSize = sizeof(te);

  if (!Thread32First(hSnapshot, &te)) {
    CloseHandle(hSnapshot);
    CloseHandle(hProcess);
    return;
  }

  DWORD target_pid = static_cast<DWORD>(pid);

  do {
    if (te.th32OwnerProcessID == target_pid) {
      HANDLE hThread = OpenThread(THREAD_QUERY_INFORMATION, FALSE, te.th32ThreadID);
      if (hThread == NULL) {
        callback(te.th32ThreadID,
                 std::numeric_limits<std::uint64_t>::max(),
                 std::numeric_limits<std::uint64_t>::max());
        continue;
      }

      FILETIME CreationTime{};
      FILETIME ExitTime{};
      FILETIME KernelTime{};
      FILETIME UserTime{};

      if (GetThreadTimes(hThread, &CreationTime, &ExitTime, &KernelTime, &UserTime)) {
        std::uint64_t user = filetime2msec(UserTime) * 1000ULL;
        std::uint64_t sys = filetime2msec(KernelTime) * 1000ULL;
        callback(te.th32ThreadID, user, sys);
      } else {
        callback(te.th32ThreadID,
                 std::numeric_limits<std::uint64_t>::max(),
                 std::numeric_limits<std::uint64_t>::max());
      }

      CloseHandle(hThread);
    }
  } while (Thread32Next(hSnapshot, &te));

  CloseHandle(hSnapshot);
  CloseHandle(hProcess);
}

} // namespace system_metrics

#else

namespace system_metrics
{

auto
WindowsSystemMetrics::get_system_boot_time() const -> std::uint64_t
{
  return 0;
}

auto
WindowsSystemMetrics::get_logical_cpu_count() const -> std::uint32_t
{
  return std::numeric_limits<std::uint32_t>::max();
}

auto
WindowsSystemMetrics::get_proc_cpu(sm_pid_t /*pid*/) const -> proc_cpu_t
{
  return proc_cpu_t{};
}

auto
WindowsSystemMetrics::get_proc_memory(sm_pid_t /*pid*/) const -> proc_mem_t
{
  return proc_mem_t{};
}

void
WindowsSystemMetrics::iterate_proc_threads(sm_pid_t /*pid*/, IterateThreadCallback callback)
{
  callback(0, std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max());
}

} // namespace system_metrics

#endif
