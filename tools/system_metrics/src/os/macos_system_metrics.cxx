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

#include "macos_system_metrics.hxx"

#include <limits>

#if defined(__APPLE__)

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <libproc.h>
#include <mach/host_info.h>
#include <mach/kern_return.h>
#include <mach/mach_error.h>
#include <mach/mach_host.h>
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/mach_traps.h>
#include <mach/shared_region.h>
#include <mach/task.h>
#include <mach/thread_act.h>
#include <mach/thread_info.h>
#include <mach/vm_map.h>
#include <sys/sysctl.h>
#include <unistd.h>

namespace system_metrics
{

constexpr int SM_OK{ 0 };

struct proc_time_t {
  std::uint64_t start_time;
  std::uint64_t user;
  std::uint64_t sys;
  std::uint64_t total;
};

inline auto
tval2msec(time_value_t tval) -> std::uint64_t
{
  return (static_cast<std::uint64_t>(tval.seconds) * 1'000ULL) +
         (static_cast<std::uint64_t>(tval.microseconds) / 1000);
}

inline auto
tv2msec(struct timeval tv) -> std::uint64_t
{
  return (static_cast<std::uint64_t>(tv.tv_sec) * 1'000ULL) +
         (static_cast<std::uint64_t>(tv.tv_usec) / 1'000ULL);
}

/**
 * Class to hold the system sized (constants which never change for the
 * lifetime of the process)
 */
class SystemConstants
{
public:
  [[nodiscard]] static auto instance() -> const SystemConstants&
  {
    static SystemConstants instance;
    return instance;
  }

  const std::uint64_t boot_time;
  const std::uint64_t ticks;

protected:
  SystemConstants()
    : boot_time(get_boot_time())
    , ticks(static_cast<std::uint64_t>(sysconf(_SC_CLK_TCK)))
  {
  }

  auto get_boot_time() -> std::uint64_t
  {
    struct timeval tv_boot_time{};
    size_t len = sizeof(tv_boot_time);
    std::vector<int> mib = { CTL_KERN, KERN_BOOTTIME };

    if (sysctl(mib.data(), static_cast<unsigned int>(mib.size()), &tv_boot_time, &len, nullptr, 0) <
        0) {
      throw std::system_error(std::error_code(errno, std::system_category()),
                              "SystemConstants::get_boot_time(): sysctl");
    }

    return static_cast<std::uint64_t>(tv_boot_time.tv_sec);
  }
};

namespace
{
inline auto
macos_nsec2msec(std::uint64_t s) -> std::uint64_t
{
  return s / 1'000'000ULL;
}

inline auto
macos_nsec2usec(std::uint64_t s) -> std::uint64_t
{
  return s / 1'000ULL;
}

auto
get_proc_times(sm_pid_t pid, proc_time_t* time) -> int
{
  unsigned int count = 0;
  time_value_t utime = { 0, 0 };
  time_value_t stime = { 0, 0 };
  task_basic_info_data_t ti;
  task_thread_times_info_data_t tti;
  task_port_t task = 0;
  task_port_t self = 0;
  kern_return_t status = 0;

  struct proc_taskinfo pti{};
  int sz = proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &pti, sizeof(pti));

  if (sz == sizeof(pti)) {
    time->user = macos_nsec2msec(pti.pti_total_user);
    time->sys = macos_nsec2msec(pti.pti_total_system);
    time->total = time->user + time->sys;
    return SM_OK;
  }

  self = mach_task_self();
  status = task_for_pid(self, pid, &task);
  if (status != KERN_SUCCESS) {
    return static_cast<int>(status);
  }

  count = TASK_BASIC_INFO_COUNT;
  status = task_info(task, TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&ti), &count);
  if (status != KERN_SUCCESS) {
    if (task != self) {
      mach_port_deallocate(self, task);
    }
    return static_cast<int>(status);
  }

  count = TASK_THREAD_TIMES_INFO_COUNT;
  status = task_info(task, TASK_THREAD_TIMES_INFO, reinterpret_cast<task_info_t>(&tti), &count);
  if (status != KERN_SUCCESS) {
    if (task != self) {
      mach_port_deallocate(self, task);
    }
    return static_cast<int>(status);
  }

  time_value_add(&utime, &ti.user_time);
  time_value_add(&stime, &ti.system_time);
  time_value_add(&utime, &tti.user_time);
  time_value_add(&stime, &tti.system_time);

  time->user = tval2msec(utime);
  time->sys = tval2msec(stime);
  time->total = time->user + time->sys;

  if (task != self) {
    mach_port_deallocate(self, task);
  }

  return SM_OK;
}
} // namespace

auto
macos_system_metrics::get_system_boot_time() const -> std::uint64_t
{
  return SystemConstants::instance().boot_time;
}

auto
macos_system_metrics::get_logical_cpu_count() const -> std::uint32_t
{
  std::uint32_t cpu_count = 0;
  size_t len = sizeof(cpu_count);
  if (sysctlbyname("hw.logicalcpu", &cpu_count, &len, nullptr, 0) != 0) {
    throw std::system_error(
      errno, std::system_category(), "get_logical_cpu_count() failed to fetch value");
  }
  return cpu_count;
}

auto
macos_system_metrics::get_proc_cpu(sm_pid_t pid) const -> proc_cpu_t
{
  std::vector<int> mib = { CTL_KERN, KERN_PROC, KERN_PROC_PID, pid };
  kinfo_proc pinfo = {};
  size_t len = sizeof(pinfo);

  if (sysctl(mib.data(), static_cast<unsigned int>(mib.size()), &pinfo, &len, nullptr, 0) < 0) {
    throw std::system_error(std::error_code(errno, std::system_category()),
                            "macos_system_metrics::get_pinfo(): sysctl");
  }

  proc_time_t proctime{};

  int st = get_proc_times(pid, &proctime);
  if (st != SM_OK) {
    std::string msg = "macos_system_metrics::get_proc_time: get_proc_times(): ";
    msg += mach_error_string(static_cast<mach_error_t>(st));
    throw std::runtime_error(msg);
  }

  proctime.start_time = tv2msec(pinfo.kp_proc.p_starttime);
  return { proctime.start_time, proctime.user, proctime.sys };
}

auto
macos_system_metrics::get_proc_memory(sm_pid_t pid) const -> proc_mem_t
{
  proc_mem_t procmem{};
  proc_taskinfo pti{};

  int sz = proc_pidinfo(pid, PROC_PIDTASKINFO, 0, &pti, sizeof(pti));
  if (sz != static_cast<int>(sizeof(pti))) {
    throw std::system_error(
      errno,
      std::system_category(),
      "macos_system_metrics::get_proc_memory(): proc_pidinfo(PROC_PIDTASKINFO)");
  }

  procmem.size = pti.pti_virtual_size;      // virtual memory size (vms) in bytes
  procmem.resident = pti.pti_resident_size; // resident set size (rss) in bytes
  procmem.page_faults =
    static_cast<std::uint64_t>(pti.pti_faults); // number of page faults (pages) in bytes
  return procmem;
}

void
macos_system_metrics::iterate_proc_threads(sm_pid_t pid, IterateThreadCallback callback)
{
  auto self = mach_task_self();
  mach_port_t task = MACH_PORT_NULL;
  auto status = task_for_pid(self, pid, &task);

  thread_array_t threads = nullptr;
  mach_msg_type_number_t count = 0;

  if (status == KERN_SUCCESS) {
    status = task_threads(task, &threads, &count);
  }

  if (status == KERN_SUCCESS) {
    // https://www.gnu.org/software/hurd/gnumach-doc/Thread-Information.html
    for (mach_msg_type_number_t ii = 0; ii < count; ii++) {
      mach_msg_type_number_t info_count = THREAD_EXTENDED_INFO_COUNT;
      thread_extended_info info{};

      status = thread_info(
        threads[ii], THREAD_EXTENDED_INFO, reinterpret_cast<thread_info_t>(&info), &info_count);
      if (status == KERN_SUCCESS) {
        callback(static_cast<sm_tid_t>(threads[ii]),
                 macos_nsec2usec(info.pth_user_time),
                 macos_nsec2usec(info.pth_system_time));
      }
    }
  }

  if (threads != nullptr && count > 0) {
    for (mach_msg_type_number_t ii = 0; ii < count; ii++) {
      if (threads[ii] != MACH_PORT_NULL) {
        mach_port_deallocate(self, threads[ii]);
      }
    }
    vm_deallocate(self, reinterpret_cast<vm_address_t>(threads), sizeof(thread_t) * count);
  }

  if (task != MACH_PORT_NULL) {
    mach_port_deallocate(self, task);
  }
}

} // namespace system_metrics

#else

namespace system_metrics
{

auto
macos_system_metrics::get_system_boot_time() const -> std::uint64_t
{
  return 0;
}

auto
macos_system_metrics::get_logical_cpu_count() const -> std::uint32_t
{
  return std::numeric_limits<std::uint32_t>::max();
}

auto
macos_system_metrics::get_proc_cpu(sm_pid_t /*pid*/) const -> proc_cpu_t
{
  return proc_cpu_t{};
}

auto
macos_system_metrics::get_proc_memory(sm_pid_t /*pid*/) const -> proc_mem_t
{
  return proc_mem_t{};
}

void
macos_system_metrics::iterate_proc_threads(sm_pid_t /*pid*/, IterateThreadCallback /*callback*/)
{
}

} // namespace system_metrics

#endif
