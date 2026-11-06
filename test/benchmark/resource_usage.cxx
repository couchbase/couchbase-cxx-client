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

#include "resource_usage.hxx"

#include <chrono>
#include <cstdint>
#include <ratio>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
// psapi.h needs the types of windows.h.
#include <psapi.h>
#else
#include <sys/resource.h>
#endif

namespace couchbase::test::bench
{
auto
current_resource_usage() -> resource_usage
{
  resource_usage u;
#if defined(_WIN32)
  // FILETIME counts 100 ns intervals.
  using filetime_ticks = std::chrono::duration<std::uint64_t, std::ratio<1, 10'000'000>>;
  const auto ticks = [](const FILETIME& t) {
    return filetime_ticks{ (static_cast<std::uint64_t>(t.dwHighDateTime) << 32) | t.dwLowDateTime };
  };
  FILETIME creation{};
  FILETIME exit{};
  FILETIME kernel{};
  FILETIME user{};
  if (GetProcessTimes(GetCurrentProcess(), &creation, &exit, &kernel, &user) != 0) {
    u.cpu_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(ticks(kernel) + ticks(user)).count());
  }
  PROCESS_MEMORY_COUNTERS memory{};
  if (GetProcessMemoryInfo(GetCurrentProcess(), &memory, sizeof(memory)) != 0) {
    u.max_rss_bytes = memory.PeakWorkingSetSize;
  }
#else
  rusage r{};
  if (getrusage(RUSAGE_SELF, &r) == 0) {
    const auto ns = [](const timeval& t) {
      return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds{ t.tv_sec } +
                                                             std::chrono::microseconds{ t.tv_usec })
          .count());
    };
    u.cpu_ns = ns(r.ru_utime) + ns(r.ru_stime);
    u.context_switches =
      static_cast<std::uint64_t>(r.ru_nvcsw) + static_cast<std::uint64_t>(r.ru_nivcsw);
#if defined(__APPLE__)
    // Bytes on macOS, KiB elsewhere.
    u.max_rss_bytes = static_cast<std::uint64_t>(r.ru_maxrss);
#else
    u.max_rss_bytes = static_cast<std::uint64_t>(r.ru_maxrss) * 1024U;
#endif
  }
#endif
  return u;
}
} // namespace couchbase::test::bench
