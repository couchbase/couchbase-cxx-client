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

#include "linux_system_metrics.hxx"
#include <cstdint>

#if defined(__linux__)

#include <cerrno>
#include <chrono>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

namespace system_metrics
{

namespace
{
auto
split(std::string_view sv, char delim, bool allow_empty = true) -> std::vector<std::string_view>
{
  std::vector<std::string_view> result;
  while (true) {
    const auto m = sv.find(delim);
    if (m == std::string_view::npos) {
      break;
    }

    if (allow_empty || m != 0) {
      result.emplace_back(sv.data(), m);
    }
    sv.remove_prefix(m + 1);
    if (!allow_empty) {
      while (!sv.empty() && sv.front() == delim) {
        sv.remove_prefix(1);
      }
    }
  }
  if (!sv.empty()) {
    result.emplace_back(sv);
  }
  return result;
}

auto
stoull_sv(std::string_view sv, std::size_t* idx = nullptr, int base = 10) -> std::uint64_t
{
  std::uint64_t result{ 0 };
  auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result, base);

  if (ec == std::errc::invalid_argument) {
    throw std::invalid_argument("stoull_sv: invalid argument");
  }
  if (ec == std::errc::result_out_of_range) {
    throw std::out_of_range("stoull_sv: out of range");
  }
  if (idx != nullptr) {
    *idx = static_cast<std::size_t>(ptr - sv.data());
  }
  return result;
}

auto
load_file_impl(const std::string& name) -> std::string
{
  std::ifstream file(name);
  if (!file.is_open()) {
    // Opening the file failed; rely on errno set by the OS open call.
    throw std::system_error(
      errno, std::system_category(), "load_file_impl(" + name + ") failed");
  }

  std::ostringstream oss;
  oss << file.rdbuf();

  // Treat EOF as success; only fail if badbit or failbit are set.
  if (!file.bad() && !file.fail()) {
    return oss.str();
  }

  throw std::system_error(std::make_error_code(std::errc::io_error),
                          "load_file_impl(" + name + ") failed");
}

auto
load_file(const std::filesystem::path& path, std::chrono::microseconds waittime) -> std::string
{
  const auto name = path.string();

  const auto timeout = std::chrono::steady_clock::now() + waittime;
  do {
    std::string content;
    bool success = false;
    try {
      content = load_file_impl(name);
      success = true;
    } catch (const std::system_error& error) {
      const auto& code = error.code();
      if (code.category() != std::system_category()) {
        throw std::system_error(code.value(), code.category(), "load_file(" + name + ") failed");
      }

      switch (code.value()) {
        case static_cast<int>(std::errc::no_such_file_or_directory):
          // we might want to retry
          break;
        default:
          throw std::system_error(code.value(), code.category(), "load_file(" + name + ") failed");
      }
    }

    if (success) {
      return content;
    }
    if (waittime.count() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  } while (std::chrono::steady_clock::now() < timeout);
  if (waittime.count() > 0) {
    throw std::system_error(static_cast<int>(std::errc::no_such_file_or_directory),
                            std::system_category(),
                            "load_file(" + name + ") failed (with retry)");
  }
  throw std::system_error(static_cast<int>(std::errc::no_such_file_or_directory),
                          std::system_category(),
                          "load_file(" + name + ") failed");
}

auto
get_proc_root() -> std::filesystem::path
{
  return std::filesystem::path{ "/proc" };
}

void
tokenize_file_line_by_line(
  sm_pid_t pid,
  const char* filename,
  const std::function<bool(const std::vector<std::string_view>&)>& callback,
  char delim = ' ')
{
  auto name = get_proc_root();
  if (pid != 0) {
    name = name / std::to_string(pid);
  }

  name = name / filename;
  auto content = load_file(name, std::chrono::microseconds{});
  auto lines = split(content, '\n');
  for (auto line : lines) {
    while (!line.empty() && line.back() == '\r') {
      line.remove_suffix(1);
    }
    if (!callback(split(line, delim, false))) {
      return;
    }
  }
}

/// The content of /proc/[pid]/stat (and /proc/[pid]/task/[tid]/stat) file
/// as described in https://man7.org/linux/man-pages/man5/proc.5.html
constexpr std::size_t stat_pid_index = 1;
constexpr std::size_t stat_name_index = 2;
constexpr std::size_t stat_ppid_index = 4;
constexpr std::size_t stat_minor_faults_index = 10;
constexpr std::size_t stat_major_faults_index = 12;
constexpr std::size_t stat_utime_index = 14;
constexpr std::size_t stat_stime_index = 15;
constexpr std::size_t stat_start_time_index = 22;
constexpr std::size_t stat_rss_index = 24;

struct linux_proc_stat_t {
  sm_pid_t pid{};
  std::uint64_t rss{};
  std::uint64_t minor_faults{};
  std::uint64_t major_faults{};
  std::uint64_t ppid{};
  std::uint64_t start_time{};
  std::uint64_t utime{};
  std::uint64_t stime{};
  std::string name;
};

struct SystemConstants {
  [[nodiscard]] static auto instance() -> const SystemConstants&
  {
    static SystemConstants inst;
    return inst;
  }

  SystemConstants()
    : pagesize(static_cast<std::uint64_t>(getpagesize()))
    , boot_time(get_boot_time())
    , ticks(static_cast<std::uint64_t>(sysconf(_SC_CLK_TCK)))
  {
  }

  static auto get_boot_time() -> std::uint64_t
  {
    std::uint64_t ret = 0;
    tokenize_file_line_by_line(
      0,
      "stat",
      [&ret](const auto& vec) -> auto {
        if (vec.size() > 1 && vec.front() == "btime") {
          ret = std::stoull(std::string(vec[1]));
          return false;
        }
        return true;
      },
      ' ');
    return ret;
  }

  const std::uint64_t pagesize;
  const std::uint64_t boot_time;
  const std::uint64_t ticks;
};

inline auto
pageshift(std::uint64_t x) -> auto
{
  return x * SystemConstants::instance().pagesize;
}

inline auto
linux_tick2msec(std::uint64_t s) -> std::uint64_t
{
  return s * 1'000ULL / SystemConstants::instance().ticks;
}

inline auto
linux_tick2usec(std::uint64_t s) -> std::uint64_t
{
  return s * 1'000'000ULL / SystemConstants::instance().ticks;
}

auto
parse_stat_file(const std::filesystem::path& name, bool use_usec = false) -> linux_proc_stat_t
{
  auto content = load_file(name.generic_string(), std::chrono::microseconds{});
  auto lines = split(content, '\n');
  if (lines.size() > 1) {
    throw std::runtime_error("parse_stat_file(): file " + name.generic_string() +
                             " contained multiple lines!");
  }

  auto line = std::move(content);
  auto fields = split(line, ' ', false);
  if (fields.size() < stat_rss_index) {
    throw std::runtime_error("parse_stat_file(): file " + name.generic_string() +
                             " does not contain enough fields");
  }

  // For some stupid reason the /proc files on linux consists of "formatted
  // ASCII" files so that we need to perform text parsing to pick out the
  // correct values (instead of the "binary" mode used on other systems
  // where you could do an ioctl / read and get the struct populated with
  // the correct values.
  // For "stat" this is extra annoying as space is used as the field
  // separator, but the command line can contain a space it is enclosed
  // in () (so using '\n' instead of ' ' would have made it easier to parse
  // :P ).
  while (fields[1].find(')') == std::string_view::npos) {
    fields[1] = { fields[1].data(), fields[1].size() + fields[2].size() + 1 };
    auto iter = fields.begin();
    iter++;
    iter++;
    fields.erase(iter);
    if (fields.size() < stat_rss_index) {
      throw std::runtime_error("parse_stat_file(): file " + name.generic_string() +
                               " does not contain enough fields");
    }
  }
  // now remove '(' and ')'
  fields[1].remove_prefix(1);
  fields[1].remove_suffix(1);

  // Insert a dummy 0 element so that the index we use map directly
  // to the number specified in
  // https://man7.org/linux/man-pages/man5/proc.5.html
  fields.insert(fields.begin(), "dummy element");
  linux_proc_stat_t ret;
  ret.pid = static_cast<sm_tid_t>(stoull_sv(fields[stat_pid_index]));
  ret.name = std::string{ fields[stat_name_index].data(), fields[stat_name_index].size() };
  ret.ppid = stoull_sv(fields[stat_ppid_index]);
  ret.minor_faults = stoull_sv(fields[stat_minor_faults_index]);
  ret.major_faults = stoull_sv(fields[stat_major_faults_index]);
  if (use_usec) {
    ret.utime = linux_tick2usec(stoull_sv(fields[stat_utime_index]));
    ret.stime = linux_tick2usec(stoull_sv(fields[stat_stime_index]));
  } else {
    ret.utime = linux_tick2msec(stoull_sv(fields[stat_utime_index]));
    ret.stime = linux_tick2msec(stoull_sv(fields[stat_stime_index]));
  }
  ret.start_time = stoull_sv(fields[stat_start_time_index]);
  ret.start_time /= SystemConstants::instance().ticks;
  ret.start_time += SystemConstants::instance().boot_time; /* seconds */
  ret.start_time *= 1'000ULL;                              /* milliseconds */
  ret.rss = stoull_sv(fields[stat_rss_index]);
  return ret;
}

auto
proc_stat_read(sm_pid_t pid) -> linux_proc_stat_t
{
  return parse_stat_file(get_proc_root() / std::to_string(pid) / "stat");
}

} // namespace

auto
LinuxSystemMetrics::get_system_boot_time() const -> std::uint64_t
{
  return SystemConstants::instance().boot_time;
}

auto
LinuxSystemMetrics::get_logical_cpu_count() const -> std::uint32_t
{
  auto count = sysconf(_SC_NPROCESSORS_ONLN);
  if (count < 0) {
    return 0;
  }
  return static_cast<std::uint32_t>(count);
}

auto
LinuxSystemMetrics::get_proc_cpu(sm_pid_t pid) const -> proc_cpu_t
{
  const auto pstat = proc_stat_read(pid);
  return { pstat.start_time, pstat.utime, pstat.stime };
}

auto
LinuxSystemMetrics::get_proc_memory(sm_pid_t pid) const -> proc_mem_t
{
  const auto pstat = proc_stat_read(pid);

  proc_mem_t procmem;
  procmem.page_faults = pstat.minor_faults + pstat.major_faults;

  tokenize_file_line_by_line(
    pid,
    "statm",
    [&procmem](const auto& vec) -> auto {
      // The format of statm is a single line with the following
      // numbers (in pages)
      // size resident shared text lib data dirty
      if (vec.size() > 2) {
        procmem.size = pageshift(std::stoull(std::string(vec[0])));
        procmem.resident = pageshift(std::stoull(std::string(vec[1])));
        procmem.share = pageshift(std::stoull(std::string(vec[2])));
        return false;
      }
      return true;
    },
    ' ');

  return procmem;
}

void
LinuxSystemMetrics::iterate_proc_threads(sm_pid_t pid, IterateThreadCallback callback)
{
  auto dir = std::filesystem::path("/proc") / std::to_string(pid) / "task";

  try {
    for (const auto& p : std::filesystem::directory_iterator(dir)) {
      if (std::filesystem::is_directory(p) && p.path().filename().string().find('.') != 0) {
        auto statfile = p.path() / "stat";
        if (std::filesystem::exists(statfile)) {
          try {
            const auto tid = static_cast<sm_tid_t>(std::stoull(p.path().filename().string()));
            auto info = parse_stat_file(statfile, true);
            callback(tid, info.utime, info.stime);
          } catch (const std::exception&) {
            continue;
          }
        }
      }
    }
  } catch (const std::filesystem::filesystem_error&) {
    // If /proc/<pid>/task cannot be iterated (e.g., PID exited or permission denied),
    // treat as having no threads rather than terminating the process.
  }
}

} // namespace system_metrics

#else

namespace system_metrics
{

auto
LinuxSystemMetrics::get_system_boot_time() const -> std::uint64_t
{
  return 0;
}

auto
LinuxSystemMetrics::get_logical_cpu_count() const -> std::uint32_t
{
  return std::numeric_limits<std::uint32_t>::max();
}

auto
LinuxSystemMetrics::get_proc_cpu(sm_pid_t /*pid*/) const -> proc_cpu_t
{
  return proc_cpu_t{};
}

auto
LinuxSystemMetrics::get_proc_memory(sm_pid_t /*pid*/) const -> proc_mem_t
{
  return proc_mem_t{};
}

void
LinuxSystemMetrics::iterate_proc_threads(sm_pid_t /*pid*/, IterateThreadCallback callback)
{
  callback(0, std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max());
}

} // namespace system_metrics

#endif
