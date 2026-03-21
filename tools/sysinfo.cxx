/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "sysinfo.hxx"

#include "system_metrics_reporter.hxx"

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/core.h>
#include <tao/json.hpp>

#include <array>
#include <chrono>
#include <csignal>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace
{

inline auto
human_readable_bytes(std::uint64_t bytes) -> auto
{
  static std::array units = { "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
  std::size_t unit_index = 0;
  auto value = static_cast<double>(bytes);
  while (value >= 1024.0 && unit_index < 5) {
    value /= 1024.0;
    ++unit_index;
  }

  std::string exact_size{};
  if (unit_index != 0) {
    exact_size = fmt::format(" ({}B)", bytes);
  }

  return fmt::format("{:.2f}{}{}", value, units[unit_index], exact_size);
}

template<typename Unit>
inline auto
to_chrono(std::uint64_t value) -> Unit
{
  if (value >= static_cast<std::uint64_t>(Unit::max().count())) {
    return Unit::max();
  }
  return Unit{ static_cast<typename Unit::rep>(value) };
}

[[nodiscard]] auto
human_readable_duration(std::chrono::microseconds us) -> std::string
{
  using duration_days = std::chrono::duration<std::int64_t, std::ratio<86400>>;
  auto days = std::chrono::duration_cast<duration_days>(us);
  us -= days;
  auto hours = std::chrono::duration_cast<std::chrono::hours>(us);
  us -= hours;
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(us);
  us -= minutes;
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(us);
  us -= seconds;
  auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(us);
  us -= milliseconds;

  auto buffer = fmt::memory_buffer();
  bool first = true;

  if (days.count() > 0) {
    fmt::format_to(std::back_inserter(buffer), "{}d", days.count());
    first = false;
  }
  if (hours.count() > 0) {
    if (!first) {
      fmt::format_to(std::back_inserter(buffer), " ");
    }
    fmt::format_to(std::back_inserter(buffer), "{}h", hours.count());
    first = false;
  }
  if (minutes.count() > 0) {
    if (!first) {
      fmt::format_to(std::back_inserter(buffer), " ");
    }
    fmt::format_to(std::back_inserter(buffer), "{}m", minutes.count());
    first = false;
  }
  if (seconds.count() > 0) {
    if (!first) {
      fmt::format_to(std::back_inserter(buffer), " ");
    }
    fmt::format_to(std::back_inserter(buffer), "{}s", seconds.count());
    first = false;
  }
  if (milliseconds.count() > 0) {
    if (!first) {
      fmt::format_to(std::back_inserter(buffer), " ");
    }
    fmt::format_to(std::back_inserter(buffer), "{}ms", milliseconds.count());
    first = false;
  }
  if (us.count() > 0 || buffer.size() == 0) {
    if (!first) {
      fmt::format_to(std::back_inserter(buffer), " ");
    }
    fmt::format_to(std::back_inserter(buffer), "{}us", us.count());
  }

  return { buffer.data(), buffer.size() };
}

inline auto
thread_time_to_string(std::uint64_t microseconds_value) -> std::string
{
  auto duration = to_chrono<std::chrono::microseconds>(microseconds_value);
  return human_readable_duration(duration);
}

struct thread_info {
  system_metrics::sm_pid_t tid{};
  std::uint64_t utime{};
  std::uint64_t stime{};
};

struct system_info {
  std::chrono::system_clock::time_point now{ std::chrono::system_clock::now() };
  std::uint64_t system_boot_time{};
  std::uint32_t logical_cpu_count{};

  system_metrics::sm_pid_t pid{};
  system_metrics::proc_mem_t memory{};
  system_metrics::proc_cpu_t cpu{};
  std::vector<thread_info> threads{};

  [[nodiscard]] auto pid_string() const -> std::string
  {
    return fmt::format("{}", static_cast<std::uint64_t>(pid));
  }

  [[nodiscard]] auto threads_string() const -> std::string
  {
    if (threads.empty()) {
      return "  <not available>";
    }

    auto buffer = fmt::memory_buffer();

    fmt::format_to(
      std::back_inserter(buffer), "  {:<8s}  {:<20s}  {:<20s}\n", "", "user", "system");
    for (const auto& thread : threads) {
      fmt::format_to(std::back_inserter(buffer),
                     "  {:>8d}  {:<20s}  {:<20s}\n",
                     thread.tid,
                     thread_time_to_string(thread.utime),
                     thread_time_to_string(thread.stime));
    }

    return { buffer.data(), buffer.size() };
  }

  [[nodiscard]] auto cpu_string() const -> std::string
  {
    auto buffer = fmt::memory_buffer();
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    auto start_time_ms = to_chrono<std::chrono::milliseconds>(cpu.start_time);
    auto start_time_s = std::chrono::duration_cast<std::chrono::seconds>(start_time_ms);
    fmt::format_to(std::back_inserter(buffer),
                   "  Start:  {:%FT%T} ({} ago, {})\n",
                   fmt::localtime(start_time_s.count()),
                   human_readable_duration(now_ms - start_time_ms),
                   start_time_ms);
    fmt::format_to(
      std::back_inserter(buffer), "  System: {}\n", to_chrono<std::chrono::milliseconds>(cpu.sys));
    fmt::format_to(
      std::back_inserter(buffer), "  User:   {}\n", to_chrono<std::chrono::milliseconds>(cpu.user));

    return { buffer.data(), buffer.size() };
  }

  [[nodiscard]] auto memory_string() const -> std::string
  {
    auto buffer = fmt::memory_buffer();

    fmt::format_to(
      std::back_inserter(buffer), "  Size:        {}\n", human_readable_bytes(memory.size));
    fmt::format_to(
      std::back_inserter(buffer), "  Resident:    {}\n", human_readable_bytes(memory.resident));
    fmt::format_to(
      std::back_inserter(buffer), "  Share:       {}\n", human_readable_bytes(memory.share));
    fmt::format_to(std::back_inserter(buffer), "  Page faults: {}\n", memory.page_faults);

    return { buffer.data(), buffer.size() };
  }

  [[nodiscard]] auto now_string() const -> std::string
  {
    auto now_time = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
    return fmt::format("{:%FT%T} ({})", fmt::localtime(now_time.count()), now_time);
  }

  [[nodiscard]] auto system_boot_time_string() const -> std::string
  {
    auto boot_time_s = to_chrono<std::chrono::seconds>(system_boot_time);
    std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> boot_time_tp{
      boot_time_s
    };
    return fmt::format(
      "{:%FT%T} ({}, {} ago)",
      fmt::localtime(boot_time_s.count()),
      boot_time_s,
      human_readable_duration(
        std::chrono::duration_cast<std::chrono::microseconds>(now - boot_time_tp)));
  }
};
} // namespace

template<>
struct tao::json::traits<system_info> {
  template<template<typename...> class Traits>
  static void assign(tao::json::basic_value<Traits>& v, const system_info& info)
  {
    v = {
      { "pid", info.pid },
      { "system_boot_time_s", info.system_boot_time },
      { "logical_cpu_count", info.logical_cpu_count },
      { "now_ms",
        std::chrono::duration_cast<std::chrono::milliseconds>(info.now.time_since_epoch())
          .count() },
      {
        "memory",
        {
          { "size", info.memory.size },
          { "resident", info.memory.resident },
          { "share", info.memory.share },
          { "page_faults", info.memory.page_faults },
        },
      },
      {
        "cpu",
        {
          { "start_time_ms", info.cpu.start_time },
          { "sys_ms", info.cpu.sys },
          { "user_ms", info.cpu.user },
        },
      },
    };
    tao::json::value threads = tao::json::empty_array;
    for (const auto& thread : info.threads) {
      threads.emplace_back(tao::json::value{
        { "tid", thread.tid },
        { "utime_us", thread.utime },
        { "stime_us", thread.stime },
      });
    }
    v["threads"] = threads;
  }
};

namespace
{
volatile std::sig_atomic_t running = 1;

void
sigint_handler(int /* signal */)
{
  running = 0;
}

class sysinfo_app : public CLI::App
{
public:
  sysinfo_app()
    : CLI::App("Display sysinfo information.", "sysinfo")
  {
    add_flag("--json", "Dump sysinfo in JSON format.");
    add_flag("--pretty-json", "Dump sysinfo in JSON format and prettify the output.");
    add_option("--pid", pid_, "PID to inspect")
#ifdef _WIN32
      ->default_val(_getpid());
#else
      ->default_val(getpid());
#endif
    add_option("--poll", poll_interval_, "Poll interval in seconds");
  }

  void execute() const
  {
    std::signal(SIGINT, sigint_handler);
    std::signal(SIGTERM, sigint_handler);

    if (pid_ > std::numeric_limits<system_metrics::sm_pid_t>::max()) {
      fmt::print(stderr,
                 "PID value {} is out of range (maximum allowed is {}).\n",
                 pid_,
                 std::numeric_limits<system_metrics::sm_pid_t>::max());
      return;
    }

    const auto valid_pid = static_cast<system_metrics::sm_pid_t>(pid_);

    do {
      system_info info{};

      auto reporter = system_metrics::system_metrics_reporter::create();
      info.system_boot_time = reporter->get_system_boot_time();
      info.logical_cpu_count = reporter->get_logical_cpu_count();

      info.pid = valid_pid;
      info.cpu = reporter->get_proc_cpu(info.pid);
      info.memory = reporter->get_proc_memory(info.pid);

      reporter->iterate_proc_threads(info.pid, [&info](auto tid, auto utime, auto stime) -> auto {
        info.threads.emplace_back(thread_info{ tid, utime, stime });
      });

      if (bool pretty_json = count("--pretty-json") > 0; pretty_json || count("--json") > 0) {
        tao::json::value json = info;
        fmt::print(
          stdout, "{}\n", pretty_json ? tao::json::to_string(json, 2) : tao::json::to_string(json));
      } else {
        fmt::print(stdout, "Logical CPU count: {}\n", info.logical_cpu_count);
        fmt::print(stdout, "Now:               {}\n", info.now_string());
        fmt::print(stdout, "System boot time:  {}\n", info.system_boot_time_string());
        fmt::print(stdout, "PID:               {}\n", info.pid_string());
        fmt::print(stdout, "\n");

        fmt::print(stdout, "CPU:\n{}\n", info.cpu_string());
        fmt::print(stdout, "Memory:\n{}\n", info.memory_string());
        fmt::print(stdout, "Threads:\n{}\n", info.threads_string());
      }
      fflush(stdout);
      if (poll_interval_ == 0) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::seconds(poll_interval_));
    } while (running != 0);
  }

private:
  std::uint64_t pid_{};
  std::uint64_t poll_interval_{ 0 };
};
} // namespace

namespace cbc
{
auto
make_sysinfo_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<sysinfo_app>();
}

auto
execute_sysinfo_command(const CLI::App* app) -> int
{
  if (const auto* sysinfo = dynamic_cast<const sysinfo_app*>(app); sysinfo != nullptr) {
    sysinfo->execute();
    return 0;
  }
  return 1;
}
} // namespace cbc
