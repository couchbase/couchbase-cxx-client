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

#include "core/logger/configuration.hxx"
#include "core/logger/logger.hxx"

#include <spdlog/details/log_msg.h>
#include <spdlog/sinks/base_sink.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#ifndef _WIN32
#include <csignal>

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#if defined(__SANITIZE_THREAD__)
#define COUCHBASE_LOGGER_EXIT_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COUCHBASE_LOGGER_EXIT_TSAN 1
#endif
#endif

#ifndef _WIN32
namespace
{
bool in_forked_child{ false };
} // namespace

// LeakSanitizer does not scan the forking thread's stack in a forked child, so everything the test
// framework holds there reads as leaked when the child exits; only the children skip the check.
extern "C" auto
__lsan_is_turned_off() -> int // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
{
  return in_forked_child ? 1 : 0;
}
#endif

namespace couchbase::test
{
namespace
{
#if !defined(_WIN32) && !defined(COUCHBASE_LOGGER_EXIT_TSAN)
constexpr std::chrono::seconds child_deadline{ 20 };

// The child's exit status, 128 plus the signal that ended it, or -1 when it could not be reaped or
// did not exit within the deadline and was killed.
auto
wait_for_child(pid_t child_pid) -> int
{
  const auto deadline = std::chrono::steady_clock::now() + child_deadline;
  int status{};
  while (std::chrono::steady_clock::now() < deadline) {
    const auto rc = waitpid(child_pid, &status, WNOHANG);
    if (rc == child_pid) {
      return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    }
    if (rc < 0 && errno != EINTR) {
      return -1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 50 });
  }
  kill(child_pid, SIGKILL);
  while (waitpid(child_pid, &status, 0) < 0 && errno == EINTR) {
  }
  return -1;
}

auto
file_logger_configuration(const std::filesystem::path& directory,
                          std::shared_ptr<spdlog::sinks::sink> sink) -> core::logger::configuration
{
  core::logger::configuration configuration{};
  configuration.filename = (directory / "child").string();
  configuration.console = false;
  configuration.log_level = core::logger::level::info;
  configuration.sink = std::move(sink);
  return configuration;
}

std::atomic<bool> before_pool_destroyed{ false };
std::atomic<bool> after_pool_destroyed{ false };

// Statics are destroyed in reverse order of construction. The marker constructed before the logger
// stands in for spdlog's registry and console mutex.
struct exit_marker {
  explicit exit_marker(std::atomic<bool>* destroyed)
    : destroyed_{ destroyed }
  {
  }
  exit_marker(const exit_marker&) = delete;
  exit_marker(exit_marker&&) = delete;
  auto operator=(const exit_marker&) -> exit_marker& = delete;
  auto operator=(exit_marker&&) -> exit_marker& = delete;
  ~exit_marker()
  {
    destroyed_->store(true);
  }

private:
  std::atomic<bool>* destroyed_;
};

// Holds the message until exit has started, then gives static destruction time to run. Exits with
// 0 when the earlier marker is still alive, and 3 when the pool outlived it.
class exit_sink : public spdlog::sinks::base_sink<std::mutex>
{
protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    while (!after_pool_destroyed.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 200 });
    std::_Exit(before_pool_destroyed.load() ? 3 : 0);
  }

  void flush_() override
  {
  }
};

// Takes longer than the periodic flush interval to write a message, then creates `written`.
class slow_sink : public spdlog::sinks::base_sink<std::mutex>
{
public:
  explicit slow_sink(std::filesystem::path written)
    : written_{ std::move(written) }
  {
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1500 });
    std::ofstream{ written_ };
  }

  void flush_() override
  {
  }

private:
  std::filesystem::path written_;
};

void
shutdown_at_exit()
{
  core::logger::shutdown();
}

void
log_and_shutdown_at_exit()
{
  CB_LOG_INFO("logged from an exit hook");
  core::logger::shutdown();
}

// 6 when the child could not set up its loggers or its exit hook.
constexpr int setup_failed{ 6 };

void
create_or_exit(const core::logger::configuration& configuration)
{
  if (core::logger::create_file_logger(configuration).has_value()) {
    std::_Exit(setup_failed);
  }
}

void
register_or_exit(void (&hook)())
{
  if (std::atexit(hook) != 0) {
    std::_Exit(setup_failed);
  }
}

auto
read_file(const std::filesystem::path& path) -> std::string
{
  std::ifstream file(path);
  return { std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>() };
}

// Redirects the child's stderr to `path`, where spdlog reports logger errors.
void
capture_stderr(const std::filesystem::path& path)
{
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd < 0 || dup2(fd, STDERR_FILENO) < 0) {
    std::_Exit(5);
  }
}
#endif

void
the_pool_drains_at_exit_before_statics_created_before_it([[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory =
    std::filesystem::temp_directory_path() / ("cb-logger-exit-" + std::to_string(getpid()));
  std::filesystem::create_directories(directory);
  const auto child_pid = fork();
  if (child_pid == 0) {
    in_forked_child = true;
    static const exit_marker before{ &before_pool_destroyed };
    create_or_exit(file_logger_configuration(directory, std::make_shared<exit_sink>()));
    static const exit_marker after{ &after_pool_destroyed };
    CB_LOG_INFO("queued at exit");
    // 4 when the message was never written
    std::exit(4);
  }
  if (child_pid < 0) {
    fail("fork() succeeds");
  }
  const auto status = wait_for_child(child_pid);
  std::filesystem::remove_all(directory);
  assert_eq(status,
            0,
            "the queue drains after statics created after the pool are destroyed, and before the "
            "ones created before it");
#endif
}

void
a_shutdown_registered_before_the_pool_runs_at_exit([[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory =
    std::filesystem::temp_directory_path() / ("cb-logger-exit-" + std::to_string(getpid()));
  std::filesystem::create_directories(directory);
  const auto child_pid = fork();
  if (child_pid == 0) {
    in_forked_child = true;
    // A synchronous logger constructs spdlog's registry, which shutdown() needs, without a pool.
    auto configuration = file_logger_configuration(directory, nullptr);
    configuration.unit_test = true;
    create_or_exit(configuration);
    register_or_exit(shutdown_at_exit);
    configuration.unit_test = false;
    create_or_exit(configuration);
    core::logger::create_console_logger();
    std::exit(0);
  }
  if (child_pid < 0) {
    fail("fork() succeeds");
  }
  const auto status = wait_for_child(child_pid);
  std::filesystem::remove_all(directory);
  // The hook runs after statics constructed after it are destroyed, so a pool owner among them is
  // released twice. Without AddressSanitizer the second release does not always crash.
  assert_eq(status, 0, "shutdown() from an exit hook leaves the child's exit clean");
#endif
}

void
the_flusher_stops_before_the_pool_drains_at_exit([[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory =
    std::filesystem::temp_directory_path() / ("cb-logger-exit-" + std::to_string(getpid()));
  std::filesystem::create_directories(directory);
  const auto stderr_path = directory / "stderr";
  const auto child_pid = fork();
  if (child_pid == 0) {
    in_forked_child = true;
    capture_stderr(stderr_path);
    create_or_exit(
      file_logger_configuration(directory, std::make_shared<slow_sink>(directory / "written")));
    CB_LOG_INFO("written slowly at exit");
    std::exit(0);
  }
  if (child_pid < 0) {
    fail("fork() succeeds");
  }
  const auto status = wait_for_child(child_pid);
  const auto errors = read_file(stderr_path);
  const bool written = std::filesystem::exists(directory / "written");
  std::filesystem::remove_all(directory);
  assert_eq(status, 0, "the child exits");
  assert_true(written, "the slow sink receives the message at exit");
  assert_eq(errors, std::string{}, "draining at exit reports no logger error");
#endif
}

#if !defined(_WIN32) && !defined(COUCHBASE_LOGGER_EXIT_TSAN)
// Forks a child that creates a logger with `first_logger`, registers a hook that logs and calls
// shutdown(), then creates an async file logger and exits. Requires a clean exit, and returns
// whether the hook's message reached the log. Discriminates only while the forking parent has not
// constructed the guard, so the parent must not use the SDK logger.
template<typename FirstLogger>
auto
exit_hook_message_is_written(FirstLogger first_logger) -> bool
{
  const auto directory =
    std::filesystem::temp_directory_path() / ("cb-logger-exit-" + std::to_string(getpid()));
  std::filesystem::create_directories(directory);
  const auto stderr_path = directory / "stderr";
  const auto child_pid = fork();
  if (child_pid == 0) {
    in_forked_child = true;
    capture_stderr(stderr_path);
    first_logger(directory);
    register_or_exit(log_and_shutdown_at_exit);
    create_or_exit(file_logger_configuration(directory, nullptr));
    std::exit(0);
  }
  if (child_pid < 0) {
    fail("fork() succeeds");
  }
  const auto status = wait_for_child(child_pid);
  const auto errors = read_file(stderr_path);
  std::string logs{};
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().filename().string().rfind("child", 0) == 0) {
      logs += read_file(entry.path());
    }
  }
  std::filesystem::remove_all(directory);
  assert_eq(status, 0, "the child exits");
  assert_eq(errors, std::string{}, "the exit hook reports no logger error");
  return logs.find("logged from an exit hook") != std::string::npos;
}
#endif

void
a_message_from_an_exit_hook_registered_after_a_sync_file_logger_is_written(
  [[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  assert_true(exit_hook_message_is_written([](const std::filesystem::path& directory) {
                auto configuration = file_logger_configuration(directory, nullptr);
                configuration.unit_test = true;
                create_or_exit(configuration);
              }),
              "the async logger writes the exit hook's message");
#endif
}

void
a_message_from_an_exit_hook_registered_after_a_console_logger_is_written(
  [[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  assert_true(exit_hook_message_is_written([](const std::filesystem::path& /* directory */) {
                core::logger::create_console_logger();
              }),
              "the async logger writes the exit hook's message");
#endif
}

void
a_message_from_an_exit_hook_registered_after_setting_log_levels_is_written(
  [[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_EXIT_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  assert_true(exit_hook_message_is_written([](const std::filesystem::path& /* directory */) {
                core::logger::set_log_levels(core::logger::level::info);
              }),
              "the async logger writes the exit hook's message");
#endif
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(the_pool_drains_at_exit_before_statics_created_before_it), {}, timeout::integration },
      { CASE(a_shutdown_registered_before_the_pool_runs_at_exit), {}, timeout::integration },
      { CASE(the_flusher_stops_before_the_pool_drains_at_exit), {}, timeout::integration },
      { CASE(a_message_from_an_exit_hook_registered_after_a_sync_file_logger_is_written),
        {},
        timeout::integration },
      { CASE(a_message_from_an_exit_hook_registered_after_a_console_logger_is_written),
        {},
        timeout::integration },
      { CASE(a_message_from_an_exit_hook_registered_after_setting_log_levels_is_written),
        {},
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
