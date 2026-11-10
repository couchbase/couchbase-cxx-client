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

#include <couchbase/fork_event.hxx>

#include <spdlog/common.h>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/null_sink.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#ifndef _WIN32
#include <csignal>

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#if defined(__SANITIZE_THREAD__)
#define COUCHBASE_LOGGER_FORK_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COUCHBASE_LOGGER_FORK_TSAN 1
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
#ifndef _WIN32
constexpr std::chrono::seconds child_deadline{ 20 };

// The child's exit status, 128 plus the signal that ended it, or -1 when it did not exit within
// the deadline and was killed.
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
file_logger_configuration(bool unit_test, const std::filesystem::path& filename)
  -> core::logger::configuration
{
  core::logger::configuration configuration{};
  configuration.unit_test = unit_test;
  configuration.filename = filename.string();
  configuration.console = false;
  configuration.log_level = core::logger::level::info;
  return configuration;
}

auto
read_logs(const std::filesystem::path& base) -> std::string
{
  std::string contents;
  for (const auto& entry : std::filesystem::directory_iterator(base.parent_path())) {
    if (entry.path().filename().string().rfind(base.filename().string(), 0) == 0) {
      std::ifstream file(entry.path());
      contents.append(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    }
  }
  return contents;
}

// A writer thread logs numbered lines until destroyed, so the fork lands on a held sink mutex and
// a non-empty file buffer.
class logging_parent
{
public:
  explicit logging_parent(bool unit_test)
  {
    std::filesystem::create_directories(directory_);
    if (auto error = core::logger::create_file_logger(
          file_logger_configuration(unit_test, directory_ / "parent"));
        error) {
      fail(fmt::format("the parent logger is created: {}", *error));
    }
    writer_ = std::thread([this]() {
      while (!stop_) {
        CB_LOG_INFO("from the writer thread of {}, line {}", getpid(), sequence_ + 1);
        ++sequence_;
        // bounds the log volume below the rotation size
        std::this_thread::sleep_for(std::chrono::microseconds{ 50 });
      }
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 10 };
    while (sequence_ < 500 && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
    }
    if (sequence_ < 500) {
      stop();
      fail("the writer thread logs numbered lines before the fork");
    }
  }

  logging_parent(const logging_parent&) = delete;
  logging_parent(logging_parent&&) = delete;
  auto operator=(const logging_parent&) -> logging_parent& = delete;
  auto operator=(logging_parent&&) -> logging_parent& = delete;

  ~logging_parent()
  {
    stop();
  }

  [[nodiscard]] auto directory() const -> const std::filesystem::path&
  {
    return directory_;
  }

private:
  void stop()
  {
    stop_ = true;
    writer_.join();
    core::logger::shutdown();
    std::filesystem::remove_all(directory_);
  }

  inline static std::atomic<int> instances_{ 0 };
  std::filesystem::path directory_{
    std::filesystem::temp_directory_path() /
    fmt::format("couchbase-logger-fork-{}-{}", getpid(), ++instances_)
  };
  std::atomic<bool> stop_{ false };
  std::atomic<std::size_t> sequence_{ 0 };
  std::thread writer_{};
};

struct forked_child {
  pid_t pid{};
  int status{};
};

template<typename Child>
auto
fork_while_logging(const Child& child) -> forked_child
{
  core::logger::notify_fork(couchbase::fork_event::prepare);
  const auto child_pid = fork();
  if (child_pid == 0) {
    in_forked_child = true;
    core::logger::notify_fork(couchbase::fork_event::child);
    std::exit(child());
  }
  core::logger::notify_fork(couchbase::fork_event::parent);
  if (child_pid < 0) {
    fail("fork() succeeds");
  }
  return { child_pid, wait_for_child(child_pid) };
}

// flush() only posts to the async logger, so poll until the worker has written the marker.
auto
read_until(const std::filesystem::path& base, std::string_view marker) -> std::string
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 10 };
  while (true) {
    core::logger::flush();
    auto logs = read_logs(base);
    if (logs.find(marker) != std::string::npos || std::chrono::steady_clock::now() >= deadline) {
      return logs;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ 10 });
  }
}

// A numbered writer line found twice was buffered at the fork and written again by the child.
auto
has_duplicate_writer_line(const std::string& logs) -> bool
{
  std::set<std::string> seen;
  std::size_t pos = 0;
  while ((pos = logs.find("from the writer thread of ", pos)) != std::string::npos) {
    const auto end = logs.find('\n', pos);
    if (end == std::string::npos) {
      break; // incomplete last line would match a shorter number
    }
    if (!seen.insert(logs.substr(pos, end - pos)).second) {
      return true;
    }
    pos = end;
  }
  return false;
}

// Every line carries [pid,thread], so a line the child wrote names its pid.
auto
written_by(const std::string& logs, pid_t pid) -> bool
{
  return logs.find(fmt::format("[{},", pid)) != std::string::npos;
}
#endif

void
child_creates_its_own_logger(bool unit_test)
{
#if defined(_WIN32)
  static_cast<void>(unit_test);
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_FORK_TSAN)
  static_cast<void>(unit_test);
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const logging_parent parent{ unit_test };
  const auto child_base = parent.directory() / "child";
  CB_LOG_INFO("parent {} logs on the forking thread", getpid());
  const auto child = fork_while_logging([&child_base, unit_test]() {
    if (core::logger::create_file_logger(file_logger_configuration(unit_test, child_base))) {
      return 2;
    }
    CB_LOG_INFO("child {} logs to its own file", getpid());
    core::logger::shutdown();
    return 0;
  });
  assert_eq(child.status, 0, "the child exits within the deadline with status 0");
  const auto child_logs = read_logs(child_base);
  assert_contains(child_logs, "logs to its own file", "the child writes its own file");
#ifdef __linux__
  // the only thread of the child that logs has the child's pid as its thread id
  const auto line_start = child_logs.rfind('\n', child_logs.find("logs to its own file")) + 1;
  assert_contains(child_logs.substr(line_start, child_logs.find('\n', line_start) - line_start),
                  fmt::format("[{},{}]", child.pid, child.pid),
                  "the child's own line carries its thread id");
#endif
  CB_LOG_INFO("parent {} still logs after the fork", getpid());
  const auto parent_logs = read_until(parent.directory() / "parent", "still logs after the fork");
  assert_contains(parent_logs, "still logs after the fork", "the parent keeps logging to its file");
  assert_false(written_by(parent_logs, child.pid), "the child writes no line to the parent's file");
  assert_false(has_duplicate_writer_line(parent_logs), "no buffered line is written twice");
#endif
}

void
child_without_a_logger_writes_nothing(bool unit_test)
{
#ifdef _WIN32
  static_cast<void>(unit_test);
  skip("fork() is not available on Windows");
#else
  const logging_parent parent{ unit_test };
  const auto child = fork_while_logging([]() {
    CB_LOG_INFO("child {} has no logger", getpid());
    return core::logger::is_initialized() ? 2 : 0;
  });
  assert_eq(child.status, 0, "the child exits within the deadline, without a logger");
  CB_LOG_INFO("parent {} logs after the child exited", getpid());
  const auto parent_logs = read_until(parent.directory() / "parent", "logs after the child exited");
  assert_contains(
    parent_logs, "logs after the child exited", "the parent keeps logging to its file");
  assert_false(written_by(parent_logs, child.pid),
               "the child writes no line to the parent's file, closing line included");
  assert_false(has_duplicate_writer_line(parent_logs), "no buffered line is written twice");
#endif
}

// Every message fails with an error report of report_size bytes.
class throwing_sink : public spdlog::sinks::base_sink<std::mutex>
{
public:
  explicit throwing_sink(std::size_t report_size)
    : report_size_{ report_size }
  {
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    throw spdlog::spdlog_ex(std::string(report_size_, 'x'));
  }

  void flush_() override
  {
  }

private:
  std::size_t report_size_;
};

// Runs at scope exit, so a failed assertion still joins threads and restores stderr.
template<typename Cleanup>
class scope_exit
{
public:
  explicit scope_exit(Cleanup cleanup)
    : cleanup_{ std::move(cleanup) }
  {
  }
  scope_exit(const scope_exit&) = delete;
  scope_exit(scope_exit&&) = delete;
  auto operator=(const scope_exit&) -> scope_exit& = delete;
  auto operator=(scope_exit&&) -> scope_exit& = delete;
  ~scope_exit()
  {
    cleanup_();
  }

private:
  Cleanup cleanup_;
};

// The writer thread blocks in the error handler on a stderr pipe that is drained once the child has
// started, or after a fallback that lets a prepare waiting for the handler's lock proceed. A
// handler lock that the fork does not hold therefore stays locked in the child, where any sink
// error then deadlocks.
void
a_child_forked_during_an_error_report_does_not_deadlock_in_the_error_handler(
  [[maybe_unused]] context& ctx)
{
#if defined(_WIN32)
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_FORK_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-error", getpid());
  std::filesystem::create_directories(directory);
  std::array<int, 2> pipe_fds{};
  std::array<int, 2> child_started{};
  // the child is forked, not exec'd, so close-on-exec would not matter; pipe2() is not on macOS
  if (pipe(pipe_fds.data()) != 0 ||      // NOLINT(android-cloexec-pipe)
      pipe(child_started.data()) != 0) { // NOLINT(android-cloexec-pipe)
    fail("the pipes are created");
  }
#ifdef __linux__
  const auto capacity = static_cast<std::size_t>(fcntl(pipe_fds[1], F_GETPIPE_SZ));
#else
  const std::size_t capacity = 64 * 1024; // the largest pipe buffer on macOS
#endif
  // twice the pipe's capacity, so the writer blocks with the handler's lock held
  auto throwing_configuration = [&directory, report_size = 2 * capacity](const char* name) {
    auto configuration = file_logger_configuration(true, directory / name);
    configuration.sink = std::make_shared<throwing_sink>(report_size);
    return configuration;
  };
  const int saved_stderr = fcntl(STDERR_FILENO, F_DUPFD_CLOEXEC, 0);
  if (saved_stderr < 0 || core::logger::create_file_logger(throwing_configuration("parent"))) {
    core::logger::shutdown();
    for (const int fd :
         { pipe_fds[0], pipe_fds[1], child_started[0], child_started[1], saved_stderr }) {
      close(fd);
    }
    fail("the parent logger is created with stderr saved");
  }
  dup2(pipe_fds[1], STDERR_FILENO);
  close(pipe_fds[1]);
  std::atomic<bool> writer_returned{ false };
  std::thread writer([&writer_returned]() {
    CB_LOG_INFO("parent {} reports a sink error", getpid());
    writer_returned = true;
  });
  std::atomic<bool> writer_blocked{ false };
  std::atomic<int> writer_at_drain{ 0 }; // 1 still blocked, 2 already returned
  std::thread drainer([read_fd = pipe_fds[0],
                       started_fd = child_started[0],
                       &writer_blocked,
                       &writer_returned,
                       &writer_at_drain]() {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 10 };
    while (!writer_blocked && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
    }
    pollfd started{ started_fd, POLLIN, 0 };
    poll(&started, 1, 2000);
    writer_at_drain = writer_returned ? 2 : 1;
    std::array<char, 4096> buffer{};
    while (read(read_fd, buffer.data(), buffer.size()) > 0) {
    }
  });
  const scope_exit cleanup{ [&]() {
    writer.join();
    core::logger::shutdown();
    dup2(saved_stderr, STDERR_FILENO);
    close(saved_stderr);
    drainer.join();
    close(pipe_fds[0]);
    close(child_started[0]);
    close(child_started[1]);
    std::filesystem::remove_all(directory);
  } };
  // a byte in the pipe means the writer is in the handler, where the report keeps it until the
  // drain
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 10 };
  int pending = 0;
  while ((ioctl(pipe_fds[0], FIONREAD, &pending) != 0 || pending == 0) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 1 });
  }
  writer_blocked = true;
  if (pending == 0) {
    fail("the writer blocks in the error handler");
  }
  const auto child = fork_while_logging([&throwing_configuration, started_fd = child_started[1]]() {
    if (write(started_fd, "x", 1) != 1) {
      return 3;
    }
    if (core::logger::create_file_logger(throwing_configuration("child"))) {
      return 2;
    }
    CB_LOG_INFO("child {} reports a sink error", getpid());
    core::logger::shutdown();
    return 0;
  });
  assert_eq(writer_at_drain.load(), 1, "the writer stays blocked in the handler until the drain");
  assert_eq(
    child.status, 0, "the child takes the error handler's lock and exits within the deadline");
#endif
}

// The forking thread still caches logger A after another thread replaced it with B, so A is in
// neither the registry nor file_logger when the child drops that cache.
void
a_child_drops_a_replaced_logger_cached_by_the_forking_thread([[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("fork() is not available on Windows");
#else
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-replaced", getpid());
  std::filesystem::create_directories(directory);
  if (core::logger::create_file_logger(file_logger_configuration(true, directory / "a"))) {
    fail("logger A is created");
  }
  CB_LOG_INFO("parent {} caches logger A", getpid());
  std::optional<std::string> replace_error;
  std::thread([&directory, &replace_error]() {
    replace_error =
      core::logger::create_file_logger(file_logger_configuration(true, directory / "b"));
  }).join();
  if (replace_error) {
    fail(fmt::format("logger B replaces A: {}", *replace_error));
  }
  const auto child = fork_while_logging([]() {
    CB_LOG_INFO("child {} drops the cached logger", getpid());
    return 0;
  });
  assert_eq(child.status, 0, "the child exits within the deadline");
  const auto a_logs = read_logs(directory / "a");
  core::logger::shutdown();
  std::filesystem::remove_all(directory);
  assert_contains(a_logs, "caches logger A", "the parent wrote to A");
  assert_false(written_by(a_logs, child.pid), "the child writes no closing line to A's file");
#endif
}

// Nothing but the periodic flusher writes an info line to the file, so the line reaches it only if
// the parent's flusher restarted after the fork.
void
the_parent_keeps_flushing_periodically_after_a_fork([[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("fork() is not available on Windows");
#else
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-flusher", getpid());
  std::filesystem::create_directories(directory);
  if (core::logger::create_file_logger(file_logger_configuration(true, directory / "parent"))) {
    fail("the parent logger is created");
  }
  const auto child = fork_while_logging([]() {
    return 0;
  });
  CB_LOG_INFO("parent {} logs after the fork", getpid());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{ 5 };
  auto logs = read_logs(directory / "parent");
  while (logs.find("logs after the fork") == std::string::npos &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 50 });
    logs = read_logs(directory / "parent");
  }
  core::logger::shutdown();
  std::filesystem::remove_all(directory);
  assert_eq(child.status, 0, "the child exits within the deadline");
  assert_contains(logs, "logs after the fork", "the periodic flusher writes the line to the file");
#endif
}

// Regression: the rotating file sink writes its closing line from its noexcept destructor, so a
// closing line that could not be written terminated the process that released the logger. The
// child sets its file size limit to zero, so that every write that reaches the file fails, and
// fills the stream's buffer so that the closing line is the write that flushes it.
void
releasing_a_logger_whose_closing_line_fails_does_not_terminate([[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("RLIMIT_FSIZE is not available on Windows");
#elif defined(COUCHBASE_LOGGER_FORK_TSAN)
  // create_file_logger() starts the periodic flusher in the child.
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory =
    std::filesystem::temp_directory_path() / fmt::format("couchbase-logger-fork-{}-full", getpid());
  std::filesystem::create_directories(directory);
  const auto child = fork_while_logging([&directory]() {
    signal(SIGXFSZ, SIG_IGN);
    rlimit limit{};
    if (getrlimit(RLIMIT_FSIZE, &limit) != 0) {
      return 2;
    }
    limit.rlim_cur = 0;
    if (setrlimit(RLIMIT_FSIZE, &limit) != 0) {
      return 2;
    }
    if (core::logger::create_file_logger(file_logger_configuration(true, directory / "full"))) {
      return 3;
    }
    auto failures = std::make_shared<std::atomic<int>>(0);
    core::logger::get()->set_error_handler([failures](const std::string& /* message */) {
      failures->fetch_add(1);
    });
    // Each failed flush empties the buffer, so the messages between two failures are those that
    // fit in it. One fewer leaves too little room for the closing line, which is longer.
    int sent = 0;
    int first = -1;
    int second = -1;
    while (second < 0 && sent < 100'000) {
      const int before = failures->load();
      CB_LOG_INFO_RAW("x");
      ++sent;
      if (failures->load() > before) {
        (first < 0 ? first : second) = sent;
      }
    }
    if (second - first < 2) {
      return 4;
    }
    for (int i = 0; i < second - first - 1; ++i) {
      CB_LOG_INFO_RAW("x");
    }
    core::logger::reset();
    return 0;
  });
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);
  assert_eq(child.status,
            0,
            "the child survives the release (2: no rlimit, 3: no logger, 4: no buffer, 134: "
            "terminated)");
#endif
}

// Logs through the SDK when it is destroyed, as a sink, or an object it holds, can.
class logs_when_destroyed_sink final : public spdlog::sinks::base_sink<std::mutex>
{
public:
  explicit logs_when_destroyed_sink(std::shared_ptr<std::atomic<int>> destroyed)
    : destroyed_{ std::move(destroyed) }
  {
  }
  logs_when_destroyed_sink(const logs_when_destroyed_sink&) = delete;
  logs_when_destroyed_sink(logs_when_destroyed_sink&&) = delete;
  auto operator=(const logs_when_destroyed_sink&) -> logs_when_destroyed_sink& = delete;
  auto operator=(logs_when_destroyed_sink&&) -> logs_when_destroyed_sink& = delete;

  ~logs_when_destroyed_sink() override
  {
    CB_LOG_INFO_RAW("destroyed");
    CB_LOG_PROTOCOL("{}", "destroyed");
    destroyed_->fetch_add(1, std::memory_order_relaxed);
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
  }

  void flush_() override
  {
  }

private:
  std::shared_ptr<std::atomic<int>> destroyed_;
};

// Regression: replacing a logger released its last reference under the logger mutex, so a sink
// logging from its destructor deadlocked reset(). Runs in a forked child bounded by a timeout.
void
a_sink_that_logs_when_destroyed_does_not_deadlock_a_reset([[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("fork() is not available on Windows");
#elif defined(COUCHBASE_LOGGER_FORK_TSAN)
  skip("ThreadSanitizer kills a child that starts a thread after a multithreaded fork");
#else
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-destroyed", getpid());
  std::filesystem::create_directories(directory);
  const auto child = fork_while_logging([&directory]() {
    auto destroyed = std::make_shared<std::atomic<int>>(0);
    core::logger::configuration file{};
    file.unit_test = true;
    file.console = false;
    file.sink = std::make_shared<logs_when_destroyed_sink>(destroyed);
    if (core::logger::create_file_logger(file)) {
      _Exit(2);
    }
    file.sink.reset();
    core::logger::configuration protocol{};
    protocol.filename = (directory / "protocol").string();
    protocol.unit_test = true;
    protocol.console = false;
    protocol.sink = std::make_shared<logs_when_destroyed_sink>(destroyed);
    if (core::logger::create_protocol_logger(protocol)) {
      _Exit(3);
    }
    protocol.sink.reset();
    core::logger::reset();
    if (destroyed->load(std::memory_order_relaxed) != 2) {
      _Exit(4);
    }
    _Exit(0);
    return 0;
  });
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);
  assert_eq(child.status,
            0,
            "reset() destroys both sinks in the child (2, 3: no logger; 4: sink kept alive; -1: "
            "killed at the deadline)");
#endif
}

// The threads hold a lock of spdlog's registry for most of their loop, iterating over many
// registered loggers or replacing the file logger, so a fork that does not hold the registry leaves
// that lock taken in the child, where notify_fork(child) then deadlocks.
void
a_child_forked_while_other_threads_use_the_logger_registry_exits([[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("fork() is not available on Windows");
#else
  constexpr int forks{ 20 };
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-registry", getpid());
  std::filesystem::create_directories(directory);
  if (core::logger::create_file_logger(file_logger_configuration(true, directory / "parent"))) {
    fail("the parent logger is created");
  }
  std::atomic<bool> stop{ false };
  std::vector<std::thread> threads;
  const scope_exit cleanup{ [&]() {
    stop = true;
    for (auto& thread : threads) {
      thread.join();
    }
    core::logger::shutdown();
    std::filesystem::remove_all(directory);
  } };
  for (int i = 0; i < 1000; ++i) {
    core::logger::register_spdlog_logger(std::make_shared<spdlog::logger>(
      fmt::format("registered-{}", i), std::make_shared<spdlog::sinks::null_sink_mt>()));
  }
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&stop, name = fmt::format("registry-user-{}", i)]() {
      while (!stop) {
        core::logger::set_log_levels(core::logger::level::info);
        static_cast<void>(core::logger::check_log_levels(core::logger::level::info));
        static_cast<void>(core::logger::get_lowest_log_level());
        core::logger::register_spdlog_logger(
          std::make_shared<spdlog::logger>(name, std::make_shared<spdlog::sinks::null_sink_mt>()));
        core::logger::unregister_spdlog_logger(name);
        std::this_thread::yield();
      }
    });
  }
  threads.emplace_back([&stop]() {
    while (!stop) {
      core::logger::create_blackhole_logger();
      core::logger::reset();
      std::this_thread::yield();
    }
  });
  for (int i = 0; i < forks; ++i) {
    const auto child = fork_while_logging([]() {
      return 0;
    });
    assert_eq(child.status, 0, fmt::format("child {} of {} exits within the deadline", i, forks));
  }
#endif
}

// One thread creates the asynchronous logger and shuts it down in a loop, holding spdlog's thread
// pool lock while it creates or joins the pool, so a fork that does not hold the registry leaves
// that lock taken in the child, where notify_fork(child) then deadlocks.
void
a_child_forked_while_another_thread_creates_and_shuts_down_the_async_logger_exits(
  [[maybe_unused]] context& ctx)
{
#ifdef _WIN32
  skip("fork() is not available on Windows");
#else
  constexpr int forks{ 100 };
  const auto directory = std::filesystem::temp_directory_path() /
                         fmt::format("couchbase-logger-fork-{}-async-pool", getpid());
  std::filesystem::create_directories(directory);
  std::atomic<bool> stop{ false };
  std::atomic<int> cycles{ 0 };
  // written by the cycler only, read after it is joined
  std::optional<std::string> create_error{};
  std::thread cycler{ [&stop, &cycles, &create_error, base = directory / "async"]() {
    while (!stop) {
      create_error = core::logger::create_file_logger(file_logger_configuration(false, base));
      if (create_error) {
        return;
      }
      core::logger::shutdown();
      ++cycles;
    }
  } };
  const scope_exit cleanup{ [&]() {
    stop = true;
    if (cycler.joinable()) {
      cycler.join();
    }
    core::logger::shutdown();
    std::filesystem::remove_all(directory);
  } };
  for (int i = 0; i < forks; ++i) {
    const auto child = fork_while_logging([]() {
      return 0;
    });
    assert_eq(child.status, 0, fmt::format("child {} of {} exits within the deadline", i, forks));
  }
  stop = true;
  cycler.join();
  assert_false(create_error.has_value(),
               fmt::format("the asynchronous logger is created: {}", create_error.value_or("")));
  assert_true(cycles > 0, "the asynchronous logger is created and shut down at least once");
#endif
}

void
a_child_forked_while_another_thread_logs_creates_its_own_logger([[maybe_unused]] context& ctx)
{
  child_creates_its_own_logger(true);
}

void
a_child_forked_while_the_async_logger_runs_creates_its_own_logger([[maybe_unused]] context& ctx)
{
  child_creates_its_own_logger(false);
}

void
a_child_without_a_logger_writes_nothing_to_the_parent_file([[maybe_unused]] context& ctx)
{
  child_without_a_logger_writes_nothing(true);
}

void
a_child_of_the_async_logger_without_a_logger_writes_nothing_to_the_parent_file(
  [[maybe_unused]] context& ctx)
{
  child_without_a_logger_writes_nothing(false);
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_child_forked_while_another_thread_logs_creates_its_own_logger),
        {},
        timeout::integration },
      { CASE(a_child_forked_while_the_async_logger_runs_creates_its_own_logger),
        {},
        timeout::integration },
      { CASE(a_child_without_a_logger_writes_nothing_to_the_parent_file),
        {},
        timeout::integration },
      { CASE(a_child_of_the_async_logger_without_a_logger_writes_nothing_to_the_parent_file),
        {},
        timeout::integration },
      { CASE(a_child_drops_a_replaced_logger_cached_by_the_forking_thread),
        {},
        timeout::integration },
      { CASE(the_parent_keeps_flushing_periodically_after_a_fork), {}, timeout::integration },
      { CASE(a_sink_that_logs_when_destroyed_does_not_deadlock_a_reset), {}, timeout::integration },
      { CASE(a_child_forked_during_an_error_report_does_not_deadlock_in_the_error_handler),
        {},
        timeout::integration },
      { CASE(a_child_forked_while_other_threads_use_the_logger_registry_exits),
        {},
        timeout::integration },
      { CASE(a_child_forked_while_another_thread_creates_and_shuts_down_the_async_logger_exits),
        {},
        timeout::integration },
      { CASE(releasing_a_logger_whose_closing_line_fails_does_not_terminate),
        {},
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
