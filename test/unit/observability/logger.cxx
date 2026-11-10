/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/logger.hxx>

#include "core/document_id.hxx"
#include "core/document_id_fmt.hxx"
#include "core/document_id_redaction.hxx"
#include "core/logger/configuration.hxx"
#include "core/logger/logger.hxx"
#include "core/logger/logger_unit_test_api.hxx"
#include "core/logger/redaction.hxx"
#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <spdlog/details/null_mutex.h>
#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/callback_sink.h>
#include <spdlog/sinks/null_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
// Counts how many times its fmt formatter runs, so a case can assert whether the logging macro
// evaluated (formatted) its arguments at all.
std::atomic<int> probe_format_count{ 0 };
struct format_probe {
};
} // namespace
} // namespace couchbase::test

template<>
struct fmt::formatter<couchbase::test::format_probe> : fmt::formatter<std::string_view> {
  auto format(couchbase::test::format_probe /*probe*/, fmt::format_context& ctx) const
    -> decltype(ctx.out())
  {
    couchbase::test::probe_format_count.fetch_add(1, std::memory_order_relaxed);
    return fmt::formatter<std::string_view>::format("probe", ctx);
  }
};

namespace couchbase::test
{
namespace
{
auto
capture_entry(std::string_view msg, couchbase::logger::log_location location) -> std::string
{
  return std::string(msg) + " [" + location.file + ":" + std::to_string(location.line) + " " +
         location.function + "]";
}

// Restores the logger state the cases mutate, however the case leaves. The callback and the levels
// are process-wide: a callback left registered holds a reference to a vector that dies with the
// case, a level left at off is inherited by whatever runs next, and an assertion failure unwinds
// past any restore written at the end of a body.
//
// Each logger's level is saved on its own because the core API offers no per-logger accessor:
// get_lowest_log_level() is a minimum across every registered logger and set_log_levels() then
// writes one value to all of them, so saving and restoring through that pair collapses loggers
// sitting at different levels onto the most verbose of them.
class logger_state_guard
{
public:
  logger_state_guard()
  {
    spdlog::apply_all([this](const std::shared_ptr<spdlog::logger>& logger) {
      levels_.emplace_back(logger->name(), logger->level());
    });
  }

  logger_state_guard(const logger_state_guard&) = delete;
  logger_state_guard(logger_state_guard&&) = delete;
  auto operator=(const logger_state_guard&) -> logger_state_guard& = delete;
  auto operator=(logger_state_guard&&) -> logger_state_guard& = delete;

  ~logger_state_guard()
  {
    couchbase::logger::unregister_log_callback();
    for (const auto& [name, level] : levels_) {
      if (const auto logger = spdlog::get(name); logger != nullptr) {
        logger->set_level(level);
      }
    }
  }

private:
  std::vector<std::pair<std::string, spdlog::level::level_enum>> levels_{};
};

// The annotation wrappers live in the core logger, and this file also drives couchbase::logger
// above. Aliased rather than qualified at every call so the two never read as the same thing.
namespace core_logger = couchbase::core::logger;

// Log redaction is process-wide state, so restore it even if an assertion fails part way through
// a case, otherwise the leak shows up as an unrelated failure elsewhere in the suite.
class scoped_log_redaction
{
public:
  explicit scoped_log_redaction(bool enable)
    : previous_{ core_logger::is_log_redaction_enabled() }
  {
    core_logger::set_log_redaction(enable);
  }

  scoped_log_redaction(const scoped_log_redaction&) = delete;
  scoped_log_redaction(scoped_log_redaction&&) = delete;
  auto operator=(const scoped_log_redaction&) -> scoped_log_redaction& = delete;
  auto operator=(scoped_log_redaction&&) -> scoped_log_redaction& = delete;

  ~scoped_log_redaction()
  {
    core_logger::set_log_redaction(previous_);
  }

private:
  bool previous_;
};

void
reset_stops_the_file_logger([[maybe_unused]] context& ctx)
{
  const auto directory = std::filesystem::temp_directory_path() /
                         ("cxx-logger-reset-" + std::to_string(std::random_device{}()));
  std::filesystem::create_directories(directory);
  core_logger::configuration config{};
  config.filename = (directory / "sdk").string();
  config.unit_test = true;
  config.console = false;
  config.log_level = core_logger::level::trace;
  const auto failure = core_logger::create_file_logger(config);
  const bool created = !failure.has_value() && core_logger::is_initialized() &&
                       core_logger::should_log(core_logger::level::trace);

  core_logger::reset();
  const bool initialized_after_reset = core_logger::is_initialized();
  const bool logs_after_reset = core_logger::should_log(core_logger::level::critical);
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_true(created, "the file logger logs at trace before the reset");
  assert_false(initialized_after_reset, "no file logger is left after the reset");
  assert_false(logs_after_reset, "nothing is logged after the reset");
}

// What a probe_sink observes, kept apart from the sink so that it is still readable once the sink
// is destroyed.
struct probe_state {
  std::mutex mutex{};
  std::condition_variable changed{};
  // While set, a message stays inside the sink until it is cleared or the wait budget runs out.
  bool hold{ false };
  bool entered{ false };
  bool inside{ false };
  bool destroyed{ false };
  bool destroyed_while_inside{ false };
};

// The waits a case makes on another thread, each well under the case budget.
auto
probe_wait() -> std::chrono::milliseconds
{
  return scaled_budget(std::chrono::seconds{ 2 });
}

class probe_sink final : public spdlog::sinks::base_sink<spdlog::details::null_mutex>
{
public:
  explicit probe_sink(std::shared_ptr<probe_state> state)
    : state_{ std::move(state) }
  {
  }

  probe_sink(const probe_sink&) = delete;
  probe_sink(probe_sink&&) = delete;
  auto operator=(const probe_sink&) -> probe_sink& = delete;
  auto operator=(probe_sink&&) -> probe_sink& = delete;

  ~probe_sink() override
  {
    const std::scoped_lock lock(state_->mutex);
    state_->destroyed = true;
    state_->destroyed_while_inside = state_->inside;
    state_->changed.notify_all();
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    // A copy, since the sink can be destroyed while the message waits.
    const auto state = state_;
    std::unique_lock lock(state->mutex);
    state->entered = true;
    state->inside = true;
    state->changed.notify_all();
    state->changed.wait_for(lock, probe_wait(), [&state]() {
      return !state->hold;
    });
    state->inside = false;
  }

  void flush_() override
  {
  }

private:
  std::shared_ptr<probe_state> state_;
};

auto
probe_directory(std::string_view name) -> std::filesystem::path
{
  auto directory = std::filesystem::temp_directory_path() /
                   (std::string{ name } + "-" + std::to_string(std::random_device{}()));
  std::filesystem::create_directories(directory);
  return directory;
}

// Regression: a thread inside CB_LOG_PROTOCOL held no reference to the protocol logger, so reset()
// on another thread freed the logger while the message was still being written.
void
the_protocol_logger_can_be_reset_while_another_thread_logs([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-protocol-logger");
  auto state = std::make_shared<probe_state>();
  state->hold = true;
  core_logger::configuration config{};
  config.filename = (directory / "protocol").string();
  config.unit_test = true;
  config.console = false;
  config.sink = std::make_shared<probe_sink>(state);
  const auto failure = core_logger::create_protocol_logger(config);
  config.sink.reset();
  // Stops the periodic flusher and empties spdlog's registry, which the flusher walks, and leaves
  // the protocol logger installed. A flush of the held sink would lock the registry until the
  // message is released, and make reset() wait for the writer.
  core_logger::shutdown();

  std::thread writer([]() {
    CB_LOG_PROTOCOL("{}", "message");
  });
  bool inside_at_reset = false;
  {
    std::unique_lock lock(state->mutex);
    inside_at_reset = state->changed.wait_for(lock, probe_wait(), [&state]() {
      return state->inside;
    });
  }
  core_logger::reset();
  bool inside_after_reset = false;
  {
    const std::scoped_lock lock(state->mutex);
    inside_after_reset = state->inside;
    state->hold = false;
    state->changed.notify_all();
  }
  writer.join();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(failure.has_value(), "the protocol logger was created");
  assert_true(inside_at_reset, "the message was inside the sink when reset() started");
  assert_true(inside_after_reset, "the message was still inside the sink when reset() returned");
  assert_false(state->destroyed_while_inside, "no logger was freed with the message inside it");
  assert_true(state->destroyed, "the logger was freed once the writer's call returned");
}

// Regression: each thread cached an owning reference to the loggers, so after reset() a thread
// that had logged and then gone idle kept the old loggers, and their open files, alive.
void
an_idle_thread_does_not_keep_a_reset_logger_alive([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-logger-idle");
  auto file_state = std::make_shared<probe_state>();
  auto protocol_state = std::make_shared<probe_state>();
  core_logger::configuration file{};
  file.unit_test = true;
  file.console = false;
  file.log_level = core_logger::level::trace;
  file.sink = std::make_shared<probe_sink>(file_state);
  const auto file_failure = core_logger::create_file_logger(file);
  file.sink.reset();
  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.unit_test = true;
  protocol.console = false;
  protocol.sink = std::make_shared<probe_sink>(protocol_state);
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);
  protocol.sink.reset();

  std::promise<void> logged;
  std::promise<void> finish;
  std::atomic<bool> writer_timed_out{ false };
  std::thread writer([&logged, &writer_timed_out, done = finish.get_future()]() {
    CB_LOG_INFO_RAW("message");
    CB_LOG_PROTOCOL("{}", "message");
    logged.set_value();
    writer_timed_out = done.wait_for(probe_wait()) == std::future_status::timeout;
  });
  const bool writer_logged =
    logged.get_future().wait_for(probe_wait()) == std::future_status::ready;
  core_logger::reset();
  bool file_destroyed = false;
  bool protocol_destroyed = false;
  {
    const std::scoped_lock lock(file_state->mutex);
    file_destroyed = file_state->destroyed;
  }
  {
    const std::scoped_lock lock(protocol_state->mutex);
    protocol_destroyed = protocol_state->destroyed;
  }
  finish.set_value();
  writer.join();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(file_failure.has_value(), "the file logger was created");
  assert_false(protocol_failure.has_value(), "the protocol logger was created");
  assert_true(writer_logged, "the writer logged and went idle");
  assert_false(writer_timed_out.load(), "the writer was still alive when reset() returned");
  assert_true(file_state->entered && protocol_state->entered, "both messages reached their sinks");
  assert_true(file_destroyed, "reset() freed the file logger the idle thread had used");
  assert_true(protocol_destroyed, "reset() freed the protocol logger the idle thread had used");
}

// Signals when the logger that owns it is destroyed, and waits until `proceed` is ready, so a case
// can observe or order another thread at the exact moment a registry entry or replaced logger is
// released.
class drop_gate_sink final : public spdlog::sinks::base_sink<spdlog::details::null_mutex>
{
public:
  drop_gate_sink(std::shared_ptr<std::promise<void>> dropped, std::shared_future<void> proceed)
    : dropped_{ std::move(dropped) }
    , proceed_{ std::move(proceed) }
  {
  }

  drop_gate_sink(const drop_gate_sink&) = delete;
  drop_gate_sink(drop_gate_sink&&) = delete;
  auto operator=(const drop_gate_sink&) -> drop_gate_sink& = delete;
  auto operator=(drop_gate_sink&&) -> drop_gate_sink& = delete;

  ~drop_gate_sink() override
  {
    dropped_->set_value();
    if (proceed_.valid()) {
      static_cast<void>(proceed_.wait_for(probe_wait()));
    }
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
  }

  void flush_() override
  {
  }

private:
  std::shared_ptr<std::promise<void>> dropped_;
  std::shared_future<void> proceed_;
};

// Pauses one watched thread on its first visit to a unit_test_api callback boundary.
class thread_hook_gate
{
public:
  using setter = void (*)(std::function<void()>);

  explicit thread_hook_gate(setter set_hook)
    : set_hook_{ set_hook }
  {
    set_hook_([state = state_]() {
      std::unique_lock lock(state->mutex);
      if (!state->armed || state->watched_thread != std::this_thread::get_id()) {
        return;
      }
      state->armed = false;
      state->entered = true;
      state->changed.notify_all();
      state->changed.wait_for(lock, probe_wait(), [&state]() {
        return state->released;
      });
    });
  }

  thread_hook_gate(const thread_hook_gate&) = delete;
  thread_hook_gate(thread_hook_gate&&) = delete;
  auto operator=(const thread_hook_gate&) -> thread_hook_gate& = delete;
  auto operator=(thread_hook_gate&&) -> thread_hook_gate& = delete;

  ~thread_hook_gate()
  {
    release();
    set_hook_(nullptr);
  }

  void watch_this_thread()
  {
    const std::scoped_lock lock(state_->mutex);
    state_->watched_thread = std::this_thread::get_id();
    state_->armed = true;
  }

  auto wait_until_entered() -> bool
  {
    std::unique_lock lock(state_->mutex);
    return state_->changed.wait_for(lock, probe_wait(), [this]() {
      return state_->entered;
    });
  }

  void release()
  {
    {
      const std::scoped_lock lock(state_->mutex);
      state_->armed = false;
      state_->released = true;
    }
    state_->changed.notify_all();
  }

private:
  struct shared_state {
    std::mutex mutex{};
    std::condition_variable changed{};
    std::optional<std::thread::id> watched_thread{};
    bool armed{ false };
    bool entered{ false };
    bool released{ false };
  };

  setter set_hook_;
  std::shared_ptr<shared_state> state_{ std::make_shared<shared_state>() };
};

class registry_unlock_gate final : public thread_hook_gate
{
public:
  registry_unlock_gate()
    : thread_hook_gate{ &core_logger::unit_test_api::set_after_registry_unlock }
  {
  }
};

class cached_version_match_gate final : public thread_hook_gate
{
public:
  cached_version_match_gate()
    : thread_hook_gate{ &core_logger::unit_test_api::set_after_cached_version_match }
  {
  }
};

class log_snapshot_gate final : public thread_hook_gate
{
public:
  log_snapshot_gate()
    : thread_hook_gate{ &core_logger::unit_test_api::set_after_log_snapshot }
  {
  }
};

// Regression: reset() cleared the file logger, released registry_mutex, dropped the protocol
// logger's registry entry, and released registry_mutex again before clearing protocol_logger, while
// create_protocol_logger() released registry_mutex before publishing protocol_logger.
void
a_protocol_logger_is_registered_exactly_while_it_logs([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-protocol-logger-reset");
  core_logger::reset();
  // spdlog's default logger sits at info until shutdown() drops all registry entries, whereas the
  // protocol logger sits at trace while registered.
  const auto baseline_lowest = core_logger::get_lowest_log_level();

  auto file_released = std::make_shared<std::promise<void>>();
  std::promise<void> allow_file_release;
  core_logger::configuration file{};
  file.unit_test = true;
  file.console = false;
  file.sink =
    std::make_shared<drop_gate_sink>(file_released, allow_file_release.get_future().share());
  const auto initial_file_failure = core_logger::create_file_logger(file);
  file.sink.reset();

  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.unit_test = true;
  protocol.console = false;
  const auto initial_protocol_failure = core_logger::create_protocol_logger(protocol);
  const auto lowest_with_protocol = core_logger::get_lowest_log_level();

  // The replaced file logger is destroyed as soon as reset() releases registry_mutex. By then
  // reset() must already have cleared protocol_logger and dropped its registry entry too.
  std::thread resetter([]() {
    core_logger::reset();
  });
  const bool saw_file_release =
    file_released->get_future().wait_for(probe_wait()) == std::future_status::ready;
  const bool file_still_active = core_logger::is_initialized();
  const bool protocol_still_active = core_logger::should_log_protocol();
  const auto lowest_after_registry_release = core_logger::get_lowest_log_level();
  allow_file_release.set_value();
  resetter.join();

  // Also pin create_protocol_logger(): at the moment creation releases registry_mutex,
  // should_log_protocol() and the spdlog registry must already reflect the new logger.
  core_logger::configuration async_protocol{};
  async_protocol.filename = (directory / "async-protocol").string();
  async_protocol.console = false;
  bool created_ok = false;
  bool paused_after_create_unlock = false;
  bool published_before_registry_release = false;
  core_logger::level lowest_after_create_unlock{};
  {
    registry_unlock_gate gate;
    std::thread creator([&]() {
      gate.watch_this_thread();
      created_ok = !core_logger::create_protocol_logger(async_protocol).has_value();
    });
    paused_after_create_unlock = gate.wait_until_entered();
    published_before_registry_release = core_logger::should_log_protocol();
    lowest_after_create_unlock = core_logger::get_lowest_log_level();
    gate.release();
    creator.join();
  }

  core_logger::shutdown();
  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(initial_file_failure.has_value(), "the file logger was created");
  assert_false(initial_protocol_failure.has_value(), "the protocol logger was created");
  assert_false(baseline_lowest == core_logger::level::trace,
               "no trace logger is registered before the case starts");
  assert_eq(
    lowest_with_protocol, core_logger::level::trace, "the protocol logger registered at trace");
  assert_true(saw_file_release, "reset() released the replaced file logger");
  assert_false(file_still_active, "reset() cleared the file logger before releasing it");
  assert_false(protocol_still_active,
               "reset() cleared protocol_logger before releasing registry_mutex");
  assert_eq(lowest_after_registry_release,
            baseline_lowest,
            "reset() dropped the protocol logger from the registry in the same critical section");
  assert_true(created_ok, "the async protocol logger was created");
  assert_true(paused_after_create_unlock,
              "create_protocol_logger() reached the registry_mutex unlock boundary");
  assert_eq(
    lowest_after_create_unlock,
    core_logger::level::trace,
    "create_protocol_logger() registered the protocol logger before releasing registry_mutex");
  assert_true(published_before_registry_release,
              "create_protocol_logger() published protocol_logger before releasing registry_mutex");
}

// Counts what it receives and records when its owning logger destroys it.
class counting_sink final : public spdlog::sinks::base_sink<spdlog::details::null_mutex>
{
public:
  counting_sink(std::shared_ptr<std::atomic<int>> received,
                std::shared_ptr<std::atomic<bool>> destroyed)
    : received_{ std::move(received) }
    , destroyed_{ std::move(destroyed) }
  {
  }

  counting_sink(const counting_sink&) = delete;
  counting_sink(counting_sink&&) = delete;
  auto operator=(const counting_sink&) -> counting_sink& = delete;
  auto operator=(counting_sink&&) -> counting_sink& = delete;

  ~counting_sink() override
  {
    destroyed_->store(true);
  }

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
    received_->fetch_add(1);
  }

  void flush_() override
  {
  }

private:
  std::shared_ptr<std::atomic<int>> received_;
  std::shared_ptr<std::atomic<bool>> destroyed_;
};

// Regression: a file logger replaced and freed between matching the cached version and locking the
// cached weak reference caused read_logger() to find no logger and drop the message.
void
replacing_the_file_logger_drops_no_message([[maybe_unused]] context& ctx)
{
  core_logger::reset();
  const auto create = [](const std::shared_ptr<std::atomic<int>>& received,
                         const std::shared_ptr<std::atomic<bool>>& destroyed) {
    core_logger::configuration config{};
    config.unit_test = true;
    config.console = false;
    config.log_level = core_logger::level::trace;
    config.sink = std::make_shared<counting_sink>(received, destroyed);
    return core_logger::create_file_logger(config);
  };
  auto first_received = std::make_shared<std::atomic<int>>(0);
  auto first_destroyed = std::make_shared<std::atomic<bool>>(false);
  const auto first_failure = create(first_received, first_destroyed);

  auto second_received = std::make_shared<std::atomic<int>>(0);
  auto second_destroyed = std::make_shared<std::atomic<bool>>(false);
  std::optional<std::string> second_failure;
  bool paused_at_boundary = false;
  int first_before_replacement = 0;
  bool expired_before_lock = false;
  int second_before_release = 0;
  {
    cached_version_match_gate gate;
    std::thread writer([&gate]() {
      // Populates this thread's weak-reference cache with the first logger's version.
      CB_LOG_INFO_RAW("prime");
      gate.watch_this_thread();
      CB_LOG_INFO_RAW("after-replacement");
    });
    paused_at_boundary = gate.wait_until_entered();
    first_before_replacement = first_received->load();
    second_failure = create(second_received, second_destroyed);
    expired_before_lock = first_destroyed->load();
    second_before_release = second_received->load();
    gate.release();
    writer.join();
  }
  core_logger::reset();

  assert_false(first_failure.has_value(), "the first file logger was created");
  assert_false(second_failure.has_value(), "the replacement file logger was created");
  assert_true(paused_at_boundary,
              "the writer paused between matching the cached version and locking the logger");
  assert_eq(first_before_replacement, 1, "the priming message reached the first logger");
  assert_true(expired_before_lock,
              "replacement destroyed the cached logger before the writer locked its weak pointer");
  assert_eq(second_before_release, 0, "the gated message waited at the boundary");
  assert_eq(first_received->load(), 1, "only the priming message reached the replaced logger");
  assert_eq(second_received->load(), 1, "the gated message reached the replacement logger");
}

// Regression: create_file_logger() replaced the pool of the logging thread, so an asynchronous
// protocol logger created before it was left without one and dropped every message.
void
a_new_file_logger_keeps_the_protocol_logger_writing([[maybe_unused]] context& ctx)
{
  const auto directory = std::filesystem::temp_directory_path() /
                         ("cxx-logger-pool-" + std::to_string(std::random_device{}()));
  std::filesystem::create_directories(directory);
  auto received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.console = false;
  protocol.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [received](const spdlog::details::log_msg& /* msg */) {
      received->fetch_add(1);
    });
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);
  core_logger::configuration file{};
  file.console = false;
  const auto file_failure = core_logger::create_file_logger(file);

  CB_LOG_PROTOCOL("{}", "message");
  // Joins the logging thread, which first writes every message queued before the call.
  core_logger::shutdown();
  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(protocol_failure.has_value(), "the protocol logger was created");
  assert_false(file_failure.has_value(), "the file logger was created");
  assert_eq(received->load(), 1, "the protocol message reached its sink");
}

// Regression: two threads creating asynchronous loggers at once could each find no pool and create
// one, and the second replaced the first, which left the first logger without a pool.
void
loggers_created_at_once_share_one_pool([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-logger-concurrent");
  int failures = 0;
  int lost = 0;
  for (int round = 0; round < 20; ++round) {
    auto file_received = std::make_shared<std::atomic<int>>(0);
    auto protocol_received = std::make_shared<std::atomic<int>>(0);
    core_logger::configuration file{};
    file.console = false;
    file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
      [file_received](const spdlog::details::log_msg& /* msg */) {
        file_received->fetch_add(1);
      });
    core_logger::configuration protocol{};
    protocol.filename = (directory / "protocol").string();
    protocol.console = false;
    protocol.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
      [protocol_received](const spdlog::details::log_msg& /* msg */) {
        protocol_received->fetch_add(1);
      });

    std::promise<void> go;
    const std::shared_future<void> start = go.get_future().share();
    std::optional<std::string> file_failure;
    std::optional<std::string> protocol_failure;
    std::thread file_creator([&]() {
      start.wait();
      file_failure = core_logger::create_file_logger(file);
    });
    std::thread protocol_creator([&]() {
      start.wait();
      protocol_failure = core_logger::create_protocol_logger(protocol);
    });
    go.set_value();
    file_creator.join();
    protocol_creator.join();

    CB_LOG_INFO_RAW("message");
    CB_LOG_PROTOCOL("{}", "message");
    // Joins the logging thread, which first writes every message queued before the call.
    core_logger::shutdown();
    core_logger::reset();
    if (file_failure.has_value() || protocol_failure.has_value()) {
      ++failures;
    }
    if (file_received->load() != 1 || protocol_received->load() != 1) {
      ++lost;
    }
  }
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_eq(failures, 0, "every logger was created");
  assert_eq(lost, 0, "both loggers wrote their message in every round");
}

// Regression: create_file_logger_impl() released registry_mutex before create_file_logger() and
// create_protocol_logger() published the new logger. A shutdown() running in that gap destroyed the
// new logger's pool before the logger was installed, so messages logged afterwards were dropped.
void
logger_creation_serializes_with_shutdown([[maybe_unused]] context& ctx)
{
  // Forward order: shutdown() entering registry_mutex right as creation releases it must observe
  // the published logger and drain a message logged at that boundary.
  core_logger::reset();
  auto file_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration file{};
  file.console = false;
  file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [file_received](const spdlog::details::log_msg& /* msg */) {
      file_received->fetch_add(1);
    });

  std::optional<std::string> file_failure;
  bool paused_after_create_unlock = false;
  bool published_before_shutdown = false;
  {
    registry_unlock_gate gate;
    std::thread file_creator([&]() {
      gate.watch_this_thread();
      file_failure = core_logger::create_file_logger(file);
    });
    paused_after_create_unlock = gate.wait_until_entered();
    published_before_shutdown = core_logger::is_initialized();
    CB_LOG_INFO_RAW("message");
    core_logger::shutdown();
    gate.release();
    file_creator.join();
  }
  core_logger::reset();

  // Reverse order: creation entering registry_mutex right after shutdown() releases it (while
  // shutdown() still holds the detached old pool before joining its worker) must give both file
  // and protocol loggers a fresh live pool.
  const auto directory = probe_directory("cxx-logger-shutdown");
  core_logger::configuration seed{};
  seed.console = false;
  const auto seed_failure = core_logger::create_file_logger(seed);

  auto after_file_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration after_file{};
  after_file.console = false;
  after_file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [after_file_received](const spdlog::details::log_msg& /* msg */) {
      after_file_received->fetch_add(1);
    });
  auto after_protocol_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration after_protocol{};
  after_protocol.filename = (directory / "protocol").string();
  after_protocol.console = false;
  after_protocol.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [after_protocol_received](const spdlog::details::log_msg& /* msg */) {
      after_protocol_received->fetch_add(1);
    });

  bool paused_after_shutdown_unlock = false;
  std::optional<std::string> after_file_failure;
  std::optional<std::string> after_protocol_failure;
  {
    registry_unlock_gate gate;
    std::thread reverse_shutter([&]() {
      gate.watch_this_thread();
      core_logger::shutdown();
    });
    paused_after_shutdown_unlock = gate.wait_until_entered();
    after_file_failure = core_logger::create_file_logger(after_file);
    after_protocol_failure = core_logger::create_protocol_logger(after_protocol);
    gate.release();
    reverse_shutter.join();
  }

  CB_LOG_INFO_RAW("message");
  CB_LOG_PROTOCOL("{}", "message");
  core_logger::shutdown();
  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(file_failure.has_value(), "the file logger was created");
  assert_true(paused_after_create_unlock, "creation reached the registry_mutex unlock boundary");
  assert_true(published_before_shutdown,
              "shutdown could not enter registry_mutex before the logger was published");
  assert_eq(file_received->load(), 1, "shutdown drained a message logged at the unlock boundary");
  assert_false(seed_failure.has_value(), "the seed logger before reverse shutdown was created");
  assert_true(paused_after_shutdown_unlock, "shutdown reached the registry_mutex unlock boundary");
  assert_false(after_file_failure.has_value(), "the file logger after shutdown was created");
  assert_false(after_protocol_failure.has_value(),
               "the protocol logger after shutdown was created");
  assert_eq(after_file_received->load(), 1, "the file logger created after shutdown wrote");
  assert_eq(after_protocol_received->load(), 1, "the protocol logger created after shutdown wrote");
}

struct flush_gate_state {
  std::mutex mutex{};
  std::condition_variable changed{};
  std::optional<std::thread::id> watched_thread{};
  bool registry_unlocked{ false };
  bool flush_entered{ false };
  bool flushed_after_registry_unlock{ false };
  bool release_flush{ false };
};

class flush_gate_sink final : public spdlog::sinks::base_sink<spdlog::details::null_mutex>
{
public:
  explicit flush_gate_sink(std::shared_ptr<flush_gate_state> state)
    : state_{ std::move(state) }
  {
  }

  flush_gate_sink(const flush_gate_sink&) = delete;
  flush_gate_sink(flush_gate_sink&&) = delete;
  auto operator=(const flush_gate_sink&) -> flush_gate_sink& = delete;
  auto operator=(flush_gate_sink&&) -> flush_gate_sink& = delete;
  ~flush_gate_sink() override = default;

protected:
  void sink_it_(const spdlog::details::log_msg& /* msg */) override
  {
  }

  void flush_() override
  {
    const auto state = state_;
    std::unique_lock lock(state->mutex);
    if (!state->watched_thread.has_value() ||
        *state->watched_thread != std::this_thread::get_id()) {
      return;
    }
    state->flush_entered = true;
    state->flushed_after_registry_unlock = state->registry_unlocked;
    state->changed.notify_all();
    state->changed.wait_for(lock, probe_wait(), [&state]() {
      return state->release_flush;
    });
  }

private:
  std::shared_ptr<flush_gate_state> state_{};
};

auto
read_logs(const std::filesystem::path& base) -> std::string
{
  std::string contents;
  const auto prefix = base.filename().string() + ".";
  for (const auto& entry : std::filesystem::directory_iterator(base.parent_path())) {
    if (entry.path().filename().string().rfind(prefix, 0) == 0) {
      std::ifstream file(entry.path(), std::ios::binary);
      contents.append(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
    }
  }
  return contents;
}

// Regression: shutdown() flushed only file_logger and did so before acquiring registry_mutex, so a
// replacement published in that gap (or any protocol_logger) had its queued messages drained into
// stdio's buffer without receiving a flush while the logger slot kept its file open.
void
shutdown_flushes_loggers_captured_under_the_registry_lock([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-logger-shutdown-flush");
  core_logger::reset();

  auto gate_state = std::make_shared<flush_gate_state>();
  core_logger::configuration initial{};
  initial.unit_test = true;
  initial.console = false;
  initial.sink = std::make_shared<flush_gate_sink>(gate_state);
  const auto initial_failure = core_logger::create_file_logger(initial);
  initial.sink.reset();

  struct unlock_observer_guard {
    explicit unlock_observer_guard(const std::shared_ptr<flush_gate_state>& state)
    {
      core_logger::unit_test_api::set_after_registry_unlock([state]() {
        const std::scoped_lock lock(state->mutex);
        if (state->watched_thread.has_value() &&
            *state->watched_thread == std::this_thread::get_id()) {
          state->registry_unlocked = true;
          state->changed.notify_all();
        }
      });
    }
    unlock_observer_guard(const unlock_observer_guard&) = delete;
    unlock_observer_guard(unlock_observer_guard&&) = delete;
    auto operator=(const unlock_observer_guard&) -> unlock_observer_guard& = delete;
    auto operator=(unlock_observer_guard&&) -> unlock_observer_guard& = delete;
    ~unlock_observer_guard()
    {
      core_logger::unit_test_api::set_after_registry_unlock(nullptr);
    }
  };

  bool reached_shutdown_boundary = false;
  bool reached_initial_flush = false;
  bool gap_replacement_attempted = false;
  std::optional<std::string> gap_file_failure;
  std::optional<std::string> gap_protocol_failure;
  std::string gap_file_on_disk;
  std::string gap_protocol_on_disk;
  {
    const unlock_observer_guard observer{ gate_state };
    std::thread shutter([&gate_state]() {
      {
        const std::scoped_lock lock(gate_state->mutex);
        gate_state->watched_thread = std::this_thread::get_id();
      }
      core_logger::shutdown();
    });
    bool flushed_before_registry = false;
    {
      std::unique_lock lock(gate_state->mutex);
      reached_shutdown_boundary = gate_state->changed.wait_for(lock, probe_wait(), [&gate_state]() {
        return gate_state->flush_entered || gate_state->registry_unlocked;
      });
      flushed_before_registry = gate_state->flush_entered && !gate_state->registry_unlocked;
    }
    if (flushed_before_registry) {
      gap_replacement_attempted = true;
      core_logger::configuration gap_file{};
      gap_file.filename = (directory / "gap-file").string();
      gap_file.console = false;
      gap_file_failure = core_logger::create_file_logger(gap_file);
      core_logger::configuration gap_protocol{};
      gap_protocol.filename = (directory / "gap-protocol").string();
      gap_protocol.console = false;
      gap_protocol_failure = core_logger::create_protocol_logger(gap_protocol);
      CB_LOG_INFO_RAW("gap-file-entry");
      CB_LOG_PROTOCOL("{}", "gap-protocol-entry");
    }
    {
      std::unique_lock lock(gate_state->mutex);
      reached_initial_flush = gate_state->changed.wait_for(lock, probe_wait(), [&gate_state]() {
        return gate_state->flush_entered;
      });
      gate_state->release_flush = true;
    }
    gate_state->changed.notify_all();
    shutter.join();
    if (gap_replacement_attempted) {
      gap_file_on_disk = read_logs(directory / "gap-file");
      gap_protocol_on_disk = read_logs(directory / "gap-protocol");
    }
  }
  core_logger::reset();

  // Publish both async loggers before shutdown() acquires registry_mutex, log short lines that fit
  // inside stdio's block buffer, and read both files before reset() closes their handles.
  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.console = false;
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);

  core_logger::configuration file{};
  file.filename = (directory / "file").string();
  file.console = false;
  std::optional<std::string> file_failure;
  bool paused_after_create_unlock = false;
  bool file_still_alive = false;
  bool protocol_still_alive = false;
  std::string file_on_disk;
  std::string protocol_on_disk;
  {
    registry_unlock_gate gate;
    std::thread file_creator([&]() {
      gate.watch_this_thread();
      file_failure = core_logger::create_file_logger(file);
    });
    paused_after_create_unlock = gate.wait_until_entered();
    CB_LOG_INFO_RAW("shutdown-flushes-file");
    CB_LOG_PROTOCOL("{}", "shutdown-flushes-protocol");
    core_logger::shutdown();
    file_still_alive = core_logger::is_initialized();
    protocol_still_alive = core_logger::should_log_protocol();
    file_on_disk = read_logs(directory / "file");
    protocol_on_disk = read_logs(directory / "protocol");
    gate.release();
    file_creator.join();
  }
  core_logger::reset();

  // flush() itself must also flush both active loggers while their files remain open.
  core_logger::configuration sync_file{};
  sync_file.filename = (directory / "sync-file").string();
  sync_file.unit_test = true;
  sync_file.console = false;
  const auto sync_file_failure = core_logger::create_file_logger(sync_file);

  core_logger::configuration sync_protocol{};
  sync_protocol.filename = (directory / "sync-protocol").string();
  sync_protocol.unit_test = true;
  sync_protocol.console = false;
  const auto sync_protocol_failure = core_logger::create_protocol_logger(sync_protocol);

  CB_LOG_INFO_RAW("flush-writes-file");
  CB_LOG_PROTOCOL("{}", "flush-writes-protocol");
  core_logger::flush();
  const auto sync_file_on_disk = read_logs(directory / "sync-file");
  const auto sync_protocol_on_disk = read_logs(directory / "sync-protocol");

  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(initial_failure.has_value(), "the initial synchronous logger was created");
  assert_true(reached_shutdown_boundary, "shutdown reached the registry or flush boundary");
  assert_true(reached_initial_flush, "shutdown flushed the initial logger");
  assert_true(gate_state->flushed_after_registry_unlock,
              "shutdown acquired and released registry_mutex before flushing");
  if (gap_replacement_attempted) {
    assert_false(gap_file_failure.has_value(), "the gap file logger was created");
    assert_false(gap_protocol_failure.has_value(), "the gap protocol logger was created");
    assert_contains(
      gap_file_on_disk, "gap-file-entry", "shutdown flushed a replacement file logger");
    assert_contains(
      gap_protocol_on_disk, "gap-protocol-entry", "shutdown flushed a replacement protocol logger");
  }
  assert_false(protocol_failure.has_value(), "the async protocol logger was created");
  assert_false(file_failure.has_value(), "the async file logger was created");
  assert_true(paused_after_create_unlock, "file creation paused right after registry_mutex unlock");
  assert_true(file_still_alive, "file_logger stayed installed across shutdown()");
  assert_true(protocol_still_alive, "protocol_logger stayed installed across shutdown()");
  assert_contains(
    file_on_disk, "shutdown-flushes-file", "shutdown flushed buffered file_logger data to disk");
  assert_contains(protocol_on_disk,
                  "shutdown-flushes-protocol",
                  "shutdown flushed buffered protocol_logger data to disk");
  assert_false(sync_file_failure.has_value(), "the synchronous file logger was created");
  assert_false(sync_protocol_failure.has_value(), "the synchronous protocol logger was created");
  assert_contains(sync_file_on_disk, "flush-writes-file", "flush() flushed file_logger to disk");
  assert_contains(
    sync_protocol_on_disk, "flush-writes-protocol", "flush() flushed protocol_logger to disk");
}

struct worker_shutdown_state {
  std::mutex mutex{};
  std::condition_variable changed{};
  bool entered_first{ false };
  bool producer_returned{ false };
  bool finished_first{ false };
  int received{ 0 };
};

// Regression: shutdown() guarded only flush_snapshot() against on_sdk_log_worker and still moved
// sdk_thread_pool()'s last strong reference onto the worker, where pool.reset() either blocked on
// a full queue or failed to join pthread_self() and terminated in ~std::thread().
void
shutdown_on_the_logging_worker_leaves_the_pool_for_another_thread([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-logger-worker-shutdown");
  core_logger::reset();

  auto state = std::make_shared<worker_shutdown_state>();

  core_logger::configuration protocol{};
  protocol.filename = (directory / "worker-protocol").string();
  protocol.console = false;
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);

  core_logger::configuration file{};
  file.filename = (directory / "worker-file").string();
  file.console = false;
  file.log_level = core_logger::level::trace;
  file.sink =
    std::make_shared<spdlog::sinks::callback_sink_mt>([state](const spdlog::details::log_msg& msg) {
      const std::string_view payload{ msg.payload.data(), msg.payload.size() };
      std::unique_lock lock(state->mutex);
      ++state->received;
      if (payload != "first") {
        return;
      }
      state->entered_first = true;
      state->changed.notify_all();
      state->changed.wait_for(lock, probe_wait(), [&state]() {
        return state->producer_returned;
      });
      lock.unlock();
      core_logger::shutdown();
      lock.lock();
      state->finished_first = true;
      state->changed.notify_all();
    });
  const auto file_failure = core_logger::create_file_logger(file);
  file.sink.reset();

  CB_LOG_INFO_RAW("first");
  bool sink_entered_first = false;
  bool sink_finished_first = false;
  {
    std::unique_lock lock(state->mutex);
    sink_entered_first = state->changed.wait_for(lock, probe_wait(), [&state]() {
      return state->entered_first;
    });
    state->producer_returned = true;
    state->changed.notify_all();
    sink_finished_first = state->changed.wait_for(lock, probe_wait(), [&state]() {
      return state->finished_first;
    });
  }

  const auto lowest_after_worker_shutdown = core_logger::get_lowest_log_level();
  CB_LOG_INFO_RAW("second");
  CB_LOG_PROTOCOL("{}", "after-worker-shutdown");
  core_logger::shutdown();

  const auto file_on_disk = read_logs(directory / "worker-file");
  const auto protocol_on_disk = read_logs(directory / "worker-protocol");
  int received = 0;
  {
    const std::scoped_lock lock(state->mutex);
    received = state->received;
  }

  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(protocol_failure.has_value(), "the async protocol logger was created");
  assert_false(file_failure.has_value(), "the async file logger was created");
  assert_true(sink_entered_first, "the worker entered the sink on the first message");
  assert_true(sink_finished_first, "shutdown() on the worker returned without terminating");
  assert_eq(lowest_after_worker_shutdown,
            core_logger::level::trace,
            "shutdown() on the worker left the registered loggers intact");
  assert_eq(received, 2, "the worker remained alive to process subsequent messages");
  assert_contains(file_on_disk, "first", "the first message reached the file sink");
  assert_contains(file_on_disk, "second", "the second message reached the file sink");
  assert_contains(protocol_on_disk,
                  "after-worker-shutdown",
                  "the protocol logger still wrote through the shared pool");
}

// Regression: detail::log() and detail::log_protocol() retained only spdlog::logger, which holds
// its thread pool weakly, so if another thread finished shutdown() before logger->log() ran,
// spdlog reported a missing pool and dropped the message.
void
in_flight_log_calls_retain_the_pool_across_shutdown([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-log-retains-pool");
  core_logger::shutdown();
  core_logger::reset();

  auto errors = std::make_shared<std::atomic<int>>(0);
  auto file_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration file{};
  file.filename = (directory / "file").string();
  file.console = false;
  file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [file_received](const spdlog::details::log_msg& /* msg */) {
      file_received->fetch_add(1, std::memory_order_relaxed);
    });
  const auto file_failure = core_logger::create_file_logger(file);
  file.sink.reset();
  if (auto* active = core_logger::get(); active != nullptr) {
    active->set_error_handler([errors](const std::string& /* message */) {
      errors->fetch_add(1, std::memory_order_relaxed);
    });
  }

  bool file_paused = false;
  {
    log_snapshot_gate gate;
    std::thread writer([&]() {
      gate.watch_this_thread();
      CB_LOG_INFO_RAW("file-across-shutdown");
    });
    file_paused = gate.wait_until_entered();
    core_logger::shutdown();
    gate.release();
    writer.join();
  }
  // Read before reset() so that flushing on release of the detached pool is verified too.
  const auto file_on_disk = read_logs(directory / "file");
  core_logger::reset();

  auto protocol_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.console = false;
  protocol.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [protocol_received](const spdlog::details::log_msg& /* msg */) {
      protocol_received->fetch_add(1, std::memory_order_relaxed);
    });
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);
  protocol.sink.reset();

  bool protocol_paused = false;
  {
    log_snapshot_gate gate;
    std::thread writer([&]() {
      gate.watch_this_thread();
      CB_LOG_PROTOCOL("{}", "protocol-across-shutdown");
    });
    protocol_paused = gate.wait_until_entered();
    core_logger::shutdown();
    core_logger::reset();
    gate.release();
    writer.join();
  }
  const auto protocol_on_disk = read_logs(directory / "protocol");

  auto cold_received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration cold_file{};
  cold_file.filename = (directory / "cold-file").string();
  cold_file.console = false;
  cold_file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [cold_received](const spdlog::details::log_msg& /* msg */) {
      cold_received->fetch_add(1, std::memory_order_relaxed);
    });
  const auto cold_failure = core_logger::create_file_logger(cold_file);
  cold_file.sink.reset();

  bool cold_paused = false;
  {
    log_snapshot_gate gate;
    std::thread writer([&]() {
      gate.watch_this_thread();
      core_logger::detail::log(__FILE__,
                               __LINE__,
                               "in_flight_log_calls_retain_the_pool_across_shutdown",
                               core_logger::level::info,
                               "cold-across-shutdown");
    });
    cold_paused = gate.wait_until_entered();
    core_logger::shutdown();
    gate.release();
    writer.join();
  }
  const auto cold_on_disk = read_logs(directory / "cold-file");

  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(file_failure.has_value(), "the async file logger was created");
  assert_true(file_paused, "file logging paused right after taking the logger snapshot");
  assert_eq(errors->load(), 0, "file logging reported no missing-pool error");
  assert_eq(file_received->load(), 1, "the file logger queued its message across shutdown()");
  assert_contains(
    file_on_disk, "file-across-shutdown", "the file message flushed to disk across shutdown()");
  assert_false(protocol_failure.has_value(), "the async protocol logger was created");
  assert_true(protocol_paused, "protocol logging paused right after taking the logger snapshot");
  assert_eq(
    protocol_received->load(), 1, "the protocol logger queued its message across shutdown()");
  assert_contains(protocol_on_disk,
                  "protocol-across-shutdown",
                  "the protocol message flushed to disk across shutdown()");
  assert_false(cold_failure.has_value(), "the cold-cache async file logger was created");
  assert_true(cold_paused, "cold-cache logging paused right after taking the logger snapshot");
  assert_eq(
    cold_received->load(), 1, "cold-cache detail::log() queued its message across shutdown()");
  assert_contains(cold_on_disk, "cold-across-shutdown", "the cold-cache message flushed to disk");
}

// Regression: after shutdown() without reset(), file_logger and protocol_logger remain installed
// with expired async pools, and read_logger_for_log() fell through to file_logger_mutex on every
// subsequent log call even when the cached logger was still alive and the slot version had not
// changed.
void
logging_after_shutdown_without_reset_avoids_the_logger_mutex([[maybe_unused]] context& ctx)
{
  const auto directory = probe_directory("cxx-log-post-shutdown-lockfree");
  core_logger::shutdown();
  core_logger::reset();
  couchbase::logger::unregister_log_callback();

  core_logger::configuration file{};
  file.console = false;
  file.sink = std::make_shared<spdlog::sinks::null_sink_mt>();
  const auto file_failure = core_logger::create_file_logger(file);
  file.sink.reset();

  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.console = false;
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);

  struct sync_state {
    std::mutex mutex{};
    std::condition_variable changed{};
    bool warmed{ false };
    bool proceed{ false };
    bool finished{ false };
  };
  auto state = std::make_shared<sync_state>();

  std::thread worker([state]() {
    CB_LOG_INFO_RAW("warm-file");
    CB_LOG_PROTOCOL("{}", "warm-protocol");
    {
      std::unique_lock lock(state->mutex);
      state->warmed = true;
      state->changed.notify_all();
      state->changed.wait_for(lock, probe_wait(), [&state]() {
        return state->proceed;
      });
    }
    CB_LOG_INFO_RAW("post-shutdown-file");
    CB_LOG_PROTOCOL("{}", "post-shutdown-protocol");
    {
      const std::scoped_lock lock(state->mutex);
      state->finished = true;
    }
    state->changed.notify_all();
  });

  bool warmed = false;
  {
    std::unique_lock lock(state->mutex);
    warmed = state->changed.wait_for(lock, probe_wait(), [&state]() {
      return state->warmed;
    });
  }

  core_logger::shutdown();

  bool finished_under_fork_prepare = false;
  // notify_fork(prepare) holds file_logger_mutex until notify_fork(parent), so a post-shutdown log
  // call on a warm thread completes before parent only if it avoids file_logger_mutex.
  core_logger::notify_fork(couchbase::fork_event::prepare);
  try {
    std::unique_lock lock(state->mutex);
    state->proceed = true;
    state->changed.notify_all();
    finished_under_fork_prepare =
      state->changed.wait_for(lock, scaled_budget(std::chrono::milliseconds{ 250 }), [&state]() {
        return state->finished;
      });
  } catch (...) {
    core_logger::notify_fork(couchbase::fork_event::parent);
    worker.join();
    throw;
  }
  core_logger::notify_fork(couchbase::fork_event::parent);
  worker.join();

  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(file_failure.has_value(), "the async file logger was created");
  assert_false(protocol_failure.has_value(), "the async protocol logger was created");
  assert_true(warmed, "the logging thread populated both logger caches before shutdown()");
  assert_true(finished_under_fork_prepare,
              "post-shutdown logging dropped messages without acquiring file_logger_mutex");
}

void
a_registered_callback_receives_every_logged_message([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level /*level*/,
                                                            couchbase::logger::log_location loc) {
    captured_logs.push_back(capture_entry(msg, loc));
  });

  CB_LOG_INFO("Test log message 1");
  CB_LOG_WARNING("Test log message 2");

  assert_eq(captured_logs.size(), std::size_t{ 2 }, "both messages reach the callback");
  assert_contains(captured_logs[0], "Test log message 1", "the first message");
  assert_contains(captured_logs[1], "Test log message 2", "the second message");
}

void
the_callback_receives_the_level_of_each_message([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });

  CB_LOG_INFO("Test log message 1");
  CB_LOG_ERROR("Test log message 2");

  assert_eq(captured_logs.size(), std::size_t{ 1 }, "a callback filtering on error keeps one");
  assert_contains(captured_logs[0], "Test log message 2", "the message logged at error");
}

void
logging_with_no_callback_registered_does_not_throw([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  // register_log_callback(nullptr) installs nothing -- the public API returns before reaching the
  // core one -- so unregister_log_callback() is what leaves the logger with no callback.
  couchbase::logger::unregister_log_callback();
  couchbase::logger::register_log_callback(nullptr);

  assert_no_throw(
    [&]() {
      CB_LOG_INFO("Test log message 1");
    },
    "logging with no callback registered reaches no callback");
}

void
registering_a_second_callback_replaces_the_first([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;

  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::trace) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });

  CB_LOG_ERROR("Test error message");
  CB_LOG_TRACE("Test trace message");

  assert_eq(captured_logs.size(), std::size_t{ 1 }, "only the second callback is delivered to");
  assert_contains(captured_logs[0], "Test trace message", "what the second callback kept");
}

void
unregistering_stops_delivery_until_a_callback_is_registered_again([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;

  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level level,
                                   couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  };

  couchbase::logger::register_log_callback(callback);
  CB_LOG_ERROR("Test error message");

  couchbase::logger::unregister_log_callback();
  CB_LOG_ERROR("Test error message 2");

  couchbase::logger::register_log_callback(callback);
  CB_LOG_ERROR("Test error message 3");

  assert_eq(captured_logs.size(), std::size_t{ 2 }, "nothing is delivered while unregistered");
  assert_contains(captured_logs[0], "Test error message", "logged before unregistering");
  assert_contains(captured_logs[1], "Test error message 3", "logged after registering again");
}

void
arguments_are_not_formatted_when_logging_is_off_and_no_callback_is_registered(
  [[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  couchbase::logger::unregister_log_callback();
  couchbase::logger::set_level(couchbase::logger::log_level::off);
  probe_format_count.store(0);

  CB_LOG_TRACE("value={}", format_probe{});

  assert_eq(probe_format_count.load(), 0, "a suppressed message does not format its arguments");
}

void
a_registered_callback_receives_formatted_arguments_while_logging_is_off(
  [[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured;
  couchbase::logger::register_log_callback(
    [&captured](std::string_view msg,
                couchbase::logger::log_level /*level*/,
                couchbase::logger::log_location /*location*/) {
      captured.emplace_back(msg);
    });
  couchbase::logger::set_level(couchbase::logger::log_level::off);
  probe_format_count.store(0);

  CB_LOG_TRACE("value={}", format_probe{});

  assert_eq(probe_format_count.load(), 1, "the argument is formatted exactly once");
  assert_false(captured.empty(), "the message reaches the callback");
  assert_contains(captured.back(), "value=probe", "the formatted argument");
}

void
redaction_annotations_are_inert_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  assert_eq(fmt::format("key={}", core_logger::user_data("my_key")), "key=my_key", "user data");
  assert_eq(
    fmt::format("bucket={}", core_logger::metadata("my_bucket")), "bucket=my_bucket", "metadata");
  assert_eq(
    fmt::format("host={}", core_logger::system_data("127.0.0.1")), "host=127.0.0.1", "system data");
}

void
redaction_annotations_wrap_values_while_redaction_is_enabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  assert_eq(
    fmt::format("key={}", core_logger::user_data("my_key")), "key=<ud>my_key</ud>", "user data");
  assert_eq(fmt::format("bucket={}", core_logger::metadata("my_bucket")),
            "bucket=<md>my_bucket</md>",
            "metadata");
  assert_eq(fmt::format("host={}", core_logger::system_data("127.0.0.1")),
            "host=<sd>127.0.0.1</sd>",
            "system data");
}

// The tool that consumes these tags is line-oriented. A value carrying a newline puts the opening
// tag and the closing tag on different lines, and the tool then reports an unmatched tag and emits
// the value in clear text -- the one outcome the annotations exist to prevent. Observed against
// cblogredaction: an HTTP error body spanning lines came through the redacted log fully readable.
const std::string multiline_body{ "<html><head>\n<title>301</title></head>\r\n</html>" };

void
a_tagged_value_never_spans_log_lines([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("body={}", core_logger::user_data(multiline_body));
  assert_eq(tagged,
            R"(body=<ud><html><head>\n<title>301</title></head>\r\n</html></ud>)",
            "both line breaks are escaped inside the span");
  assert_eq(tagged.find('\n'), std::string::npos, "no newline survives");
  assert_eq(tagged.find('\r'), std::string::npos, "no carriage return survives");

  assert_eq(fmt::format("{}", core_logger::metadata(multiline_body)).find('\n'),
            std::string::npos,
            "metadata escapes the same way");
  assert_eq(fmt::format("{}", core_logger::system_data(multiline_body)).find('\n'),
            std::string::npos,
            "system data escapes the same way");
}

void
every_entry_of_a_tagged_list_is_escaped([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::vector<std::string> values{ "one\ntwo", "three" };
  assert_eq(core_logger::user_data_list(values),
            R"(<ud>one\ntwo</ud>, <ud>three</ud>)",
            "an entry is escaped like any other tagged value");
}

void
a_value_with_newlines_is_untouched_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  // Annotating a statement must not change what it prints for anyone who has not opted in.
  assert_eq(fmt::format("body={}", core_logger::user_data(multiline_body)),
            "body=" + multiline_body,
            "nothing is escaped while redaction is off");
}

void
the_exclusion_markers_never_escape_their_value([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // not_redacted() exists so a configuration dump shows the exact bytes that crossed the wire.
  assert_eq(fmt::format("{}", core_logger::not_redacted(multiline_body)),
            multiline_body,
            "not_redacted passes the value through");
  assert_eq(fmt::format("{}", core_logger::not_sensitive(multiline_body)),
            multiline_body,
            "not_sensitive passes the value through");
}

void
a_conditionally_tagged_value_is_only_tagged_on_the_sensitive_branch([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // Tagging a constant hashes it to a fixed digest, which protects nothing and leaves a reader
  // unable to tell a value the SDK withheld from one the redaction tool replaced.
  //
  // The values are typed rather than bare literals, matching what the call sites pass: cppcheck
  // reads a string literal beside a bool parameter as a boolean conversion and reports it.
  const std::string_view hidden{ "[hidden]" };
  const std::string_view body{ R"({"a":1})" };
  const std::string_view multi{ "one\ntwo" };

  assert_eq(fmt::format("body={}", core_logger::user_data_if(false, hidden)),
            "body=[hidden]",
            "the insensitive branch carries no tag");
  assert_eq(fmt::format("body={}", core_logger::user_data_if(true, body)),
            R"(body=<ud>{"a":1}</ud>)",
            "the sensitive branch is tagged");

  // The tagged branch goes through the same formatter as user_data(), so it escapes identically.
  assert_eq(fmt::format("{}", core_logger::user_data_if(true, multi)),
            R"(<ud>one\ntwo</ud>)",
            "and escapes like user_data()");
}

// A value carrying a tag sequence would end its span early and leave the rest of itself outside any
// tag. Not an attack, since whoever controls the value already knows it, but a silent failure: an
// application whose document happens to contain one of these hands out a log it believes was
// redacted.
const std::string_view injected_tag{ "</ud>secret<ud>" };

void
a_tagged_value_cannot_close_its_own_span([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("key={}", core_logger::user_data(injected_tag));
  assert_eq(tagged,
            R"(key=<ud>\u003c/ud>secret\u003cud></ud>)",
            "only the \"<\" of a tag sequence is escaped");

  // The span has to be the only one, and it has to close exactly once, at the end.
  assert_eq(tagged.find("</ud>"), tagged.size() - 5, "the span closes once, at the end");
  assert_eq(tagged.find("<ud>"), tagged.find("key=") + 4, "and opens once, at the start");
}

void
markup_that_is_not_a_tag_sequence_is_left_alone([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // Escaping every "<" would buy nothing here and would mangle any logged body carrying markup.
  assert_eq(fmt::format("{}", core_logger::user_data("<html><body>")),
            "<ud><html><body></ud>",
            "ordinary markup survives unescaped");
}

void
tag_injection_is_escaped_in_every_category_and_in_a_list([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("{}", core_logger::metadata(injected_tag));
  assert_eq(tagged.find("</md>"), tagged.size() - 5, "metadata closes once, at the end");
  assert_eq(core_logger::user_data_list(std::vector<std::string_view>{ injected_tag }),
            R"(<ud>\u003c/ud>secret\u003cud></ud>)",
            "a list entry is escaped like any other tagged value");
}

void
a_dynamic_format_specification_still_resolves_inside_a_tag([[maybe_unused]] context& ctx)
{
  // The tagged branch formats through a second context, which has to carry the caller's arguments:
  // a width given as "{:{}}" is looked up through the context, so without them this threw while the
  // unredacted branch worked. Annotating a statement must not depend on the redaction level.
  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("[{:{}}]", core_logger::metadata("ab"), 6),
              "[ab    ]",
              "the width is applied while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("[{:{}}]", core_logger::metadata("ab"), 6),
            "[<md>ab    </md>]",
            "and inside the tag while it is on");
}

void
a_serialised_document_carries_no_annotations_of_its_own([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // The origin dump is annotated as a whole, by the cluster open paths that log it. A tag written
  // into one of its values instead would be read back as part of that value by anything that parses
  // the line as JSON, so the document has to come out of here clean even with redaction on.
  const auto connstr =
    couchbase::core::utils::parse_connection_string("couchbase://10.0.0.1,10.0.0.2");
  const auto dump = couchbase::core::origin({}, connstr).to_json();

  assert_ne(dump.find("10.0.0.1"), std::string::npos, "the address is in the dump");
  assert_eq(dump.find('<'), std::string::npos, "and nothing in the document is tagged");
}

void
redaction_annotations_work_for_non_string_values([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::string host{ "10.0.0.1" };
  const std::string_view bucket{ "travel-sample" };

  assert_eq(fmt::format("{}", core_logger::system_data(11210)), "<sd>11210</sd>", "an integer");
  assert_eq(
    fmt::format("{}", core_logger::system_data(host)), "<sd>10.0.0.1</sd>", "a std::string");
  assert_eq(fmt::format("{}", core_logger::metadata(bucket)),
            "<md>travel-sample</md>",
            "a std::string_view");
}

void
redaction_annotations_may_be_mixed_in_a_single_statement([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  assert_eq(fmt::format("[{}/{}] <{}:{}> key={}",
                        "client-id",
                        core_logger::metadata("travel-sample"),
                        core_logger::system_data("10.0.0.1"),
                        core_logger::system_data(11210),
                        core_logger::user_data("airline_10")),
            "[client-id/<md>travel-sample</md>] <<sd>10.0.0.1</sd>:<sd>11210</sd>> "
            "key=<ud>airline_10</ud>",
            "each argument carries its own category");
}

// One tag around a joined list would hash the whole list as a single token, so an entry would match
// nothing else in the log, not even the same value logged beside it in a span of its own. Both of
// those shapes occur: dns_config.cxx logs the server list and the selected server on one line, and
// cluster.cxx logs bootstrap nodes that mcbp_session.cxx logs individually.
const std::vector<std::string> list_addresses{ "10.0.0.1:11210", "10.0.0.2:11210" };

void
a_list_is_inert_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  assert_eq(core_logger::system_data_list(list_addresses),
            "10.0.0.1:11210, 10.0.0.2:11210",
            "no tags while redaction is off");
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted),
            R"("10.0.0.1:11210", "10.0.0.2:11210")",
            "and the quotes are still rendered");
}

void
a_list_is_tagged_one_entry_at_a_time([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::vector<std::string> buckets{ "travel-sample", "beer-sample" };

  assert_eq(core_logger::system_data_list(list_addresses),
            "<sd>10.0.0.1:11210</sd>, <sd>10.0.0.2:11210</sd>",
            "system data");
  assert_eq(core_logger::metadata_list(buckets),
            "<md>travel-sample</md>, <md>beer-sample</md>",
            "metadata");
  assert_eq(core_logger::user_data_list(buckets),
            "<ud>travel-sample</ud>, <ud>beer-sample</ud>",
            "user data");
}

void
a_list_tag_sits_inside_the_quotes_an_entry_renders([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // A span that swallowed the punctuation would hash to something matching no other line.
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted),
            R"("<sd>10.0.0.1:11210</sd>", "<sd>10.0.0.2:11210</sd>")",
            "the quotes stay outside the span");
}

void
a_list_uses_the_separator_the_caller_gives([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // The separator is the caller's, since the existing lines disagree about it.
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted, ","),
            R"("<sd>10.0.0.1:11210</sd>","<sd>10.0.0.2:11210</sd>")",
            "the given separator is used verbatim");
}

void
an_empty_list_renders_nothing([[maybe_unused]] context& ctx)
{
  const std::vector<std::string> empty{};
  {
    const scoped_log_redaction redaction{ false };
    assert_true(core_logger::system_data_list(empty).empty(), "empty while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_true(core_logger::system_data_list(empty).empty(), "empty while redaction is on");
  assert_true(core_logger::system_data_list(empty, core_logger::list_entries::quoted).empty(),
              "and no stray quotes are rendered");
}

void
a_document_id_splits_its_redaction_categories([[maybe_unused]] context& ctx)
{
  // A document id renders as bucket/scope.collection/key, and those parts do not share a category:
  // the names are metadata and only the key is user data. Wrapping the rendered form as a whole
  // would put a bucket name inside a <ud> span.
  const couchbase::core::document_id id{ "travel-sample", "inventory", "airline", "airline_10" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("{}", core_logger::document(id)),
              "travel-sample/inventory.airline/airline_10",
              "the rendered text while redaction is off");

    // The plain formatter is also used outside logging, so the two must agree while redaction is
    // off. If they ever diverge, a log line changes text for users who never enabled anything.
    assert_eq(fmt::format("{}", core_logger::document(id)),
              fmt::format("{}", id),
              "and it matches the plain formatter");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("{}", core_logger::document(id)),
            "<md>travel-sample</md>/<md>inventory</md>.<md>airline</md>/<ud>airline_10</ud>",
            "each component takes its own category");
}

void
a_document_id_built_without_a_collection_keeps_its_shape([[maybe_unused]] context& ctx)
{
  // This constructor leaves the collection path empty, so the id renders with an empty middle
  // component. Keep that shape rather than joining an empty scope and collection into a ".".
  const couchbase::core::document_id id{ "travel-sample", "airline_10" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("{}", core_logger::document(id)),
              "travel-sample//airline_10",
              "the empty middle component is kept");
    assert_eq(fmt::format("{}", core_logger::document(id)),
              fmt::format("{}", id),
              "and it matches the plain formatter");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("{}", core_logger::document(id)),
            "<md>travel-sample</md>//<ud>airline_10</ud>",
            "and the shape survives tagging");
}

void
exclusion_markers_never_change_what_is_printed([[maybe_unused]] context& ctx)
{
  // Both markers record a reviewed decision for the annotation checker and for anyone reading the
  // statement. Neither may alter the output, in either redaction state.
  const std::string host{ "10.0.0.1" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("host={}", core_logger::not_sensitive(host)),
              "host=10.0.0.1",
              "not_sensitive while redaction is off");
    assert_eq(fmt::format("host={}", core_logger::not_redacted(host)),
              "host=10.0.0.1",
              "not_redacted while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("host={}", core_logger::not_sensitive(host)),
            "host=10.0.0.1",
            "not_sensitive while redaction is on");
  assert_eq(fmt::format("host={}", core_logger::not_redacted(host)),
            "host=10.0.0.1",
            "not_redacted while redaction is on");
}

void
format_specifications_survive_the_wrappers([[maybe_unused]] context& ctx)
{
  const std::string bucket{ "bucket" };
  const std::vector<std::byte> body{ std::byte{ 0xde }, std::byte{ 0xad } };

  const scoped_log_redaction redaction{ true };

  // Every wrapper inherits parse() from the formatter of the value, so a specification keeps
  // working. It applies to the value alone, which means padding lands inside the tags.
  assert_eq(fmt::format("{:>8}", core_logger::metadata(bucket)),
            "<md>  bucket</md>",
            "the padding lands inside the span");

  // That inheritance is what lets a value keep a spec as unusual as the "{:a}" of a hex dump. No
  // site pairs the two today, since a hex dump is never tagged and never marked, but the wrappers
  // have to be spec-transparent in general and this is the strongest spec to pin that with.
  assert_eq(fmt::format("{:a}", core_logger::not_redacted(spdlog::to_hex(body))),
            fmt::format("{:a}", spdlog::to_hex(body)),
            "a hex dump renders identically through the marker");
}

void
connection_context_prefixes_bake_in_their_tags([[maybe_unused]] context& ctx)
{
  // Connection-context prefixes are formatted once, when a bucket or session is constructed, and
  // reused for the lifetime of that object. Their tags are therefore fixed at construction time and
  // do not follow later changes to the redaction setting. This is why redaction must be enabled
  // before connecting; see couchbase::core::logger::set_log_redaction().
  std::string prefix;
  {
    const scoped_log_redaction redaction{ true };
    prefix = fmt::format("[{}/{}]", "client-id", core_logger::metadata("travel-sample"));
    assert_eq(prefix, "[client-id/<md>travel-sample</md>]", "built while redaction was on");
  }

  assert_false(core_logger::is_log_redaction_enabled(), "redaction is off again");
  assert_eq(prefix, "[client-id/<md>travel-sample</md>]", "and the prefix keeps its tags");

  // Conversely, a prefix built while redaction was disabled stays untagged even once it is on.
  std::string untagged_prefix;
  {
    const scoped_log_redaction redaction{ false };
    untagged_prefix = fmt::format("[{}/{}]", "client-id", core_logger::metadata("travel-sample"));
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(untagged_prefix, "[client-id/travel-sample]", "and an untagged prefix stays untagged");
}

void
enabling_redaction_is_observable([[maybe_unused]] context& ctx)
{
  assert_false(core_logger::is_log_redaction_enabled(), "off to begin with");
  {
    const scoped_log_redaction redaction{ true };
    assert_true(core_logger::is_log_redaction_enabled(), "on while the guard is alive");
  }
  assert_false(core_logger::is_log_redaction_enabled(), "and off again afterwards");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(reset_stops_the_file_logger) },
      { CASE(the_protocol_logger_can_be_reset_while_another_thread_logs) },
      { CASE(an_idle_thread_does_not_keep_a_reset_logger_alive) },
      { CASE(a_protocol_logger_is_registered_exactly_while_it_logs) },
      { CASE(replacing_the_file_logger_drops_no_message) },
      { CASE(a_new_file_logger_keeps_the_protocol_logger_writing) },
      { CASE(loggers_created_at_once_share_one_pool) },
      { CASE(logger_creation_serializes_with_shutdown) },
      { CASE(shutdown_flushes_loggers_captured_under_the_registry_lock) },
      { CASE(shutdown_on_the_logging_worker_leaves_the_pool_for_another_thread) },
      { CASE(in_flight_log_calls_retain_the_pool_across_shutdown) },
      { CASE(logging_after_shutdown_without_reset_avoids_the_logger_mutex) },
      { CASE(a_registered_callback_receives_every_logged_message) },
      { CASE(the_callback_receives_the_level_of_each_message) },
      { CASE(logging_with_no_callback_registered_does_not_throw) },
      { CASE(registering_a_second_callback_replaces_the_first) },
      { CASE(unregistering_stops_delivery_until_a_callback_is_registered_again) },
      { CASE(arguments_are_not_formatted_when_logging_is_off_and_no_callback_is_registered) },
      { CASE(a_registered_callback_receives_formatted_arguments_while_logging_is_off) },
      { CASE(redaction_annotations_are_inert_while_redaction_is_disabled) },
      { CASE(redaction_annotations_wrap_values_while_redaction_is_enabled) },
      { CASE(a_tagged_value_never_spans_log_lines) },
      { CASE(every_entry_of_a_tagged_list_is_escaped) },
      { CASE(a_value_with_newlines_is_untouched_while_redaction_is_disabled) },
      { CASE(the_exclusion_markers_never_escape_their_value) },
      { CASE(a_conditionally_tagged_value_is_only_tagged_on_the_sensitive_branch) },
      { CASE(a_tagged_value_cannot_close_its_own_span) },
      { CASE(markup_that_is_not_a_tag_sequence_is_left_alone) },
      { CASE(tag_injection_is_escaped_in_every_category_and_in_a_list) },
      { CASE(a_dynamic_format_specification_still_resolves_inside_a_tag) },
      { CASE(a_serialised_document_carries_no_annotations_of_its_own) },
      { CASE(redaction_annotations_work_for_non_string_values) },
      { CASE(redaction_annotations_may_be_mixed_in_a_single_statement) },
      { CASE(a_list_is_inert_while_redaction_is_disabled) },
      { CASE(a_list_is_tagged_one_entry_at_a_time) },
      { CASE(a_list_tag_sits_inside_the_quotes_an_entry_renders) },
      { CASE(a_list_uses_the_separator_the_caller_gives) },
      { CASE(an_empty_list_renders_nothing) },
      { CASE(a_document_id_splits_its_redaction_categories) },
      { CASE(a_document_id_built_without_a_collection_keeps_its_shape) },
      { CASE(exclusion_markers_never_change_what_is_printed) },
      { CASE(format_specifications_survive_the_wrappers) },
      { CASE(connection_context_prefixes_bake_in_their_tags) },
      { CASE(enabling_redaction_is_observable) },
    },
  };
}

} // namespace couchbase::test
