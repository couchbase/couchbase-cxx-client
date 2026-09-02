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

#include "core/logger/logger.hxx"

#include <spdlog/fmt/bundled/format.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
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
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_registered_callback_receives_every_logged_message) },
      { CASE(the_callback_receives_the_level_of_each_message) },
      { CASE(logging_with_no_callback_registered_does_not_throw) },
      { CASE(registering_a_second_callback_replaces_the_first) },
      { CASE(unregistering_stops_delivery_until_a_callback_is_registered_again) },
      { CASE(arguments_are_not_formatted_when_logging_is_off_and_no_callback_is_registered) },
      { CASE(a_registered_callback_receives_formatted_arguments_while_logging_is_off) },
    },
  };
}

} // namespace couchbase::test
