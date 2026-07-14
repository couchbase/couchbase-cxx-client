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

#include "test_helper.hxx"

#include <couchbase/logger.hxx>

#include "core/logger/logger.hxx"

#include <spdlog/fmt/bundled/format.h>

#include <atomic>
#include <string>
#include <string_view>
#include <vector>

namespace
{
// Counts how many times its fmt formatter runs, so a test can assert whether the logging macro
// evaluated (formatted) its arguments at all.
std::atomic<int> probe_format_count{ 0 };
struct format_probe {
};
} // namespace

template<>
struct fmt::formatter<format_probe> : fmt::formatter<std::string_view> {
  auto format(format_probe /*probe*/, fmt::format_context& ctx) const -> decltype(ctx.out())
  {
    probe_format_count.fetch_add(1, std::memory_order_relaxed);
    return fmt::formatter<std::string_view>::format("probe", ctx);
  }
};

TEST_CASE("unit: simple callback", "[unit]")
{
  std::vector<std::string> captured_logs;
  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level /*level*/,
                                   couchbase::logger::log_location location) {
    std::string log_entry = std::string(msg) + " [" + location.file + ":" +
                            std::to_string(location.line) + " " + location.function + "]";
    captured_logs.push_back(log_entry);
  };
  couchbase::logger::register_log_callback(callback);

  CB_LOG_INFO("Test log message 1");
  CB_LOG_WARNING("Test log message 2");

  assert(captured_logs.size() == 2);
  assert(captured_logs[0].find("Test log message 1") != std::string::npos);
  assert(captured_logs[1].find("Test log message 2") != std::string::npos);
}

TEST_CASE("unit: custom callback level filtering", "[unit]")
{
  std::vector<std::string> captured_logs;
  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level level,
                                   couchbase::logger::log_location location) {
    if (level == couchbase::logger::log_level::error) {
      std::string log_entry = std::string(msg) + " [" + location.file + ":" +
                              std::to_string(location.line) + " " + location.function + "]";
      captured_logs.push_back(log_entry);
    }
  };
  couchbase::logger::register_log_callback(callback);

  CB_LOG_INFO("Test log message 1");
  CB_LOG_ERROR("Test log message 2");

  assert(captured_logs.size() == 1);
  assert(captured_logs[0].find("Test log message 2") != std::string::npos);
}

TEST_CASE("unit: custom callback nullptr", "[unit]")
{
  couchbase::logger::register_log_callback(nullptr);

  CB_LOG_INFO("Test log message 1");
}

TEST_CASE("unit: overwrite custom log callback", "[unit]")
{
  std::vector<std::string> captured_logs;

  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level level,
                                   couchbase::logger::log_location location) {
    if (level == couchbase::logger::log_level::error) {
      std::string log_entry = std::string(msg) + " [" + location.file + ":" +
                              std::to_string(location.line) + " " + location.function + "]";
      captured_logs.push_back(log_entry);
    }
  };

  auto callback2 = [&captured_logs](std::string_view msg,
                                    couchbase::logger::log_level level,
                                    couchbase::logger::log_location location) {
    if (level == couchbase::logger::log_level::trace) {
      std::string log_entry = std::string(msg) + " [" + location.file + ":" +
                              std::to_string(location.line) + " " + location.function + "]";
      captured_logs.push_back(log_entry);
    }
  };

  couchbase::logger::register_log_callback(callback);
  couchbase::logger::register_log_callback(callback2);

  CB_LOG_ERROR("Test error message");
  CB_LOG_TRACE("Test trace message");

  assert(captured_logs.size() == 1);
  assert(captured_logs[0].find("Test trace message") != std::string::npos);
}

TEST_CASE("unit: reregister custom log callback", "[unit]")
{
  std::vector<std::string> captured_logs;

  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level level,
                                   couchbase::logger::log_location location) {
    if (level == couchbase::logger::log_level::error) {
      std::string log_entry = std::string(msg) + " [" + location.file + ":" +
                              std::to_string(location.line) + " " + location.function + "]";
      captured_logs.push_back(log_entry);
    }
  };

  couchbase::logger::register_log_callback(callback);

  CB_LOG_ERROR("Test error message");

  couchbase::logger::unregister_log_callback();

  CB_LOG_ERROR("Test error message 2");

  couchbase::logger::register_log_callback(callback);

  CB_LOG_ERROR("Test error message 3");

  assert(captured_logs.size() == 2);
  assert(captured_logs[0].find("Test error message") != std::string::npos);
  assert(captured_logs[1].find("Test error message 3") != std::string::npos);
}

TEST_CASE("unit: no argument formatting when logging disabled and no callback", "[unit]")
{
  couchbase::logger::unregister_log_callback();
  couchbase::logger::set_level(couchbase::logger::log_level::off);
  probe_format_count.store(0);

  CB_LOG_TRACE("value={}", format_probe{});

  REQUIRE(probe_format_count.load() == 0);
}

TEST_CASE("unit: arguments formatted and delivered when a callback is registered", "[unit]")
{
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

  couchbase::logger::unregister_log_callback();
  REQUIRE(probe_format_count.load() == 1);
  REQUIRE_FALSE(captured.empty());
  REQUIRE(captured.back().find("value=probe") != std::string::npos);
}
