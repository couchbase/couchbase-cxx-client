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
#include "core/logger/redaction.hxx"

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

// Log redaction is process-wide state, so restore it even if an assertion fails part way through
// a test, otherwise the leak shows up as an unrelated failure elsewhere in the suite.
class scoped_log_redaction
{
public:
  explicit scoped_log_redaction(bool enable)
    : previous_{ couchbase::core::logger::is_log_redaction_enabled() }
  {
    couchbase::core::logger::set_log_redaction(enable);
  }

  scoped_log_redaction(const scoped_log_redaction&) = delete;
  scoped_log_redaction(scoped_log_redaction&&) = delete;
  auto operator=(const scoped_log_redaction&) -> scoped_log_redaction& = delete;
  auto operator=(scoped_log_redaction&&) -> scoped_log_redaction& = delete;

  ~scoped_log_redaction()
  {
    couchbase::core::logger::set_log_redaction(previous_);
  }

private:
  bool previous_;
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

TEST_CASE("unit: redaction annotations are inert while redaction is disabled", "[unit]")
{
  const scoped_log_redaction redaction{ false };
  namespace logger = couchbase::core::logger;

  REQUIRE(fmt::format("key={}", logger::user_data("my_key")) == "key=my_key");
  REQUIRE(fmt::format("bucket={}", logger::metadata("my_bucket")) == "bucket=my_bucket");
  REQUIRE(fmt::format("host={}", logger::system_data("127.0.0.1")) == "host=127.0.0.1");
}

TEST_CASE("unit: redaction annotations wrap values while redaction is enabled", "[unit]")
{
  const scoped_log_redaction redaction{ true };
  namespace logger = couchbase::core::logger;

  REQUIRE(fmt::format("key={}", logger::user_data("my_key")) == "key=<ud>my_key</ud>");
  REQUIRE(fmt::format("bucket={}", logger::metadata("my_bucket")) == "bucket=<md>my_bucket</md>");
  REQUIRE(fmt::format("host={}", logger::system_data("127.0.0.1")) == "host=<sd>127.0.0.1</sd>");
}

TEST_CASE("unit: redaction annotations work for non-string values", "[unit]")
{
  const scoped_log_redaction redaction{ true };
  namespace logger = couchbase::core::logger;

  const std::string host{ "10.0.0.1" };
  const std::string_view bucket{ "travel-sample" };

  REQUIRE(fmt::format("{}", logger::system_data(11210)) == "<sd>11210</sd>");
  REQUIRE(fmt::format("{}", logger::system_data(host)) == "<sd>10.0.0.1</sd>");
  REQUIRE(fmt::format("{}", logger::metadata(bucket)) == "<md>travel-sample</md>");
}

TEST_CASE("unit: redaction annotations may be mixed in a single statement", "[unit]")
{
  const scoped_log_redaction redaction{ true };
  namespace logger = couchbase::core::logger;

  REQUIRE(fmt::format("[{}/{}] <{}:{}> key={}",
                      "client-id",
                      logger::metadata("travel-sample"),
                      logger::system_data("10.0.0.1"),
                      logger::system_data(11210),
                      logger::user_data("airline_10")) ==
          "[client-id/<md>travel-sample</md>] <<sd>10.0.0.1</sd>:<sd>11210</sd>> "
          "key=<ud>airline_10</ud>");
}

TEST_CASE("unit: connection-context prefixes bake in their tags", "[unit]")
{
  namespace logger = couchbase::core::logger;

  // Connection-context prefixes are formatted once, when a bucket or session is constructed, and
  // reused for the lifetime of that object. Their tags are therefore fixed at construction time
  // and do not follow later changes to the redaction setting. This is why redaction must be
  // enabled before connecting; see couchbase::core::logger::set_log_redaction().
  std::string prefix;
  {
    const scoped_log_redaction redaction{ true };
    prefix = fmt::format("[{}/{}]", "client-id", logger::metadata("travel-sample"));
    REQUIRE(prefix == "[client-id/<md>travel-sample</md>]");
  }

  REQUIRE_FALSE(logger::is_log_redaction_enabled());
  REQUIRE(prefix == "[client-id/<md>travel-sample</md>]");

  // Conversely, a prefix built while redaction was disabled stays untagged even once it is on.
  std::string untagged_prefix;
  {
    const scoped_log_redaction redaction{ false };
    untagged_prefix = fmt::format("[{}/{}]", "client-id", logger::metadata("travel-sample"));
  }
  const scoped_log_redaction redaction{ true };
  REQUIRE(untagged_prefix == "[client-id/travel-sample]");
}

TEST_CASE("unit: enabling redaction is observable", "[unit]")
{
  namespace logger = couchbase::core::logger;

  REQUIRE_FALSE(logger::is_log_redaction_enabled());
  {
    const scoped_log_redaction redaction{ true };
    REQUIRE(logger::is_log_redaction_enabled());
  }
  REQUIRE_FALSE(logger::is_log_redaction_enabled());
}
