/*
 *     Copyright 2022 Couchbase, Inc.
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
#include "core/transactions/internal/logging.hxx"

#include <spdlog/sinks/base_sink.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
class capturing_sink : public spdlog::sinks::base_sink<std::mutex>
{
public:
  auto output() -> std::string
  {
    // prevent data race if sink_it_ is called
    const std::scoped_lock<std::mutex> lock(mut_);
    return out_.str();
  }

protected:
  void sink_it_(const spdlog::details::log_msg& msg) override
  {
    spdlog::memory_buf_t formatted;
    base_sink<std::mutex>::formatter_->format(msg, formatted);
    // prevent data race when calling output()
    const std::scoped_lock<std::mutex> lock(mut_);
    out_.write(formatted.data(), static_cast<std::streamsize>(formatted.size()));
  }
  void flush_() override
  {
  }

private:
  std::stringstream out_;
  // needed since we examine the internal state of this object
  std::mutex mut_;
};

// The logger is asynchronous, so flush() returns before the sink has been written.
auto
sink_has_output(const std::shared_ptr<capturing_sink>& sink) -> bool
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (sink->output().empty()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return true;
}

// A message that the level filter drops never arrives, so there is nothing to wait for. Waiting
// instead for a later message that the filter accepts settles it: the queue is ordered, so once
// the sentinel is in the sink, a dropped message would already be there had it been accepted.
void
assert_absent(const std::shared_ptr<capturing_sink>& sink,
              const std::string& text,
              std::string_view message)
{
  if (const auto output = sink->output(); output.find(text) != std::string::npos) {
    fail(std::string{ message } + " (the sink holds: " + output + ")");
  }
}

void
create_logger(const std::shared_ptr<capturing_sink>& sink, couchbase::core::logger::level level)
{
  couchbase::core::logger::configuration conf{};
  conf.log_level = level;
  conf.sink = sink;
  conf.console = false;
  if (const auto error = couchbase::core::logger::create_file_logger(conf); error.has_value()) {
    fail("the capturing logger was not created: " + *error);
  }
}

void
a_custom_sink_receives_transaction_logs([[maybe_unused]] context& ctx)
{
  const std::string log_message = "I am a log";
  auto sink = std::make_shared<capturing_sink>();
  create_logger(sink, couchbase::core::logger::level::trace);

  CB_TXN_LOG_DEBUG("{}", log_message);
  couchbase::core::logger::flush();

  assert_true(sink_has_output(sink), "the configured sink is written to");
  assert_contains(sink->output(), log_message, "the message reaches the sink verbatim");
}

void
a_custom_sink_respects_the_configured_level([[maybe_unused]] context& ctx)
{
  const std::string below_level = "dropped-by-level";
  const std::string at_level = "kept-by-level";
  auto sink = std::make_shared<capturing_sink>();
  create_logger(sink, couchbase::core::logger::level::info);

  CB_TXN_LOG_DEBUG("{}", below_level);
  CB_TXN_LOG_INFO("{}", at_level);
  couchbase::core::logger::flush();

  assert_true(sink_has_output(sink), "a message at the configured level is written");
  assert_contains(sink->output(), at_level, "the message reaches the sink verbatim");
  assert_absent(sink, below_level, "a message below the configured level is dropped");
}

void
a_custom_sink_respects_a_level_raised_after_creation([[maybe_unused]] context& ctx)
{
  const std::string below_level = "dropped-after-raise";
  const std::string sentinel = "kept-after-raise";
  auto sink = std::make_shared<capturing_sink>();
  create_logger(sink, couchbase::core::logger::level::trace);
  couchbase::core::logger::set_log_levels(couchbase::core::logger::level::info);

  CB_TXN_LOG_DEBUG("{}", below_level);
  CB_TXN_LOG_INFO("{}", sentinel);
  couchbase::core::logger::flush();

  assert_true(sink_has_output(sink), "a message at the raised level is written");
  assert_absent(sink, below_level, "the level set after creation is the one enforced");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_custom_sink_receives_transaction_logs) },
      { CASE(a_custom_sink_respects_the_configured_level) },
      { CASE(a_custom_sink_respects_a_level_raised_after_creation) },
    },
  };
}

} // namespace couchbase::test
