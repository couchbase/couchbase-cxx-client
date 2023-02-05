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

#include "test/utils/wait_until.hxx"
#include "test_helper.hxx"
#include <core/logger/configuration.hxx>
#include <core/logger/logger.hxx>
#include <core/transactions/internal/logging.hxx>

#include <spdlog/sinks/base_sink.h>

#include <iostream>
#include <iterator>

using namespace couchbase::core::transactions;

class TrivialFileSink : public spdlog::sinks::base_sink<std::mutex>
{
  public:
    std::string output()
    {
        // prevent data race if sink_it_ is called
        std::lock_guard<std::mutex> lock(mut_);
        return out_.str();
    }

  protected:
    void sink_it_(const spdlog::details::log_msg& msg) override
    {
        spdlog::memory_buf_t formatted;
        base_sink<std::mutex>::formatter_->format(msg, formatted);
        // prevent data race when calling output()
        std::lock_guard<std::mutex> lock(mut_);
        out_ << formatted.data();
    }
    void flush_() override
    {
    }

  private:
    std::stringstream out_;
    // needed since we examine the internal state of this object
    std::mutex mut_;
};

bool
sink_has_output(std::shared_ptr<TrivialFileSink> sink)
{
    return test::utils::wait_until([&]() { return !sink->output().empty(); }, std::chrono::seconds(2), std::chrono::milliseconds(100));
}

bool
sink_is_empty(std::shared_ptr<TrivialFileSink> sink)
{
    // now, we need to be sure it is empty, and stays that way for some period of time,
    // since async loggers don't flush immediately.   The logger is set to flush every
    // second, so waiting 2 or 3 seems reasonable
    if (sink->output().empty()) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        return sink->output().empty();
    }
    return false;
}

bool
sink_output_contains(std::shared_ptr<TrivialFileSink> sink, const std::string& msg)
{
    return std::string::npos != sink->output().find(msg);
}

void
create_logger(std::shared_ptr<TrivialFileSink> sink, couchbase::core::logger::level level)
{
    couchbase::core::logger::configuration conf{};
    conf.log_level = level;
    conf.sink = sink;
    conf.console = false;
    couchbase::core::logger::create_file_logger(conf);
}

TEST_CASE("transactions: can use custom sink", "[unit]")
{
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_logger(sink, couchbase::core::logger::level::trace);
    CB_TXN_LOG_DEBUG(log_message);
    couchbase::core::logger::flush();
    // ASYNC logger, so flush returns immediately.   Gotta wait...
    REQUIRE(sink_has_output(sink));
    REQUIRE(sink_output_contains(sink, log_message));
}

TEST_CASE("transactions: custom sink respects log levels", "[unit]")
{
    std::string log_message = "I am a log";
    std::string log_message2 = "I am also a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_logger(sink, couchbase::core::logger::level::info);
    CB_TXN_LOG_DEBUG(log_message);
    couchbase::core::logger::flush();
    REQUIRE(sink_is_empty(sink));
    CB_TXN_LOG_INFO(log_message2);
    couchbase::core::logger::flush();
    REQUIRE(sink_has_output(sink));
    REQUIRE(sink_output_contains(sink, log_message2));
}

TEST_CASE("transactions: custom sink respects log level changes", "[unit]")
{
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_logger(sink, couchbase::core::logger::level::trace);
    couchbase::core::logger::set_log_levels(couchbase::core::logger::level::info);
    CB_TXN_LOG_DEBUG(log_message);
    couchbase::core::logger::flush();
    REQUIRE(sink_is_empty(sink));
}