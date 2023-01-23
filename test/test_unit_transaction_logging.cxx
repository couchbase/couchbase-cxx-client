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

#include "core/transactions/internal/logging.hxx"
#include "test_helper.hxx"
#include <core/transactions.hxx>

#include <spdlog/sinks/base_sink.h>

#include <iostream>
#include <iterator>

using namespace couchbase::core::transactions;

class TrivialFileSink : public spdlog::sinks::base_sink<std::mutex>
{
  public:
    std::string output() const
    {
        return out_.str();
    }

  protected:
    void sink_it_(const spdlog::details::log_msg& msg) override
    {
        spdlog::memory_buf_t formatted;
        base_sink<std::mutex>::formatter_->format(msg, formatted);
        out_ << formatted.data();
    }
    void flush_() override
    {
    }

  private:
    std::stringstream out_;
};

TEST_CASE("transactions: can use custom sink", "[unit]")
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_loggers(couchbase::core::logger::level::debug, sink);
    txn_log->debug(log_message);
    txn_log->flush();
    REQUIRE_FALSE(sink->output().empty());
    REQUIRE(std::string::npos != sink->output().find(log_message));
}

TEST_CASE("transactions: custom sink respects log levels", "[unit]")
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_loggers(couchbase::core::logger::level::info, sink);
    txn_log->debug(log_message);
    txn_log->flush();
    REQUIRE(sink->output().empty());
    couchbase::core::logger::create_console_logger();
}

TEST_CASE("transactions: custom sink respects log level changes", "[unit]")
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    create_loggers(couchbase::core::logger::level::debug, sink);
    set_transactions_log_level(couchbase::core::logger::level::info);
    txn_log->debug(log_message);
    txn_log->flush();
    REQUIRE(sink->output().empty());
    couchbase::core::logger::create_console_logger();
}