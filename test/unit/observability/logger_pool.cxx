/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026 Couchbase, Inc.
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

#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/callback_sink.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace core_logger = couchbase::core::logger;

// Regression: the SDK's asynchronous loggers adopted spdlog's global pool, which belongs to the
// application. Replacing that pool left the SDK's loggers without one, and they dropped every
// message.
void
the_sdk_loggers_keep_writing_when_the_application_replaces_its_pool([[maybe_unused]] context& ctx)
{
  spdlog::init_thread_pool(16, 1);
  auto received = std::make_shared<std::atomic<int>>(0);
  core_logger::configuration file{};
  file.console = false;
  file.sink = std::make_shared<spdlog::sinks::callback_sink_mt>(
    [received](const spdlog::details::log_msg& /* msg */) {
      received->fetch_add(1);
    });
  const auto failure = core_logger::create_file_logger(file);
  file.sink.reset();

  spdlog::details::registry::instance().set_tp(nullptr);
  CB_LOG_INFO_RAW("message");
  // Joins the logging thread, which first writes every message queued before the call.
  core_logger::shutdown();
  core_logger::reset();

  assert_false(failure.has_value(), "the file logger was created");
  assert_eq(received->load(), 1, "the message reached its sink");
}

// Regression: creating an asynchronous SDK logger replaced spdlog's global pool, which an
// application logger created before it was using, and the application logger then dropped every
// message.
void
the_application_logger_keeps_writing_after_the_sdk_loggers_start([[maybe_unused]] context& ctx)
{
  spdlog::init_thread_pool(16, 1);
  auto received = std::make_shared<std::atomic<int>>(0);
  const auto application =
    std::make_shared<spdlog::async_logger>("application",
                                           std::make_shared<spdlog::sinks::callback_sink_mt>(
                                             [received](const spdlog::details::log_msg& /* msg */) {
                                               received->fetch_add(1);
                                             }),
                                           spdlog::thread_pool(),
                                           spdlog::async_overflow_policy::block);
  const auto directory = std::filesystem::temp_directory_path() /
                         ("cxx-logger-application-" + std::to_string(std::random_device{}()));
  std::filesystem::create_directories(directory);
  core_logger::configuration file{};
  file.console = false;
  const auto file_failure = core_logger::create_file_logger(file);
  core_logger::configuration protocol{};
  protocol.filename = (directory / "protocol").string();
  protocol.console = false;
  const auto protocol_failure = core_logger::create_protocol_logger(protocol);

  application->info("message");
  // Before the SDK's shutdown(), which also shuts down spdlog's registry and its global pool.
  const auto deadline = std::chrono::steady_clock::now() + scaled_budget(std::chrono::seconds{ 2 });
  while (received->load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{ 10 });
  }
  const int delivered = received->load();
  core_logger::shutdown();
  core_logger::reset();
  std::error_code ignored;
  std::filesystem::remove_all(directory, ignored);

  assert_false(file_failure.has_value(), "the file logger was created");
  assert_false(protocol_failure.has_value(), "the protocol logger was created");
  assert_eq(delivered, 1, "the application's message reached its sink");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(the_sdk_loggers_keep_writing_when_the_application_replaces_its_pool) },
      { CASE(the_application_logger_keeps_writing_after_the_sdk_loggers_start) },
    },
  };
}
} // namespace couchbase::test
