/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#pragma once

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/document_id.hxx"
#include "core/protostellar/component.hxx"
#include "core/protostellar/credentials.hxx"
#include "core/tls_verify_mode.hxx"
#include "core/utils/connection_string.hxx"

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

namespace couchbase::cng::test
{

// RAII owner of the io_context worker thread the live tests run against. It keeps a work guard so
// io_context::run() stays alive on a dedicated thread, and on scope exit -- normal OR via an
// assertion/skip exception -- it releases the guard and joins the thread. Declare it after the
// io_context and before the cluster so the cluster (which drains its gRPC calls in its destructor)
// tears down while the io thread is still running, and the thread is joined afterwards. Without
// this an early throw would unwind past a still-joinable std::thread and call std::terminate.
class io_thread_guard
{
public:
  explicit io_thread_guard(asio::io_context& io)
    : work_{ asio::make_work_guard(io) }
    , thread_{ [&io]() {
      io.run();
    } }
  {
  }

  io_thread_guard(const io_thread_guard&) = delete;
  io_thread_guard(io_thread_guard&&) = delete;
  auto operator=(const io_thread_guard&) -> io_thread_guard& = delete;
  auto operator=(io_thread_guard&&) -> io_thread_guard& = delete;

  ~io_thread_guard()
  {
    work_.reset();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

private:
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  std::thread thread_;
};

// A component wired to the gateway named by TEST_CONNECTION_STRING, with the io_context already
// running on its own thread so an operation can be awaited inline.
//
// Constructing one skips the case unless the environment actually describes a couchbase2://
// endpoint, so a case body can assume it has a live gateway. The alternative -- repeating the
// parse/skip/credentials/channel preamble per case -- is about forty lines each, which buries the
// one operation the case exists to check and drifts between copies.
class live_kv_fixture
{
public:
  live_kv_fixture()
  {
    const auto connection_string = safe_getenv("TEST_CONNECTION_STRING");
    if (!connection_string.has_value()) {
      skip("TEST_CONNECTION_STRING is not set");
    }
    const auto parsed = core::utils::parse_connection_string(connection_string.value());
    if (!parsed.uses_protostellar()) {
      skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
    }
    if (parsed.bootstrap_nodes.empty()) {
      skip("no nodes in TEST_CONNECTION_STRING");
    }

    const auto& node = parsed.bootstrap_nodes.front();
    const auto port = node.port > 0 ? node.port : parsed.default_port;

    core::cluster_options options;
    options.enable_tls = true;
    options.tls_verify = core::tls_verify_mode::none; // dev gateway certificate is not chainable

    credentials_.username = env_or("TEST_CB2_USERNAME", "Administrator");
    credentials_.password = env_or("TEST_CB2_PASSWORD", "password");
    bucket_ = env_or("TEST_CB2_BUCKET", "default");

    // Only the two fields whose names are stable across the series are set here. The per-operation
    // deadline is applied in execute() instead of through the config, because the config's timeout
    // members are reshaped partway up the stack and a fixture that named them would have to be
    // edited in step with that.
    core::protostellar::component_config config;
    config.channel = core::protostellar::make_channel(
      node.address + ":" + std::to_string(port), options, credentials_);
    config.credentials = credentials_;
    component_ = std::make_unique<core::protostellar::component>(io_, config);
  }

  live_kv_fixture(const live_kv_fixture&) = delete;
  live_kv_fixture(live_kv_fixture&&) = delete;
  auto operator=(const live_kv_fixture&) -> live_kv_fixture& = delete;
  auto operator=(live_kv_fixture&&) -> live_kv_fixture& = delete;
  ~live_kv_fixture() = default;

  // Dispatches one operation and blocks until its handler runs. The handler runs on the io thread
  // while this waits, so a case reads as a sequence of operations rather than as nested callbacks.
  template<typename Request>
  [[nodiscard]] auto execute(Request request) -> typename Request::response_type
  {
    // A real gateway over a real network needs more than the SDK's default KV deadline, and a case
    // that wants a different one can set it before calling.
    if (!request.timeout.has_value()) {
      request.timeout = std::chrono::milliseconds{ 20000 };
    }
    std::promise<typename Request::response_type> promise;
    auto future = promise.get_future();
    component_->execute(std::move(request),
                        [&promise](typename Request::response_type response) mutable {
                          promise.set_value(std::move(response));
                        });
    return future.get();
  }

  [[nodiscard]] auto id(const std::string& key) const -> core::document_id
  {
    return core::document_id{ bucket_, "_default", "_default", key };
  }

  [[nodiscard]] auto bucket() const -> const std::string&
  {
    return bucket_;
  }

private:
  [[nodiscard]] static auto env_or(const char* name, const char* fallback) -> std::string
  {
    return safe_getenv(name).value_or(fallback);
  }

  asio::io_context io_{};
  // Declared after io_ and before component_: the component drains its gRPC calls as it is
  // destroyed, which needs the io thread still running, and the guard joins it afterwards.
  io_thread_guard runner_{ io_ };
  core::cluster_credentials credentials_{};
  std::string bucket_{};
  std::unique_ptr<core::protostellar::component> component_{};
};

} // namespace couchbase::cng::test
