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

// Views are deprecated in the public API but still part of the core surface these fixtures reach,
// so the core includes below are parsed with the deprecation suppressed -- the same convention
// component.hxx and cluster.cxx use. push/pop so a translation unit that already defined the macro
// keeps its own state.
//
// The order matters and is not cosmetic: an include-guarded header is parsed once, by whoever
// reaches it first. core/operations.hxx pulls in core/operations/document_view.hxx, and if that
// happens before this #define, document_view_request is declared [[deprecated]] -- after which
// component.hxx's own declaration of the view overload is an error under -Werror, in every
// translation unit that includes this file without the macro of its own.
#pragma push_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

// core/operations.hxx (complete operation types) must precede core/cluster.hxx.
#include "core/operations.hxx"

#include "core/cluster.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/document_id.hxx"
#include "core/origin.hxx"
#include "core/protostellar/component.hxx"
#include "core/protostellar/credentials.hxx"
#include "core/tls_verify_mode.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

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

// Skip the case when -- and only when -- the gateway is what does not implement the service.
//
// feature_not_available is ambiguous, and skipping on the bare error code makes a case
// unfalsifiable. The client produces exactly that code for its own refusals: cluster.cxx answers a
// request type it does not route over couchbase2 with a plain error context, and a converter's
// can_encode() rejects an option the schema cannot express the same way. A case that skips on the
// code alone therefore also skips when the routing it exists to cover is deleted -- it reports
// "Skipped" instead of failing, so it cannot detect the absence of the thing it tests.
//
// The gateway's own answer is distinguishable. An UNIMPLEMENTED reply arrives as a gRPC status and
// the component copies its message into first_error_message; the client-side refusals leave that
// field empty because no status was ever received. Requiring the message is what makes the skip
// evidence that the request reached a gateway and was declined there, rather than a belief about
// what the gateway supports.
//
// loc is forwarded so a failure points at the case rather than at this line.
template<typename Context>
void
skip_unless_service_implemented(const Context& ctx,
                                std::string_view service,
                                source_location loc = source_location::current())
{
  if (ctx.ec != errc::common::feature_not_available) {
    return;
  }
  assert_false(ctx.first_error_message.empty(),
               fmt::format("{} reported feature_not_available with no gateway message, so the "
                           "request was refused by the client and never reached the gateway",
                           service),
               loc);
  skip(fmt::format("gateway does not implement {} ({})", service, ctx.first_error_message));
}

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

// The same idea one layer up: an opened core::cluster rather than a bare component.
//
// A case built on this exercises the routing in cluster.cxx as well as the transport, which is
// where a request type reaches (or fails to reach) the couchbase2 component at all. Services
// other than KV have no component-level entry point in the tests, so this is the fixture they
// use.
//
// The preamble it replaces -- parse, skip, credentials, origin, io thread, open, close -- is
// about thirty lines, and hand-rolling it is what let a raw std::getenv into live_query.cxx while
// live_connect.cxx used the framework wrapper.
class live_cluster_fixture
{
public:
  live_cluster_fixture()
  {
    const auto connection_string = safe_getenv("TEST_CONNECTION_STRING");
    if (!connection_string.has_value()) {
      skip("TEST_CONNECTION_STRING is not set");
    }
    auto parsed = core::utils::parse_connection_string(connection_string.value());
    if (!parsed.uses_protostellar()) {
      skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
    }

    core::cluster_credentials credentials;
    credentials.username = env_or("TEST_CB2_USERNAME", "Administrator");
    credentials.password = env_or("TEST_CB2_PASSWORD", "password");
    bucket_ = env_or("TEST_CB2_BUCKET", "default");

    core::origin cluster_origin{ credentials, parsed };
    cluster_origin.options().tls_verify =
      core::tls_verify_mode::none; // dev gateway certificate is not chainable

    std::promise<std::error_code> opened;
    cluster_.open(cluster_origin, [&opened](std::error_code ec) {
      opened.set_value(ec);
    });
    // Recorded rather than asserted: a throw here would skip the destructor, leaving an opened
    // cluster to be torn down without close(). The case checks it as its first assertion.
    open_ec_ = opened.get_future().get();
    opened_ = !open_ec_;
  }

  live_cluster_fixture(const live_cluster_fixture&) = delete;
  live_cluster_fixture(live_cluster_fixture&&) = delete;
  auto operator=(const live_cluster_fixture&) -> live_cluster_fixture& = delete;
  auto operator=(live_cluster_fixture&&) -> live_cluster_fixture& = delete;

  ~live_cluster_fixture()
  {
    if (!opened_) {
      return;
    }
    std::promise<void> closed;
    cluster_.close([&closed]() {
      closed.set_value();
    });
    closed.get_future().get();
  }

  // Fails the case unless open() succeeded. Call it first; every other method assumes it passed.
  void require_open() const
  {
    assert_false(static_cast<bool>(open_ec_), "connect(couchbase2://) succeeds");
  }

  template<typename Request>
  [[nodiscard]] auto execute(Request request) -> typename Request::response_type
  {
    return execute_on(std::move(request)).first;
  }

  // The response plus the thread the handler ran on. Every completion is supposed to arrive on the
  // SDK's execution context, so a handler that runs on the calling thread means some path called
  // back inline out of execute() -- which re-enters the caller before it has its pending_call, and
  // is invisible to a case that only looks at the response.
  template<typename Request>
  [[nodiscard]] auto execute_on(Request request)
    -> std::pair<typename Request::response_type, std::thread::id>
  {
    if (!request.timeout.has_value()) {
      request.timeout = std::chrono::milliseconds{ 20000 };
    }
    std::promise<std::pair<typename Request::response_type, std::thread::id>> promise;
    auto future = promise.get_future();
    cluster_.execute(std::move(request),
                     [&promise](typename Request::response_type response) mutable {
                       promise.set_value({ std::move(response), std::this_thread::get_id() });
                     });
    return future.get();
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
  // io_ -> runner_ -> cluster_, so the cluster tears down while the io thread is still running
  // and the thread is joined afterwards.
  io_thread_guard runner_{ io_ };
  core::cluster cluster_{ io_ };
  std::string bucket_{};
  std::error_code open_ec_{};
  bool opened_{ false };
};

} // namespace couchbase::cng::test

#pragma pop_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
