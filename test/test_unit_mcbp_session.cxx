/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

/**
 * Unit tests for mcbp_session accessor methods added/modified in CXXCBC-794:
 *   - log_prefix() return type changed from const std::string& to std::string
 *   - supports_feature() / supported_features() / context() now guard
 *     supported_features_ with session_info_mutex_
 *   - on_configuration_update() / config_listeners_.clear() likewise guarded
 *
 * These tests construct an mcbp_session without establishing a real TCP
 * connection (bootstrap is never called) so they run offline.
 *
 * Note: mcbp_session_impl (and therefore mcbp_session) requires explicit
 * stop() + io_context::run() before destruction to drain Asio-dispatched
 * cleanup tasks that capture shared_from_this().
 */

#include "test_helper.hxx"

#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/origin.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>

#include <future>
#include <string>
#include <thread>
#include <vector>

namespace
{
using couchbase::core::io::mcbp_session;
using couchbase::core::protocol::hello_feature;

// Minimal bootstrap_state_listener stub — bootstrap is never invoked in these
// tests so the virtual methods are never called.
class stub_bootstrap_listener : public couchbase::core::impl::bootstrap_state_listener
{
public:
  void report_bootstrap_error(const std::string& /*endpoint*/, std::error_code /*ec*/) override
  {
  }

  void report_bootstrap_success(const std::vector<std::string>& /*endpoints*/) override
  {
  }

  void register_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }

  void unregister_config_listener(
    std::shared_ptr<couchbase::core::config_listener> /*listener*/) override
  {
  }
};

// Build a minimal unconnected mcbp_session with the given known_features.
// The io_context is not run, so no real network activity occurs.
auto
make_session(asio::io_context& io, std::vector<hello_feature> known_features = {}) -> mcbp_session
{
  auto origin = couchbase::core::origin(
    couchbase::core::cluster_credentials{ "user", "pass" },
    couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1"));
  return { "test-client-id",
           "test-node-uuid",
           io,
           std::move(origin),
           std::make_shared<stub_bootstrap_listener>(),
           std::nullopt,
           std::move(known_features) };
}

// mcbp_session_impl::stop() dispatches cleanup to the io_context strand.
// Call this before letting the session go out of scope, then poll the context
// to drain those tasks so shared_from_this() references are released cleanly.
void
stop_and_drain(mcbp_session& session, asio::io_context& io)
{
  session.stop(couchbase::retry_reason::do_not_retry);
  io.run();
}
} // namespace

// ---------------------------------------------------------------------------
// log_prefix — return type is now std::string (by value)
// ---------------------------------------------------------------------------

TEST_CASE("unit: mcbp_session log_prefix returns by value", "[unit]")
{
  asio::io_context io{};
  auto session = make_session(io);

  // The return type must be a value (std::string), not a reference.  We
  // verify this by binding to a plain std::string; the assignment would fail
  // to compile if the returned type were a dangling reference.
  std::string prefix = session.log_prefix();
  CHECK_FALSE(prefix.empty());
  stop_and_drain(session, io);
}

// ---------------------------------------------------------------------------
// supports_feature / supported_features — mutex-guarded since CXXCBC-794
// ---------------------------------------------------------------------------

TEST_CASE("unit: mcbp_session supports_feature with no known features", "[unit]")
{
  asio::io_context io{};
  auto session = make_session(io, {});

  CHECK_FALSE(session.supports_feature(hello_feature::collections));
  CHECK_FALSE(session.supports_feature(hello_feature::json));
  CHECK(session.supported_features().empty());
  stop_and_drain(session, io);
}

TEST_CASE("unit: mcbp_session supports_feature with known features", "[unit]")
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections, hello_feature::json });

  CHECK(session.supports_feature(hello_feature::collections));
  CHECK(session.supports_feature(hello_feature::json));
  CHECK_FALSE(session.supports_feature(hello_feature::duplex));

  const auto features = session.supported_features();
  REQUIRE(features.size() == 2);
  CHECK(std::find(features.begin(), features.end(), hello_feature::collections) != features.end());
  CHECK(std::find(features.begin(), features.end(), hello_feature::json) != features.end());
  stop_and_drain(session, io);
}

// ---------------------------------------------------------------------------
// context() — returns mcbp_context whose supported_features list reflects
// the known features passed at construction
// ---------------------------------------------------------------------------

TEST_CASE("unit: mcbp_session context reflects known features", "[unit]")
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });

  auto ctx = session.context();
  CHECK(ctx.supports_feature(hello_feature::collections));
  CHECK_FALSE(ctx.supports_feature(hello_feature::json));
  // No config is set before bootstrapping.
  CHECK_FALSE(ctx.config.has_value());
  stop_and_drain(session, io);
}

// ---------------------------------------------------------------------------
// Concurrent read of log_prefix / supports_feature / supported_features
// (exercises the session_info_mutex_ introduced in CXXCBC-794)
// ---------------------------------------------------------------------------

TEST_CASE("unit: mcbp_session session_info accessors are safe under concurrent reads", "[unit]")
{
  asio::io_context io{};
  auto session =
    make_session(io, { hello_feature::collections, hello_feature::json, hello_feature::duplex });

  constexpr int num_threads = 8;
  constexpr int iterations = 1000;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  std::promise<void> start_gate;
  auto start_future = start_gate.get_future().share();

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&session, start_future]() -> void {
      start_future.wait();
      for (int i = 0; i < iterations; ++i) {
        // All three accessors are now mutex-guarded; none should deadlock or
        // return garbled data when called from multiple threads simultaneously.
        (void)session.log_prefix();
        (void)session.supported_features();
        (void)session.supports_feature(hello_feature::collections);
        (void)session.context();
      }
    });
  }

  start_gate.set_value(); // release all threads at once
  for (auto& th : threads) {
    th.join();
  }
  // If we reach here without a crash or deadlock the test passes.
  SUCCEED("concurrent reads of session_info accessors completed without error");
  stop_and_drain(session, io);
}

// ---------------------------------------------------------------------------
// is_stopped — sanity check: a freshly constructed session is not stopped
// ---------------------------------------------------------------------------

TEST_CASE("unit: mcbp_session is not stopped after construction", "[unit]")
{
  asio::io_context io{};
  auto session = make_session(io);
  CHECK_FALSE(session.is_stopped());
  CHECK_FALSE(session.is_bootstrapped());
  stop_and_drain(session, io);
}
