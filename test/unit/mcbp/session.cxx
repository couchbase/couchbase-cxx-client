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

// The session accessors that report what was negotiated: log_prefix(), supports_feature(),
// supported_features() and context(), each of which reads supported_features_ under
// session_info_mutex_.
//
// Every case here constructs an mcbp_session and never bootstraps it, so the socket is never
// opened and no case touches the network.

#include "framework/test_registry.hxx"

#include "core/impl/bootstrap_state_listener.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/origin.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/retry_reason.hxx>

#include <asio/io_context.hpp>

#include <algorithm>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::mcbp_session;
using couchbase::core::protocol::hello_feature;

// Bootstrap is never invoked here, so none of these is ever called.
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

// The io_context is never run until stop_and_drain(), so the session performs no network activity.
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

// mcbp_session_impl::stop() dispatches cleanup to the io_context strand. Call this before letting
// the session go out of scope, then run the context to drain those tasks so the shared_from_this()
// references they hold are released.
void
stop_and_drain(mcbp_session& session, asio::io_context& io)
{
  session.stop(couchbase::retry_reason::do_not_retry);
  io.run();
}

void
log_prefix_is_returned_by_value([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io);

  // Binding to a std::string rather than to a reference: the accessor builds its text under
  // session_info_mutex_, so a reference would outlive the lock that made it safe to read.
  std::string prefix = session.log_prefix();
  assert_false(prefix.empty(), "a session identifies itself in the log");
  stop_and_drain(session, io);
}

void
a_session_with_no_known_features_supports_none([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, {});

  assert_false(session.supports_feature(hello_feature::collections),
               "a feature that was not negotiated");
  assert_false(session.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_true(session.supported_features().empty(), "nothing was negotiated");
  stop_and_drain(session, io);
}

void
a_session_reports_exactly_the_features_it_was_given([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections, hello_feature::json });

  assert_true(session.supports_feature(hello_feature::collections), "a negotiated feature");
  assert_true(session.supports_feature(hello_feature::json), "a negotiated feature");
  assert_false(session.supports_feature(hello_feature::duplex),
               "a feature that was not negotiated");

  const auto features = session.supported_features();
  assert_eq(features.size(), std::size_t{ 2 }, "the negotiated features, and no others");
  assert_true(std::find(features.begin(), features.end(), hello_feature::collections) !=
                features.end(),
              "collections is listed");
  assert_true(std::find(features.begin(), features.end(), hello_feature::json) != features.end(),
              "json is listed");
  stop_and_drain(session, io);
}

void
the_mcbp_context_reflects_the_known_features([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io, { hello_feature::collections });

  auto mcbp_ctx = session.context();
  assert_true(mcbp_ctx.supports_feature(hello_feature::collections), "a negotiated feature");
  assert_false(mcbp_ctx.supports_feature(hello_feature::json), "a feature that was not negotiated");
  assert_false(mcbp_ctx.config.has_value(), "no configuration arrives before bootstrap");
  stop_and_drain(session, io);
}

void
session_info_accessors_are_safe_under_concurrent_reads([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session =
    make_session(io, { hello_feature::collections, hello_feature::json, hello_feature::duplex });

  constexpr int num_threads = 8;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  std::promise<void> start_gate;
  auto start_future = start_gate.get_future().share();

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&session, start_future]() -> void {
      start_future.wait();
      constexpr int iterations = 1000;
      for (int i = 0; i < iterations; ++i) {
        // All four accessors read supported_features_ under session_info_mutex_. A lock taken
        // twice on one path deadlocks here; the case's timeout is what reports it.
        static_cast<void>(session.log_prefix());
        static_cast<void>(session.supported_features());
        static_cast<void>(session.supports_feature(hello_feature::collections));
        static_cast<void>(session.context());
      }
    });
  }

  start_gate.set_value(); // release all threads at once
  for (auto& th : threads) {
    th.join();
  }
  stop_and_drain(session, io);
}

void
a_freshly_constructed_session_is_neither_stopped_nor_bootstrapped([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  auto session = make_session(io);
  assert_false(session.is_stopped(), "a session that has not been stopped");
  assert_false(session.is_bootstrapped(), "a session that has not bootstrapped");
  stop_and_drain(session, io);
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(log_prefix_is_returned_by_value) },
      { CASE(a_session_with_no_known_features_supports_none) },
      { CASE(a_session_reports_exactly_the_features_it_was_given) },
      { CASE(the_mcbp_context_reflects_the_known_features) },
      { CASE(session_info_accessors_are_safe_under_concurrent_reads) },
      { CASE(a_freshly_constructed_session_is_neither_stopped_nor_bootstrapped) },
    },
  };
}

} // namespace couchbase::test
