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

// wait_until_ready() over couchbase2 (CXXCBC-1089). Readiness is the gRPC channel reaching READY;
// the classic path, which pings and opens the bucket over MCBP, never succeeds against a gateway.

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_registry.hxx"

#include <couchbase/cluster.hxx>

#include <asio/io_context.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;

auto
env_or(const char* name, const char* fallback) -> std::string
{
  return safe_getenv(name).value_or(fallback);
}

auto
connect_unreachable() -> couchbase::cluster
{
  couchbase::cluster_options options{ "Administrator", "password" };
  options.security().tls_verify(couchbase::tls_verify_mode::none);
  // Port 1 has no listener, and the couchbase2 open is lazy, so no gateway is needed.
  auto [connect_err, cluster] =
    couchbase::cluster::connect("couchbase2://127.0.0.1:1", options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds (lazy)");
  return cluster;
}

// Regression: open_bucket over couchbase2 dialled the gateway with MCBP and failed only at
// bootstrap_timeout.
void
open_bucket_is_feature_not_available_over_couchbase2([[maybe_unused]] context& ctx)
{
  ::couchbase::core::cluster_credentials credentials;
  credentials.username = "Administrator";
  credentials.password = "password";
  ::couchbase::core::origin cluster_origin{
    credentials, ::couchbase::core::utils::parse_connection_string("couchbase2://127.0.0.1:1")
  };
  cluster_origin.options().tls_verify = ::couchbase::core::tls_verify_mode::none;

  asio::io_context io;
  io_thread_guard runner{ io };
  ::couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  assert_false(static_cast<bool>(opened.get_future().get()), "couchbase2 open succeeds (lazy)");

  // Shared: on a timeout the callback still fires after this frame has unwound.
  auto bucket_opened = std::make_shared<std::promise<std::error_code>>();
  cluster.open_bucket("default", [bucket_opened](std::error_code ec) {
    bucket_opened->set_value(ec);
  });
  auto bucket_future = bucket_opened->get_future();
  const bool completed = bucket_future.wait_for(5s) == std::future_status::ready;
  const auto bucket_ec = completed ? bucket_future.get() : std::error_code{};

  // Closed before asserting, so that a failed assertion does not destroy the cluster while a
  // bucket bootstrap is still running.
  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();

  assert_true(completed, "open_bucket over couchbase2 completes without a bootstrap");
  assert_true(bucket_ec == couchbase::errc::common::feature_not_available,
              "open_bucket over couchbase2 is feature_not_available: " + bucket_ec.message());
}

// A channel poll that blocked, or that reported TRANSIENT_FAILURE as an error, would overrun the
// deadline or return a different code.
void
wait_until_ready_times_out_on_unreachable_gateway([[maybe_unused]] context& ctx)
{
  auto cluster = connect_unreachable();

  auto started = std::chrono::steady_clock::now();
  auto cluster_err = cluster.wait_until_ready(1s).get();
  const auto cluster_elapsed = std::chrono::steady_clock::now() - started;

  started = std::chrono::steady_clock::now();
  auto bucket_err = cluster.bucket("default").wait_until_ready(1s).get();
  const auto bucket_elapsed = std::chrono::steady_clock::now() - started;

  cluster.close().get();

  assert_true(cluster_err.ec() == couchbase::errc::common::unambiguous_timeout,
              "cluster.wait_until_ready() on an unreachable gateway is unambiguous_timeout: " +
                cluster_err.ec().message());
  assert_true(cluster_elapsed < 5s, "the cluster wait honours the caller's deadline");
  assert_true(bucket_err.ec() == couchbase::errc::common::unambiguous_timeout,
              "bucket.wait_until_ready() on an unreachable gateway is unambiguous_timeout: " +
                bucket_err.ec().message());
  assert_true(bucket_elapsed < 5s, "the bucket wait honours the caller's deadline");
}

// A wait on a closed cluster completes with cluster_closed instead of pending on the stopped
// io_context.
void
wait_until_ready_after_close_is_cluster_closed([[maybe_unused]] context& ctx)
{
  auto cluster = connect_unreachable();
  cluster.close().get();

  auto cluster_wait = cluster.wait_until_ready(1s);
  auto bucket_wait = cluster.bucket("default").wait_until_ready(1s);
  const bool cluster_done = cluster_wait.wait_for(5s) == std::future_status::ready;
  const bool bucket_done = bucket_wait.wait_for(5s) == std::future_status::ready;

  assert_true(cluster_done, "cluster.wait_until_ready() on a closed cluster completes");
  assert_true(bucket_done, "bucket.wait_until_ready() on a closed cluster completes");
  const auto cluster_ec = cluster_wait.get().ec();
  const auto bucket_ec = bucket_wait.get().ec();
  assert_true(cluster_ec == couchbase::errc::network::cluster_closed,
              "cluster.wait_until_ready() on a closed cluster is cluster_closed: " +
                cluster_ec.message());
  assert_true(bucket_ec == couchbase::errc::network::cluster_closed,
              "bucket.wait_until_ready() on a closed cluster is cluster_closed: " +
                bucket_ec.message());
}

void
wait_until_ready_succeeds_against_gateway([[maybe_unused]] context& ctx)
{
  const auto connstr = safe_getenv("TEST_CONNECTION_STRING"); // NOLINT(concurrency-mt-unsafe)
  if (!connstr) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  if (!::couchbase::core::utils::parse_connection_string(connstr.value()).uses_protostellar()) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }

  couchbase::cluster_options options{ env_or("TEST_CB2_USERNAME", "Administrator"),
                                      env_or("TEST_CB2_PASSWORD", "password") };
  // Dev gateway certificate is not chainable; skip verification for the test.
  options.security().tls_verify(couchbase::tls_verify_mode::none);
  auto [connect_err, cluster] = couchbase::cluster::connect(connstr.value(), options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds");

  auto cluster_err = cluster.wait_until_ready(10s).get();
  auto bucket_err =
    cluster.bucket(env_or("TEST_CB2_BUCKET", "default")).wait_until_ready(10s).get();
  cluster.close().get();

  assert_false(cluster_err.ec().operator bool(),
               "cluster.wait_until_ready() succeeds: " + cluster_err.ec().message());
  assert_false(bucket_err.ec().operator bool(),
               "bucket.wait_until_ready() succeeds: " + bucket_err.ec().message());
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(open_bucket_is_feature_not_available_over_couchbase2), {}, timeout::integration },
      { CASE(wait_until_ready_times_out_on_unreachable_gateway), {}, timeout::integration },
      { CASE(wait_until_ready_after_close_is_cluster_closed), {}, timeout::integration },
      { CASE(wait_until_ready_succeeds_against_gateway),
        { needs::real_cluster() },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
