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

// wait_until_ready over couchbase2 (CXXCBC-908). The couchbase2 branch polls the gRPC channel
// connectivity state (no MCBP bootstrap to ping). Two cases: an unreachable gateway must end in
// unambiguous_timeout (env-agnostic -- points at a dead port, no cluster required), and a reachable
// gateway must become ready (cluster_only, needs TEST_CONNECTION_STRING).

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/live_fixture.hxx"
#include "framework/test_runner.hxx"

// core/operations.hxx (complete operation types) must precede core/cluster.hxx, whose execute()
// overloads require the full request definitions.
#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/impl/wait_until_ready.hxx"
#include "core/origin.hxx"
#include "core/tls_verify_mode.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/cluster_state.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <cstdlib>
#include <future>
#include <set>
#include <string>

namespace couchbase::cng::test
{
namespace
{
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::origin;
using ::couchbase::core::service_type;
using ::couchbase::core::tls_verify_mode;
using ::couchbase::core::utils::parse_connection_string;
using namespace std::chrono_literals;

auto
env_or(const char* name, const char* fallback) -> std::string
{
  // safe_getenv, not std::getenv: MSVC deprecates getenv (C4996) and these builds are -Werror.
  return safe_getenv(name).value_or(fallback);
}

// Opens `origin`, runs the couchbase2 wait_until_ready free function, and returns its error code.
auto
open_and_wait(const origin& cluster_origin, std::chrono::milliseconds timeout) -> std::error_code
{
  asio::io_context io;
  io_thread_guard runner{ io };

  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  const auto open_ec = opened.get_future().get();
  assert_false(static_cast<bool>(open_ec), "open(couchbase2://) succeeds (lazy connect)");

  std::promise<std::error_code> ready;
  couchbase::core::impl::wait_until_ready(cluster,
                                          std::nullopt,
                                          timeout,
                                          couchbase::cluster_state::online,
                                          std::set<service_type>{},
                                          [&ready](std::error_code ec) {
                                            ready.set_value(ec);
                                          });
  const auto ready_ec = ready.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();

  return ready_ec;
}

void
wait_until_ready_times_out_against_unreachable_gateway()
{
  // Port 1 has no listener, so the channel never reaches READY. Env-agnostic: no gateway needed.
  auto parsed = parse_connection_string("couchbase2://127.0.0.1:1");
  cluster_credentials credentials;
  credentials.username = "Administrator";
  credentials.password = "password";
  origin cluster_origin{ credentials, parsed };
  cluster_origin.options().tls_verify = tls_verify_mode::none;

  const auto ec = open_and_wait(cluster_origin, 1500ms);
  assert_true(ec == couchbase::errc::common::unambiguous_timeout,
              "an unreachable gateway ends in unambiguous_timeout");
}

void
wait_until_ready_succeeds_against_reachable_gateway()
{
  const auto connstr = safe_getenv("TEST_CONNECTION_STRING");
  if (!connstr.has_value()) {
    skip("TEST_CONNECTION_STRING is not set");
  }
  auto parsed = parse_connection_string(connstr.value());
  if (!parsed.uses_protostellar()) {
    skip("TEST_CONNECTION_STRING is not a couchbase2:// endpoint");
  }

  cluster_credentials credentials;
  credentials.username = env_or("TEST_CB2_USERNAME", "Administrator");
  credentials.password = env_or("TEST_CB2_PASSWORD", "password");
  origin cluster_origin{ credentials, parsed };
  cluster_origin.options().tls_verify = tls_verify_mode::none;

  const auto ec = open_and_wait(cluster_origin, timeout::integration);
  assert_false(static_cast<bool>(ec), "a reachable gateway becomes ready");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_wait_until_ready",
    {
      { "wait_until_ready_times_out_against_unreachable_gateway",
        wait_until_ready_times_out_against_unreachable_gateway,
        timeout::network,
        test_env::agnostic },
      { "wait_until_ready_succeeds_against_reachable_gateway",
        wait_until_ready_succeeds_against_reachable_gateway,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::cng::test
