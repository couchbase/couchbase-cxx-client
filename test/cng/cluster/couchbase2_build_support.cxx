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

// What a couchbase2:// connection string does in a build WITHOUT couchbase2 support (CXXCBC-891).
//
// COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 defaults to OFF, so this is the shipping configuration, and
// this file is registered outside the couchbase2 guard in CMakeLists.txt so it is one of the few
// CNG tests that a default build compiles and runs.
//
// The contract being pinned is that the scheme is not a compile-time feature. Parsing behaves
// identically in both builds -- no API or ABI depends on the build mode -- and the difference is a
// runtime error from open(). Before that, the open fell past the feature check into the MCBP
// bootstrap and tried an MCBP handshake against the gateway's gRPC port, so the user saw a
// bootstrap timeout or a TLS error that said nothing about the real cause.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "framework/test_runner.hxx"

#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <future>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::origin;
using ::couchbase::core::utils::parse_connection_string;

// Parsing is deliberately identical in both builds: it is the library's public behaviour and must
// not depend on how the library was compiled. Only the open outcome differs.
void
couchbase2_parses_identically_regardless_of_build_support()
{
  const auto parsed = parse_connection_string("couchbase2://gateway.example.com");
  assert_true(parsed.error == std::nullopt, "couchbase2:// parses without error in any build");
  assert_true(parsed.uses_protostellar(), "couchbase2:// is recognised as protostellar");
  assert_eq(parsed.default_port, std::uint16_t{ 18098 }, "couchbase2:// default port");
  assert_eq(parsed.bootstrap_nodes.size(), std::size_t{ 1 }, "single bootstrap node");
}

void
opening_couchbase2_without_build_support_reports_feature_not_available()
{
#ifdef COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2
  skip("this build has couchbase2 support, so the unsupported-build branch is not compiled");
#else
  auto parsed = parse_connection_string("couchbase2://gateway.example.com");
  assert_true(parsed.error == std::nullopt, "the connection string itself is valid");

  cluster_credentials credentials;
  credentials.username = "Administrator";
  credentials.password = "password";
  const origin cluster_origin{ credentials, parsed };

  asio::io_context io;
  auto work = asio::make_work_guard(io);
  std::thread io_thread{ [&io]() {
    io.run();
  } };

  couchbase::core::cluster cluster{ io };

  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  const auto open_ec = opened.get_future().get();

  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();

  // Tear down before asserting: an assertion throws, and unwinding past a joinable std::thread
  // calls std::terminate, which would replace the message below with a bare "terminate called".
  work.reset();
  if (io_thread.joinable()) {
    io_thread.join();
  }

  assert_true(open_ec == couchbase::errc::common::feature_not_available,
              "couchbase2:// in a build without couchbase2 support reports feature_not_available, "
              "got: " +
                open_ec.message());
#endif
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "cluster_couchbase2_build_support",
    {
      { "couchbase2_parses_identically_regardless_of_build_support",
        couchbase2_parses_identically_regardless_of_build_support },
      { "opening_couchbase2_without_build_support_reports_feature_not_available",
        opening_couchbase2_without_build_support_reports_feature_not_available },
    },
  };
}

} // namespace couchbase::test
