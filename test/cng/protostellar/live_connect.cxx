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

// Live end-to-end test of the couchbase2:// connect branch (CXXCBC-891). It drives the
// core::cluster public path -- open() (which builds the gRPC component instead of MCBP sessions)
// and execute() (which routes KV ops to that component) -- through a real upsert/get/remove
// against a CNG gateway. cluster_only: skips unless TEST_CONNECTION_STRING points at a
// couchbase2:// endpoint.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_registry.hxx"

// core/operations.hxx (complete operation types) must precede core/cluster.hxx, whose execute()
// overloads and with_legacy_durability aliases require the full request definitions.
#include "core/operations.hxx"

#include "core/cluster.hxx"

#include "core/cluster_credentials.hxx"
#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/origin.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/tls_verify_mode.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/connection_string.hxx"

#include <asio/io_context.hpp>

#include <cstdlib>
#include <future>
#include <string>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
namespace pk = ::couchbase::core::protostellar::kv;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::document_id;
using ::couchbase::core::origin;
using ::couchbase::core::tls_verify_mode;
using ::couchbase::core::utils::parse_connection_string;

auto
env_or(const char* name, const char* fallback) -> std::string
{
  return safe_getenv(name).value_or(fallback);
}

void
connect_and_round_trip_kv([[maybe_unused]] context& ctx)
{
  const auto connstr = safe_getenv("TEST_CONNECTION_STRING"); // NOLINT(concurrency-mt-unsafe)
  if (!connstr) {
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
  // Dev gateway certificate is not chainable; skip verification for the test.
  cluster_origin.options().tls_verify = tls_verify_mode::none;

  const auto bucket = env_or("TEST_CB2_BUCKET", "default");
  const auto id = document_id{ bucket, "_default", "_default", "cng-connect-key" };
  const std::string document = "{\"cng-connect\":true}";

  asio::io_context io;
  io_thread_guard runner{ io };

  couchbase::core::cluster cluster{ io };

  // open
  std::promise<std::error_code> opened;
  cluster.open(cluster_origin, [&opened](std::error_code ec) {
    opened.set_value(ec);
  });
  const auto open_ec = opened.get_future().get();
  assert_false(static_cast<bool>(open_ec), "connect(couchbase2://) succeeds");

  // upsert
  ops::upsert_request upsert;
  upsert.id = id;
  upsert.value = cu::to_binary(document);
  upsert.flags = 0x06U;
  std::promise<ops::upsert_response> upserted;
  cluster.execute(std::move(upsert), [&upserted](ops::upsert_response r) {
    upserted.set_value(std::move(r));
  });
  const auto upsert_result = upserted.get_future().get();
  assert_false(static_cast<bool>(upsert_result.ctx.ec()), "upsert via connected cluster succeeds");

  // get
  ops::get_request get;
  get.id = id;
  std::promise<ops::get_response> got;
  cluster.execute(std::move(get), [&got](ops::get_response r) {
    got.set_value(std::move(r));
  });
  const auto get_result = got.get_future().get();
  assert_false(static_cast<bool>(get_result.ctx.ec()), "get via connected cluster succeeds");
  assert_eq(cu::to_string(get_result.value), document, "value round-trips through connect path");
  // The upsert set these so the document is stored as JSON; without this assertion a converter that
  // dropped content_flags entirely would still pass.
  assert_eq(get_result.flags, 0x06U, "content flags round-trip through connect path");

  // remove
  ops::remove_request remove;
  remove.id = id;
  std::promise<ops::remove_response> removed;
  cluster.execute(std::move(remove), [&removed](ops::remove_response r) {
    removed.set_value(std::move(r));
  });
  const auto remove_result = removed.get_future().get();
  assert_false(static_cast<bool>(remove_result.ctx.ec()), "remove via connected cluster succeeds");

  // close
  std::promise<void> closed;
  cluster.close([&closed]() {
    closed.set_value();
  });
  closed.get_future().get();
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(connect_and_round_trip_kv), { needs::real_cluster() }, timeout::integration },
    },
  };
}

} // namespace couchbase::test
