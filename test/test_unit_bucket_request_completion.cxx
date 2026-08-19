/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

/*
 * A response path either completes a request or hands it to something that will. resolve_response()
 * decides that for a cancelled request, and the caller it decides against -- a session that has
 * gone away -- is not there to notice a return value, so a request it neither completes nor retries
 * is never answered at all.
 *
 * bucket_impl needs an io_context but no connection to construct, and resolve_response() performs
 * no I/O, so these tests never call io_context::run().
 */

#include "core/bucket.hxx"
#include "core/bucket_unit_test_api.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/origin.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/tls_context_provider.hxx"

#include <catch2/catch_test_macros.hpp>
#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>
#include <couchbase/retry_strategy.hxx>

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

namespace
{
using couchbase::core::mcbp::queue_request;

// Minimal, network-free bucket: the response path only stores the observability wrappers, so
// nullptr is sufficient. Mirrors the fixture in test_unit_bucket_deferred_queue.cxx.
auto
make_detached_bucket(asio::io_context& ctx, couchbase::core::tls_context_provider& tls)
  -> std::shared_ptr<couchbase::core::bucket>
{
  return std::make_shared<couchbase::core::bucket>(
    "test-client-id",
    ctx,
    tls,
    nullptr, // tracer_wrapper
    nullptr, // meter_wrapper
    nullptr, // orphan_reporter
    nullptr, // app_telemetry_meter
    "test-bucket",
    couchbase::core::origin{},
    std::vector<couchbase::core::protocol::hello_feature>{},
    nullptr); // bootstrap_state_listener
}

class retry_everything : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    return couchbase::retry_action{ std::chrono::milliseconds{ 10 } };
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "retry_everything";
  }
};
} // namespace

TEST_CASE("unit: a cancelled request whose retry is refused is completed", "[unit]")
{
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};
  auto bucket = make_detached_bucket(ctx, tls);

  std::atomic_bool invoked{ false };
  std::error_code observed{};
  auto request = std::make_shared<queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [&invoked, &observed](auto /* response */, auto /* request */, std::error_code ec) {
      observed = ec;
      invoked.store(true);
    });

  // No retry strategy, so retry_orchestrator::should_retry() refuses any reason that is not
  // always-retry, and node_not_available -- what do_not_retry is mapped to below -- is not one.
  REQUIRE(request->retry_strategy() == nullptr);

  bucket->unit_test_api().resolve_response(request,
                                           {},
                                           couchbase::errc::common::request_canceled,
                                           couchbase::retry_reason::do_not_retry,
                                           {});

  // Before the fix the refused retry ended the function with nobody having answered the request:
  // the caller of the operation waited on its own deadline, or forever once the io_context was
  // gone.
  REQUIRE(invoked.load());
  REQUIRE(observed == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: a cancelled request that is retried is not completed", "[unit]")
{
  // The other half of the contract: completing a request whose retry was accepted would answer the
  // caller twice, once here and once when the retry finishes.
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};
  auto bucket = make_detached_bucket(ctx, tls);

  std::atomic_bool invoked{ false };
  auto request = std::make_shared<queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [&invoked](auto /* response */, auto /* request */, auto /* error */) {
      invoked.store(true);
    });
  request->retry_strategy_ = std::make_shared<retry_everything>();

  bucket->unit_test_api().resolve_response(request,
                                           {},
                                           couchbase::errc::common::request_canceled,
                                           couchbase::retry_reason::do_not_retry,
                                           {});

  REQUIRE_FALSE(invoked.load());

  // The retry is waiting on a timer this test never runs; cancelling releases it.
  request->cancel(couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: a cancelled non-idempotent request is completed without consulting a retry",
          "[unit]")
{
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};
  auto bucket = make_detached_bucket(ctx, tls);

  std::atomic_bool invoked{ false };
  std::error_code observed{};
  auto request = std::make_shared<queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::upsert,
    [&invoked, &observed](auto /* response */, auto /* request */, std::error_code ec) {
      observed = ec;
      invoked.store(true);
    });
  // Says yes to everything, so a consulted strategy would park the request instead of answering it.
  request->retry_strategy_ = std::make_shared<retry_everything>();

  bucket->unit_test_api().resolve_response(request,
                                           {},
                                           couchbase::errc::common::request_canceled,
                                           couchbase::retry_reason::do_not_retry,
                                           {});

  REQUIRE(invoked.load());
  REQUIRE(observed == couchbase::errc::common::request_canceled);
}
