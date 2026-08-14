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

// Live end-to-end test of map/reduce views over the couchbase2:// streaming transport
// (CXXCBC-899).
//
// The gateway registers no view service, so the round trip is expected to reach it and be declined
// there. That is a skip rather than a failure -- but only when the gateway is what declined it:
// skip_unless_service_implemented() requires the gRPC status message, so a client that stopped
// routing views fails here instead of reporting "Skipped".

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/operations.hxx"
#include "core/operations/document_view.hxx"

#include <couchbase/error_codes.hxx>

#include <chrono>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;

auto
view(const std::string& bucket) -> ops::document_view_request
{
  ops::document_view_request request;
  request.bucket_name = bucket;
  request.document_name = "cng";
  request.view_name = "all";
  return request;
}

void
view_round_trip_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(view(fixture.bucket()));

  skip_unless_service_implemented(result.ctx, "view.v1");

  // Reached only against a gateway that serves view.v1, which no released gateway does, so the
  // specific error for a missing design document is not pinned here -- asserting a mapping that
  // cannot be exercised would be a guess. What is pinned is the provenance: any failure past this
  // point carries the gateway's own status message, which a client-side refusal never has.
  if (result.ctx.ec) {
    assert_false(result.ctx.first_error_message.empty(),
                 "the failure came from the view service, with its status message");
  }
}

// A view option the schema cannot express must be refused before anything is sent. The assertion on
// first_error_message is what separates this refusal from a gateway UNIMPLEMENTED -- the same
// signal skip_unless_service_implemented() relies on, so this case keeps that helper honest for the
// view suite. Without it the suite would consist of one case that always skips.
void
an_unsupported_option_is_refused_by_the_client()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = view(fixture.bucket());
  request.full_set = true;
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::feature_not_available,
              "full_set is refused rather than silently dropped");
  assert_true(result.rows.empty(), "nothing was executed");
  assert_true(result.ctx.first_error_message.empty(),
              "the refusal is the client's own, so it carries no gateway status message");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

// A spent budget is refused before anything is dispatched, and the response still names the view:
// the failure has to be attributable to a request that never reached the wire.
void
an_expired_budget_is_refused_on_the_io_context()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = view(fixture.bucket());
  request.client_context_id = "cng-expired-view";
  request.timeout = std::chrono::milliseconds::zero();
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::unambiguous_timeout,
              "a spent budget is an unambiguous timeout: nothing reached the server");
  assert_eq(result.ctx.design_document_name,
            std::string{ "cng" },
            "the expired response still identifies the design document");
  assert_eq(
    result.ctx.view_name, std::string{ "all" }, "the expired response still names the view");
  assert_eq(result.ctx.client_context_id,
            std::string{ "cng-expired-view" },
            "the expired response still carries the caller's context id");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the expired response is delivered on the io context");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_view",
    {
      { "an_expired_budget_is_refused_on_the_io_context",
        an_expired_budget_is_refused_on_the_io_context,
        timeout::integration,
        test_env::cluster_only },
      { "view_round_trip_against_live_gateway",
        view_round_trip_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "an_unsupported_option_is_refused_by_the_client",
        an_unsupported_option_is_refused_by_the_client,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::test
