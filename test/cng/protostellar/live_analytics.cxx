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

// Live tests for analytics over the couchbase2:// streaming transport (CXXCBC-898), driving
// core::cluster open()/execute(analytics_query_request) against a real CNG gateway.
//
// The gateway does not serve couchbase.analytics.v1 yet -- gateway/system/system.go registers
// query, search and their admin services and no analytics service at all, so the whole service
// answers UNIMPLEMENTED. Only the round-trip case needs a gateway that serves it, and that case
// skips through skip_unless_service_implemented(), which requires the gateway's own UNIMPLEMENTED
// message and so cannot be satisfied by the client refusing the request by itself.
//
// The remaining cases are about what this client does before anything is sent, so they run and
// assert against today's gateway rather than skipping: a request the schema cannot express is
// refused, and the refusal is stamped and delivered on the io context.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_registry.hxx"

#include <couchbase/error_codes.hxx>

#include <chrono>
#include <string>
#include <thread>
#include <utility>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;

auto
analytics(const std::string& statement) -> ops::analytics_request
{
  ops::analytics_request request;
  request.statement = statement;
  return request;
}

void
analytics_round_trip_against_live_gateway([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(analytics(R"(SELECT "cng" AS greeting)"));

  skip_unless_service_implemented(result.ctx, "analytics.v1");

  assert_false(static_cast<bool>(result.ctx.ec), "analytics over couchbase2 succeeds");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "single projected row buffered");
  assert_true(result.rows.at(0).find("cng") != std::string::npos, "row carries the projection");
  assert_true(result.meta.status == ops::analytics_response::success, "terminal status decoded");
  assert_false(result.meta.request_id.empty(), "request_id decoded from metadata");
}

// Scope-qualified analytics is refused by the converter, because the pinned schema has no field for
// it: AnalyticsQueryRequest reserves field 8 (the old bucket_name) and field 9 is now a single
// analytics_scope_name rather than the bucket/scope pair the core request carries.
//
// The assertion on first_error_message is the point of the case. It is what separates this refusal
// from a gateway UNIMPLEMENTED, which carries a gRPC message -- and it is the same signal
// skip_unless_service_implemented() relies on, so this case is what keeps that helper honest.
void
scope_qualified_analytics_is_refused_by_the_client([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = analytics(R"(SELECT "cng" AS greeting)");
  request.bucket_name = fixture.bucket();
  request.scope_name = "_default";
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::feature_not_available,
              "scoped analytics is refused rather than silently unscoped");
  assert_true(result.rows.empty(), "nothing was executed");
  assert_true(result.ctx.first_error_message.empty(),
              "the refusal is the client's own, so it carries no gateway status message");
  assert_eq(result.ctx.statement,
            std::string{ R"(SELECT "cng" AS greeting)" },
            "the refusal still identifies the statement");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

void
an_unsupported_option_is_refused_without_a_round_trip([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = analytics(R"(SELECT "cng" AS greeting)");
  request.raw.emplace("some_future_option", couchbase::core::json_string{ std::string{ "true" } });
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::feature_not_available,
              "the raw passthrough is refused rather than dropped");
  assert_true(result.rows.empty(), "nothing was executed");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

// A budget that is already spent must not be dispatched: dispatcher::server_stream only sets a
// deadline for a positive timeout, so passing a non-positive one would produce a stream with no
// deadline at all. Nothing is sent, which is why the timeout is unambiguous even though analytics
// statements can mutate.
void
an_expired_budget_is_refused_on_the_io_context([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = analytics(R"(SELECT "cng" AS greeting)");
  request.client_context_id = "expired-budget";
  request.timeout = std::chrono::milliseconds::zero();
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::unambiguous_timeout,
              "an exhausted budget is unambiguous because nothing was sent");
  assert_eq(result.ctx.client_context_id,
            std::string{ "expired-budget" },
            "the expired refusal still identifies the caller's request");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(analytics_round_trip_against_live_gateway),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(scope_qualified_analytics_is_refused_by_the_client),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_unsupported_option_is_refused_without_a_round_trip),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_expired_budget_is_refused_on_the_io_context),
        { needs::real_cluster() },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
