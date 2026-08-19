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

// Live end-to-end test of FTS search over the couchbase2:// streaming transport (CXXCBC-899).
//
// The gateway serves search.v1, so the round trip is expected to reach it and come back with the
// gateway's own answer. Asserting on a provisioned index would need index admin (CXXCBC-901), so
// the round trip is pinned by the error the gateway returns for an index that does not exist:
// that answer can only come from a gateway that ran the query.

#include "fixtures/live_fixture.hxx"
#include "framework/test_runner.hxx"

#include "core/operations.hxx"

#include "core/json_string.hxx"

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
search(std::string query) -> ops::search_request
{
  ops::search_request request;
  request.index_name = env_or("TEST_CB2_SEARCH_INDEX", "cng-index");
  request.query = couchbase::core::json_string{ std::move(query) };
  return request;
}

void
search_round_trip_against_live_gateway()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(search(R"({"match_all":{}})"));

  skip_unless_service_implemented(result.ctx, "search.v1");

  // Either the index exists and the query ran, or it does not and the gateway says so. Both are
  // answers from the search service; a transport that mis-encoded the request, or routing that
  // never reached the gateway, produces neither.
  assert_true(!result.ctx.ec || result.ctx.ec == errc::common::index_not_found,
              "match_all over couchbase2 returns hits or a genuine index error");
  if (result.ctx.ec) {
    assert_false(result.ctx.first_error_message.empty(),
                 "the index error carries the gateway's own message");
  }
}

// An FTS query shape the converter does not translate must be refused before anything is sent. The
// assertion on first_error_message is what separates this refusal from a gateway UNIMPLEMENTED --
// the same signal skip_unless_service_implemented() relies on, so this case keeps that helper
// honest for the search suite.
void
an_untranslated_query_shape_is_refused_by_the_client()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto [result, handler_thread] = fixture.execute_on(search(R"({"term":"x","field":"f"})"));

  assert_true(result.ctx.ec == errc::common::feature_not_available,
              "a term query is refused rather than silently reshaped");
  assert_true(result.rows.empty(), "nothing was executed");
  assert_true(result.ctx.first_error_message.empty(),
              "the refusal is the client's own, so it carries no gateway status message");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

// A query that is not JSON is the caller's own error. Reporting it as feature_not_available would
// tell them their cluster cannot do search, which is both wrong and unactionable.
void
a_malformed_query_is_reported_as_invalid_argument()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(search(R"({"match_all":)"));

  assert_true(result.ctx.ec == errc::common::invalid_argument,
              "malformed query JSON is an argument error, not a missing feature");
  assert_true(result.rows.empty(), "nothing was executed");
}

// A budget that is already spent must not be dispatched, and the response must still say which
// search it was: a caller correlating by index or client_context_id has no other handle on a
// request that never reached the wire.
void
an_expired_budget_is_refused_on_the_io_context()
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = search(R"({"match_all":{}})");
  request.client_context_id = "cng-expired-search";
  request.timeout = std::chrono::milliseconds::zero();
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::unambiguous_timeout,
              "a spent budget is an unambiguous timeout: nothing reached the server");
  assert_eq(result.ctx.index_name,
            env_or("TEST_CB2_SEARCH_INDEX", "cng-index"),
            "the expired response still identifies the index");
  assert_eq(result.ctx.client_context_id,
            std::string{ "cng-expired-search" },
            "the expired response still carries the caller's context id");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the expired response is delivered on the io context");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_live_search",
    {
      { "an_expired_budget_is_refused_on_the_io_context",
        an_expired_budget_is_refused_on_the_io_context,
        timeout::integration,
        test_env::cluster_only },
      { "search_round_trip_against_live_gateway",
        search_round_trip_against_live_gateway,
        timeout::integration,
        test_env::cluster_only },
      { "an_untranslated_query_shape_is_refused_by_the_client",
        an_untranslated_query_shape_is_refused_by_the_client,
        timeout::integration,
        test_env::cluster_only },
      { "a_malformed_query_is_reported_as_invalid_argument",
        a_malformed_query_is_reported_as_invalid_argument,
        timeout::integration,
        test_env::cluster_only },
    },
  };
}

} // namespace couchbase::test
