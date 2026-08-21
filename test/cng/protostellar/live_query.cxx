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

// Live end-to-end tests of N1QL query over the couchbase2:// streaming transport (CXXCBC-897).
//
// Each case drives the core::cluster public path -- open() builds the gRPC component,
// execute(query_request) routes to the component's server-streaming query path -- against a real
// CNG gateway. cluster_only: the whole suite skips unless TEST_CONNECTION_STRING points at a
// couchbase2:// endpoint.
//
// Every expectation is taken from the gateway's own sources rather than from this client, so a case
// asserts the answer the gateway defines rather than the behaviour this client happens to have. The
// error-condition cases in particular are the gateway's acceptance suite
// (stellar-gateway/gateway/test/query_test.go) re-expressed at this SDK's surface.

#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

#include "cng/fixtures/live_fixture.hxx"
#include "framework/test_registry.hxx"

#include "core/json_string.hxx"

#include <couchbase/error_codes.hxx>

#include <chrono>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace ops = ::couchbase::core::operations;
using namespace std::chrono_literals;

// Query parameters reach the core request as codec::binary, which selects json_string's binary
// arm. Building the string arm here instead would exercise a path core/impl/query.cxx never
// produces -- and would have hidden the empty-parameter defect this suite exists to catch.
[[nodiscard]] auto
binary_json(const std::string& json) -> couchbase::core::json_string
{
  std::vector<std::byte> bytes;
  bytes.reserve(json.size());
  for (const char c : json) {
    bytes.push_back(static_cast<std::byte>(c));
  }
  return couchbase::core::json_string{ std::move(bytes) };
}

[[nodiscard]] auto
select(const std::string& statement) -> ops::query_request
{
  ops::query_request request;
  request.statement = statement;
  return request;
}

void
a_constant_projection_round_trips([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  // Needs no bucket or dataset and still exercises the whole stream: one row plus the terminal
  // metadata message.
  const auto result = fixture.execute(select(R"(SELECT "cng" AS greeting)"));

  assert_false(static_cast<bool>(result.ctx.ec), "query over couchbase2 succeeds");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "single projected row buffered");
  assert_true(result.rows.at(0).find("cng") != std::string::npos, "row carries the projection");
  assert_eq(result.meta.status, std::string{ "success" }, "terminal status decoded");
  assert_true(!result.meta.request_id.empty(), "request_id decoded from metadata");
}

// The regression test for the #1036 blocker, at the level where it bites. json_string::str()
// returns a static empty string for the binary arm, so before the fix the gateway received
// `positional_parameters: [""]` and echoed an empty value here -- a wrong answer, not an error.
void
a_positional_parameter_reaches_the_query_engine([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT $1 AS echo, $2 AS count");
  request.positional_parameters.emplace_back(binary_json(R"("parameterised")"));
  request.positional_parameters.emplace_back(binary_json("7"));
  const auto result = fixture.execute(std::move(request));

  assert_false(static_cast<bool>(result.ctx.ec), "parameterised query succeeds");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "one row");
  assert_true(result.rows.at(0).find(R"("parameterised")") != std::string::npos,
              "the string parameter reached the query engine");
  assert_true(result.rows.at(0).find('7') != std::string::npos,
              "the numeric parameter reached the query engine");
}

void
a_named_parameter_reaches_the_query_engine([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT $word AS echo");
  request.named_parameters.emplace("word", binary_json(R"("named-value")"));
  const auto result = fixture.execute(std::move(request));

  assert_false(static_cast<bool>(result.ctx.ec), "named-parameter query succeeds");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "one row");
  assert_true(result.rows.at(0).find(R"("named-value")") != std::string::npos,
              "the named parameter reached the query engine");
}

// queryserver.go flushes its row cache once it exceeds 1024 bytes, so a result this size arrives
// across several QueryResponse messages and the last one carries rows AND metadata together. A
// reader that stops at the first message with metadata, or that treats one message as one row,
// loses part of the result.
void
a_result_larger_than_one_batch_arrives_complete_and_in_order([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  constexpr int row_count = 500;
  const auto result = fixture.execute(
    select("SELECT RAW v FROM ARRAY_RANGE(0, " + std::to_string(row_count) + ") AS v ORDER BY v"));

  assert_false(static_cast<bool>(result.ctx.ec), "multi-batch query succeeds");
  assert_eq(result.rows.size(), static_cast<std::size_t>(row_count), "every row is buffered");
  for (int i = 0; i < row_count; ++i) {
    assert_eq(result.rows.at(static_cast<std::size_t>(i)),
              std::to_string(i),
              "row " + std::to_string(i) + " is in position " + std::to_string(i));
  }
  assert_eq(result.meta.status, std::string{ "success" }, "metadata survives the last batch");
}

void
an_empty_result_still_carries_metadata([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  // An empty source rather than a false predicate: a FROM-less SELECT still emits its single
  // projected row, `WHERE false` and all, so that spelling would not produce an empty result.
  const auto result = fixture.execute(select("SELECT RAW v FROM ARRAY_RANGE(0, 0) AS v"));

  assert_false(static_cast<bool>(result.ctx.ec), "an empty query succeeds");
  assert_eq(result.rows.size(), std::size_t{ 0 }, "no rows");
  assert_eq(result.meta.status, std::string{ "success" }, "status still decoded");
  assert_true(!result.meta.request_id.empty(), "request_id still decoded");
}

// The core request defaults metrics off, and the gateway defaults them on -- so the converter has
// to send disable_metrics for the default to hold. Without that line the SDK would return metrics
// nobody asked for.
void
metrics_are_absent_unless_requested([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(select(R"(SELECT "cng" AS greeting)"));
  assert_false(static_cast<bool>(result.ctx.ec), "query succeeds");
  assert_false(result.meta.metrics.has_value(), "metrics are off by default");
}

void
metrics_are_returned_when_requested([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT RAW v FROM ARRAY_RANGE(0, 3) AS v");
  request.metrics = true;
  const auto result = fixture.execute(std::move(request));

  assert_false(static_cast<bool>(result.ctx.ec), "query succeeds");
  assert_true(result.meta.metrics.has_value(), "metrics returned when requested");
  assert_eq(result.meta.metrics->result_count, std::uint64_t{ 3 }, "result_count matches the rows");
  assert_true(result.meta.metrics->elapsed_time > std::chrono::nanoseconds::zero(),
              "elapsed_time is populated");
}

void
a_profile_is_returned_when_requested([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select(R"(SELECT "cng" AS greeting)");
  request.profile = couchbase::query_profile::timings;
  const auto result = fixture.execute(std::move(request));

  assert_false(static_cast<bool>(result.ctx.ec), "query succeeds");
  assert_true(result.meta.profile.has_value(), "profile returned when requested");
}

// The gateway collapses a parsing failure into InvalidArgument (errorhandler.go
// NewInvalidQueryStatus), so this is `invalid_argument` over couchbase2 where the classic path
// reports `parsing_failure`. couchbase-jvm-clients asserts the same divergence.
void
a_syntax_error_is_reported_as_invalid_argument([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(select("FINAGLE * FROM default"));

  assert_true(result.ctx.ec == errc::common::invalid_argument,
              "a syntax error is invalid_argument over couchbase2");
  assert_true(!result.ctx.first_error_message.empty(), "the server message is carried through");
  assert_eq(result.ctx.statement,
            std::string{ "FINAGLE * FROM default" },
            "the statement is in the error context");
}

void
a_write_in_a_read_only_query_is_rejected([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select(R"(UPSERT INTO default(KEY, VALUE) VALUES ("cng-never-written", {"x":1}))");
  request.readonly = true;
  const auto result = fixture.execute(std::move(request));

  assert_true(result.ctx.ec == errc::common::invalid_argument,
              "a write under read_only is rejected");
}

// NOT_FOUND carries a ResourceInfo whose type is "queryindex", which is what separates this from
// the KV-flavoured document_not_found the bare code would map to.
void
dropping_a_missing_index_reports_index_not_found([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  const auto result = fixture.execute(
    select("DROP INDEX `cng-index-that-does-not-exist` ON `" + fixture.bucket() + "`"));

  assert_true(result.ctx.ec == errc::common::index_not_found,
              "a missing query index is index_not_found, not document_not_found");
}

// queryserver.go:144 refuses a request that carries both, and the converter sends both whenever
// the caller sets both -- so this pins what the caller sees rather than leaving it to chance.
void
conflicting_consistency_is_rejected([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT RAW 1");
  request.scan_consistency = couchbase::query_scan_consistency::request_plus;
  request.mutation_state.emplace_back(1ULL, 1ULL, std::uint16_t{ 0 }, fixture.bucket());
  const auto result = fixture.execute(std::move(request));

  assert_true(result.ctx.ec == errc::common::invalid_argument,
              "scan consistency and mutation tokens together are rejected");
}

// queryserver.go:178. Same shape as above: the converter forwards both, the gateway decides.
void
named_and_positional_parameters_together_are_rejected([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT $1 AS a, $b AS b");
  request.positional_parameters.emplace_back(binary_json("1"));
  request.named_parameters.emplace("b", binary_json("2"));
  const auto result = fixture.execute(std::move(request));

  assert_true(result.ctx.ec == errc::common::invalid_argument,
              "named and positional parameters must be used exclusively");
}

// can_encode refuses this before anything is sent, so the caller is told the feature is missing
// rather than getting a result computed without the option they asked for.
//
// The refusal still has to arrive like every other completion: on the SDK's execution context,
// after execute() has returned. Checking the thread is what separates "rejected" from "rejected by
// calling back inline", which no assertion on the response can see.
void
an_unsupported_option_is_refused_without_a_round_trip([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select(R"(SELECT "cng" AS greeting)");
  request.raw.emplace("some_future_option", couchbase::core::json_string{ std::string{ "true" } });
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::feature_not_available,
              "the raw passthrough is refused rather than dropped");
  assert_true(result.rows.empty(), "nothing was executed");
  assert_eq(result.ctx.statement,
            std::string{ R"(SELECT "cng" AS greeting)" },
            "the refusal still identifies the query");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the refusal is delivered on the io context, not inline out of execute()");
}

// The same contract on the other path that answers without sending anything.
void
an_expired_budget_is_refused_on_the_io_context([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select(R"(SELECT "cng" AS greeting)");
  request.timeout = 0ms;
  const auto [result, handler_thread] = fixture.execute_on(std::move(request));

  assert_true(result.ctx.ec == errc::common::unambiguous_timeout,
              "a spent budget is an unambiguous timeout: nothing reached the server");
  assert_eq(result.ctx.statement,
            std::string{ R"(SELECT "cng" AS greeting)" },
            "the expired response still identifies the query");
  assert_true(handler_thread != std::this_thread::get_id(),
              "the expired response is delivered on the io context");
}

// RFC 77 keys the timeout distinction on read-only status, not on idempotency. A read that ran out
// of time provably changed nothing, so reporting it as ambiguous would tell a caller that declines
// to retry ambiguous operations to give up on one that is safe to repeat.
void
an_expired_deadline_is_unambiguous_for_a_read_only_query([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select("SELECT RAW v FROM ARRAY_RANGE(0, 10000) AS v");
  request.readonly = true;
  request.timeout = 1ms;
  const auto result = fixture.execute(std::move(request));

  assert_true(result.ctx.ec == errc::common::unambiguous_timeout,
              "a timed-out read-only query is unambiguous");
}

void
an_expired_deadline_is_ambiguous_for_a_mutating_query([[maybe_unused]] context& ctx)
{
  live_cluster_fixture fixture;
  fixture.require_open();

  auto request = select(R"(UPSERT INTO default(KEY, VALUE) VALUES ("cng-timeout", {"x":1}))");
  request.readonly = false;
  request.timeout = 1ms;
  const auto result = fixture.execute(std::move(request));

  assert_true(result.ctx.ec == errc::common::ambiguous_timeout,
              "a timed-out mutating query is ambiguous");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_constant_projection_round_trips), { needs::real_cluster() }, timeout::integration },
      { CASE(a_positional_parameter_reaches_the_query_engine),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(a_named_parameter_reaches_the_query_engine),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(a_result_larger_than_one_batch_arrives_complete_and_in_order),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_empty_result_still_carries_metadata),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(metrics_are_absent_unless_requested),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(metrics_are_returned_when_requested),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(a_profile_is_returned_when_requested),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(a_syntax_error_is_reported_as_invalid_argument),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(a_write_in_a_read_only_query_is_rejected),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(dropping_a_missing_index_reports_index_not_found),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(conflicting_consistency_is_rejected),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(named_and_positional_parameters_together_are_rejected),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_unsupported_option_is_refused_without_a_round_trip),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_expired_budget_is_refused_on_the_io_context),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_expired_deadline_is_unambiguous_for_a_read_only_query),
        { needs::real_cluster() },
        timeout::integration },
      { CASE(an_expired_deadline_is_ambiguous_for_a_mutating_query),
        { needs::real_cluster() },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
