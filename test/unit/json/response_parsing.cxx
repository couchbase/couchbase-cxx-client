/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

// parse_query_meta / map_query_error / encode_query_options and the analytics equivalents are the
// single source of truth shared by the buffered and streaming operations, so they are exercised
// directly rather than through either path.

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/analytics_scan_consistency.hxx"
#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/analytics_response_parsing.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/document_search.hxx"
#include "core/operations/query_response_parsing.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace ops = couchbase::core::operations;

auto
query_meta_with(std::string status, std::uint64_t code) -> ops::query_response::query_meta_data
{
  ops::query_response::query_meta_data meta{};
  meta.status = std::move(status);
  if (code != 0) {
    ops::query_response::query_problem problem{};
    problem.code = code;
    meta.errors = std::vector{ problem };
  }
  return meta;
}

auto
analytics_meta_with(ops::analytics_response::analytics_status status, std::uint64_t code)
  -> ops::analytics_response::analytics_meta_data
{
  ops::analytics_response::analytics_meta_data meta{};
  meta.status = status;
  if (code != 0) {
    ops::analytics_response::analytics_problem problem{};
    problem.code = code;
    meta.errors.push_back(problem);
  }
  return meta;
}

void
parse_query_meta_extracts_the_fields_of_a_query_response([[maybe_unused]] context& ctx)
{
  auto payload = couchbase::core::utils::json::parse(
    R"({"requestID":"req-1","clientContextID":"ctx-1","status":"success",)"
    R"("signature":{"a":"number"},)"
    R"("metrics":{"resultCount":2,"resultSize":30,"elapsedTime":"1ms","executionTime":"1ms",)"
    R"("sortCount":1,"warningCount":1},)"
    R"("warnings":[{"code":42,"msg":"careful"}]})");

  auto meta = ops::parse_query_meta(payload);

  assert_eq(meta.request_id, std::string{ "req-1" }, "the request id");
  assert_eq(meta.client_context_id, std::string{ "ctx-1" }, "the client context id");
  assert_eq(meta.status, std::string{ "success" }, "the status");
  assert_true(meta.signature.has_value(), "the signature is carried over");
  assert_true(meta.metrics.has_value(), "the metrics block is carried over");
  assert_eq(meta.metrics->result_count, std::uint64_t{ 2 }, "the result count");
  assert_eq(meta.metrics->result_size, std::uint64_t{ 30 }, "the result size");
  assert_eq(meta.metrics->sort_count, std::uint64_t{ 1 }, "the sort count");
  assert_eq(meta.metrics->warning_count, std::uint64_t{ 1 }, "the warning count");
  assert_true(meta.warnings.has_value(), "the warnings block is carried over");
  assert_eq(meta.warnings->size(), std::size_t{ 1 }, "the number of warnings");
  assert_eq(meta.warnings->front().code, std::uint64_t{ 42 }, "the warning's code");
}

void
map_query_error_classifies_n1ql_error_codes([[maybe_unused]] context& ctx)
{
  assert_success(ops::map_query_error(query_meta_with("success", 0)),
                 "a successful response carries no error");
  assert_error(ops::map_query_error(query_meta_with("fatal", 1080)),
               couchbase::errc::common::unambiguous_timeout,
               "a server-side timeout");
  assert_error(ops::map_query_error(query_meta_with("fatal", 3000)),
               couchbase::errc::common::parsing_failure,
               "a statement the server could not parse");
  assert_error(ops::map_query_error(query_meta_with("fatal", 4040)),
               couchbase::errc::query::prepared_statement_failure,
               "a prepared statement the server no longer holds");
  assert_error(ops::map_query_error(query_meta_with("fatal", 2120)),
               couchbase::errc::common::authentication_failure,
               "a rejected credential");
  assert_error(ops::map_query_error(query_meta_with("fatal", 0)),
               couchbase::errc::common::internal_server_failure,
               "a non-success status with no errors block");
}

void
map_query_error_reports_an_unrecognised_400_as_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_error(ops::map_query_error(query_meta_with("fatal", 1050), 400),
               couchbase::errc::common::invalid_argument,
               "a 400 whose error code has no specific mapping");
  assert_error(ops::map_query_error(query_meta_with("fatal", 0), 400),
               couchbase::errc::common::invalid_argument,
               "a 400 with no errors block");
  assert_error(ops::map_query_error(query_meta_with("fatal", 3000), 400),
               couchbase::errc::common::parsing_failure,
               "a 400 whose error code has a specific mapping keeps it");
  assert_error(ops::map_query_error(query_meta_with("fatal", 1050), 500),
               couchbase::errc::common::internal_server_failure,
               "only a 400 is the caller's error");
}

void
map_query_error_keeps_the_rfc_58_ranges_on_a_400([[maybe_unused]] context& ctx)
{
  assert_error(ops::map_query_error(query_meta_with("fatal", 5001), 400),
               couchbase::errc::common::internal_server_failure,
               "5xxx is an internal server failure");
  assert_error(ops::map_query_error(query_meta_with("fatal", 10500), 400),
               couchbase::errc::common::authentication_failure,
               "10xxx is an authentication failure");
  assert_error(ops::map_query_error(query_meta_with("fatal", 10500), 500),
               couchbase::errc::common::authentication_failure,
               "the range decides whatever the status");
}

void
encode_query_options_emits_the_shared_request_body_fields([[maybe_unused]] context& ctx)
{
  ops::query_request req{ "SELECT 1" };
  req.readonly = true;
  req.scan_consistency = couchbase::query_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.positional_parameters.emplace_back(couchbase::core::json_string{ R"("foo")" });
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  assert_success(ops::encode_query_options(body, req), "valid query options encode");

  assert_true(body.at("readonly").get_boolean(), "the readonly flag");
  assert_eq(body.at("scan_consistency").get_string(),
            std::string{ "request_plus" },
            "the scan consistency");
  assert_eq(body.at("$age").get_unsigned(),
            std::uint64_t{ 42 },
            "a named parameter is $-prefixed and spliced as parsed JSON");
  assert_eq(body.at("args").get_array().size(), std::size_t{ 1 }, "the positional parameters");
  assert_eq(body.at("args").get_array().front().get_string(),
            std::string{ "foo" },
            "the positional parameter's value");
  assert_true(body.at("custom").get_boolean(), "a raw value is spliced as parsed JSON");
}

void
parse_analytics_meta_extracts_the_fields_of_an_analytics_response([[maybe_unused]] context& ctx)
{
  auto payload = couchbase::core::utils::json::parse(
    R"({"requestID":"areq","clientContextID":"actx","status":"success",)"
    R"("signature":{"*":"*"},)"
    R"("metrics":{"resultCount":3,"resultSize":90,"elapsedTime":"2ms","executionTime":"2ms",)"
    R"("processedObjects":3}})");

  auto meta = ops::parse_analytics_meta(payload);

  assert_eq(meta.request_id, std::string{ "areq" }, "the request id");
  assert_eq(meta.client_context_id, std::string{ "actx" }, "the client context id");
  assert_eq(meta.status,
            ops::analytics_response::analytics_status::success,
            "the status is decoded into the enumeration");
  assert_true(meta.signature.has_value(), "the signature is carried over");
  assert_eq(meta.metrics.result_count, std::uint64_t{ 3 }, "the result count");
  assert_eq(meta.metrics.processed_objects, std::uint64_t{ 3 }, "the processed object count");
}

void
map_analytics_error_classifies_analytics_error_codes([[maybe_unused]] context& ctx)
{
  assert_success(ops::map_analytics_error(
                   analytics_meta_with(ops::analytics_response::analytics_status::success, 0)),
                 "a successful response carries no error");
  assert_error(ops::map_analytics_error(
                 analytics_meta_with(ops::analytics_response::analytics_status::fatal, 21002)),
               couchbase::errc::common::unambiguous_timeout,
               "a server-side timeout");
  assert_error(ops::map_analytics_error(
                 analytics_meta_with(ops::analytics_response::analytics_status::fatal, 24044)),
               couchbase::errc::analytics::dataset_not_found,
               "a dataset the server does not have");
  assert_error(ops::map_analytics_error(
                 analytics_meta_with(ops::analytics_response::analytics_status::fatal, 0)),
               couchbase::errc::common::internal_server_failure,
               "a non-success status with no errors block");
}

void
map_analytics_error_reports_an_unrecognised_400_as_invalid_argument([[maybe_unused]] context& ctx)
{
  using status = ops::analytics_response::analytics_status;
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 22001), 400),
               couchbase::errc::common::invalid_argument,
               "a 400 whose error code has no specific mapping");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 0), 400),
               couchbase::errc::common::invalid_argument,
               "a 400 with no errors block");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 24044), 400),
               couchbase::errc::analytics::dataset_not_found,
               "a 400 whose error code has a specific mapping keeps it");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 22001), 500),
               couchbase::errc::common::internal_server_failure,
               "only a 400 is the caller's error");
}

void
map_analytics_error_keeps_the_rfc_58_ranges_on_a_400([[maybe_unused]] context& ctx)
{
  using status = ops::analytics_response::analytics_status;
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 25001), 400),
               couchbase::errc::common::internal_server_failure,
               "25xxx is an internal server failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 20001), 400),
               couchbase::errc::common::authentication_failure,
               "20xxx is an authentication failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 24500), 400),
               couchbase::errc::analytics::compilation_failure,
               "24xxx is a compilation failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 24000), 400),
               couchbase::errc::common::parsing_failure,
               "24000 is a parsing failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 23000), 400),
               couchbase::errc::common::temporary_failure,
               "23000 is a temporary failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 23003), 400),
               couchbase::errc::common::temporary_failure,
               "23003 is a temporary failure");
  assert_error(ops::map_analytics_error(analytics_meta_with(status::fatal, 20001), 500),
               couchbase::errc::common::authentication_failure,
               "the range decides whatever the status");
}

auto
buffered_error(std::uint32_t status, const std::string& body, bool analytics) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  if (analytics) {
    return ops::analytics_request{}.make_response({}, encoded).ctx.ec;
  }
  return ops::query_request{}.make_response({}, encoded).ctx.ec;
}

void
an_undecodable_400_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  using couchbase::core::io::invalid_argument_for_undecodable_400;
  const std::string syntax_error =
    R"({"status":"fatal","errors":[{"code":3000,"msg":"syntax error"}]})";

  assert_error(invalid_argument_for_undecodable_400(
                 couchbase::errc::common::parsing_failure, 400, "<html>Bad Request</html>"),
               couchbase::errc::common::invalid_argument,
               "a 400 whose body is not JSON");
  assert_error(invalid_argument_for_undecodable_400(
                 couchbase::errc::streaming_json_lexer::stray_token, 400, ""),
               couchbase::errc::common::invalid_argument,
               "a 400 whose body the streaming lexer rejected");
  assert_error(invalid_argument_for_undecodable_400(
                 couchbase::errc::common::parsing_failure, 400, syntax_error),
               couchbase::errc::common::parsing_failure,
               "a parsing_failure mapped from a JSON body is a service error and is kept");
  assert_error(invalid_argument_for_undecodable_400(
                 buffered_error(400, syntax_error, false), 400, syntax_error),
               couchbase::errc::common::parsing_failure,
               "a buffered query syntax error keeps its mapping through completion");
  assert_error(invalid_argument_for_undecodable_400(
                 couchbase::errc::common::parsing_failure, 200, "<html>OK</html>"),
               couchbase::errc::common::parsing_failure,
               "a body that is not JSON is still a parsing failure for another status");
  assert_error(invalid_argument_for_undecodable_400(
                 couchbase::errc::streaming_json_lexer::stray_token, 502, ""),
               couchbase::errc::streaming_json_lexer::stray_token,
               "a lexer failure for another status is left alone");
  assert_success(invalid_argument_for_undecodable_400({}, 400, ""), "a success is left alone");
}

void
a_buffered_statement_rejected_with_a_400_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  assert_error(
    buffered_error(400, R"({"status":"fatal","errors":[{"code":1050,"msg":"boom"}]})", false),
    couchbase::errc::common::invalid_argument,
    "query: a 400 whose error code has no specific mapping");
  assert_error(buffered_error(400, "", false),
               couchbase::errc::common::invalid_argument,
               "query: a 400 with an empty body");
  assert_error(
    buffered_error(400, R"({"status":"fatal","errors":[{"code":22001,"msg":"boom"}]})", true),
    couchbase::errc::common::invalid_argument,
    "analytics: a 400 whose error code has no specific mapping");
  assert_error(buffered_error(400, R"({"status":"success","results":[]})", false),
               couchbase::errc::common::invalid_argument,
               "query: a 400 whose body reports success");
  assert_error(buffered_error(400, R"({"status":"success","results":[]})", true),
               couchbase::errc::common::invalid_argument,
               "analytics: a 400 whose body reports success");
  assert_error(
    buffered_error(500, R"({"status":"fatal","errors":[{"code":1050,"msg":"boom"}]})", false),
    couchbase::errc::common::internal_server_failure,
    "only a 400 is the caller's error");
}

void
encode_analytics_options_emits_the_shared_request_body_fields([[maybe_unused]] context& ctx)
{
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.readonly = true;
  req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  assert_success(ops::encode_analytics_options(body, req), "valid analytics options encode");

  assert_true(body.at("readonly").get_boolean(), "the readonly flag");
  assert_eq(body.at("scan_consistency").get_string(),
            std::string{ "request_plus" },
            "the scan consistency");
  assert_eq(body.at("$age").get_unsigned(),
            std::uint64_t{ 42 },
            "a named parameter is $-prefixed and spliced as parsed JSON");
  assert_true(body.at("custom").get_boolean(), "a raw value is spliced as parsed JSON");
}

void
encode_analytics_options_quotes_both_halves_of_the_query_context([[maybe_unused]] context& ctx)
{
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.bucket_name = "bkt";
  req.scope_name = "scp";

  tao::json::value body{};
  assert_success(ops::encode_analytics_options(body, req), "query_context names encode");

  assert_eq(body.at("query_context").get_string(),
            std::string{ "default:`bkt`.`scp`" },
            "both names go through the identifier encoder");
}

void
encode_analytics_options_escapes_a_backslash_in_the_query_context([[maybe_unused]] context& ctx)
{
  // The query context is lexed as SQL++, and a backslash followed by u0060 is expanded into a
  // backtick before lexing unless the backslash run preceding the 'u' is of even length. Left raw,
  // this value would close the scope identifier and have its tail parsed as SQL++ instead.
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.bucket_name = "bkt";
  req.scope_name = "\\u0060.`Metadata`.`Dataverse";

  tao::json::value body{};
  assert_success(ops::encode_analytics_options(body, req), "escaped query_context names encode");

  assert_eq(body.at("query_context").get_string(),
            std::string{ "default:`bkt`.`\\\\u0060.``Metadata``.``Dataverse`" },
            "the backslash cannot introduce a unicode escape");
}

void
encode_analytics_options_passes_a_caller_supplied_scope_qualifier_through(
  [[maybe_unused]] context& ctx)
{
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.scope_qualifier = "default:`bkt`.`scp`";
  req.bucket_name = "ignored";
  req.scope_name = "ignored";

  tao::json::value body{};
  assert_success(ops::encode_analytics_options(body, req), "scope_qualifier encodes");

  assert_eq(body.at("query_context").get_string(),
            std::string{ "default:`bkt`.`scp`" },
            "the qualifier is used verbatim and the names are not consulted");
}

void
search_response_parses_negative_numeric_range_facet_bounds([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = 200;
  encoded.body.append(R"({"took":100,"max_score":1.5,"total_hits":3,)"
                      R"("status":{"failed":0,"successful":1},)"
                      R"("facets":{"temp":{"field":"temperature","total":16,"missing":0,"other":0,)"
                      R"("numeric_ranges":[)"
                      R"({"name":"freezing","min":-20,"max":-5,"count":5},)"
                      R"({"name":"subzero_float","min":-12.5,"max":-0.5,"count":3},)"
                      R"({"name":"warm","min":10,"max":30,"count":8})"
                      R"(]}}})");

  auto resp = ops::search_request{}.make_response({}, encoded);
  assert_success(resp.ctx.ec, "search response with negative numeric facet bounds parses");
  assert_eq(resp.facets.size(), std::size_t{ 1 }, "one facet decoded");
  const auto& ranges = resp.facets.front().numeric_ranges;
  assert_eq(ranges.size(), std::size_t{ 3 }, "three numeric ranges decoded");

  assert_eq(ranges[0].name, std::string{ "freezing" }, "first range name");
  assert_true(std::holds_alternative<double>(ranges[0].min),
              "negative integer min is stored as double");
  assert_eq(std::get<double>(ranges[0].min), -20.0, "negative integer min value");
  assert_true(std::holds_alternative<double>(ranges[0].max),
              "negative integer max is stored as double");
  assert_eq(std::get<double>(ranges[0].max), -5.0, "negative integer max value");

  assert_eq(ranges[1].name, std::string{ "subzero_float" }, "second range name");
  assert_true(std::holds_alternative<double>(ranges[1].min), "float min is stored as double");
  assert_eq(std::get<double>(ranges[1].min), -12.5, "float min value");
  assert_true(std::holds_alternative<double>(ranges[1].max), "float max is stored as double");
  assert_eq(std::get<double>(ranges[1].max), -0.5, "float max value");

  assert_eq(ranges[2].name, std::string{ "warm" }, "third range name");
  assert_true(std::holds_alternative<std::uint64_t>(ranges[2].min),
              "positive integer min is stored as uint64_t");
  assert_eq(std::get<std::uint64_t>(ranges[2].min), std::uint64_t{ 10 }, "positive min value");
  assert_true(std::holds_alternative<std::uint64_t>(ranges[2].max),
              "positive integer max is stored as uint64_t");
  assert_eq(std::get<std::uint64_t>(ranges[2].max), std::uint64_t{ 30 }, "positive max value");
}

void
search_response_reports_parsing_failure_on_missing_or_mistyped_fields([[maybe_unused]] context& ctx)
{
  for (
    const char* body : {
      "{}",
      R"({"took":"invalid","max_score":1.0,"total_hits":1,"status":{"failed":0,"successful":1}})",
      R"({"took":1,"max_score":1.0,"total_hits":1,"status":{"failed":0,"successful":1},)"
      R"("facets":{"f":{"field":123,"total":1,"missing":0,"other":0}}})",
    }) {
    couchbase::core::io::http_response encoded{};
    encoded.status_code = 200;
    encoded.body.append(body);
    auto resp = ops::search_request{}.make_response({}, encoded);
    assert_error(resp.ctx.ec,
                 couchbase::errc::common::parsing_failure,
                 std::string{ "malformed 200 search payload reports parsing_failure: " } + body);
  }
}

void
encode_options_reject_empty_named_parameters_and_invalid_json([[maybe_unused]] context& ctx)
{
  couchbase::core::topology::configuration config{};
  couchbase::core::cluster_options options{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{ config, options, cache, "", 0, "", 0 };

  {
    ops::query_request req{ "SELECT 1" };
    req.named_parameters["age"] = couchbase::core::json_string{ "{not-json" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "query request rejects malformed JSON in named parameter");
  }
  {
    ops::query_request req{ "SELECT 1" };
    req.named_parameters[""] = couchbase::core::json_string{ "42" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "query request rejects empty named parameter key");
  }
  {
    ops::query_request req{ "SELECT 1" };
    req.named_parameters["$"] = couchbase::core::json_string{ "42" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "query request rejects bare dollar named parameter key");
  }
  {
    ops::analytics_request req{};
    req.statement = "SELECT 1";
    req.named_parameters[""] = couchbase::core::json_string{ "42" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "analytics request rejects empty named parameter key");
  }
  {
    ops::search_request req{};
    req.index_name = "idx";
    req.query = couchbase::core::json_string{ R"({"match":"x"})" };
    req.raw["bad"] = couchbase::core::json_string{ "{not-json" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "search request rejects malformed JSON in raw parameter");
  }
}

void
search_error_responses_and_mistyped_facet_bounds_do_not_throw([[maybe_unused]] context& ctx)
{
  for (const char* body : { "{}",
                            "[]",
                            R"({"status":42,"error":"index not found"})",
                            R"({"status":"fail","error":7})" }) {
    couchbase::core::io::http_response encoded{};
    encoded.status_code = 400;
    encoded.body.append(body);
    auto resp = ops::search_request{}.make_response({}, encoded);
    assert_error(resp.ctx.ec,
                 couchbase::errc::common::invalid_argument,
                 "400 search response with missing or mistyped fields maps to invalid_argument");
  }
  {
    couchbase::core::io::http_response encoded{};
    encoded.status_code = 404;
    encoded.body.append("{}");
    auto resp = ops::search_request{}.make_response({}, encoded);
    assert_error(resp.ctx.ec,
                 couchbase::errc::common::feature_not_available,
                 "404 search response without status/error fields maps to feature_not_available");
  }
  {
    couchbase::core::io::http_response encoded{};
    encoded.status_code = 429;
    encoded.body.append("{}");
    auto resp = ops::search_request{}.make_response({}, encoded);
    assert_error(resp.ctx.ec,
                 couchbase::errc::common::internal_server_failure,
                 "429 search response without status/error fields does not throw");
  }
  for (const auto status_code : { std::uint32_t{ 404 }, std::uint32_t{ 429 } }) {
    for (const char* body :
         { R"({"status":42,"error":"bad"})", R"({"status":"fail","error":7})" }) {
      couchbase::core::io::http_response encoded{};
      encoded.status_code = status_code;
      encoded.body.append(body);
      auto resp = ops::search_request{}.make_response({}, encoded);
      assert_error(resp.ctx.ec,
                   couchbase::errc::common::parsing_failure,
                   "mistyped status or error in 404/429 search payload reports parsing_failure");
    }
  }
  for (const char* body : {
         R"({"took":1,"max_score":1.0,"total_hits":1,"status":42})",
         R"({"took":1,"max_score":1.0,"total_hits":1,)"
         R"("status":{"failed":1,"successful":0,"errors":"bad"}})",
         R"({"took":1,"max_score":1.0,"total_hits":1,)"
         R"("status":{"failed":0,"successful":1},"hits":"not-an-array"})",
         R"({"took":1,"max_score":1.0,"total_hits":1,)"
         R"("status":{"failed":0,"successful":1},"facets":"not-an-object"})",
         R"({"took":1,"max_score":1.0,"total_hits":1,"status":{"failed":0,"successful":1},)"
         R"("facets":{"f":{"field":"a","total":1,"missing":0,"other":0,)"
         R"("numeric_ranges":[{"name":"r","count":1,"min":"bad"}]}}})",
         R"({"took":1,"max_score":1.0,"total_hits":1,"status":{"failed":0,"successful":1},)"
         R"("facets":{"f":{"field":"a","total":1,"missing":0,"other":0,)"
         R"("date_ranges":[{"name":"r","count":1,"start":123}]}}})",
       }) {
    couchbase::core::io::http_response encoded{};
    encoded.status_code = 200;
    encoded.body.append(body);
    auto resp = ops::search_request{}.make_response({}, encoded);
    assert_error(resp.ctx.ec,
                 couchbase::errc::common::parsing_failure,
                 std::string{ "mistyped search response field reports parsing_failure: " } + body);
  }
}

void
query_and_analytics_preserve_control_fields_and_validate_response_types(
  [[maybe_unused]] context& ctx)
{
  couchbase::core::topology::configuration config{};
  couchbase::core::cluster_options options{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{ config, options, cache, "", 0, "", 0 };

  {
    ops::query_request req{ "SELECT 1" };
    req.raw["statement"] = couchbase::core::json_string{ R"("SELECT 2")" };
    req.raw["prepared"] = couchbase::core::json_string{ R"("injected_prep")" };
    req.raw["encoded_plan"] = couchbase::core::json_string{ R"("injected_plan")" };
    req.raw["auto_execute"] = couchbase::core::json_string{ "true" };
    req.raw["client_context_id"] = couchbase::core::json_string{ R"("overridden")" };
    couchbase::core::io::http_request encoded{};
    encoded.client_context_id = "ctx-1";
    assert_success(req.encode_to(encoded, http_ctx), "query request with raw fields encodes");
    const auto body = couchbase::core::utils::json::parse(encoded.body);
    assert_eq(body.at("statement").get_string(),
              std::string{ "SELECT 1" },
              "query statement is not overwritten by raw");
    assert_true(body.find("prepared") == nullptr, "adhoc query drops raw prepared");
    assert_true(body.find("encoded_plan") == nullptr, "adhoc query drops raw encoded_plan");
    assert_true(body.find("auto_execute") == nullptr, "adhoc query drops raw auto_execute");
    assert_eq(body.at("client_context_id").get_string(),
              std::string{ "ctx-1" },
              "query client_context_id is not overwritten by raw");
  }
  {
    cache.put("SELECT 1", "cached_prep");
    ops::query_request req{ "SELECT 1" };
    req.adhoc = false;
    req.raw["statement"] = couchbase::core::json_string{ R"("SELECT 2")" };
    req.raw["prepared"] = couchbase::core::json_string{ R"("injected_prep")" };
    req.raw["encoded_plan"] = couchbase::core::json_string{ R"("injected_plan")" };
    req.raw["auto_execute"] = couchbase::core::json_string{ "true" };
    couchbase::core::io::http_request encoded{};
    encoded.client_context_id = "ctx-prep";
    assert_success(req.encode_to(encoded, http_ctx), "cached prepared query encodes");
    const auto body = couchbase::core::utils::json::parse(encoded.body);
    assert_eq(body.at("prepared").get_string(),
              std::string{ "cached_prep" },
              "cached prepared name is preserved over raw");
    assert_true(body.find("statement") == nullptr, "cached prepared query drops raw statement");
    assert_true(body.find("encoded_plan") == nullptr,
                "cached prepared query without plan drops raw encoded_plan");
    assert_true(body.find("auto_execute") == nullptr,
                "cached prepared query drops raw auto_execute");
  }
  {
    ops::search_request req{};
    req.index_name = "idx";
    req.query = couchbase::core::json_string{ R"({"match":"sdk"})" };
    req.mutation_state = { couchbase::mutation_token{ 42, 99, 3, "bkt" } };
    req.raw["query"] = couchbase::core::json_string{ R"({"match":"raw"})" };
    req.raw["ctl"] = couchbase::core::json_string{
      R"({"timeout":1,"consistency":{"level":"not_bounded"},"custom_ctl":true})"
    };
    couchbase::core::io::http_request encoded{};
    encoded.timeout = std::chrono::milliseconds{ 2500 };
    assert_success(req.encode_to(encoded, http_ctx), "search request with raw collisions encodes");
    const auto body = couchbase::core::utils::json::parse(encoded.body);
    assert_eq(body.at("query").at("match").get_string(),
              std::string{ "sdk" },
              "search query is not overwritten by raw");
    assert_eq(body.at("ctl").at("timeout").get_unsigned(),
              std::uint64_t{ 2500 },
              "search ctl.timeout is not overwritten by raw");
    assert_eq(body.at("ctl").at("consistency").at("level").get_string(),
              std::string{ "at_plus" },
              "search ctl.consistency is not overwritten by raw");
    assert_eq(body.at("ctl").at("consistency").at("vectors").at("idx").at("3/42").get_unsigned(),
              std::uint64_t{ 99 },
              "search ctl.consistency vectors are preserved");
    assert_true(body.at("ctl").at("custom_ctl").get_boolean(),
                "unrelated raw ctl option is retained");
  }
  {
    ops::search_request req{};
    req.index_name = "idx";
    req.query = couchbase::core::json_string{ R"({"match":"sdk"})" };
    req.raw["ctl"] = couchbase::core::json_string{ "123" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "search request rejects non-object raw ctl");
  }
  {
    ops::analytics_request req{};
    req.statement = "SELECT 1";
    req.raw["statement"] = couchbase::core::json_string{ R"("SELECT 2")" };
    couchbase::core::io::http_request encoded{};
    encoded.client_context_id = "ctx-2";
    assert_success(req.encode_to(encoded, http_ctx), "analytics request with raw fields encodes");
    const auto body = couchbase::core::utils::json::parse(encoded.body);
    assert_eq(body.at("statement").get_string(),
              std::string{ "SELECT 1" },
              "analytics statement is not overwritten by raw");
  }
  {
    ops::query_request req{ "SELECT 1" };
    req.raw[""] = couchbase::core::json_string{ "1" };
    couchbase::core::io::http_request encoded{};
    assert_error(req.encode_to(encoded, http_ctx),
                 couchbase::errc::common::invalid_argument,
                 "query request rejects empty raw key");
  }
  for (const char* body : {
         R"({"status":"success","results":null})",
         R"({"status":42,"results":[]})",
         R"({"status":null,"results":[]})",
       }) {
    couchbase::core::io::http_response encoded_200{};
    encoded_200.status_code = 200;
    encoded_200.body.append(body);
    auto query_200 = ops::query_request{ "SELECT 1" }.make_response({}, encoded_200);
    assert_error(query_200.ctx.ec,
                 couchbase::errc::common::parsing_failure,
                 std::string{ "query 200 response with mistyped field reports parsing_failure: " } +
                   body);
    auto analytics_200 = ops::analytics_request{}.make_response({}, encoded_200);
    assert_error(
      analytics_200.ctx.ec,
      couchbase::errc::common::parsing_failure,
      std::string{ "analytics 200 response with mistyped field reports parsing_failure: " } + body);

    couchbase::core::io::http_response encoded_400{};
    encoded_400.status_code = 400;
    encoded_400.body.append(body);
    auto query_400 = ops::query_request{ "SELECT 1" }.make_response({}, encoded_400);
    assert_error(
      query_400.ctx.ec,
      couchbase::errc::common::invalid_argument,
      std::string{ "query 400 response with mistyped field reports invalid_argument: " } + body);
    auto analytics_400 = ops::analytics_request{}.make_response({}, encoded_400);
    assert_error(
      analytics_400.ctx.ec,
      couchbase::errc::common::invalid_argument,
      std::string{ "analytics 400 response with mistyped field reports invalid_argument: " } +
        body);
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(parse_query_meta_extracts_the_fields_of_a_query_response) },
      { CASE(map_query_error_classifies_n1ql_error_codes) },
      { CASE(map_query_error_reports_an_unrecognised_400_as_invalid_argument) },
      { CASE(map_query_error_keeps_the_rfc_58_ranges_on_a_400) },
      { CASE(encode_query_options_emits_the_shared_request_body_fields) },
      { CASE(parse_analytics_meta_extracts_the_fields_of_an_analytics_response) },
      { CASE(map_analytics_error_classifies_analytics_error_codes) },
      { CASE(map_analytics_error_reports_an_unrecognised_400_as_invalid_argument) },
      { CASE(map_analytics_error_keeps_the_rfc_58_ranges_on_a_400) },
      { CASE(a_buffered_statement_rejected_with_a_400_is_an_invalid_argument) },
      { CASE(an_undecodable_400_is_an_invalid_argument) },
      { CASE(encode_analytics_options_emits_the_shared_request_body_fields) },
      { CASE(encode_analytics_options_quotes_both_halves_of_the_query_context) },
      { CASE(encode_analytics_options_escapes_a_backslash_in_the_query_context) },
      { CASE(encode_analytics_options_passes_a_caller_supplied_scope_qualifier_through) },
      { CASE(search_response_parses_negative_numeric_range_facet_bounds) },
      { CASE(search_response_reports_parsing_failure_on_missing_or_mistyped_fields) },
      { CASE(encode_options_reject_empty_named_parameters_and_invalid_json) },
      { CASE(search_error_responses_and_mistyped_facet_bounds_do_not_throw) },
      { CASE(query_and_analytics_preserve_control_fields_and_validate_response_types) },
    },
  };
}

} // namespace couchbase::test
