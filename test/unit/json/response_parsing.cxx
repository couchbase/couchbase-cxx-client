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
#include "core/operations/analytics_response_parsing.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/query_response_parsing.hxx"
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
encode_query_options_emits_the_shared_request_body_fields([[maybe_unused]] context& ctx)
{
  ops::query_request req{ "SELECT 1" };
  req.readonly = true;
  req.scan_consistency = couchbase::query_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.positional_parameters.emplace_back(couchbase::core::json_string{ R"("foo")" });
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  ops::encode_query_options(body, req);

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
encode_analytics_options_emits_the_shared_request_body_fields([[maybe_unused]] context& ctx)
{
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.readonly = true;
  req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  ops::encode_analytics_options(body, req);

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
  ops::encode_analytics_options(body, req);

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
  ops::encode_analytics_options(body, req);

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
  ops::encode_analytics_options(body, req);

  assert_eq(body.at("query_context").get_string(),
            std::string{ "default:`bkt`.`scp`" },
            "the qualifier is used verbatim and the names are not consulted");
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
      { CASE(encode_query_options_emits_the_shared_request_body_fields) },
      { CASE(parse_analytics_meta_extracts_the_fields_of_an_analytics_response) },
      { CASE(map_analytics_error_classifies_analytics_error_codes) },
      { CASE(encode_analytics_options_emits_the_shared_request_body_fields) },
      { CASE(encode_analytics_options_quotes_both_halves_of_the_query_context) },
      { CASE(encode_analytics_options_escapes_a_backslash_in_the_query_context) },
      { CASE(encode_analytics_options_passes_a_caller_supplied_scope_qualifier_through) },
    },
  };
}

} // namespace couchbase::test
