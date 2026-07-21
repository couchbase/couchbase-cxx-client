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

// Unit coverage for the response-parsing and request-body encoding helpers extracted from the
// buffered query/analytics operations (parse_query_meta / map_query_error / encode_query_options
// and the analytics equivalents). These functions are the single source of truth shared by the
// buffered and streaming paths, so they are exercised directly here.

#include "core/analytics_scan_consistency.hxx"
#include "core/operations/analytics_response_parsing.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/query_response_parsing.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <catch2/catch_test_macros.hpp>
#include <tao/json/value.hpp>

namespace ops = couchbase::core::operations;

TEST_CASE("unit: parse_query_meta extracts fields from a query response", "[unit]")
{
  auto payload = couchbase::core::utils::json::parse(
    R"({"requestID":"req-1","clientContextID":"ctx-1","status":"success",)"
    R"("signature":{"a":"number"},)"
    R"("metrics":{"resultCount":2,"resultSize":30,"elapsedTime":"1ms","executionTime":"1ms",)"
    R"("sortCount":1,"warningCount":1},)"
    R"("warnings":[{"code":42,"msg":"careful"}]})");

  auto meta = ops::parse_query_meta(payload);

  REQUIRE(meta.request_id == "req-1");
  REQUIRE(meta.client_context_id == "ctx-1");
  REQUIRE(meta.status == "success");
  REQUIRE(meta.signature.has_value());
  REQUIRE(meta.metrics.has_value());
  REQUIRE(meta.metrics->result_count == 2);
  REQUIRE(meta.metrics->result_size == 30);
  REQUIRE(meta.metrics->sort_count == 1);
  REQUIRE(meta.metrics->warning_count == 1);
  REQUIRE(meta.warnings.has_value());
  REQUIRE(meta.warnings->size() == 1);
  REQUIRE(meta.warnings->front().code == 42);
}

TEST_CASE("unit: map_query_error classifies N1QL error codes", "[unit]")
{
  auto make = [](std::string status, std::uint64_t code) {
    ops::query_response::query_meta_data meta{};
    meta.status = std::move(status);
    if (code != 0) {
      ops::query_response::query_problem p{};
      p.code = code;
      meta.errors = std::vector{ p };
    }
    return meta;
  };

  REQUIRE_FALSE(ops::map_query_error(make("success", 0))); // success => no error
  REQUIRE(ops::map_query_error(make("fatal", 1080)) ==
          couchbase::errc::common::unambiguous_timeout);
  REQUIRE(ops::map_query_error(make("fatal", 3000)) == couchbase::errc::common::parsing_failure);
  REQUIRE(ops::map_query_error(make("fatal", 4040)) ==
          couchbase::errc::query::prepared_statement_failure);
  // Non-success status without an errors block falls back to internal_server_failure.
  REQUIRE(ops::map_query_error(make("fatal", 0)) ==
          couchbase::errc::common::internal_server_failure);
}

TEST_CASE("unit: encode_query_options emits the shared request-body fields", "[unit]")
{
  ops::query_request req{ "SELECT 1" };
  req.readonly = true;
  req.scan_consistency = couchbase::query_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.positional_parameters.emplace_back(couchbase::core::json_string{ R"("foo")" });
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  ops::encode_query_options(body, req);

  REQUIRE(body.at("readonly").get_boolean() == true);
  REQUIRE(body.at("scan_consistency").get_string() == "request_plus");
  REQUIRE(body.at("$age").get_unsigned() == 42); // $-prefixed, parsed from json_string
  REQUIRE(body.at("args").get_array().size() == 1);
  REQUIRE(body.at("args").get_array().front().get_string() == "foo");
  REQUIRE(body.at("custom").get_boolean() == true); // raw values spliced as parsed JSON
}

TEST_CASE("unit: parse_analytics_meta extracts fields and status", "[unit]")
{
  auto payload = couchbase::core::utils::json::parse(
    R"({"requestID":"areq","clientContextID":"actx","status":"success",)"
    R"("signature":{"*":"*"},)"
    R"("metrics":{"resultCount":3,"resultSize":90,"elapsedTime":"2ms","executionTime":"2ms",)"
    R"("processedObjects":3}})");

  auto meta = ops::parse_analytics_meta(payload);

  REQUIRE(meta.request_id == "areq");
  REQUIRE(meta.client_context_id == "actx");
  REQUIRE(meta.status == ops::analytics_response::analytics_status::success);
  REQUIRE(meta.signature.has_value());
  REQUIRE(meta.metrics.result_count == 3);
  REQUIRE(meta.metrics.processed_objects == 3);
}

TEST_CASE("unit: map_analytics_error classifies analytics error codes", "[unit]")
{
  auto make = [](ops::analytics_response::analytics_status status, std::uint64_t code) {
    ops::analytics_response::analytics_meta_data meta{};
    meta.status = status;
    if (code != 0) {
      ops::analytics_response::analytics_problem p{};
      p.code = code;
      meta.errors.push_back(p);
    }
    return meta;
  };

  REQUIRE_FALSE(
    ops::map_analytics_error(make(ops::analytics_response::analytics_status::success, 0)));
  REQUIRE(ops::map_analytics_error(make(ops::analytics_response::analytics_status::fatal, 21002)) ==
          couchbase::errc::common::unambiguous_timeout);
  REQUIRE(ops::map_analytics_error(make(ops::analytics_response::analytics_status::fatal, 24044)) ==
          couchbase::errc::analytics::dataset_not_found);
  REQUIRE(ops::map_analytics_error(make(ops::analytics_response::analytics_status::fatal, 0)) ==
          couchbase::errc::common::internal_server_failure);
}

TEST_CASE("unit: encode_analytics_options emits the shared request-body fields", "[unit]")
{
  ops::analytics_request req{};
  req.statement = "SELECT 1";
  req.readonly = true;
  req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
  req.named_parameters["age"] = couchbase::core::json_string{ "42" };
  req.raw["custom"] = couchbase::core::json_string{ "true" };

  tao::json::value body{};
  ops::encode_analytics_options(body, req);

  REQUIRE(body.at("readonly").get_boolean() == true);
  REQUIRE(body.at("scan_consistency").get_string() == "request_plus");
  REQUIRE(body.at("$age").get_unsigned() == 42);
  REQUIRE(body.at("custom").get_boolean() == true);
}
