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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "utils/http_context.hxx"

#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all_deferred.hxx"
#include "core/utils/json.hxx"

#include <tao/json/value.hpp>

#include <cstdint>
#include <regex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
// Captures the index name and the key list of a CREATE INDEX statement, so a case asserts on the
// two parts it quotes rather than on the whole rendered statement.
const std::regex create_index{ "CREATE INDEX (.+) ON .*\\((.*)\\) .* USING GSI.*" };

auto
encoded_statement(std::vector<std::string> keys) -> std::string
{
  couchbase::core::io::http_request http_req;
  couchbase::core::operations::management::query_index_create_request req{
    "bucket_name", "scope_name", "collection_name",
    "test_index",  {},           { "bucket_name", "scope_name" },
  };
  req.keys = std::move(keys);
  auto http_ctx = ::test::utils::make_http_context();

  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");

  auto body = couchbase::core::utils::json::parse(http_req.body);
  assert_true(body.is_object(), "the encoded body is a JSON object");
  assert_true(body.get_object().at("statement").is_string(), "the statement is a JSON string");
  return body.get_object().at("statement").get_string();
}

void
a_single_key_is_wrapped_in_backticks([[maybe_unused]] context& ctx)
{
  const auto statement = encoded_statement({ "test_field" });
  std::smatch match;
  assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
  assert_eq(match[1].str(), "`test_index`", "the index name");
  assert_eq(match[2].str(), "`test_field`", "the key");
}

void
multiple_keys_are_wrapped_individually([[maybe_unused]] context& ctx)
{
  const auto statement = encoded_statement({ "field-1", "field-2", "field-3" });
  std::smatch match;
  assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
  assert_eq(match[1].str(), "`test_index`", "the index name");
  assert_eq(match[2].str(), "`field-1`, `field-2`, `field-3`", "the key list");
}

void
a_key_that_already_has_backticks_is_not_quoted_twice([[maybe_unused]] context& ctx)
{
  const auto statement = encoded_statement({ "field-1", "`field-2`", "`field-3`" });
  std::smatch match;
  assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
  assert_eq(match[1].str(), "`test_index`", "the index name");
  assert_eq(match[2].str(), "`field-1`, `field-2`, `field-3`", "the key list");
}

template<typename Request>
auto
mapped_error(std::uint32_t status, const std::string& body) -> std::error_code
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  const Request request{};
  return couchbase::core::io::complete_http_response(
           request,
           [] {
             return couchbase::core::error_context::http{};
           },
           encoded)
    .ctx.ec;
}

void
a_rejection_with_no_errors_listed_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  using create = couchbase::core::operations::management::query_index_create_request;
  using drop = couchbase::core::operations::management::query_index_drop_request;
  const std::string body = R"({"status":"fatal","errors":[]})";
  assert_eq(mapped_error<create>(400, body),
            couchbase::errc::common::invalid_argument,
            "create: a 400 is not reported as a success");
  assert_eq(mapped_error<drop>(400, body),
            couchbase::errc::common::invalid_argument,
            "drop: a 400 is not reported as a success");
  assert_eq(mapped_error<create>(400, "{}"),
            couchbase::errc::common::invalid_argument,
            "create: a 400 whose body has no status or errors");
  assert_eq(mapped_error<drop>(400, "{}"),
            couchbase::errc::common::invalid_argument,
            "drop: a 400 whose body has no status or errors");
}

void
a_rejection_that_claims_success_is_an_invalid_argument([[maybe_unused]] context& ctx)
{
  using build = couchbase::core::operations::management::query_index_build_request;
  using create = couchbase::core::operations::management::query_index_create_request;
  using drop = couchbase::core::operations::management::query_index_drop_request;
  const std::string body = R"({"status":"success"})";
  assert_eq(mapped_error<build>(400, body), couchbase::errc::common::invalid_argument, "build");
  assert_eq(mapped_error<create>(400, body), couchbase::errc::common::invalid_argument, "create");
  assert_eq(mapped_error<drop>(400, body), couchbase::errc::common::invalid_argument, "drop");
}

void
a_failure_body_without_usable_errors_is_not_a_success([[maybe_unused]] context& ctx)
{
  using create = couchbase::core::operations::management::query_index_create_request;
  using drop = couchbase::core::operations::management::query_index_drop_request;
  using deferred = couchbase::core::operations::management::query_index_get_all_deferred_request;
  assert_eq(mapped_error<create>(500, R"({"status":"fatal","errors":[]})"),
            couchbase::errc::common::internal_server_failure,
            "create: a failed status with no errors listed");
  assert_eq(mapped_error<drop>(500, R"({"status":"fatal","errors":[]})"),
            couchbase::errc::common::internal_server_failure,
            "drop: a failed status with no errors listed");
  assert_eq(mapped_error<create>(500, R"({"status":"fatal","errors":null})"),
            couchbase::errc::common::parsing_failure,
            "create: errors that are not an array");
  assert_eq(mapped_error<create>(500, "{}"),
            couchbase::errc::common::parsing_failure,
            "create: a 500 whose body has no status");
  assert_eq(mapped_error<deferred>(200, "{}"),
            couchbase::errc::common::parsing_failure,
            "get_all_deferred: a 200 whose body has no status is not an empty list");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_single_key_is_wrapped_in_backticks) },
      { CASE(multiple_keys_are_wrapped_individually) },
      { CASE(a_key_that_already_has_backticks_is_not_quoted_twice) },
      { CASE(a_rejection_with_no_errors_listed_is_an_invalid_argument) },
      { CASE(a_rejection_that_claims_success_is_an_invalid_argument) },
      { CASE(a_failure_body_without_usable_errors_is_not_a_success) },
    },
  };
}

} // namespace couchbase::test
