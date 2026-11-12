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
#include "core/operations/management/query_index_get_all.hxx"
#include "core/operations/management/query_index_get_all_deferred.hxx"
#include "core/query_context.hxx"
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

template<typename Request>
void
assert_rfc_58_codes_on_a_400(const std::string& name)
{
  const auto body = [](std::uint32_t code) {
    return R"({"status":"fatal","errors":[{"code":)" + std::to_string(code) + R"(,"msg":"boom"}]})";
  };
  assert_eq(mapped_error<Request>(400, body(5000)),
            couchbase::errc::common::internal_server_failure,
            name + ": 5000 with a message no case matches is an internal server failure");
  assert_eq(mapped_error<Request>(400, body(5001)),
            couchbase::errc::common::internal_server_failure,
            name + ": 5xxx is an internal server failure");
  assert_eq(mapped_error<Request>(400, body(10500)),
            couchbase::errc::common::authentication_failure,
            name + ": 10xxx is an authentication failure");
  assert_eq(mapped_error<Request>(400, body(1050)),
            couchbase::errc::common::invalid_argument,
            name + ": a code without a mapping falls back to invalid_argument");
}

void
a_coded_rejection_keeps_its_rfc_58_mapping([[maybe_unused]] context& ctx)
{
  namespace mgmt = couchbase::core::operations::management;
  assert_rfc_58_codes_on_a_400<mgmt::query_index_build_request>("build");
  assert_rfc_58_codes_on_a_400<mgmt::query_index_create_request>("create");
  assert_rfc_58_codes_on_a_400<mgmt::query_index_drop_request>("drop");
  assert_rfc_58_codes_on_a_400<mgmt::query_index_get_all_request>("get_all");
  assert_rfc_58_codes_on_a_400<mgmt::query_index_get_all_deferred_request>("get_all_deferred");
}

void
the_first_mapped_error_decides([[maybe_unused]] context& ctx)
{
  namespace mgmt = couchbase::core::operations::management;
  const std::string body = R"({"status":"fatal","errors":[{"code":5001,"msg":"boom"},)"
                           R"({"code":1050,"msg":"boom"}]})";
  assert_eq(mapped_error<mgmt::query_index_build_request>(400, body),
            couchbase::errc::common::internal_server_failure,
            "build: a later unmapped error does not replace the mapping");
  assert_eq(mapped_error<mgmt::query_index_create_request>(400, body),
            couchbase::errc::common::internal_server_failure,
            "create: a later unmapped error does not replace the mapping");
  assert_eq(mapped_error<mgmt::query_index_drop_request>(400, body),
            couchbase::errc::common::internal_server_failure,
            "drop: a later unmapped error does not replace the mapping");
}

void
the_per_scope_index_limit_is_quota_limited([[maybe_unused]] context& ctx)
{
  namespace mgmt = couchbase::core::operations::management;
  const std::string body =
    R"({"status":"errors","errors":[{"code":5000,"msg":"Limit for number of indexes)"
    R"( that can be created per scope has been reached"}]})";
  assert_eq(mapped_error<mgmt::query_index_create_request>(500, body),
            couchbase::errc::common::quota_limited,
            "create");
  assert_eq(mapped_error<mgmt::query_index_drop_request>(500, body),
            couchbase::errc::common::quota_limited,
            "drop");
}

void
a_drop_of_an_index_that_does_not_exist_is_index_not_found([[maybe_unused]] context& ctx)
{
  using drop = couchbase::core::operations::management::query_index_drop_request;
  const std::string body = R"({"status":"errors","errors":[{"code":5000,)"
                           R"("msg":"GSI Drop() - cause: Index does not exist."}]})";
  assert_eq(mapped_error<drop>(500, body), couchbase::errc::common::index_not_found, "a 500");
  assert_eq(mapped_error<drop>(400, body), couchbase::errc::common::index_not_found, "a 400");
}
template<typename Request>
auto
statement_of(const Request& req) -> std::string
{
  couchbase::core::io::http_request http_req;
  auto http_ctx = ::test::utils::make_http_context();
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");
  return couchbase::core::utils::json::parse(http_req.body).at("statement").get_string();
}

void
backticks_and_backslashes_in_identifiers_are_escaped([[maybe_unused]] context& ctx)
{
  using couchbase::core::operations::management::query_index_build_request;
  using couchbase::core::operations::management::query_index_create_request;
  using couchbase::core::operations::management::query_index_drop_request;

  query_index_create_request create{};
  create.bucket_name = "b`1";
  create.scope_name = "s`1";
  create.collection_name = "c`1";
  create.index_name = "i`1";
  create.keys = { "f`1" };
  assert_eq(statement_of(create),
            "CREATE INDEX `i``1` ON default:`b``1`.`s``1`.`c``1`(`f``1`)  USING GSI ",
            "CREATE INDEX on a collection");

  query_index_drop_request drop{};
  drop.bucket_name = "b`1";
  drop.scope_name = "s`1";
  drop.collection_name = "c`1";
  drop.index_name = "i`1";
  assert_eq(statement_of(drop),
            "DROP INDEX `i``1` ON default:`b``1`.`s``1`.`c``1` USING GSI",
            "DROP INDEX on a collection");

  query_index_drop_request legacy_drop{};
  legacy_drop.bucket_name = "b`1";
  legacy_drop.index_name = "i`1";
  assert_eq(
    statement_of(legacy_drop), "DROP INDEX `b``1`.`i``1` USING GSI", "DROP INDEX on a bucket");

  query_index_build_request build{};
  build.bucket_name = "b`1";
  build.index_names = { "i`1", R"(i\2)" };
  assert_eq(statement_of(build),
            R"(BUILD INDEX ON default:`b``1` (`i``1`,`i\\2`))",
            "BUILD INDEX on a bucket");

  // query_context is parsed by the service without unescaping, so only the statement escapes it
  query_index_create_request scoped{};
  scoped.query_ctx = couchbase::core::query_context{ "b`1", R"(s\1)" };
  scoped.collection_name = "c`1";
  scoped.index_name = "i";
  scoped.keys = { "f" };
  couchbase::core::io::http_request scoped_req;
  auto http_ctx = ::test::utils::make_http_context();
  assert_success(scoped.encode_to(scoped_req, http_ctx), "the request is encoded");
  const auto scoped_body = couchbase::core::utils::json::parse(scoped_req.body);
  assert_eq(scoped_body.at("statement").get_string(),
            R"(CREATE INDEX `i` ON default:`b``1`.`s\\1`.`c``1`(`f`)  USING GSI )",
            "CREATE INDEX with a query context");
  assert_eq(scoped_body.at("query_context").get_string(),
            R"(default:`b`1`.`s\1`)",
            "the query context field");
}

void
empty_index_names_and_keys_are_invalid_arguments([[maybe_unused]] context& ctx)
{
  using couchbase::core::operations::management::query_index_build_request;
  using couchbase::core::operations::management::query_index_create_request;
  using couchbase::core::operations::management::query_index_drop_request;
  auto http_ctx = ::test::utils::make_http_context();
  couchbase::core::io::http_request http_req;

  query_index_create_request create{};
  create.bucket_name = "b";
  create.index_name = "i";
  assert_eq(create.encode_to(http_req, http_ctx),
            couchbase::errc::common::invalid_argument,
            "CREATE INDEX with no keys");
  create.keys = { "" };
  assert_eq(create.encode_to(http_req, http_ctx),
            couchbase::errc::common::invalid_argument,
            "CREATE INDEX with an empty key");

  query_index_drop_request drop{};
  drop.bucket_name = "b";
  assert_eq(drop.encode_to(http_req, http_ctx),
            couchbase::errc::common::invalid_argument,
            "DROP INDEX with no index name");
  drop.is_primary = true;
  assert_eq(
    statement_of(drop), "DROP PRIMARY INDEX ON default:`b` USING GSI", "DROP PRIMARY INDEX");

  query_index_build_request build{};
  build.bucket_name = "b";
  assert_eq(build.encode_to(http_req, http_ctx),
            couchbase::errc::common::invalid_argument,
            "BUILD INDEX with no index names");
  build.index_names = { "i", "" };
  assert_eq(build.encode_to(http_req, http_ctx),
            couchbase::errc::common::invalid_argument,
            "BUILD INDEX with an empty index name");
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
      { CASE(a_coded_rejection_keeps_its_rfc_58_mapping) },
      { CASE(the_first_mapped_error_decides) },
      { CASE(the_per_scope_index_limit_is_quota_limited) },
      { CASE(a_drop_of_an_index_that_does_not_exist_is_index_not_found) },
      { CASE(backticks_and_backslashes_in_identifiers_are_escaped) },
      { CASE(empty_index_names_and_keys_are_invalid_arguments) },
    },
  };
}

} // namespace couchbase::test
