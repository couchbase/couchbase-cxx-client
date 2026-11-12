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
#include "core/utils/json.hxx"

#include <tao/json/value.hpp>

#include <cstdint>
#include <regex>
#include <string>
#include <string_view>
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

auto
decode_quoted_identifier(std::string_view token) -> std::string
{
  assert_true(token.size() >= 2 && token.front() == '`' && token.back() == '`',
              "quoted identifier is enclosed in backticks");
  std::string name;
  for (std::size_t i = 1; i + 1 < token.size(); ++i) {
    if (token[i] == '\\' && i + 2 < token.size()) {
      ++i;
    }
    name.push_back(token[i]);
  }
  return name;
}

void
an_empty_or_single_backtick_key_in_create_index_does_not_throw([[maybe_unused]] context& ctx)
{
  auto http_ctx = ::test::utils::make_http_context();

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_create_request req{
      "bucket_name", "scope_name", "collection_name",
      "test_index",  { "" },       { "bucket_name", "scope_name" },
    };
    assert_eq(req.encode_to(http_req, http_ctx),
              couchbase::errc::common::invalid_argument,
              "an empty key in create_index returns invalid_argument without throwing");
  }

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_create_request req{
      "bucket_name", "scope_name", "collection_name",
      "test_index",  {},           { "bucket_name", "scope_name" },
    };
    assert_eq(req.encode_to(http_req, http_ctx),
              couchbase::errc::common::invalid_argument,
              "a secondary index with no keys returns invalid_argument");
  }

  {
    const auto statement = encoded_statement({ "`" });
    std::smatch match;
    assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
    assert_eq(match[2].str(), R"(`\``)", "a single backtick key is quoted and escaped");
    assert_eq(decode_quoted_identifier(match[2].str()),
              "`",
              "decoding the quoted key recovers the original backtick");
  }
}

void
backticks_in_index_and_keyspace_identifiers_are_escaped([[maybe_unused]] context& ctx)
{
  auto http_ctx = ::test::utils::make_http_context();

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_create_request req{};
    req.bucket_name = "bkt`1";
    req.scope_name = "scp`1";
    req.collection_name = "col`1";
    req.index_name = "idx`1";
    req.keys = { "fld`1" };
    assert_success(req.encode_to(http_req, http_ctx), "create_index encodes");
    const auto body = couchbase::core::utils::json::parse(http_req.body);
    assert_eq(
      body.at("statement").get_string(),
      R"(CREATE INDEX `idx\`1` ON default:`bkt\`1`.`scp\`1`.`col\`1`(`fld\`1`)  USING GSI )",
      "create_index backslash-escapes internal backticks in identifiers");
  }

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_drop_request req{};
    req.bucket_name = "bkt`1";
    req.scope_name = "scp`1";
    req.collection_name = "col`1";
    req.index_name = "idx`1";
    assert_success(req.encode_to(http_req, http_ctx), "drop_index encodes");
    const auto body = couchbase::core::utils::json::parse(http_req.body);
    assert_eq(body.at("statement").get_string(),
              R"(DROP INDEX `idx\`1` ON default:`bkt\`1`.`scp\`1`.`col\`1` USING GSI)",
              "drop_index backslash-escapes internal backticks in identifiers");
  }

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_build_request req{};
    req.bucket_name = "bkt`1";
    req.scope_name = "scp`1";
    req.collection_name = "col`1";
    req.index_names = { "idx`1", "idx`2" };
    assert_success(req.encode_to(http_req, http_ctx), "build_index encodes");
    const auto body = couchbase::core::utils::json::parse(http_req.body);
    assert_eq(body.at("statement").get_string(),
              R"(BUILD INDEX ON default:`bkt\`1`.`scp\`1`.`col\`1` (`idx\`1`,`idx\`2`))",
              "build_index backslash-escapes internal backticks in identifiers");
  }

  for (const auto* raw : { "bkt`1", "scp`1", "col`1", "idx`1", "idx`2", "fld`1" }) {
    const auto quoted = couchbase::core::utils::escape_query_identifier(raw);
    assert_eq(decode_quoted_identifier(quoted),
              raw,
              "decoding quoted identifier recovers the original name");
  }
}

void
pre_quoted_keys_with_interior_backticks_or_backslashes_and_query_context_are_escaped(
  [[maybe_unused]] context& ctx)
{
  auto http_ctx = ::test::utils::make_http_context();

  {
    const auto statement = encoded_statement({ "`a`,`b`", "``", R"(`a\)" });
    std::smatch match;
    assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
    assert_eq(match[2].str(),
              R"(`\`a\`,\`b\``, `\`\``, `\`a\\`)",
              "keys that close early, are empty quotes, or end with a backslash are escaped");
    for (const auto* raw : { "`a`,`b`", "``", R"(`a\)" }) {
      const auto quoted = couchbase::core::utils::escape_query_identifier(raw);
      assert_eq(decode_quoted_identifier(quoted),
                raw,
                "decoding escaped hostile key recovers the original key");
    }
  }

  {
    const auto statement = encoded_statement({
      "`a``b`",
      R"(`a\`b`)",
      "`profile`.`name`",
      "`profile`.name",
      "`profile`.address.`zip`",
      "`addresses`[0].`city`",
      "`addresses`[1+1].`city`",
      "`addresses`[4/2].`city`",
      "`price` * `quantity`",
      "`price` * 1.5 + `tax`",
      "`active` AND `verified`",
      "`profile` . `name`",
      R"(`a\.b`.`c\`d`)",
    });
    std::smatch match;
    assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
    assert_eq(match[2].str(),
              R"(`a``b`, `a\`b`, `profile`.`name`, `profile`.name, `profile`.address.`zip`, )"
              R"(`addresses`[0].`city`, `addresses`[1+1].`city`, `addresses`[4/2].`city`, )"
              R"(`price` * `quantity`, `price` * 1.5 + `tax`, `active` AND `verified`, )"
              R"(`profile` . `name`, `a\.b`.`c\`d`)",
              "valid pre-quoted single, dotted, subscripted, and mixed-quoting keys are preserved");
    assert_eq(encoded_statement({
                "`addresses`[(1+1)].`city`",
                "`profile`.\n`name`",
                "`profile`.\r\n`name`",
              }),
              "CREATE INDEX `test_index` ON "
              "default:`bucket_name`.`scope_name`.`collection_name`(`addresses`[(1+1)].`city`, "
              "`profile`.\n`name`, `profile`.\r\n`name`)  USING GSI ",
              "pre-quoted parenthesized-subscript and LF/CRLF dotted keys are preserved");
  }

  {
    const auto statement = encoded_statement({
      "net-`tax`",
      "`profile`..`name`",
      "`profile`.123.`zip`",
      "`addresses`[0) WHERE 1=1--].`city`",
      "`profile`.`name`) WHERE 1=1 --`",
    });
    std::smatch match;
    assert_true(std::regex_search(statement, match, create_index), "a CREATE INDEX statement");
    assert_eq(match[2].str(),
              R"(`net-\`tax\``, `\`profile\`..\`name\``, `\`profile\`.123.\`zip\``, )"
              R"(`\`addresses\`[0) WHERE 1=1--].\`city\``, )"
              R"(`\`profile\`.\`name\`) WHERE 1=1 --\``)",
              "malformed or injected dotted keys are escaped as single identifiers");
  }

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_create_request req{
      "ignored_bucket", "",           "",
      "test_index",     { "field1" }, couchbase::core::query_context{ "bkt`1", "scp`1" },
    };
    assert_eq(req.encode_to(http_req, http_ctx),
              couchbase::errc::common::invalid_argument,
              "query_context with an empty collection_name is rejected");
  }

  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::management::query_index_drop_request req{};
    req.bucket_name = "bkt`1";
    req.collection_name = "col`1";
    req.index_name = "idx`1";
    req.query_ctx = couchbase::core::query_context{ "bkt`1", R"(scp\1)" };
    assert_success(req.encode_to(http_req, http_ctx), "drop_index with query_ctx encodes");
    const auto body = couchbase::core::utils::json::parse(http_req.body);
    assert_eq(body.at("statement").get_string(),
              R"(DROP INDEX `idx\`1` ON default:`bkt\`1`.`scp\\1`.`col\`1` USING GSI)",
              "drop_index with query_ctx uses the full escaped keyspace");
    assert_eq(body.at("query_context").get_string(),
              R"(default:`bkt\`1`.`scp\\1`)",
              "query_context escapes backticks and backslashes");
    assert_eq(decode_quoted_identifier(couchbase::core::utils::escape_query_identifier(R"(scp\1)")),
              R"(scp\1)",
              "decoding quoted scope with backslash recovers the original name");
  }
}

// `query_index_create_request::keys` carries one string list for both raw field names and
// pre-quoted SQL++ index keys. Inputs outside `utils::is_escaped_query_identifier` -- string
// literals, function calls, `[*]`, `BETWEEN`, multi-word operators -- are quoted as single literal
// field names, which GSI accepts because `CREATE INDEX` is schemaless.
void
sqlpp_expressions_outside_recognized_subset_are_quoted_as_literal_field_names(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view prefix =
    "CREATE INDEX `test_index` ON default:`bucket_name`.`scope_name`.`collection_name`(";
  constexpr std::string_view suffix = ")  USING GSI ";

  for (const auto* recognized : {
         "`email`",
         "`profile`.`name`",
         "`addresses`[0].`city`",
         "`addresses`[(1+1)].`city`",
         "`price` * 1.5 + `tax`",
         "`price` * .5 + `tax`",
         "`price` * 1e3 + `tax`",
         "`price` * 1.5e-3 + `tax`",
         "`price` * 2E+4 + `tax`",
         "`active` AND NOT `deleted`",
       }) {
    assert_true(couchbase::core::utils::is_escaped_query_identifier(recognized),
                "recognized leading-backtick identifier, path, or basic expression passes through");
    assert_eq(encoded_statement({ recognized }),
              std::string{ prefix } + recognized + std::string{ suffix },
              "recognized SQL++ key is emitted verbatim");
  }

  const std::vector<std::pair<const char*, const char*>> unquoted_as_literal_fields{
    { "`first` || ' ' || `last`", R"(`\`first\` || ' ' || \`last\``)" },
    { "`prefix` || LOWER(`email`)", R"(`\`prefix\` || LOWER(\`email\`)`)" },
    { "LOWER(`email`)", R"(`LOWER(\`email\`)`)" },
    { "`items`[*].`id`", R"(`\`items\`[*].\`id\``)" },
    { "`age` BETWEEN 18 AND `max_age`", R"(`\`age\` BETWEEN 18 AND \`max_age\``)" },
    { "`email` NOT LIKE `pattern`", R"(`\`email\` NOT LIKE \`pattern\``)" },
    { "`price` * 1e + `tax`", R"(`\`price\` * 1e + \`tax\``)" },
    { "`price` * 1e+ + `tax`", R"(`\`price\` * 1e+ + \`tax\``)" },
    { "`price` * 1e3abc + `tax`", R"(`\`price\` * 1e3abc + \`tax\``)" },
  };

  for (const auto& [expr, expected_token] : unquoted_as_literal_fields) {
    assert_false(couchbase::core::utils::is_escaped_query_identifier(expr),
                 "SQL++ expression outside the recognized subset is not passed through");
    const auto statement = encoded_statement({ expr });
    assert_eq(statement,
              std::string{ prefix } + expected_token + std::string{ suffix },
              "unrecognized SQL++ expression is quoted as one literal field-name token");
    const auto emitted_token =
      statement.substr(prefix.size(), statement.size() - prefix.size() - suffix.size());
    assert_eq(decode_quoted_identifier(emitted_token),
              expr,
              "the quoted token decodes to a single literal field named after the expression text");
  }
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
      { CASE(an_empty_or_single_backtick_key_in_create_index_does_not_throw) },
      { CASE(backticks_in_index_and_keyspace_identifiers_are_escaped) },
      { CASE(
        pre_quoted_keys_with_interior_backticks_or_backslashes_and_query_context_are_escaped) },
      { CASE(sqlpp_expressions_outside_recognized_subset_are_quoted_as_literal_field_names) },
    },
  };
}

} // namespace couchbase::test
