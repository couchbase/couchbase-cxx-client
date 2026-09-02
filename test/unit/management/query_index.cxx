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

#include "core/io/http_message.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/utils/json.hxx"

#include <tao/json/value.hpp>

#include <regex>
#include <string>
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
    },
  };
}

} // namespace couchbase::test
