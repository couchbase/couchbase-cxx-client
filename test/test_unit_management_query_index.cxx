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

#include "test_helper_integration.hxx"

#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/io/query_cache.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/topology/configuration.hxx"

#include <tao/json/value.hpp>

#include <regex>
#include <string>

couchbase::core::http_context
make_http_context()
{
  static couchbase::core::topology::configuration config{};
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  couchbase::core::http_context ctx{ config, cluster_options, query_cache, hostname, port };
  return ctx;
}

TEST_CASE("unit: create query index key encoding", "[unit]")
{
  couchbase::core::io::http_request http_req;
  couchbase::core::operations::management::query_index_create_request req{
    "bucket_name", "scope_name", "collection_name",
    "test_index",  {},           { "bucket_name", "scope_name" },
  };
  auto ctx = make_http_context();
  std::regex r{ "CREATE INDEX (.+) ON .*\\((.*)\\) .* USING GSI.*" };

  SECTION("single key")
  {
    req.keys = { "test_field" };
    auto ec = req.encode_to(http_req, ctx);

    REQUIRE_SUCCESS(ec);

    auto body = couchbase::core::utils::json::parse(http_req.body);

    REQUIRE(body.is_object());
    REQUIRE(body.get_object().at("statement").is_string());

    auto statement = body.get_object().at("statement").get_string();
    std::smatch match;

    REQUIRE(std::regex_search(statement, match, r));
    REQUIRE(match[1] == "`test_index`");
    REQUIRE(match[2] == "`test_field`");
  }

  SECTION("multiple keys")
  {
    req.keys = { "field-1", "field-2", "field-3" };
    auto ec = req.encode_to(http_req, ctx);

    REQUIRE_SUCCESS(ec);

    auto body = couchbase::core::utils::json::parse(http_req.body);

    REQUIRE(body.is_object());
    REQUIRE(body.get_object().at("statement").is_string());

    auto statement = body.get_object().at("statement").get_string();
    std::smatch match;

    REQUIRE(std::regex_search(statement, match, r));
    REQUIRE(match[1] == "`test_index`");
    REQUIRE(match[2] == "`field-1`, `field-2`, `field-3`");
  }

  SECTION("key already has backticks")
  {
    req.keys = { "field-1", "`field-2`", "`field-3`" };
    auto ec = req.encode_to(http_req, ctx);

    REQUIRE_SUCCESS(ec);

    auto body = couchbase::core::utils::json::parse(http_req.body);

    REQUIRE(body.is_object());
    REQUIRE(body.get_object().at("statement").is_string());

    auto statement = body.get_object().at("statement").get_string();
    std::smatch match;

    REQUIRE(std::regex_search(statement, match, r));
    REQUIRE(match[1] == "`test_index`");
    REQUIRE(match[2] == "`field-1`, `field-2`, `field-3`");
  }
}
