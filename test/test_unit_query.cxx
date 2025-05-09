/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023 Couchbase, Inc.
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

#include "utils/move_only_context.hxx"

#include "core/operations/document_query.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json/value.hpp>

couchbase::core::http_context
make_http_context(couchbase::core::topology::configuration& config)
{
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  couchbase::core::http_context ctx{ config, cluster_options, query_cache, hostname, port };
  return ctx;
}

TEST_CASE("unit: query with read from replica", "[unit]")
{
  couchbase::core::topology::configuration config{};
  config.capabilities.cluster.insert(couchbase::core::cluster_capability::n1ql_read_from_replica);
  auto ctx = make_http_context(config);

  SECTION("use_replica true")
  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::query_request req{};
    req.use_replica = true;
    auto ec = req.encode_to(http_req, ctx);
    REQUIRE_SUCCESS(ec);
    auto body = couchbase::core::utils::json::parse(http_req.body);
    REQUIRE(body.is_object());
    REQUIRE(body.get_object().at("use_replica").get_string() == "on");
  }

  SECTION("use_replica false")
  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::query_request req{};
    req.use_replica = false;
    auto ec = req.encode_to(http_req, ctx);
    REQUIRE_SUCCESS(ec);
    auto body = couchbase::core::utils::json::parse(http_req.body);
    REQUIRE(body.is_object());
    REQUIRE(body.get_object().at("use_replica").get_string() == "off");
  }

  SECTION("use_replica not set")
  {
    couchbase::core::io::http_request http_req;
    couchbase::core::operations::query_request req{};
    auto ec = req.encode_to(http_req, ctx);
    REQUIRE_SUCCESS(ec);
    auto body = couchbase::core::utils::json::parse(http_req.body);
    REQUIRE(body.is_object());
    REQUIRE_FALSE(body.get_object().count("use_replica"));
  }
}

TEST_CASE("unit: Public API query options - add/clear parameters")
{
  SECTION("positional parameters")
  {
    couchbase::query_options opts;
    opts.positional_parameters(10, 20);
    REQUIRE(opts.build().positional_parameters ==
            std::vector<couchbase::codec::binary>{
              couchbase::codec::tao_json_serializer::serialize(10),
              couchbase::codec::tao_json_serializer::serialize(20) });

    opts.clear_positional_parameters();
    REQUIRE(opts.build().positional_parameters.empty());

    opts.add_positional_parameter(25);
    REQUIRE(opts.build().positional_parameters ==
            std::vector<couchbase::codec::binary>{
              couchbase::codec::tao_json_serializer::serialize(25) });

    opts.add_positional_parameter("foo");
    REQUIRE(opts.build().positional_parameters ==
            std::vector<couchbase::codec::binary>{
              couchbase::codec::tao_json_serializer::serialize(25),
              couchbase::codec::tao_json_serializer::serialize("foo") });

    opts.positional_parameters(4, 5);
    REQUIRE(
      opts.build().positional_parameters ==
      std::vector<couchbase::codec::binary>{ couchbase::codec::tao_json_serializer::serialize(4),
                                             couchbase::codec::tao_json_serializer::serialize(5) });
  }

  SECTION("named parameters")
  {
    couchbase::query_options opts;
    opts.named_parameters(std::make_pair("foo", 10), std::make_pair("bar", 20));
    REQUIRE(opts.build().named_parameters ==
            std::map<std::string, couchbase::codec::binary, std::less<>>{
              { "foo", couchbase::codec::tao_json_serializer::serialize(10) },
              { "bar", couchbase::codec::tao_json_serializer::serialize(20) },
            });

    opts.clear_named_parameters();
    REQUIRE(opts.build().named_parameters.empty());

    opts.add_named_parameter("foo", 25);
    REQUIRE(opts.build().named_parameters ==
            std::map<std::string, couchbase::codec::binary, std::less<>>{
              { "foo", couchbase::codec::tao_json_serializer::serialize(25) },
            });

    opts.add_named_parameter("bar", "baz");
    REQUIRE(opts.build().named_parameters ==
            std::map<std::string, couchbase::codec::binary, std::less<>>{
              { "foo", couchbase::codec::tao_json_serializer::serialize(25) },
              { "bar", couchbase::codec::tao_json_serializer::serialize("baz") },
            });

    opts.named_parameters(std::make_pair("foo", 3), std::make_pair("bar", 4));
    REQUIRE(opts.build().named_parameters ==
            std::map<std::string, couchbase::codec::binary, std::less<>>{
              { "foo", couchbase::codec::tao_json_serializer::serialize(3) },
              { "bar", couchbase::codec::tao_json_serializer::serialize(4) },
            });
  }
}
