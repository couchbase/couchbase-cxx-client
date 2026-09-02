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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_message.hxx"
#include "core/io/query_cache.hxx"
#include "core/operations/document_query.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/query_row.hxx>
#include <couchbase/query_stream_result.hxx>

#include <tao/json/value.hpp>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
// The encoder only emits use_replica when the cluster advertises the capability, so the
// configuration the context carries is part of what these cases pin.
auto
encoded_body(couchbase::core::operations::query_request& req) -> tao::json::value
{
  couchbase::core::topology::configuration config{};
  config.capabilities.cluster.insert(couchbase::core::cluster_capability::n1ql_read_from_replica);

  couchbase::core::query_cache query_cache{};
  couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  std::string canonical_hostname{};
  std::uint16_t canonical_port{};
  couchbase::core::http_context http_ctx{
    config, cluster_options, query_cache, hostname, port, canonical_hostname, canonical_port,
  };

  couchbase::core::io::http_request http_req;
  assert_success(req.encode_to(http_req, http_ctx), "the request is encoded");

  auto body = couchbase::core::utils::json::parse(http_req.body);
  assert_true(body.is_object(), "the encoded body is a JSON object");
  return body;
}

void
use_replica_set_true_asks_the_server_for_replica_reads([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::query_request req{};
  req.use_replica = true;

  assert_eq(encoded_body(req).get_object().at("use_replica").get_string(),
            "on",
            "the read-from-replica flag");
}

void
use_replica_set_false_forbids_replica_reads([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::query_request req{};
  req.use_replica = false;

  assert_eq(encoded_body(req).get_object().at("use_replica").get_string(),
            "off",
            "the read-from-replica flag");
}

void
use_replica_left_unset_omits_the_flag([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::query_request req{};

  assert_eq(encoded_body(req).get_object().count("use_replica"),
            0U,
            "an unset option leaves the server's own default in force");
}

void
positional_parameters_can_be_replaced_added_and_cleared([[maybe_unused]] context& ctx)
{
  using couchbase::codec::tao_json_serializer;

  couchbase::query_options opts;
  opts.positional_parameters(10, 20);
  assert_true(opts.build().positional_parameters ==
                std::vector<couchbase::codec::binary>{ tao_json_serializer::serialize(10),
                                                       tao_json_serializer::serialize(20) },
              "the parameters given");

  opts.clear_positional_parameters();
  assert_true(opts.build().positional_parameters.empty(), "clearing removes every parameter");

  opts.add_positional_parameter(25);
  assert_true(opts.build().positional_parameters ==
                std::vector<couchbase::codec::binary>{ tao_json_serializer::serialize(25) },
              "adding to an empty list");

  opts.add_positional_parameter("foo");
  assert_true(opts.build().positional_parameters ==
                std::vector<couchbase::codec::binary>{ tao_json_serializer::serialize(25),
                                                       tao_json_serializer::serialize("foo") },
              "adding appends rather than replacing");

  opts.positional_parameters(4, 5);
  assert_true(opts.build().positional_parameters ==
                std::vector<couchbase::codec::binary>{ tao_json_serializer::serialize(4),
                                                       tao_json_serializer::serialize(5) },
              "setting replaces what was there");
}

void
named_parameters_can_be_replaced_added_and_cleared([[maybe_unused]] context& ctx)
{
  using couchbase::codec::tao_json_serializer;
  using named_parameters = std::map<std::string, couchbase::codec::binary, std::less<>>;

  couchbase::query_options opts;
  opts.named_parameters(std::make_pair("foo", 10), std::make_pair("bar", 20));
  assert_true(opts.build().named_parameters ==
                named_parameters{
                  { "foo", tao_json_serializer::serialize(10) },
                  { "bar", tao_json_serializer::serialize(20) },
                },
              "the parameters given");

  opts.clear_named_parameters();
  assert_true(opts.build().named_parameters.empty(), "clearing removes every parameter");

  opts.add_named_parameter("foo", 25);
  assert_true(opts.build().named_parameters ==
                named_parameters{
                  { "foo", tao_json_serializer::serialize(25) },
                },
              "adding to an empty map");

  opts.add_named_parameter("bar", "baz");
  assert_true(opts.build().named_parameters ==
                named_parameters{
                  { "foo", tao_json_serializer::serialize(25) },
                  { "bar", tao_json_serializer::serialize("baz") },
                },
              "adding leaves the existing entries in place");

  opts.named_parameters(std::make_pair("foo", 3), std::make_pair("bar", 4));
  assert_true(opts.build().named_parameters ==
                named_parameters{
                  { "foo", tao_json_serializer::serialize(3) },
                  { "bar", tao_json_serializer::serialize(4) },
                },
              "setting replaces what was there");
}

void
a_query_row_decodes_the_json_it_carries([[maybe_unused]] context& ctx)
{
  auto bytes = couchbase::core::utils::to_binary(std::string{ R"({"a":7})" });
  const couchbase::query_row row{ bytes };

  auto value = row.content_as<couchbase::codec::tao_json_serializer, tao::json::value>();
  assert_eq(value.at("a").as<int>(), 7, "the decoded field");
  assert_true(row.content_as_binary() == bytes, "the undecoded bytes are handed back unchanged");
}

void
a_default_constructed_query_stream_result_cancels_safely([[maybe_unused]] context& ctx)
{
  couchbase::query_stream_result empty{};
  empty.cancel(); // no-op on empty handle, must not crash
  assert_false(empty.signature().has_value(), "an empty handle carries no signature");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(use_replica_set_true_asks_the_server_for_replica_reads) },
      { CASE(use_replica_set_false_forbids_replica_reads) },
      { CASE(use_replica_left_unset_omits_the_flag) },
      { CASE(positional_parameters_can_be_replaced_added_and_cleared) },
      { CASE(named_parameters_can_be_replaced_added_and_cleared) },
      { CASE(a_query_row_decodes_the_json_it_carries) },
      { CASE(a_default_constructed_query_stream_result_cancels_safely) },
    },
  };
}

} // namespace couchbase::test
