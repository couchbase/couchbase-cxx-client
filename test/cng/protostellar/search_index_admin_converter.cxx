/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Unit tests for the search-index-admin <-> couchbase.admin.search.v1 converter (CXXCBC-901).
// Two properties: a definition survives the round trip whole, and one that cannot be represented
// is refused rather than truncated. Pure, no server.

#include "framework/test_runner.hxx"

#include "core/protostellar/search_index_admin_converter.hxx"
#include "core/utils/json.hxx"

#include <string>

namespace couchbase::test
{
namespace
{
namespace si = ::couchbase::core::protostellar::search_index_admin;
namespace v1 = ::couchbase::admin::search::v1;

// Three deliberately distinct payloads. A converter that crossed two of the blobs would still
// round-trip each one against itself if they held the same value, so no two of these do.
constexpr auto params = R"({"mapping":{"default_analyzer":"standard"}})";
constexpr auto plan_params = R"({"numReplicas":2})";
constexpr auto source_params = R"({"feedBufferSizeBytes":1048576})";

[[nodiscard]] auto
json_eq(const std::string& lhs, const std::string& rhs) -> bool
{
  return couchbase::core::utils::json::parse(lhs) == couchbase::core::utils::json::parse(rhs);
}

[[nodiscard]] auto
full_index() -> couchbase::core::management::search::index
{
  couchbase::core::management::search::index index;
  index.name = "idx";
  index.type = "fulltext-index";
  index.source_name = "travel";
  index.source_type = "couchbase";
  index.source_uuid = "src-uuid";
  index.params_json = params;
  index.plan_params_json = plan_params;
  index.source_params_json = source_params;
  return index;
}

void
apply_index_maps_fields_and_params([[maybe_unused]] context& ctx)
{
  auto index = full_index();

  v1::CreateIndexRequest proto;
  assert_true(si::apply_index(index, proto), "a representable definition is accepted");
  assert_eq(proto.name(), std::string{ "idx" }, "name mapped");
  assert_eq(proto.type(), std::string{ "fulltext-index" }, "type mapped");
  assert_eq(proto.source_name(), std::string{ "travel" }, "source_name mapped");
  assert_eq(proto.source_type(), std::string{ "couchbase" }, "source_type mapped");
  assert_eq(proto.source_uuid(), std::string{ "src-uuid" }, "source_uuid mapped");
  assert_true(proto.params().count("mapping") == 1, "params exploded into the map");
  assert_true(proto.plan_params().count("numReplicas") == 1, "plan_params exploded into the map");
  assert_true(proto.source_params().count("feedBufferSizeBytes") == 1,
              "source_params exploded into the map");
}

void
a_create_carries_no_uuid([[maybe_unused]] context& ctx)
{
  auto index = full_index();
  index.uuid = "existing-uuid";

  v1::CreateIndexRequest proto;
  assert_true(si::apply_index(index, proto), "encode accepted");
  // prev_index_uuid is not the update channel: the gateway passes it to cbsearchx as PrevIndexUUID,
  // which reaches FTS as a body member FTS does not read. A definition with a uuid is an update and
  // belongs in UpdateIndexRequest, so nothing here may carry it.
  assert_false(proto.has_prev_index_uuid(), "the create request carries no prev_index_uuid");
}

void
an_update_carries_the_uuid([[maybe_unused]] context& ctx)
{
  auto index = full_index();
  index.uuid = "existing-uuid";

  v1::UpdateIndexRequest proto;
  assert_true(si::apply_index(index, *proto.mutable_index()), "encode accepted");
  assert_eq(proto.index().uuid(), std::string{ "existing-uuid" }, "uuid reaches Index.uuid");
  assert_eq(proto.index().name(), std::string{ "idx" }, "name mapped on the update path too");
  assert_true(proto.index().params().count("mapping") == 1, "params mapped on the update path");
}

void
all_three_parameter_blobs_survive_the_round_trip([[maybe_unused]] context& ctx)
{
  const auto index = full_index();

  v1::CreateIndexRequest request;
  assert_true(si::apply_index(index, request), "encode accepted");

  // The proto map is the only carrier, so rebuild an Index from the request's three maps and
  // decode that -- the same path GetIndex takes on the way back.
  v1::Index proto;
  *proto.mutable_params() = request.params();
  *proto.mutable_plan_params() = request.plan_params();
  *proto.mutable_source_params() = request.source_params();

  const auto decoded = si::decode_index(proto);
  assert_true(decoded.has_value(), "decode accepted");
  assert_true(json_eq(decoded->params_json, params), "params survives");
  assert_true(json_eq(decoded->plan_params_json, plan_params), "plan_params survives");
  assert_true(json_eq(decoded->source_params_json, source_params), "source_params survives");
}

void
an_unset_parameter_blob_stays_unset([[maybe_unused]] context& ctx)
{
  couchbase::core::management::search::index index;
  index.name = "idx";
  index.type = "fulltext-index";
  index.plan_params_json = plan_params;

  v1::CreateIndexRequest request;
  assert_true(si::apply_index(index, request), "a definition with two unset blobs is accepted");
  assert_true(request.params().empty(), "an unset params contributes no map entry");
  assert_true(request.source_params().empty(), "an unset source_params contributes no map entry");

  v1::Index proto;
  *proto.mutable_plan_params() = request.plan_params();
  const auto decoded = si::decode_index(proto);
  assert_true(decoded.has_value(), "decode accepted");
  assert_true(decoded->params_json.empty(), "an empty map decodes back to unset, not \"{}\"");
  assert_true(decoded->source_params_json.empty(), "and so does the third one");
  assert_true(json_eq(decoded->plan_params_json, plan_params), "the set blob is unaffected");
}

void
an_unparseable_parameter_blob_is_refused([[maybe_unused]] context& ctx)
{
  // Each position separately: a guard on one blob does not cover the other two.
  for (const auto* position : { "params", "plan_params", "source_params" }) {
    auto index = full_index();
    const std::string broken{ "not json" };
    if (std::string{ position } == "params") {
      index.params_json = broken;
    } else if (std::string{ position } == "plan_params") {
      index.plan_params_json = broken;
    } else {
      index.source_params_json = broken;
    }
    v1::CreateIndexRequest request;
    assert_false(si::apply_index(index, request),
                 std::string{ "an unparseable " } + position + " is refused on create");
    v1::Index update;
    assert_false(si::apply_index(index, update),
                 std::string{ "an unparseable " } + position + " is refused on update");
  }
}

void
a_non_object_parameter_blob_is_refused([[maybe_unused]] context& ctx)
{
  // Valid JSON, but the map is keyed by an object's members, so an array or a scalar has nowhere
  // to go. Dropping it is what loses a definition silently.
  for (const auto* payload : { "[1,2,3]", "\"a string\"", "42", "null" }) {
    auto index = full_index();
    index.params_json = payload;
    v1::CreateIndexRequest request;
    assert_false(si::apply_index(index, request),
                 std::string{ "a non-object params (" } + payload + ") is refused");
  }
}

void
a_map_member_that_is_not_json_is_refused([[maybe_unused]] context& ctx)
{
  for (const auto* position : { "params", "plan_params", "source_params" }) {
    v1::Index proto;
    proto.set_name("idx");
    if (std::string{ position } == "params") {
      (*proto.mutable_params())["mapping"] = "not json";
    } else if (std::string{ position } == "plan_params") {
      (*proto.mutable_plan_params())["numReplicas"] = "not json";
    } else {
      (*proto.mutable_source_params())["feedBufferSizeBytes"] = "not json";
    }
    assert_false(si::decode_index(proto).has_value(),
                 std::string{ "a non-JSON " } + position + " member fails the decode");
  }
}

void
params_to_json_keeps_the_unset_convention_for_an_empty_map([[maybe_unused]] context& ctx)
{
  ::google::protobuf::Map<std::string, std::string> empty;
  const auto rebuilt = si::params_to_json(empty);
  assert_true(rebuilt.has_value(), "an empty map is not an error");
  assert_true(rebuilt->empty(),
              "an empty map rebuilds to the unset (empty) convention, not \"{}\"");
}

void
decode_index_maps_fields([[maybe_unused]] context& ctx)
{
  v1::Index proto;
  proto.set_uuid("u1");
  proto.set_name("idx");
  proto.set_type("fulltext-index");
  proto.set_source_name("travel");
  proto.set_source_type("couchbase");
  proto.set_source_uuid("src-uuid");
  (*proto.mutable_params())["mapping"] = R"({"x":1})";

  const auto index = si::decode_index(proto);
  assert_true(index.has_value(), "decode accepted");
  assert_eq(index->uuid, std::string{ "u1" }, "uuid decoded");
  assert_eq(index->name, std::string{ "idx" }, "name decoded");
  assert_eq(index->type, std::string{ "fulltext-index" }, "type decoded");
  assert_eq(index->source_name, std::string{ "travel" }, "source_name decoded");
  assert_eq(index->source_type, std::string{ "couchbase" }, "source_type decoded");
  assert_eq(index->source_uuid, std::string{ "src-uuid" }, "source_uuid decoded");
  assert_true(json_eq(index->params_json, R"({"mapping":{"x":1}})"),
              "params object carries the mapping member");
}

void
a_definition_survives_the_full_encode_decode_cycle([[maybe_unused]] context& ctx)
{
  // get_index -> edit -> upsert is the cycle that loses members when a conversion drops what it
  // cannot represent, so the whole definition is compared, not one blob.
  auto original = full_index();
  original.uuid = "u1";

  v1::UpdateIndexRequest request;
  assert_true(si::apply_index(original, *request.mutable_index()), "encode accepted");
  const auto decoded = si::decode_index(request.index());
  assert_true(decoded.has_value(), "decode accepted");

  assert_eq(decoded->uuid, original.uuid, "uuid survives");
  assert_eq(decoded->name, original.name, "name survives");
  assert_eq(decoded->type, original.type, "type survives");
  assert_eq(decoded->source_name, original.source_name, "source_name survives");
  assert_eq(decoded->source_type, original.source_type, "source_type survives");
  assert_eq(decoded->source_uuid, original.source_uuid, "source_uuid survives");
  assert_true(json_eq(decoded->params_json, original.params_json), "params survives");
  assert_true(json_eq(decoded->plan_params_json, original.plan_params_json),
              "plan_params survives");
  assert_true(json_eq(decoded->source_params_json, original.source_params_json),
              "source_params survives");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_search_index_admin_converter",
    {
      { "apply_index_maps_fields_and_params", apply_index_maps_fields_and_params },
      { "a_create_carries_no_uuid", a_create_carries_no_uuid },
      { "an_update_carries_the_uuid", an_update_carries_the_uuid },
      { "all_three_parameter_blobs_survive_the_round_trip",
        all_three_parameter_blobs_survive_the_round_trip },
      { "an_unset_parameter_blob_stays_unset", an_unset_parameter_blob_stays_unset },
      { "an_unparseable_parameter_blob_is_refused", an_unparseable_parameter_blob_is_refused },
      { "a_non_object_parameter_blob_is_refused", a_non_object_parameter_blob_is_refused },
      { "a_map_member_that_is_not_json_is_refused", a_map_member_that_is_not_json_is_refused },
      { "params_to_json_keeps_the_unset_convention_for_an_empty_map",
        params_to_json_keeps_the_unset_convention_for_an_empty_map },
      { "decode_index_maps_fields", decode_index_maps_fields },
      { "a_definition_survives_the_full_encode_decode_cycle",
        a_definition_survives_the_full_encode_decode_cycle },
    },
  };
}

} // namespace couchbase::test
