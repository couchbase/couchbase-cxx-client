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

#pragma once

// Translates core FTS index settings to and from couchbase.admin.search.v1. The one non-trivial
// bit: core stores params/plan_params/source_params as single JSON-object strings, while the proto
// Index uses map<string,bytes> keyed by the object's top-level members (each value a raw-JSON
// blob). fill_params/params_to_json round-trip between the two representations.
//
// Every conversion that can lose a member reports failure instead. A definition that cannot be
// represented must not be truncated in silence: the caller edits what get_index returned and
// upserts it back, so a dropped member deletes part of the index definition while the call reports
// success.

#include "core/management/search_index.hxx"
#include "core/utils/json.hxx"

#include <tao/json/value.hpp>

#include <couchbase/admin/search/v1/search.pb.h>

#include <google/protobuf/map.h>

#include <optional>
#include <string>

namespace couchbase::core::protostellar::search_index_admin
{
namespace v1 = ::couchbase::admin::search::v1;

// Explode a JSON-object string into the proto map: one entry per top-level member, value = the
// member's raw JSON. An empty input is the "unset" convention and contributes nothing. Anything
// that is not a JSON object has no representation in the map and is refused.
[[nodiscard]] inline auto
fill_params(const std::string& json, ::google::protobuf::Map<std::string, std::string>* target)
  -> bool
{
  if (json.empty()) {
    return true;
  }
  tao::json::value parsed;
  try {
    parsed = utils::json::parse(json);
  } catch (...) {
    return false;
  }
  if (!parsed.is_object()) {
    return false;
  }
  for (const auto& [key, value] : parsed.get_object()) {
    (*target)[key] = utils::json::generate(value);
  }
  return true;
}

// Rebuild a JSON-object string from the proto map (inverse of fill_params). Empty map -> empty
// string so the core struct keeps its "unset" convention. A member whose bytes are not JSON cannot
// be placed in the object, and dropping it would hand back a definition short of what the server
// holds, so the whole conversion fails.
[[nodiscard]] inline auto
params_to_json(const ::google::protobuf::Map<std::string, std::string>& source)
  -> std::optional<std::string>
{
  if (source.empty()) {
    return std::string{};
  }
  tao::json::value object = tao::json::empty_object;
  for (const auto& [key, value] : source) {
    try {
      object[key] = utils::json::parse(value);
    } catch (...) {
      return {};
    }
  }
  return utils::json::generate(object);
}

// The fields shared by CreateIndexRequest and Index. The two messages carry the same index
// definition under the same field names, but they are unrelated proto types and neither derives
// from the other, so the assignments are written once against whichever is passed.
template<typename Proto>
[[nodiscard]] auto
apply_index_fields(const management::search::index& index, Proto& proto) -> bool
{
  proto.set_name(index.name);
  proto.set_type(index.type);
  if (!index.source_name.empty()) {
    proto.set_source_name(index.source_name);
  }
  if (!index.source_type.empty()) {
    proto.set_source_type(index.source_type);
  }
  if (!index.source_uuid.empty()) {
    proto.set_source_uuid(index.source_uuid);
  }
  return fill_params(index.params_json, proto.mutable_params()) &&
         fill_params(index.plan_params_json, proto.mutable_plan_params()) &&
         fill_params(index.source_params_json, proto.mutable_source_params());
}

// Encode a core index into a CreateIndexRequest. A create carries no uuid: an upsert that supplies
// one is an update and goes to UpdateIndex instead.
[[nodiscard]] inline auto
apply_index(const management::search::index& index, v1::CreateIndexRequest& proto) -> bool
{
  return apply_index_fields(index, proto);
}

// Encode a core index into the Index of an UpdateIndexRequest. The uuid is what makes it an
// update: the gateway rejects the call without one, and the server matches it against the current
// definition so a concurrent update is not overwritten.
[[nodiscard]] inline auto
apply_index(const management::search::index& index, v1::Index& proto) -> bool
{
  proto.set_uuid(index.uuid);
  return apply_index_fields(index, proto);
}

[[nodiscard]] inline auto
decode_index(const v1::Index& proto) -> std::optional<management::search::index>
{
  auto params = params_to_json(proto.params());
  auto plan_params = params_to_json(proto.plan_params());
  auto source_params = params_to_json(proto.source_params());
  if (!params.has_value() || !plan_params.has_value() || !source_params.has_value()) {
    return {};
  }
  management::search::index index;
  index.uuid = proto.uuid();
  index.name = proto.name();
  index.type = proto.type();
  index.source_uuid = proto.source_uuid();
  index.source_name = proto.source_name();
  index.source_type = proto.source_type();
  index.params_json = std::move(*params);
  index.plan_params_json = std::move(*plan_params);
  index.source_params_json = std::move(*source_params);
  return index;
}

} // namespace couchbase::core::protostellar::search_index_admin
