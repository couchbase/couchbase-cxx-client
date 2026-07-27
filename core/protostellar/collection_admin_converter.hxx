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

// Decodes couchbase.admin.collection.v1 ListCollections into a core collections_manifest, and maps
// a collection's max_expiry onto the wire in the other direction. The rest of the scope/collection
// encodes are string setters and live inline in the component; max_expiry does not, because its
// two halves only agree if they are read together. Per-scope and per-collection uids are not
// carried by the proto, so only the top-level manifest uid is populated.

#include "core/topology/collections_manifest.hxx"

#include <couchbase/admin/collection/v1/collection.pb.h>

#include <cstdint>
#include <optional>
#include <utility>

namespace couchbase::core::protostellar::collection_admin
{
namespace v1 = ::couchbase::admin::collection::v1;

// Maps the core max_expiry sentinel onto max_expiry_secs. The proto field is unsigned, so the sign
// the core struct carries has to become the field's presence: a negative value ("no expiry") is an
// explicit 0, and 0 has no encoding at all -- nullopt here means leave the field unset. What an
// unset field then means belongs to the caller, and it differs between the two RPCs: on create it
// is "inherit the bucket default", which is what 0 asked for, and on update it is "leave the
// collection as it is", which is not.
//
// -1 is the only negative the core struct gives a meaning; the caller refuses anything below it
// with invalid_argument before reaching here, as the classic path does. Every negative maps to the
// same wire form, so an unvalidated -5 would silently become "no expiry".
[[nodiscard]] inline auto
encode_max_expiry(std::int32_t max_expiry) -> std::optional<std::uint32_t>
{
  if (max_expiry < 0) {
    return 0U;
  }
  if (max_expiry == 0) {
    return {};
  }
  return static_cast<std::uint32_t>(max_expiry);
}

[[nodiscard]] inline auto
decode_manifest(const v1::ListCollectionsResponse& proto) -> topology::collections_manifest
{
  topology::collections_manifest manifest;
  manifest.uid = proto.manifest_uid();
  for (const auto& proto_scope : proto.scopes()) {
    topology::collections_manifest::scope scope;
    scope.name = proto_scope.name();
    for (const auto& proto_collection : proto_scope.collections()) {
      topology::collections_manifest::collection collection;
      collection.name = proto_collection.name();
      // max_expiry_secs is an unsigned optional, so the gateway spends the presence bit on the
      // distinction the core struct makes with a sign: the field left unset is "inherit the bucket
      // default" (0), an explicit 0 is "no expiry" (-1). The value getter alone collapses the two,
      // because it returns 0 for an absent field as well.
      if (!proto_collection.has_max_expiry_secs()) {
        collection.max_expiry = 0;
      } else if (proto_collection.max_expiry_secs() == 0) {
        collection.max_expiry = -1;
      } else {
        collection.max_expiry = static_cast<std::int32_t>(proto_collection.max_expiry_secs());
      }
      if (proto_collection.has_history_retention_enabled()) {
        collection.history = proto_collection.history_retention_enabled();
      }
      scope.collections.push_back(std::move(collection));
    }
    manifest.scopes.push_back(std::move(scope));
  }
  return manifest;
}

} // namespace couchbase::core::protostellar::collection_admin
