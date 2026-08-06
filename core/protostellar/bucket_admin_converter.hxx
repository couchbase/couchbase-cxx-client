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

// Translates core bucket management settings to and from couchbase.admin.bucket.v1 (unary admin
// RPCs). The proto enums have no `unknown` sentinel, so the core `unknown` values fall back to the
// server default on encode. memcached buckets have no couchbase2 representation;
// apply_create_only_settings() returns false for them so the component surfaces
// feature_not_available.
//
// Every field the encode side writes is read back by decode_bucket(), because get-modify-update is
// the ordinary way these settings are changed: a field that decodes to a default is sent back as a
// default on the next update and silently resets the bucket.

#include "core/management/bucket_settings.hxx"
#include "core/operations/management/bucket_get_all.hxx"

#include <couchbase/durability_level.hxx>

#include <couchbase/admin/bucket/v1/bucket.pb.h>
#include <couchbase/kv/v1/kv.pb.h>

#include <cstdint>
#include <limits>
#include <optional>

namespace couchbase::core::protostellar::bucket_admin
{
namespace v1 = ::couchbase::admin::bucket::v1;
namespace mgmt = ::couchbase::core::management::cluster;

[[nodiscard]] inline auto
to_proto_durability(couchbase::durability_level level)
  -> std::optional<::couchbase::kv::v1::DurabilityLevel>
{
  switch (level) {
    case couchbase::durability_level::none:
      return std::nullopt;
    case couchbase::durability_level::majority:
      return ::couchbase::kv::v1::DURABILITY_LEVEL_MAJORITY;
    case couchbase::durability_level::majority_and_persist_to_active:
      return ::couchbase::kv::v1::DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE;
    case couchbase::durability_level::persist_to_majority:
      return ::couchbase::kv::v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY;
  }
  return std::nullopt;
}

// Populate the bucket-settings fields that are mutable on both Create and Update requests.
template<typename Request>
void
apply_settings(const mgmt::bucket_settings& settings, Request& proto)
{
  proto.set_bucket_name(settings.name);
  // ram_quota_mb == 0 means "unset" in the core settings (create defaults it to 100, update omits
  // it); only send an explicit value so an unset quota does not overwrite the server-side value.
  if (settings.ram_quota_mb != 0) {
    proto.set_ram_quota_mb(settings.ram_quota_mb);
  }

  // Omitted when the caller expressed no opinion, so the server applies its own default. Sending an
  // explicit zero instead would create a bucket with no replicas on their behalf.
  if (settings.num_replicas.has_value()) {
    proto.set_num_replicas(*settings.num_replicas);
  }
  if (settings.flush_enabled.has_value()) {
    proto.set_flush_enabled(*settings.flush_enabled);
  }
  if (settings.max_expiry.has_value()) {
    proto.set_max_expiry_secs(*settings.max_expiry);
  }
  if (settings.minimum_durability_level.has_value()) {
    if (auto durability = to_proto_durability(*settings.minimum_durability_level);
        durability.has_value()) {
      proto.set_minimum_durability_level(*durability);
    }
  }
  switch (settings.eviction_policy) {
    case mgmt::bucket_eviction_policy::full:
      proto.set_eviction_mode(v1::EVICTION_MODE_FULL);
      break;
    case mgmt::bucket_eviction_policy::value_only:
      proto.set_eviction_mode(v1::EVICTION_MODE_VALUE_ONLY);
      break;
    case mgmt::bucket_eviction_policy::no_eviction:
      proto.set_eviction_mode(v1::EVICTION_MODE_NONE);
      break;
    case mgmt::bucket_eviction_policy::not_recently_used:
      proto.set_eviction_mode(v1::EVICTION_MODE_NOT_RECENTLY_USED);
      break;
    case mgmt::bucket_eviction_policy::unknown:
      break; // leave server default
  }
  switch (settings.compression_mode) {
    case mgmt::bucket_compression::off:
      proto.set_compression_mode(v1::COMPRESSION_MODE_OFF);
      break;
    case mgmt::bucket_compression::active:
      proto.set_compression_mode(v1::COMPRESSION_MODE_ACTIVE);
      break;
    case mgmt::bucket_compression::passive:
      proto.set_compression_mode(v1::COMPRESSION_MODE_PASSIVE);
      break;
    case mgmt::bucket_compression::unknown:
      break;
  }
  if (settings.history_retention_collection_default.has_value()) {
    proto.set_history_retention_collection_default(*settings.history_retention_collection_default);
  }
  if (settings.history_retention_bytes.has_value()) {
    proto.set_history_retention_bytes(*settings.history_retention_bytes);
  }
  if (settings.history_retention_duration.has_value()) {
    proto.set_history_retention_duration_secs(*settings.history_retention_duration);
  }
}

// Bucket type, replica indexes, storage backend, and conflict resolution are immutable after
// creation, so they are only set on CreateBucketRequest. Returns false when the bucket type has no
// couchbase2 representation (memcached).
[[nodiscard]] inline auto
apply_create_only_settings(const mgmt::bucket_settings& settings, v1::CreateBucketRequest& proto)
  -> bool
{
  switch (settings.bucket_type) {
    case mgmt::bucket_type::couchbase:
      proto.set_bucket_type(v1::BUCKET_TYPE_COUCHBASE);
      break;
    case mgmt::bucket_type::unknown:
      // Leave bucket_type unset so the gateway applies its server-side default rather than
      // silently forcing a couchbase bucket.
      break;
    case mgmt::bucket_type::ephemeral:
      proto.set_bucket_type(v1::BUCKET_TYPE_EPHEMERAL);
      break;
    case mgmt::bucket_type::memcached:
      return false;
  }
  // A bucket must be created with a RAM quota. Core uses ram_quota_mb == 0 to mean "unset" (and
  // apply_settings() omits it in that case), so mirror the classic create path and fall back to the
  // historical 100 MiB default rather than sending a zero quota.
  if (settings.ram_quota_mb == 0) {
    proto.set_ram_quota_mb(100);
  }
  if (settings.replica_indexes.has_value()) {
    proto.set_replica_indexes(*settings.replica_indexes);
  }
  switch (settings.storage_backend) {
    case mgmt::bucket_storage_backend::couchstore:
      proto.set_storage_backend(v1::STORAGE_BACKEND_COUCHSTORE);
      break;
    case mgmt::bucket_storage_backend::magma:
      proto.set_storage_backend(v1::STORAGE_BACKEND_MAGMA);
      break;
    case mgmt::bucket_storage_backend::unknown:
      break;
  }
  switch (settings.conflict_resolution_type) {
    case mgmt::bucket_conflict_resolution::timestamp:
      proto.set_conflict_resolution_type(v1::CONFLICT_RESOLUTION_TYPE_TIMESTAMP);
      break;
    case mgmt::bucket_conflict_resolution::sequence_number:
      proto.set_conflict_resolution_type(v1::CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER);
      break;
    case mgmt::bucket_conflict_resolution::custom:
      proto.set_conflict_resolution_type(v1::CONFLICT_RESOLUTION_TYPE_CUSTOM);
      break;
    case mgmt::bucket_conflict_resolution::unknown:
      break;
  }
  return true;
}

// Decode one ListBucketsResponse bucket back into core settings.
[[nodiscard]] inline auto
decode_bucket(const v1::ListBucketsResponse_Bucket& proto) -> mgmt::bucket_settings
{
  mgmt::bucket_settings settings;
  settings.name = proto.bucket_name();
  settings.ram_quota_mb = proto.ram_quota_mb();
  switch (proto.bucket_type()) {
    case v1::BUCKET_TYPE_EPHEMERAL:
      settings.bucket_type = mgmt::bucket_type::ephemeral;
      break;
    default:
      settings.bucket_type = mgmt::bucket_type::couchbase;
      break;
  }
  settings.num_replicas = proto.num_replicas();
  settings.flush_enabled = proto.flush_enabled();
  settings.replica_indexes = proto.replica_indexes();
  settings.max_expiry = proto.max_expiry_secs();
  switch (proto.eviction_mode()) {
    case v1::EVICTION_MODE_FULL:
      settings.eviction_policy = mgmt::bucket_eviction_policy::full;
      break;
    case v1::EVICTION_MODE_VALUE_ONLY:
      settings.eviction_policy = mgmt::bucket_eviction_policy::value_only;
      break;
    case v1::EVICTION_MODE_NONE:
      settings.eviction_policy = mgmt::bucket_eviction_policy::no_eviction;
      break;
    case v1::EVICTION_MODE_NOT_RECENTLY_USED:
      settings.eviction_policy = mgmt::bucket_eviction_policy::not_recently_used;
      break;
    default:
      break;
  }
  switch (proto.compression_mode()) {
    case v1::COMPRESSION_MODE_OFF:
      settings.compression_mode = mgmt::bucket_compression::off;
      break;
    case v1::COMPRESSION_MODE_ACTIVE:
      settings.compression_mode = mgmt::bucket_compression::active;
      break;
    case v1::COMPRESSION_MODE_PASSIVE:
      settings.compression_mode = mgmt::bucket_compression::passive;
      break;
    default:
      break;
  }
  // storage_backend and minimum_durability_level are `optional` in the response, so the plain value
  // getter cannot tell an explicit couchstore (enum 0) from a field the gateway did not send. Read
  // through the presence bit: absent leaves the core `unknown` / empty optional, which is what the
  // gateway means by omitting it.
  if (proto.has_storage_backend()) {
    switch (proto.storage_backend()) {
      case v1::STORAGE_BACKEND_MAGMA:
        settings.storage_backend = mgmt::bucket_storage_backend::magma;
        break;
      case v1::STORAGE_BACKEND_COUCHSTORE:
        settings.storage_backend = mgmt::bucket_storage_backend::couchstore;
        break;
      default:
        break;
    }
  }
  if (proto.has_minimum_durability_level()) {
    switch (proto.minimum_durability_level()) {
      case ::couchbase::kv::v1::DURABILITY_LEVEL_MAJORITY:
        settings.minimum_durability_level = couchbase::durability_level::majority;
        break;
      case ::couchbase::kv::v1::DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE:
        settings.minimum_durability_level =
          couchbase::durability_level::majority_and_persist_to_active;
        break;
      case ::couchbase::kv::v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY:
        settings.minimum_durability_level = couchbase::durability_level::persist_to_majority;
        break;
      default:
        break;
    }
  }
  // conflict_resolution_type carries no presence bit on this message, unlike the two above, so an
  // unset field is indistinguishable from an explicit timestamp (enum 0) and is read as one.
  switch (proto.conflict_resolution_type()) {
    case v1::CONFLICT_RESOLUTION_TYPE_TIMESTAMP:
      settings.conflict_resolution_type = mgmt::bucket_conflict_resolution::timestamp;
      break;
    case v1::CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER:
      settings.conflict_resolution_type = mgmt::bucket_conflict_resolution::sequence_number;
      break;
    case v1::CONFLICT_RESOLUTION_TYPE_CUSTOM:
      settings.conflict_resolution_type = mgmt::bucket_conflict_resolution::custom;
      break;
    default:
      break;
  }
  if (proto.has_history_retention_collection_default()) {
    settings.history_retention_collection_default = proto.history_retention_collection_default();
  }
  // The proto carries 64 bits and the core setting holds 32. Narrowing a larger value would report
  // a retention size the bucket does not have, and the usual get-modify-update cycle would then
  // send that smaller number back and shrink the setting. Leaving the optional empty is what the
  // encode side omits, so the server keeps its own value.
  if (proto.has_history_retention_bytes() &&
      proto.history_retention_bytes() <= std::numeric_limits<std::uint32_t>::max()) {
    settings.history_retention_bytes = static_cast<std::uint32_t>(proto.history_retention_bytes());
  }
  if (proto.has_history_retention_duration_secs()) {
    settings.history_retention_duration = proto.history_retention_duration_secs();
  }
  return settings;
}

} // namespace couchbase::core::protostellar::bucket_admin
