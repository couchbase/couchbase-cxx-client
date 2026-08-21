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

// Unit tests for the bucket-admin <-> couchbase.admin.bucket.v1 converter (CXXCBC-900). Pure,
// no server.

#include "framework/test_registry.hxx"

#include "core/protostellar/bucket_admin_converter.hxx"

#include <cstdint>
#include <string>

namespace couchbase::test
{
namespace
{
namespace ba = ::couchbase::core::protostellar::bucket_admin;
namespace mgmt = ::couchbase::core::management::cluster;
namespace v1 = ::couchbase::admin::bucket::v1;

void
apply_settings_maps_fields([[maybe_unused]] context& ctx)
{
  mgmt::bucket_settings settings;
  settings.name = "b";
  settings.ram_quota_mb = 256;
  settings.bucket_type = mgmt::bucket_type::couchbase;
  settings.num_replicas = 2;
  settings.flush_enabled = true;
  settings.max_expiry = 3600;
  settings.eviction_policy = mgmt::bucket_eviction_policy::full;
  settings.compression_mode = mgmt::bucket_compression::active;

  v1::CreateBucketRequest proto;
  ba::apply_settings(settings, proto);
  assert_true(ba::apply_create_only_settings(settings, proto), "couchbase bucket encodes");
  assert_eq(proto.bucket_name(), std::string{ "b" }, "name mapped");
  assert_eq(proto.ram_quota_mb(), std::uint64_t{ 256 }, "ram quota mapped");
  assert_true(proto.bucket_type() == v1::BUCKET_TYPE_COUCHBASE, "bucket type mapped");
  assert_eq(proto.num_replicas(), std::uint32_t{ 2 }, "num_replicas mapped");
  assert_true(proto.flush_enabled(), "flush_enabled mapped");
  assert_eq(proto.max_expiry_secs(), std::uint32_t{ 3600 }, "max_expiry mapped");
  assert_true(proto.eviction_mode() == v1::EVICTION_MODE_FULL, "eviction mapped");
  assert_true(proto.compression_mode() == v1::COMPRESSION_MODE_ACTIVE, "compression mapped");
}

void
apply_settings_omits_unset_ram_quota([[maybe_unused]] context& ctx)
{
  mgmt::bucket_settings settings;
  settings.name = "b";
  settings.ram_quota_mb = 0; // 0 means "unset" in core; must not be sent to the gateway
  settings.bucket_type = mgmt::bucket_type::couchbase;

  v1::CreateBucketRequest proto;
  ba::apply_settings(settings, proto);
  assert_false(proto.has_ram_quota_mb(), "apply_settings does not send an unset (0) ram quota");

  // Create still needs a quota: the create-only path backfills the historical 100 MiB default.
  assert_true(ba::apply_create_only_settings(settings, proto), "couchbase bucket encodes");
  assert_eq(proto.ram_quota_mb(), std::uint64_t{ 100 }, "create backfills the default ram quota");
}

void
apply_settings_rejects_memcached([[maybe_unused]] context& ctx)
{
  mgmt::bucket_settings settings;
  settings.name = "legacy";
  settings.bucket_type = mgmt::bucket_type::memcached;
  v1::CreateBucketRequest proto;
  ba::apply_settings(settings, proto);
  assert_false(ba::apply_create_only_settings(settings, proto),
               "memcached bucket has no couchbase2 mapping");
}

void
decode_bucket_maps_fields([[maybe_unused]] context& ctx)
{
  v1::ListBucketsResponse_Bucket proto;
  proto.set_bucket_name("travel");
  proto.set_ram_quota_mb(512);
  proto.set_bucket_type(v1::BUCKET_TYPE_EPHEMERAL);
  proto.set_num_replicas(1);
  proto.set_flush_enabled(false);
  proto.set_storage_backend(v1::STORAGE_BACKEND_MAGMA);

  const auto settings = ba::decode_bucket(proto);
  assert_eq(settings.name, std::string{ "travel" }, "name decoded");
  assert_eq(settings.ram_quota_mb, std::uint64_t{ 512 }, "ram quota decoded");
  assert_true(settings.bucket_type == mgmt::bucket_type::ephemeral, "ephemeral decoded");
  assert_true(settings.num_replicas.has_value() && settings.num_replicas.value() == 1U,
              "num_replicas decoded");
  assert_true(settings.flush_enabled.has_value() && !settings.flush_enabled.value(),
              "flush_enabled decoded");
  assert_true(settings.storage_backend == mgmt::bucket_storage_backend::magma,
              "storage backend decoded");
}

// Each of these settings is one the encode side writes, so a decode that skips it makes
// get_bucket -> modify -> update_bucket send a default back and reset the bucket.
void
decode_bucket_reads_the_settings_update_would_resend([[maybe_unused]] context& ctx)
{
  v1::ListBucketsResponse_Bucket proto;
  proto.set_bucket_name("magma");
  proto.set_storage_backend(v1::STORAGE_BACKEND_MAGMA);
  proto.set_minimum_durability_level(::couchbase::kv::v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY);
  proto.set_conflict_resolution_type(v1::CONFLICT_RESOLUTION_TYPE_SEQUENCE_NUMBER);
  proto.set_history_retention_bytes(3221225472ULL); // 3 GiB — fits uint32, exceeds int32

  const auto settings = ba::decode_bucket(proto);
  assert_true(settings.storage_backend == mgmt::bucket_storage_backend::magma,
              "magma does not decode as couchstore");
  assert_true(settings.minimum_durability_level.has_value() &&
                settings.minimum_durability_level.value() ==
                  couchbase::durability_level::persist_to_majority,
              "minimum durability decoded");
  assert_true(settings.conflict_resolution_type ==
                mgmt::bucket_conflict_resolution::sequence_number,
              "conflict resolution decoded");
  assert_true(settings.history_retention_bytes.has_value() &&
                settings.history_retention_bytes.value() == 3221225472U,
              "history retention decoded without truncation");
}

void
decode_bucket_leaves_absent_optional_settings_unset([[maybe_unused]] context& ctx)
{
  v1::ListBucketsResponse_Bucket proto;
  proto.set_bucket_name("b");

  const auto settings = ba::decode_bucket(proto);
  assert_true(settings.storage_backend == mgmt::bucket_storage_backend::unknown,
              "an absent storage_backend is not reported as couchstore");
  assert_false(settings.minimum_durability_level.has_value(), "absent durability stays unset");
  assert_false(settings.history_retention_bytes.has_value(), "absent retention stays unset");
  // conflict_resolution_type is the one field with no presence bit, so an absent field and an
  // explicit timestamp are the same message on the wire and decode alike.
  assert_true(settings.conflict_resolution_type == mgmt::bucket_conflict_resolution::timestamp,
              "an absent conflict resolution decodes as the proto's zero value");
}

void
decode_bucket_drops_a_history_retention_the_core_field_cannot_hold([[maybe_unused]] context& ctx)
{
  v1::ListBucketsResponse_Bucket proto;
  proto.set_bucket_name("b");
  proto.set_history_retention_bytes(std::uint64_t{ 1 } << 33U); // 8 GiB — beyond uint32

  const auto settings = ba::decode_bucket(proto);
  // Truncating would report 0 bytes and the next update would send that back, shrinking the
  // retention. An empty optional is omitted on update, so the server keeps the value it has.
  assert_false(settings.history_retention_bytes.has_value(),
               "a retention size beyond the core field is dropped rather than wrapped");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(apply_settings_maps_fields) },
      { CASE(apply_settings_omits_unset_ram_quota) },
      { CASE(apply_settings_rejects_memcached) },
      { CASE(decode_bucket_maps_fields) },
      { CASE(decode_bucket_reads_the_settings_update_would_resend) },
      { CASE(decode_bucket_leaves_absent_optional_settings_unset) },
      { CASE(decode_bucket_drops_a_history_retention_the_core_field_cannot_hold) },
    },
  };
}

} // namespace couchbase::test
