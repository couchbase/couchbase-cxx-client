/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "bucket_management.hxx"
#include "../exceptions.hxx"
#include "common.hxx"

#include "core/meta/features.hxx"

namespace fit_cxx::commands::bucket_management
{
namespace
{
couchbase::create_bucket_options
to_create_bucket_options(const protocol::sdk::cluster::bucket_manager::CreateBucketRequest& cmd,
                         observability::span_owner* spans)
{
  couchbase::create_bucket_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::update_bucket_options
to_update_bucket_options(const protocol::sdk::cluster::bucket_manager::UpdateBucketRequest& cmd,
                         observability::span_owner* spans)
{
  couchbase::update_bucket_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::drop_bucket_options
to_drop_bucket_options(const protocol::sdk::cluster::bucket_manager::DropBucketRequest& cmd,
                       observability::span_owner* spans)
{
  couchbase::drop_bucket_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_bucket_options
to_get_bucket_options(const protocol::sdk::cluster::bucket_manager::GetBucketRequest& cmd,
                      observability::span_owner* spans)
{
  couchbase::get_bucket_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_all_buckets_options
to_get_all_buckets_options(const protocol::sdk::cluster::bucket_manager::GetAllBucketsRequest& cmd,
                           observability::span_owner* spans)
{
  couchbase::get_all_buckets_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::flush_bucket_options
to_flush_bucket_options(const protocol::sdk::cluster::bucket_manager::FlushBucketRequest& cmd,
                        observability::span_owner* spans)
{
  couchbase::flush_bucket_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::management::cluster::bucket_type
to_bucket_type(const protocol::sdk::cluster::bucket_manager::BucketType& type)
{
  if (type == protocol::sdk::cluster::bucket_manager::BucketType::COUCHBASE) {
    return couchbase::management::cluster::bucket_type::couchbase;
  }
  if (type == protocol::sdk::cluster::bucket_manager::EPHEMERAL) {
    return couchbase::management::cluster::bucket_type::ephemeral;
  }
  if (type == protocol::sdk::cluster::bucket_manager::MEMCACHED) {
    return couchbase::management::cluster::bucket_type::memcached;
  }
  return couchbase::management::cluster::bucket_type::unknown;
}

protocol::sdk::cluster::bucket_manager::BucketType
from_bucket_type(const couchbase::management::cluster::bucket_type& type)
{
  if (type == couchbase::management::cluster::bucket_type::couchbase) {
    return protocol::sdk::cluster::bucket_manager::COUCHBASE;
  }
  if (type == couchbase::management::cluster::bucket_type::memcached) {
    return protocol::sdk::cluster::bucket_manager::MEMCACHED;
  }
  if (type == couchbase::management::cluster::bucket_type::ephemeral) {
    return protocol::sdk::cluster::bucket_manager::EPHEMERAL;
  }
  throw performer_exception::internal("Unexpected bucket type from SDK");
}

couchbase::management::cluster::bucket_eviction_policy
to_eviction_policy(const protocol::sdk::cluster::bucket_manager::EvictionPolicyType& type)
{
  if (type == protocol::sdk::cluster::bucket_manager::FULL) {
    return couchbase::management::cluster::bucket_eviction_policy::full;
  }
  if (type == protocol::sdk::cluster::bucket_manager::NO_EVICTION) {
    return couchbase::management::cluster::bucket_eviction_policy::no_eviction;
  }
  if (type == protocol::sdk::cluster::bucket_manager::NOT_RECENTLY_USED) {
    return couchbase::management::cluster::bucket_eviction_policy::not_recently_used;
  }
  if (type == protocol::sdk::cluster::bucket_manager::VALUE_ONLY) {
    return couchbase::management::cluster::bucket_eviction_policy::value_only;
  }
  return couchbase::management::cluster::bucket_eviction_policy::unknown;
}

protocol::sdk::cluster::bucket_manager::EvictionPolicyType
from_eviction_policy(const couchbase::management::cluster::bucket_eviction_policy& type)
{
  if (type == couchbase::management::cluster::bucket_eviction_policy::full) {
    return protocol::sdk::cluster::bucket_manager::FULL;
  }
  if (type == couchbase::management::cluster::bucket_eviction_policy::no_eviction) {
    return protocol::sdk::cluster::bucket_manager::NO_EVICTION;
  }
  if (type == couchbase::management::cluster::bucket_eviction_policy::not_recently_used) {
    return protocol::sdk::cluster::bucket_manager::NOT_RECENTLY_USED;
  }
  if (type == couchbase::management::cluster::bucket_eviction_policy::value_only) {
    return protocol::sdk::cluster::bucket_manager::VALUE_ONLY;
  }
  throw performer_exception::internal("Unexpected eviction policy from SDK");
}

couchbase::management::cluster::bucket_compression
to_compression_mode(const protocol::sdk::cluster::bucket_manager::CompressionMode& mode)
{
  if (mode == protocol::sdk::cluster::bucket_manager::ACTIVE) {
    return couchbase::management::cluster::bucket_compression::active;
  }
  if (mode == protocol::sdk::cluster::bucket_manager::OFF) {
    return couchbase::management::cluster::bucket_compression::off;
  }
  if (mode == protocol::sdk::cluster::bucket_manager::PASSIVE) {
    return couchbase::management::cluster::bucket_compression::passive;
  }
  return couchbase::management::cluster::bucket_compression::unknown;
}

protocol::sdk::cluster::bucket_manager::CompressionMode
from_compression_mode(const couchbase::management::cluster::bucket_compression& mode)
{
  if (mode == couchbase::management::cluster::bucket_compression::active) {
    return protocol::sdk::cluster::bucket_manager::ACTIVE;
  }
  if (mode == couchbase::management::cluster::bucket_compression::off) {
    return protocol::sdk::cluster::bucket_manager::OFF;
  }
  if (mode == couchbase::management::cluster::bucket_compression::passive) {
    return protocol::sdk::cluster::bucket_manager::PASSIVE;
  }
  throw performer_exception::internal("Unexpected compression mode from SDK");
}

couchbase::management::cluster::bucket_storage_backend
to_storage_backend(const protocol::sdk::cluster::bucket_manager::StorageBackend& backend)
{
  if (backend == protocol::sdk::cluster::bucket_manager::COUCHSTORE) {
    return couchbase::management::cluster::bucket_storage_backend::couchstore;
  }
  if (backend == protocol::sdk::cluster::bucket_manager::MAGMA) {
    return couchbase::management::cluster::bucket_storage_backend::magma;
  }
  return couchbase::management::cluster::bucket_storage_backend::unknown;
}

protocol::sdk::cluster::bucket_manager::StorageBackend
from_storage_backend(const couchbase::management::cluster::bucket_storage_backend& backend)
{
  if (backend == couchbase::management::cluster::bucket_storage_backend::couchstore) {
    return protocol::sdk::cluster::bucket_manager::COUCHSTORE;
  }
  if (backend == couchbase::management::cluster::bucket_storage_backend::magma) {
    return protocol::sdk::cluster::bucket_manager::MAGMA;
  }
  throw performer_exception::internal("Unexpected storage backend from SDK");
}

couchbase::management::cluster::bucket_conflict_resolution
to_conflict_resolution_type(
  const protocol::sdk::cluster::bucket_manager::ConflictResolutionType& type)
{
  if (type == protocol::sdk::cluster::bucket_manager::TIMESTAMP) {
    return couchbase::management::cluster::bucket_conflict_resolution::timestamp;
  }
  if (type == protocol::sdk::cluster::bucket_manager::SEQUENCE_NUMBER) {
    return couchbase::management::cluster::bucket_conflict_resolution::sequence_number;
  }
  if (type == protocol::sdk::cluster::bucket_manager::CUSTOM) {
    return couchbase::management::cluster::bucket_conflict_resolution::custom;
  }
  return couchbase::management::cluster::bucket_conflict_resolution::unknown;
}

couchbase::management::cluster::bucket_settings
to_bucket_settings(const protocol::sdk::cluster::bucket_manager::BucketSettings& settings)
{
  couchbase::management::cluster::bucket_settings bucket_settings{};
  bucket_settings.name = settings.name();
  bucket_settings.ram_quota_mb = static_cast<std::uint64_t>(settings.ram_quota_mb());
  if (settings.has_flush_enabled()) {
    bucket_settings.flush_enabled = settings.flush_enabled();
  }
  if (settings.has_num_replicas()) {
    bucket_settings.num_replicas = settings.num_replicas();
  }
  if (settings.has_replica_indexes()) {
    bucket_settings.replica_indexes = settings.replica_indexes();
  }
  if (settings.has_bucket_type()) {
    bucket_settings.bucket_type = to_bucket_type(settings.bucket_type());
  }
  if (settings.has_eviction_policy()) {
    bucket_settings.eviction_policy = to_eviction_policy(settings.eviction_policy());
  }
  if (settings.has_max_expiry_seconds()) {
    bucket_settings.max_expiry = settings.max_expiry_seconds();
  }
  if (settings.has_compression_mode()) {
    bucket_settings.compression_mode = to_compression_mode(settings.compression_mode());
  }
  if (settings.has_minimum_durability_level()) {
    bucket_settings.minimum_durability_level =
      common::to_durability_level(settings.minimum_durability_level());
  }
  if (settings.has_storage_backend()) {
    bucket_settings.storage_backend = to_storage_backend(settings.storage_backend());
  }
  if (settings.has_history_retention_collection_default()) {
    bucket_settings.history_retention_collection_default =
      settings.history_retention_collection_default();
  }
  if (settings.has_history_retention_seconds()) {
    bucket_settings.history_retention_duration = settings.history_retention_seconds();
  }
  if (settings.has_history_retention_bytes()) {
    bucket_settings.history_retention_bytes = settings.history_retention_bytes();
  }
#ifdef COUCHBASE_CXX_CLIENT_HAS_BUCKET_SETTINGS_NUM_VBUCKETS
  if (settings.has_num_vbuckets()) {
    bucket_settings.num_vbuckets = settings.num_vbuckets();
  }
#endif
  return bucket_settings;
}

couchbase::management::cluster::bucket_settings
to_create_bucket_settings(const protocol::sdk::cluster::bucket_manager::CreateBucketRequest& cmd)
{
  auto settings = cmd.settings();
  auto bucket_settings = to_bucket_settings(settings.settings());
  if (settings.has_conflict_resolution_type()) {
    bucket_settings.conflict_resolution_type =
      to_conflict_resolution_type(settings.conflict_resolution_type());
  }
  return bucket_settings;
}

void
from_bucket_settings(const couchbase::management::cluster::bucket_settings& result,
                     protocol::sdk::cluster::bucket_manager::BucketSettings* res)
{
  res->set_name(result.name);
  res->set_ram_quota_mb(static_cast<std::int64_t>(result.ram_quota_mb));

  if (result.bucket_type != couchbase::management::cluster::bucket_type::unknown) {
    res->set_bucket_type(from_bucket_type(result.bucket_type));
  }
  if (result.eviction_policy != couchbase::management::cluster::bucket_eviction_policy::unknown) {
    res->set_eviction_policy(from_eviction_policy(result.eviction_policy));
  }
  if (result.compression_mode != couchbase::management::cluster::bucket_compression::unknown) {
    res->set_compression_mode(from_compression_mode(result.compression_mode));
  }
  if (result.storage_backend != couchbase::management::cluster::bucket_storage_backend::unknown) {
    res->set_storage_backend(from_storage_backend(result.storage_backend));
  }
  if (result.flush_enabled.has_value()) {
    res->set_flush_enabled(result.flush_enabled.value());
  }
  if (result.num_replicas.has_value()) {
    res->set_num_replicas(static_cast<std::int32_t>(result.num_replicas.value()));
  }
  if (result.replica_indexes.has_value()) {
    res->set_replica_indexes(result.replica_indexes.value());
  }
  if (result.max_expiry.has_value()) {
    res->set_max_expiry_seconds(static_cast<std::int32_t>(result.max_expiry.value()));
  }
  if (result.minimum_durability_level.has_value()) {
    res->set_minimum_durability_level(
      common::from_durability_level(result.minimum_durability_level.value()));
  }
  if (result.history_retention_collection_default.has_value()) {
    res->set_history_retention_collection_default(
      result.history_retention_collection_default.value());
  }
  if (result.history_retention_bytes.has_value()) {
    res->set_history_retention_bytes(result.history_retention_bytes.value());
  }
  if (result.history_retention_duration.has_value()) {
    res->set_history_retention_seconds(result.history_retention_duration.value());
  }
#ifdef COUCHBASE_CXX_CLIENT_HAS_BUCKET_SETTINGS_NUM_VBUCKETS
  if (result.num_vbuckets.has_value()) {
    res->set_num_vbuckets(result.num_vbuckets.value());
  }
#endif
}

void
from_get_all_buckets_result(
  const std::vector<couchbase::management::cluster::bucket_settings>& result,
  protocol::sdk::cluster::bucket_manager::GetAllBucketsResult* res)
{
  for (const auto& settings : result) {
    protocol::sdk::cluster::bucket_manager::BucketSettings proto_settings;
    from_bucket_settings(settings, &proto_settings);
    (*res->mutable_result())[settings.name] = proto_settings;
  }
}
} // namespace

protocol::run::Result
execute_command(const protocol::sdk::cluster::bucket_manager::Command& cmd,
                const command_args& args)
{
  protocol::run::Result res;
  auto manager = args.cluster->buckets();
  if (cmd.has_get_bucket()) {
    const auto& get_bucket = cmd.get_bucket();
    const auto opts = to_get_bucket_options(get_bucket, args.spans);
    auto [err, result] = manager.get_bucket(get_bucket.bucket_name(), opts).get();
    if (err) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_bucket_settings(
          result, res.mutable_sdk()->mutable_bucket_manager_result()->mutable_bucket_settings());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else if (cmd.has_get_all_buckets()) {
    const auto& get_all_buckets = cmd.get_all_buckets();
    const auto opts = to_get_all_buckets_options(get_all_buckets, args.spans);
    auto [err, result] = manager.get_all_buckets(opts).get();
    if (err) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_get_all_buckets_result(
          result,
          res.mutable_sdk()->mutable_bucket_manager_result()->mutable_get_all_buckets_result());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else if (cmd.has_create_bucket()) {
    const auto& create_bucket = cmd.create_bucket();
    const auto opts = to_create_bucket_options(create_bucket, args.spans);
    const auto bucket = to_create_bucket_settings(create_bucket);
    if (auto err = manager.create_bucket(bucket, opts).get()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_update_bucket()) {
    const auto& update_bucket = cmd.update_bucket();
    const auto opts = to_update_bucket_options(update_bucket, args.spans);
    const auto bucket = to_bucket_settings(update_bucket.settings());
    if (auto err = manager.update_bucket(bucket, opts).get()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_drop_bucket()) {
    const auto& drop_bucket = cmd.drop_bucket();
    const auto opts = to_drop_bucket_options(drop_bucket, args.spans);
    if (auto err = manager.drop_bucket(drop_bucket.bucket_name(), opts).get()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_flush_bucket()) {
    const auto& flush_bucket = cmd.flush_bucket();
    const auto opts = to_flush_bucket_options(flush_bucket, args.spans);
    if (auto err = manager.flush_bucket(flush_bucket.bucket_name(), opts).get()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  }
  return res;
}
} // namespace fit_cxx::commands::bucket_management
