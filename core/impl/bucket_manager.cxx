/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/cluster.hxx"

#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_drop.hxx"
#include "core/operations/management/bucket_flush.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "core/operations/management/bucket_get_all.hxx"
#include "core/operations/management/bucket_update.hxx"
#include "internal_manager_error_context.hxx"

#include <couchbase/bucket_manager.hxx>

#include <memory>

namespace couchbase
{
namespace
{
template<typename Response>
manager_error_context
build_context(Response& resp)
{
    return manager_error_context(internal_manager_error_context{ resp.ctx.ec,
                                                                 resp.ctx.last_dispatched_to,
                                                                 resp.ctx.last_dispatched_from,
                                                                 resp.ctx.retry_attempts,
                                                                 std::move(resp.ctx.retry_reasons),
                                                                 std::move(resp.ctx.client_context_id),
                                                                 resp.ctx.http_status,
                                                                 std::move(resp.ctx.http_body),
                                                                 std::move(resp.ctx.path) });
}

couchbase::management::cluster::bucket_settings
map_bucket_settings(const couchbase::core::management::cluster::bucket_settings& bucket)
{
    couchbase::management::cluster::bucket_settings bucket_settings{};
    bucket_settings.name = bucket.name;
    bucket_settings.ram_quota_mb = bucket.ram_quota_mb;
    bucket_settings.minimum_durability_level = bucket.minimum_durability_level;
    bucket_settings.history_retention_collection_default = bucket.history_retention_collection_default;
    if (bucket.max_expiry.has_value()) {
        bucket_settings.max_expiry = bucket.max_expiry.value();
    }
    if (bucket.num_replicas.has_value()) {
        bucket_settings.num_replicas = bucket.num_replicas.value();
    }
    if (bucket.replica_indexes.has_value()) {
        bucket_settings.replica_indexes = bucket.replica_indexes.value();
    }
    if (bucket.flush_enabled.has_value()) {
        bucket_settings.flush_enabled = bucket.flush_enabled.value();
    }
    if (bucket.history_retention_bytes.has_value()) {
        bucket_settings.history_retention_bytes = bucket.history_retention_bytes.value();
    }
    if (bucket.history_retention_duration.has_value()) {
        bucket_settings.history_retention_duration = bucket.history_retention_duration.value();
    }
    switch (bucket.conflict_resolution_type) {
        case core::management::cluster::bucket_conflict_resolution::unknown:
            bucket_settings.conflict_resolution_type = management::cluster::bucket_conflict_resolution::unknown;
            break;
        case core::management::cluster::bucket_conflict_resolution::timestamp:
            bucket_settings.conflict_resolution_type = management::cluster::bucket_conflict_resolution::timestamp;
            break;
        case core::management::cluster::bucket_conflict_resolution::sequence_number:
            bucket_settings.conflict_resolution_type = management::cluster::bucket_conflict_resolution::sequence_number;
            break;
        case core::management::cluster::bucket_conflict_resolution::custom:
            bucket_settings.conflict_resolution_type = management::cluster::bucket_conflict_resolution::custom;
            break;
    }
    switch (bucket.eviction_policy) {
        case core::management::cluster::bucket_eviction_policy::unknown:
            bucket_settings.eviction_policy = management::cluster::bucket_eviction_policy::unknown;
            break;
        case core::management::cluster::bucket_eviction_policy::full:
            bucket_settings.eviction_policy = management::cluster::bucket_eviction_policy::full;
            break;
        case core::management::cluster::bucket_eviction_policy::value_only:
            bucket_settings.eviction_policy = management::cluster::bucket_eviction_policy::value_only;
            break;
        case core::management::cluster::bucket_eviction_policy::no_eviction:
            bucket_settings.eviction_policy = management::cluster::bucket_eviction_policy::no_eviction;
            break;
        case core::management::cluster::bucket_eviction_policy::not_recently_used:
            bucket_settings.eviction_policy = management::cluster::bucket_eviction_policy::not_recently_used;
            break;
    }
    switch (bucket.compression_mode) {
        case core::management::cluster::bucket_compression::unknown:
            bucket_settings.compression_mode = management::cluster::bucket_compression::unknown;
            break;
        case core::management::cluster::bucket_compression::off:
            bucket_settings.compression_mode = management::cluster::bucket_compression::off;
            break;
        case core::management::cluster::bucket_compression::active:
            bucket_settings.compression_mode = management::cluster::bucket_compression::active;
            break;
        case core::management::cluster::bucket_compression::passive:
            bucket_settings.compression_mode = management::cluster::bucket_compression::passive;
            break;
    }
    switch (bucket.bucket_type) {
        case core::management::cluster::bucket_type::unknown:
            bucket_settings.bucket_type = management::cluster::bucket_type::unknown;
            break;
        case core::management::cluster::bucket_type::couchbase:
            bucket_settings.bucket_type = management::cluster::bucket_type::couchbase;
            break;
        case core::management::cluster::bucket_type::memcached:
            bucket_settings.bucket_type = management::cluster::bucket_type::memcached;
            break;
        case core::management::cluster::bucket_type::ephemeral:
            bucket_settings.bucket_type = management::cluster::bucket_type::ephemeral;
            break;
    }
    switch (bucket.storage_backend) {
        case core::management::cluster::bucket_storage_backend::unknown:
            bucket_settings.storage_backend = management::cluster::bucket_storage_backend::unknown;
            break;
        case core::management::cluster::bucket_storage_backend::couchstore:
            bucket_settings.storage_backend = management::cluster::bucket_storage_backend::couchstore;
            break;
        case core::management::cluster::bucket_storage_backend::magma:
            bucket_settings.storage_backend = management::cluster::bucket_storage_backend::magma;
            break;
    }
    return bucket_settings;
}

std::vector<couchbase::management::cluster::bucket_settings>
map_all_bucket_settings(const std::vector<couchbase::core::management::cluster::bucket_settings>& buckets)
{
    std::vector<couchbase::management::cluster::bucket_settings> buckets_settings{};
    for (const auto& bucket : buckets) {
        auto converted_bucket = map_bucket_settings(bucket);
        buckets_settings.emplace_back(converted_bucket);
    }
    return buckets_settings;
}

couchbase::core::management::cluster::bucket_settings
map_bucket_settings(const couchbase::management::cluster::bucket_settings& bucket)
{

    couchbase::core::management::cluster::bucket_settings bucket_settings{};

    bucket_settings.name = bucket.name;
    bucket_settings.ram_quota_mb = bucket.ram_quota_mb;
    bucket_settings.max_expiry = bucket.max_expiry;
    bucket_settings.minimum_durability_level = bucket.minimum_durability_level;
    bucket_settings.num_replicas = bucket.num_replicas;
    bucket_settings.replica_indexes = bucket.replica_indexes;
    bucket_settings.flush_enabled = bucket.flush_enabled;
    bucket_settings.history_retention_collection_default = bucket.history_retention_collection_default;
    bucket_settings.history_retention_bytes = bucket.history_retention_bytes;
    bucket_settings.history_retention_duration = bucket.history_retention_duration;
    switch (bucket.conflict_resolution_type) {
        case management::cluster::bucket_conflict_resolution::unknown:
            bucket_settings.conflict_resolution_type = core::management::cluster::bucket_conflict_resolution::unknown;
            break;
        case management::cluster::bucket_conflict_resolution::timestamp:
            bucket_settings.conflict_resolution_type = core::management::cluster::bucket_conflict_resolution::timestamp;
            break;
        case management::cluster::bucket_conflict_resolution::sequence_number:
            bucket_settings.conflict_resolution_type = core::management::cluster::bucket_conflict_resolution::sequence_number;
            break;
        case management::cluster::bucket_conflict_resolution::custom:
            bucket_settings.conflict_resolution_type = core::management::cluster::bucket_conflict_resolution::custom;
            break;
    }
    switch (bucket.eviction_policy) {
        case management::cluster::bucket_eviction_policy::unknown:
            bucket_settings.eviction_policy = core::management::cluster::bucket_eviction_policy::unknown;
            break;
        case management::cluster::bucket_eviction_policy::full:
            bucket_settings.eviction_policy = core::management::cluster::bucket_eviction_policy::full;
            break;
        case management::cluster::bucket_eviction_policy::value_only:
            bucket_settings.eviction_policy = core::management::cluster::bucket_eviction_policy::value_only;
            break;
        case management::cluster::bucket_eviction_policy::no_eviction:
            bucket_settings.eviction_policy = core::management::cluster::bucket_eviction_policy::no_eviction;
            break;
        case management::cluster::bucket_eviction_policy::not_recently_used:
            bucket_settings.eviction_policy = core::management::cluster::bucket_eviction_policy::not_recently_used;
            break;
    }
    switch (bucket.compression_mode) {
        case management::cluster::bucket_compression::unknown:
            bucket_settings.compression_mode = core::management::cluster::bucket_compression::unknown;
            break;
        case management::cluster::bucket_compression::active:
            bucket_settings.compression_mode = core::management::cluster::bucket_compression::active;
            break;
        case management::cluster::bucket_compression::passive:
            bucket_settings.compression_mode = core::management::cluster::bucket_compression::passive;
            break;
        case management::cluster::bucket_compression::off:
            bucket_settings.compression_mode = core::management::cluster::bucket_compression::off;
            break;
    }
    switch (bucket.bucket_type) {
        case management::cluster::bucket_type::unknown:
            bucket_settings.bucket_type = core::management::cluster::bucket_type::unknown;
            break;
        case management::cluster::bucket_type::couchbase:
            bucket_settings.bucket_type = core::management::cluster::bucket_type::couchbase;
            break;
        case management::cluster::bucket_type::memcached:
            bucket_settings.bucket_type = core::management::cluster::bucket_type::memcached;
            break;
        case management::cluster::bucket_type::ephemeral:
            bucket_settings.bucket_type = core::management::cluster::bucket_type::ephemeral;
            break;
    }
    switch (bucket.storage_backend) {
        case management::cluster::bucket_storage_backend::unknown:
            bucket_settings.storage_backend = core::management::cluster::bucket_storage_backend::unknown;
            break;
        case management::cluster::bucket_storage_backend::magma:
            bucket_settings.storage_backend = core::management::cluster::bucket_storage_backend::magma;
            break;
        case management::cluster::bucket_storage_backend::couchstore:
            bucket_settings.storage_backend = core::management::cluster::bucket_storage_backend::couchstore;
            break;
    }
    return bucket_settings;
}
} // namespace

class bucket_manager_impl
{
  public:
    explicit bucket_manager_impl(core::cluster core)
      : core_{ std::move(core) }
    {
    }

    void get_bucket(std::string bucket_name, const get_bucket_options::built& options, get_bucket_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_get_request{
            std::move(bucket_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp), map_bucket_settings(resp.bucket)); });
    }

    void get_all_buckets(const get_all_buckets_options::built& options, get_all_buckets_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_get_all_request{
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable {
              return handler(build_context(resp), map_all_bucket_settings(resp.buckets));
          });
    }

    void create_bucket(const management::cluster::bucket_settings& bucket_settings,
                       const create_bucket_options::built& options,
                       create_bucket_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_create_request{
            map_bucket_settings(bucket_settings),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void update_bucket(const management::cluster::bucket_settings& bucket_settings,
                       const update_bucket_options::built& options,
                       update_bucket_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_update_request{
            map_bucket_settings(bucket_settings),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void drop_bucket(std::string bucket_name, const drop_bucket_options::built& options, drop_bucket_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_drop_request{
            std::move(bucket_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

    void flush_bucket(std::string bucket_name, const flush_bucket_options::built& options, flush_bucket_handler&& handler) const
    {
        core_.execute(
          core::operations::management::bucket_flush_request{
            std::move(bucket_name),
            {},
            options.timeout,
          },
          [handler = std::move(handler)](auto resp) mutable { return handler(build_context(resp)); });
    }

  private:
    core::cluster core_;
};

bucket_manager::bucket_manager(core::cluster core)
  : impl_(std::make_shared<bucket_manager_impl>(std::move(core)))
{
}

void
bucket_manager::get_bucket(std::string bucket_name, const get_bucket_options& options, get_bucket_handler&& handler) const
{
    return impl_->get_bucket(std::move(bucket_name), options.build(), std::move(handler));
}

auto
bucket_manager::get_bucket(std::string bucket_name, const get_bucket_options& options) const
  -> std::future<std::pair<manager_error_context, management::cluster::bucket_settings>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, management::cluster::bucket_settings>>>();
    get_bucket(std::move(bucket_name), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

void
bucket_manager::get_all_buckets(const get_all_buckets_options& options, get_all_buckets_handler&& handler) const
{
    return impl_->get_all_buckets(options.build(), std::move(handler));
}

auto
bucket_manager::get_all_buckets(const get_all_buckets_options& options) const
  -> std::future<std::pair<manager_error_context, std::vector<management::cluster::bucket_settings>>>
{
    auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<management::cluster::bucket_settings>>>>();
    get_all_buckets(options,
                    [barrier](auto ctx, auto result) mutable { barrier->set_value(std::make_pair(std::move(ctx), std::move(result))); });
    return barrier->get_future();
}

void
bucket_manager::create_bucket(const management::cluster::bucket_settings& bucket_settings,
                              const create_bucket_options& options,
                              create_bucket_handler&& handler) const
{
    return impl_->create_bucket(bucket_settings, options.build(), std::move(handler));
}

auto
bucket_manager::create_bucket(const management::cluster::bucket_settings& bucket_settings, const create_bucket_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    create_bucket(bucket_settings, options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
bucket_manager::update_bucket(const management::cluster::bucket_settings& bucket_settings,
                              const update_bucket_options& options,
                              update_bucket_handler&& handler) const
{
    return impl_->update_bucket(bucket_settings, options.build(), std::move(handler));
}

auto
bucket_manager::update_bucket(const management::cluster::bucket_settings& bucket_settings, const update_bucket_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    update_bucket(bucket_settings, options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
bucket_manager::drop_bucket(std::string bucket_name, const drop_bucket_options& options, drop_bucket_handler&& handler) const
{
    return impl_->drop_bucket(std::move(bucket_name), options.build(), std::move(handler));
}

auto
bucket_manager::drop_bucket(std::string bucket_name, const drop_bucket_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    drop_bucket(std::move(bucket_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}

void
bucket_manager::flush_bucket(std::string bucket_name, const flush_bucket_options& options, flush_bucket_handler&& handler) const
{
    return impl_->flush_bucket(std::move(bucket_name), options.build(), std::move(handler));
}

auto
bucket_manager::flush_bucket(std::string bucket_name, const flush_bucket_options& options) const -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    flush_bucket(std::move(bucket_name), options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
