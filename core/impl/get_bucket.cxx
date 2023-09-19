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

#include <couchbase/manager_error_context.hxx>
#include <utility>

#include "core/cluster.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "couchbase/bucket_manager.hxx"

namespace couchbase
{
template<typename Response>
static manager_error_context
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

static core::operations::management::bucket_get_request
build_get_bucket_request(std::string bucket_name, const get_bucket_options::built& options)
{
    core::operations::management::bucket_get_request request{ std::move(bucket_name), {}, options.timeout };
    return request;
}

static couchbase::management::cluster::bucket_settings
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

void
bucket_manager::get_bucket(std::string bucket_name, const get_bucket_options& options, get_bucket_handler&& handler) const
{
    auto request = build_get_bucket_request(std::move(bucket_name), options.build());

    core_->execute(std::move(request), [handler = std::move(handler)](core::operations::management::bucket_get_response resp) mutable {
        return handler(build_context(resp), map_bucket_settings(resp.bucket));
    });
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
} // namespace couchbase
