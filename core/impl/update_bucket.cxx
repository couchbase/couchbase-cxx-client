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
#include "core/operations/management/bucket_update.hxx"
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

static core::operations::management::bucket_update_request
build_update_bucket_request(couchbase::core::management::cluster::bucket_settings bucket_settings,
                            const update_bucket_options::built& options)
{
    core::operations::management::bucket_update_request request{ std::move(bucket_settings), {}, options.timeout };
    return request;
}

static couchbase::core::management::cluster::bucket_settings
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
    return bucket_settings;
}

void
bucket_manager::update_bucket(const management::cluster::bucket_settings& bucket_settings,
                              const update_bucket_options& options,
                              update_bucket_handler&& handler) const
{
    auto request = build_update_bucket_request(map_bucket_settings(bucket_settings), options.build());

    core_->execute(std::move(request), [handler = std::move(handler)](core::operations::management::bucket_update_response resp) mutable {
        return handler(build_context(resp));
    });
}

auto
bucket_manager::update_bucket(const management::cluster::bucket_settings& bucket_settings, const update_bucket_options& options) const
  -> std::future<manager_error_context>
{
    auto barrier = std::make_shared<std::promise<manager_error_context>>();
    update_bucket(bucket_settings, options, [barrier](auto ctx) mutable { barrier->set_value(std::move(ctx)); });
    return barrier->get_future();
}
} // namespace couchbase
