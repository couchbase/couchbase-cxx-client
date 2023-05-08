/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "test_helper_integration.hxx"

#include <catch2/catch_approx.hpp>

#include "core/management/analytics_link.hxx"
#include "core/operations/management/analytics.hxx"
#include "core/operations/management/bucket.hxx"
#include "core/operations/management/collections.hxx"
#include "core/operations/management/eventing.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/operations/management/query.hxx"
#include "core/operations/management/search.hxx"
#include "core/operations/management/user.hxx"
#include "couchbase/cluster.hxx"
#include "couchbase/create_primary_query_index_options.hxx"
#include "couchbase/create_query_index_options.hxx"
#include "couchbase/drop_query_index_options.hxx"
#include "couchbase/watch_query_indexes_options.hxx"

using Catch::Approx;

static couchbase::core::operations::management::bucket_get_response
wait_for_bucket_created(test::utils::integration_test_guard& integration, const std::string& bucket_name)
{
    test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name);
    couchbase::core::operations::management::bucket_get_request req{ bucket_name };
    auto resp = test::utils::execute(integration.cluster, req);
    return resp;
}

template<typename Request>
auto
retry_on_error(test::utils::integration_test_guard& integration, Request req, std::error_code error)
{
    using response_type = typename Request::response_type;
    response_type resp;
    test::utils::wait_until([&]() {
        resp = test::utils::execute(integration.cluster, req);
        return resp.ctx.ec != error;
    });
    return resp;
}

TEST_CASE("integration: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_bucket_management()) {
        SKIP("cluster does not support bucket management");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    SECTION("crud")
    {
        couchbase::core::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.ram_quota_mb = 100;
        bucket_settings.num_replicas = 1;
        bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
        bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::value_only;
        bucket_settings.flush_enabled = true;
        if (integration.cluster_version().is_enterprise()) {
            bucket_settings.max_expiry = 10;
            bucket_settings.compression_mode = couchbase::core::management::cluster::bucket_compression::active;
        }
        bucket_settings.replica_indexes = true;
        bucket_settings.conflict_resolution_type = couchbase::core::management::cluster::bucket_conflict_resolution::sequence_number;

        {
            couchbase::core::operations::management::bucket_create_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            auto resp = wait_for_bucket_created(integration, bucket_name);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(bucket_settings.bucket_type == resp.bucket.bucket_type);
            REQUIRE(bucket_settings.name == resp.bucket.name);
            REQUIRE(Approx(bucket_settings.ram_quota_mb).margin(5) == resp.bucket.ram_quota_mb);
            REQUIRE(bucket_settings.num_replicas == resp.bucket.num_replicas);
            REQUIRE(bucket_settings.flush_enabled == resp.bucket.flush_enabled);
            REQUIRE(bucket_settings.max_expiry == resp.bucket.max_expiry);
            REQUIRE(bucket_settings.eviction_policy == resp.bucket.eviction_policy);
            REQUIRE(bucket_settings.compression_mode == resp.bucket.compression_mode);
            REQUIRE(bucket_settings.replica_indexes == resp.bucket.replica_indexes);
        }
        std::uint64_t old_quota_mb{ 0 };
        {
            couchbase::core::operations::management::bucket_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            INFO(resp.ctx.http_body);
            REQUIRE_SUCCESS(resp.ctx.ec);
            bool found = false;
            for (const auto& bucket : resp.buckets) {
                if (bucket.name != bucket_name) {
                    continue;
                }
                found = true;
                REQUIRE(bucket_settings.bucket_type == bucket.bucket_type);
                REQUIRE(bucket_settings.name == bucket.name);
                REQUIRE(bucket_settings.ram_quota_mb == bucket.ram_quota_mb);
                old_quota_mb = bucket_settings.ram_quota_mb;
                REQUIRE(bucket_settings.num_replicas == bucket.num_replicas);
                REQUIRE(bucket_settings.flush_enabled == bucket.flush_enabled);
                REQUIRE(bucket_settings.max_expiry == bucket.max_expiry);
                REQUIRE(bucket_settings.eviction_policy == bucket.eviction_policy);
                REQUIRE(bucket_settings.compression_mode == bucket.compression_mode);
                REQUIRE(bucket_settings.replica_indexes == bucket.replica_indexes);
                break;
            }
            REQUIRE(found);
        }

        {
            bucket_settings.ram_quota_mb = old_quota_mb + 20;
            couchbase::core::operations::management::bucket_update_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        auto ram_quota_updated = test::utils::wait_until([&integration, &bucket_name, old_quota_mb]() {
            couchbase::core::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return !resp.ctx.ec && resp.bucket.ram_quota_mb > old_quota_mb;
        });
        REQUIRE(ram_quota_updated);

        {
            couchbase::core::operations::management::bucket_drop_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::bucket_get_request req{ bucket_name };
            auto resp = retry_on_error(integration, req, {});
            REQUIRE(resp.ctx.ec == couchbase::errc::common::bucket_not_found);
        }

        {
            couchbase::core::operations::management::bucket_get_all_request req;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(!resp.buckets.empty());
            auto known_buckets =
              std::count_if(resp.buckets.begin(), resp.buckets.end(), [&bucket_name](auto& entry) { return entry.name == bucket_name; });
            REQUIRE(known_buckets == 0);
        }
    }

    SECTION("flush")
    {
        SECTION("flush item")
        {
            couchbase::core::document_id id{ bucket_name, "_default", "_default", test::utils::uniq_id("foo") };

            {
                couchbase::core::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

            test::utils::open_bucket(integration.cluster, bucket_name);

            {
                const tao::json::value value = {
                    { "a", 1.0 },
                };
                couchbase::core::operations::insert_request req{ id, couchbase::core::utils::json::generate_binary(value) };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
            }

            {
                couchbase::core::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
            }

            {
                couchbase::core::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            auto flushed = test::utils::wait_until([&integration, id]() {
                couchbase::core::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec() == couchbase::errc::key_value::document_not_found;
            });
            REQUIRE(flushed);
        }

        SECTION("no bucket")
        {
            couchbase::core::operations::management::bucket_flush_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::bucket_not_found);
        }

        SECTION("flush disabled")
        {
            {
                couchbase::core::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

            {
                couchbase::core::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::errc::management::bucket_not_flushable);
            }
        }
    }

    if (integration.cluster_version().supports_memcached_buckets()) {
        SECTION("memcached")
        {
            {
                couchbase::core::management::cluster::bucket_settings bucket_settings;
                bucket_settings.name = bucket_name;
                bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::memcached;
                bucket_settings.num_replicas = 0;
                couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::memcached);
            }
        }
    }

    SECTION("ephemeral")
    {
        couchbase::core::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::ephemeral;

        SECTION("default eviction")
        {
            {
                couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::core::management::cluster::bucket_eviction_policy::no_eviction);
            }
        }

        SECTION("nru eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used;
                couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                {
                    bucket_settings.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::couchstore;
                    couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                }

                {
                    auto resp = wait_for_bucket_created(integration, bucket_name);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                    REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::ephemeral);
                    REQUIRE(resp.bucket.storage_backend == couchbase::core::management::cluster::bucket_storage_backend::unknown);
                }
            }
        }
    }

    SECTION("couchbase")
    {
        couchbase::core::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;

        SECTION("default eviction")
        {
            {

                couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::core::management::cluster::bucket_eviction_policy::value_only);
            }
        }

        SECTION("full eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::core::management::cluster::bucket_eviction_policy::full;
                couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::core::management::cluster::bucket_eviction_policy::full);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                SECTION("couchstore")
                {
                    {
                        bucket_settings.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::couchstore;
                        couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend == couchbase::core::management::cluster::bucket_storage_backend::couchstore);
                    }
                }

                SECTION("magma")
                {
                    {
                        bucket_settings.ram_quota_mb = integration.cluster_version().is_neo() ? 1'024 : 256;
                        bucket_settings.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::magma;
                        couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::core::management::cluster::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend == couchbase::core::management::cluster::bucket_storage_backend::magma);
                    }
                }
            }
        }
    }

    SECTION("update no bucket")
    {

        couchbase::core::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        couchbase::core::operations::management::bucket_update_request req;
        req.bucket = bucket_settings;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::bucket_not_found);
    }

    if (integration.cluster_version().supports_minimum_durability_level()) {
        SECTION("minimum durability level")
        {
            couchbase::core::management::cluster::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;

            SECTION("default")
            {
                {
                    couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                }

                {
                    auto resp = wait_for_bucket_created(integration, bucket_name);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                    REQUIRE(resp.bucket.minimum_durability_level == couchbase::durability_level::none);
                }
            }

            if (integration.number_of_nodes() >= 2) {
                SECTION("majority")
                {
                    {
                        bucket_settings.minimum_durability_level = couchbase::durability_level::majority;
                        couchbase::core::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_SUCCESS(resp.ctx.ec);
                        REQUIRE(resp.bucket.minimum_durability_level == couchbase::durability_level::majority);
                    }
                }
            }
        }
    }

    test::utils::close_bucket(integration.cluster, bucket_name);

    // drop bucket if not already dropped
    {
        couchbase::core::operations::management::bucket_drop_request req{ bucket_name };
        test::utils::execute(integration.cluster, req);
    }
}

std::optional<couchbase::core::topology::collections_manifest::collection>
get_collection(std::shared_ptr<couchbase::core::cluster> cluster,
               const std::string& bucket_name,
               const std::string& scope_name,
               const std::string& collection_name)
{
    couchbase::core::operations::management::scope_get_all_request req{ bucket_name };
    auto resp = test::utils::execute(cluster, req);
    if (!resp.ctx.ec) {
        for (const auto& scope : resp.manifest.scopes) {
            if (scope.name == scope_name) {
                for (const auto& collection : scope.collections) {
                    if (collection.name == collection_name) {
                        return collection;
                    }
                }
            }
        }
    }
    return std::nullopt;
}

bool
scope_exists(std::shared_ptr<couchbase::core::cluster> cluster, const std::string& bucket_name, const std::string& scope_name)
{
    couchbase::core::operations::management::scope_get_all_request req{ bucket_name };
    auto resp = test::utils::execute(cluster, req);
    if (!resp.ctx.ec) {
        for (const auto& scope : resp.manifest.scopes) {
            if (scope.name == scope_name) {
                return true;
            }
        }
    }
    return false;
}

TEST_CASE("integration: collection management", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");
    std::uint32_t max_expiry = 5;

    {
        couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        auto created = test::utils::wait_until([&]() { return scope_exists(integration.cluster, integration.ctx.bucket, scope_name); });
        REQUIRE(created);
    }

    {
        couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::scope_exists);
    }

    {
        couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        if (integration.cluster_version().is_enterprise()) {
            req.max_expiry = max_expiry;
        }
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::core::topology::collections_manifest::collection collection;
        auto created = test::utils::wait_until([&]() {
            auto coll = get_collection(integration.cluster, integration.ctx.bucket, scope_name, collection_name);
            if (coll) {
                collection = *coll;
                return true;
            }
            return false;
        });
        REQUIRE(created);
        if (integration.cluster_version().is_enterprise()) {
            REQUIRE(collection.max_expiry == max_expiry);
        }
    }

    {
        couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::collection_exists);
    }

    {
        couchbase::core::operations::management::collection_drop_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until(
          [&]() { return !get_collection(integration.cluster, integration.ctx.bucket, scope_name, collection_name).has_value(); });
        REQUIRE(dropped);
    }

    {
        couchbase::core::operations::management::collection_drop_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::collection_not_found);
    }

    {
        couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until([&]() { return !scope_exists(integration.cluster, integration.ctx.bucket, scope_name); });
        REQUIRE(dropped);
    }

    {
        couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::scope_not_found);
    }
}

void
assert_user_and_metadata(const couchbase::core::management::rbac::user_and_metadata& user,
                         const couchbase::core::management::rbac::user_and_metadata& expected)
{
    REQUIRE(user.username == expected.username);
    REQUIRE(user.groups == expected.groups);
    REQUIRE(user.roles.size() == expected.roles.size());
    for (const auto& role : user.roles) {
        auto expected_role =
          std::find_if(expected.roles.begin(), expected.roles.end(), [&role](const auto& exp_role) { return role.name == exp_role.name; });
        REQUIRE(expected_role != expected.roles.end());
        REQUIRE(role.name == expected_role->name);
        REQUIRE(role.bucket == expected_role->bucket);
        REQUIRE(role.scope == expected_role->scope);
        REQUIRE(role.collection == expected_role->collection);
    }
    REQUIRE(user.display_name == expected.display_name);
    REQUIRE(user.domain == expected.domain);
    REQUIRE(user.effective_roles.size() == expected.effective_roles.size());
    for (const auto& role : user.effective_roles) {
        auto expected_role = std::find_if(expected.effective_roles.begin(), expected.effective_roles.end(), [&role](const auto& exp_role) {
            return role.name == exp_role.name;
        });
        REQUIRE(expected_role != expected.effective_roles.end());
        REQUIRE(role.name == expected_role->name);
        REQUIRE(role.bucket == expected_role->bucket);
        REQUIRE(role.scope == expected_role->scope);
        REQUIRE(role.collection == expected_role->collection);
        REQUIRE(role.origins.size() == expected_role->origins.size());
        for (const auto& origin : role.origins) {
            auto expected_origin = std::find_if(expected_role->origins.begin(),
                                                expected_role->origins.end(),
                                                [&origin](const auto& exp_origin) { return origin.name == exp_origin.name; });
            REQUIRE(expected_origin != expected_role->origins.end());
            REQUIRE(origin.name == expected_origin->name);
            REQUIRE(origin.type == expected_origin->type);
        }
    }
}

TEST_CASE("integration: user groups management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_user_groups()) {
        SKIP("cluster does not support user groups");
    }

    SECTION("group crud")
    {
        auto group_name_1 = test::utils::uniq_id("group");
        auto group_name_2 = test::utils::uniq_id("group");

        couchbase::core::management::rbac::group group{};
        group.name = group_name_1;
        group.description = "this is a test";
        group.roles = { couchbase::core::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::core::management::rbac::role{ "replication_admin" } };
        group.ldap_group_reference = "asda=price";

        {
            couchbase::core::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::group_get_request req{ group_name_1 };
            auto resp = retry_on_error(integration, req, couchbase::errc::management::group_not_found);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.group.name == group.name);
            REQUIRE(resp.group.description == group.description);
            REQUIRE(resp.group.ldap_group_reference == group.ldap_group_reference);
        }

        {
            group.description = "this is still a test";
            group.roles.push_back(couchbase::core::management::rbac::role{ "query_system_catalog" });
            couchbase::core::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            auto updated = test::utils::wait_until([&]() {
                couchbase::core::operations::management::group_get_request req{ group_name_1 };
                auto resp = test::utils::execute(integration.cluster, req);
                return !resp.ctx.ec && resp.group.description == group.description;
            });
            REQUIRE(updated);
        }

        {
            group.name = group_name_2;
            couchbase::core::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            auto created = test::utils::wait_until([&]() {
                couchbase::core::operations::management::group_get_all_request req{ group_name_2 };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                return resp.groups.size() == 2;
            });
            REQUIRE(created);
        }

        {
            couchbase::core::operations::management::role_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.roles.size() > 0);
        }

        {
            couchbase::core::operations::management::group_drop_request req{ group_name_1 };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::group_drop_request req{ group_name_2 };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }
    }

    SECTION("get missing group")
    {
        couchbase::core::operations::management::group_get_request req{ test::utils::uniq_id("group") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::group_not_found);
    }

    SECTION("drop missing group")
    {
        couchbase::core::operations::management::group_drop_request req{ test::utils::uniq_id("group") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::group_not_found);
    }

    SECTION("user and groups crud")
    {
        auto group_name = test::utils::uniq_id("group");
        auto user_name = test::utils::uniq_id("user");

        couchbase::core::management::rbac::group group{};
        group.name = group_name;
        group.description = "this is a test";
        group.roles = { couchbase::core::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::core::management::rbac::role{ "replication_admin" } };

        {
            couchbase::core::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        couchbase::core::management::rbac::user user{ user_name };
        user.display_name = "display_name";
        user.password = "password";
        user.roles = {
            couchbase::core::management::rbac::role{ "bucket_admin", integration.ctx.bucket },
        };
        user.groups = { group_name };

        {
            couchbase::core::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = retry_on_error(integration, req, couchbase::errc::common::invalid_argument);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        couchbase::core::management::rbac::user_and_metadata expected{};
        expected.username = user.username;
        expected.display_name = user.display_name;
        expected.roles = user.roles;
        expected.groups = user.groups;
        expected.domain = couchbase::core::management::rbac::auth_domain::local;

        couchbase::core::management::rbac::role_and_origins expected_role_1{};
        expected_role_1.name = "bucket_admin";
        expected_role_1.bucket = integration.ctx.bucket;
        expected_role_1.origins = { couchbase::core::management::rbac::origin{ "user" } };

        couchbase::core::management::rbac::role_and_origins expected_role_2{};
        expected_role_2.name = "replication_target";
        expected_role_2.bucket = integration.ctx.bucket;
        expected_role_2.origins = { couchbase::core::management::rbac::origin{ "group", group_name } };

        couchbase::core::management::rbac::role_and_origins expected_role_3{};
        expected_role_3.name = "replication_admin";
        expected_role_3.origins = { couchbase::core::management::rbac::origin{ "group", group_name } };

        expected.effective_roles = { expected_role_1, expected_role_2, expected_role_3 };

        {
            couchbase::core::operations::management::user_get_request req{ user_name };
            auto resp = retry_on_error(integration, req, couchbase::errc::management::user_not_found);
            REQUIRE_SUCCESS(resp.ctx.ec);
            assert_user_and_metadata(resp.user, expected);
        }

        user.display_name = "different_display_name";
        expected.display_name = "different_display_name";

        {
            couchbase::core::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::user_get_response resp;
            test::utils::wait_until([&]() {
                couchbase::core::operations::management::user_get_request req{ user.username };
                resp = test::utils::execute(integration.cluster, req);
                return !resp.ctx.ec && resp.user.display_name == user.display_name;
            });
            REQUIRE_SUCCESS(resp.ctx.ec);
            assert_user_and_metadata(resp.user, expected);
        }

        {
            couchbase::core::operations::management::user_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE_FALSE(resp.users.empty());
            auto upserted_user =
              std::find_if(resp.users.begin(), resp.users.end(), [&user_name](const auto& u) { return u.username == user_name; });
            REQUIRE(upserted_user != resp.users.end());
            assert_user_and_metadata(*upserted_user, expected);
        }

        {
            couchbase::core::operations::management::user_drop_request req{ user_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::group_drop_request req{ group_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }
    }
}

TEST_CASE("integration: user management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_user_management()) {
        SKIP("cluster does not support user management");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("get missing user")
    {
        couchbase::core::operations::management::user_get_request req{ test::utils::uniq_id("user") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::user_not_found);
    }

    SECTION("drop missing user")
    {
        couchbase::core::operations::management::user_drop_request req{ test::utils::uniq_id("user") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::management::user_not_found);
    }

    SECTION("get roles")
    {
        couchbase::core::operations::management::role_get_all_request req{};
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.roles.size() > 0);
    }

    if (integration.cluster_version().is_enterprise()) {

        SECTION("change user password")
        {
            asio::io_context io;
            auto guard = asio::make_work_guard(io);
            std::thread io_thread([&io]() { io.run(); });
            auto user_name = test::utils::uniq_id("newUser");

            // Create options
            auto options_original = couchbase::cluster_options(integration.ctx.username, integration.ctx.password);
            auto options_outdated = couchbase::cluster_options(user_name, integration.ctx.password);
            auto options_updated = couchbase::cluster_options(user_name, "newPassword");

            // Create new user and upsert
            couchbase::core::management::rbac::user new_user{ user_name };
            new_user.display_name = "change_password_user";
            new_user.password = integration.ctx.password;
            new_user.roles = {
                couchbase::core::management::rbac::role{ "admin" },
            };
            auto [cluster, ec] = couchbase::cluster::connect(io, integration.ctx.connection_string, options_original).get();
            auto coreCluster = cluster.core();
            couchbase::core::operations::management::user_upsert_request upsertReq{};
            upsertReq.user = new_user;
            auto upsertResp = test::utils::execute(coreCluster, upsertReq);
            REQUIRE_SUCCESS(upsertResp.ctx.ec);
            test::utils::wait_until_user_present(integration.cluster, user_name);
            cluster.close();
            guard.reset();
            io_thread.join();

            // Connect with new credentials and change password
            asio::io_context io2;
            auto guard2 = asio::make_work_guard(io2);
            std::thread io_thread2([&io2]() { io2.run(); });
            auto [cluster_new, ec_new] = couchbase::cluster::connect(io2, integration.ctx.connection_string, options_outdated).get();
            auto coreCluster_new = cluster_new.core();
            couchbase::core::operations::management::change_password_request changePasswordReq{};
            changePasswordReq.newPassword = "newPassword";
            auto changePasswordResp = test::utils::execute(coreCluster_new, changePasswordReq);
            REQUIRE_SUCCESS(changePasswordResp.ctx.ec);
            test::utils::wait_until_cluster_connected(user_name, changePasswordReq.newPassword, integration.ctx.connection_string);
            cluster_new.close();
            guard2.reset();
            io_thread2.join();

            // Connect with old credentials, should fail
            asio::io_context io3;
            auto guard3 = asio::make_work_guard(io3);
            std::thread io_thread3([&io3]() { io3.run(); });
            auto [cluster_fail, ec_fail] = couchbase::cluster::connect(io3, integration.ctx.connection_string, options_outdated).get();
            REQUIRE(ec_fail == couchbase::errc::common::authentication_failure);

            // Make connection with new credentials, should succeed
            auto [cluster_success, ec_success] = couchbase::cluster::connect(io3, integration.ctx.connection_string, options_updated).get();
            REQUIRE_SUCCESS(ec_success);
            cluster_success.close();
            guard3.reset();
            io_thread3.join();
        }
    }
}

TEST_CASE("integration: user management collections roles", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_user_management()) {
        SKIP("cluster does not support user management");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }
    if (integration.cluster_version().is_community()) {
        SKIP("cluster is community edition");
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");
    auto user_name = test::utils::uniq_id("user");

    {
        couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    couchbase::core::management::rbac::user user{ user_name };
    user.display_name = "display_name";
    user.password = "password";
    user.roles = {
        couchbase::core::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name },
    };

    {
        couchbase::core::operations::management::user_upsert_request req{};
        req.user = user;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::user_get_request req{ user_name };
        auto resp = retry_on_error(integration, req, couchbase::errc::management::user_not_found);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.user.roles.size() == 1);
        REQUIRE(resp.user.roles[0].name == "data_reader");
        REQUIRE(resp.user.roles[0].bucket == integration.ctx.bucket);
        REQUIRE(resp.user.roles[0].scope == scope_name);
    }

    user.roles = {
        couchbase::core::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name, collection_name },
    };

    {
        couchbase::core::operations::management::user_upsert_request req{};
        req.user = user;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::user_get_request req{ user_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.user.roles.size() == 1);
        REQUIRE(resp.user.roles[0].name == "data_reader");
        REQUIRE(resp.user.roles[0].bucket == integration.ctx.bucket);
        REQUIRE(resp.user.roles[0].scope == scope_name);
        REQUIRE(resp.user.roles[0].collection == collection_name);
    }
}

TEST_CASE("integration: query index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query_index_management()) {
        SKIP("cluster does not support query index management");
    }

    if (integration.cluster_version().supports_bucket_management()) {
        SECTION("primary index")
        {
            auto bucket_name = test::utils::uniq_id("bucket");

            {
                couchbase::core::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.bucket_type = couchbase::core::management::cluster::bucket_type::couchbase;
                req.bucket.num_replicas = 0;
                auto resp = test::utils::execute(integration.cluster, req);
            }

            REQUIRE(!wait_for_bucket_created(integration, bucket_name).ctx.ec);
            SECTION("core API")
            {

                {
                    couchbase::core::operations::management::query_index_create_response resp;
                    bool operation_completed = test::utils::wait_until([&integration, &bucket_name, &resp]() {
                        couchbase::core::operations::management::query_index_create_request req{};
                        req.bucket_name = bucket_name;
                        req.is_primary = true;
                        resp = test::utils::execute(integration.cluster, req);
                        return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                    });
                    REQUIRE(operation_completed);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                }

                {
                    couchbase::core::operations::management::query_index_get_all_request req{};
                    req.bucket_name = bucket_name;
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_SUCCESS(resp.ctx.ec);
                    REQUIRE(resp.indexes.size() == 1);
                    REQUIRE(resp.indexes[0].name == "#primary");
                    REQUIRE(resp.indexes[0].is_primary);
                }

                {
                    couchbase::core::operations::management::bucket_drop_request req{ bucket_name };
                    test::utils::execute(integration.cluster, req);
                }
            }
            SECTION("public api")
            {
                couchbase::cluster c(integration.cluster);
                {
                    std::error_code ec;
                    bool operation_completed = test::utils::wait_until([&bucket_name, &ec, &c]() {
                        auto ctx = c.query_indexes().create_primary_index(bucket_name, {}).get();
                        ec = ctx.ec();
                        return ec != couchbase::errc::common::bucket_not_found;
                    });
                    REQUIRE(operation_completed);
                    REQUIRE_SUCCESS(ec);
                }
                {
                    auto [ctx, indexes] = c.query_indexes().get_all_indexes(bucket_name, {}).get();
                    REQUIRE_SUCCESS(ctx.ec());
                    REQUIRE(indexes.size() == 1);
                    REQUIRE(indexes[0].name == "#primary");
                    REQUIRE(indexes[0].is_primary);
                }
                {
                    auto ctx =
                      c.query_indexes().watch_indexes(bucket_name, {}, couchbase::watch_query_indexes_options().watch_primary(true)).get();
                    REQUIRE_SUCCESS(ctx.ec());
                }
                {
                    auto ctx = c.query_indexes().drop_primary_index(bucket_name, {}).get();
                    REQUIRE_SUCCESS(ctx.ec());
                }
            }
        }
    }

    SECTION("non primary index")
    {
        SECTION("core API")
        {
            auto index_name = test::utils::uniq_id("index");
            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.index_name = index_name;
                    req.fields = { "field", "field2 DESC", "`two words` DESC" };
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.fields = { "field" };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::errc::common::index_exists);
            }

            {
                couchbase::core::operations::management::query_index_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.fields = { "field" };
                req.ignore_if_exists = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                auto index = std::find_if(
                  resp.indexes.begin(), resp.indexes.end(), [&index_name](const auto& exp_index) { return exp_index.name == index_name; });
                REQUIRE(index != resp.indexes.end());
                REQUIRE(index->name == index_name);
                REQUIRE_FALSE(index->is_primary);
                REQUIRE(index->index_key.size() == 3);
                REQUIRE(index->index_key[0] == "`field`");
                REQUIRE(index->index_key[1] == "`field2` DESC");
                REQUIRE(index->index_key[2] == "`two words` DESC");
                REQUIRE(index->bucket_name == integration.ctx.bucket);
                REQUIRE(index->state == "online");
            }
            {
                couchbase::core::operations::management::query_index_drop_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_drop_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
            }
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto index_name = test::utils::uniq_id("index");
            {
                std::error_code ec;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, c, &ec]() {
                    auto ctx = c.query_indexes()
                                 .create_index(integration.ctx.bucket, index_name, { "field", "field2 DESC", "`two words` DESC" }, {})
                                 .get();
                    ec = ctx.ec();
                    return ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(ec);
            }
            {
                auto ctx = c.query_indexes().watch_indexes(integration.ctx.bucket, { index_name }, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }

            {
                auto ctx = c.query_indexes().create_index(integration.ctx.bucket, index_name, { "field" }, {}).get();
                REQUIRE(ctx.ec() == couchbase::errc::common::index_exists);
            }

            {
                auto ctx =
                  c.query_indexes()
                    .create_index(
                      integration.ctx.bucket, index_name, { "field" }, couchbase::create_query_index_options().ignore_if_exists(true))
                    .get();
                REQUIRE_SUCCESS(ctx.ec());
            }

            {
                auto [ctx, indexes] = c.query_indexes().get_all_indexes(integration.ctx.bucket, {}).get();
                auto index = std::find_if(
                  indexes.begin(), indexes.end(), [&index_name](const auto& exp_index) { return exp_index.name == index_name; });
                REQUIRE(index != indexes.end());
                REQUIRE(index->name == index_name);
                REQUIRE_FALSE(index->is_primary);
                REQUIRE(index->index_key.size() == 3);
                REQUIRE(index->index_key[0] == "`field`");
                REQUIRE(index->index_key[1] == "`field2` DESC");
                REQUIRE(index->index_key[2] == "`two words` DESC");
                REQUIRE(index->bucket_name == integration.ctx.bucket);
                REQUIRE(index->state == "online");
            }
            {
                auto ctx = c.query_indexes().drop_index(integration.ctx.bucket, index_name, {}).get();
                couchbase::core::operations::management::query_index_drop_request req{};
                REQUIRE_SUCCESS(ctx.ec());
            }

            {
                auto ctx = c.query_indexes().drop_index(integration.ctx.bucket, index_name, {}).get();
                couchbase::core::operations::management::query_index_drop_request req{};
                REQUIRE(ctx.ec() == couchbase::errc::common::index_not_found);
            }
            {
                auto ctx =
                  c.query_indexes()
                    .drop_index(integration.ctx.bucket, index_name, couchbase::drop_query_index_options().ignore_if_not_exists(true))
                    .get();
                couchbase::core::operations::management::query_index_drop_request req{};
                REQUIRE_SUCCESS(ctx.ec());
            }
        }
    }

    SECTION("deferred index")
    {
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto index_name = test::utils::uniq_id("index");
            {
                std::error_code ec;
                bool operation_completed = test::utils::wait_until([c, &ec, &index_name, &integration]() {
                    auto ctx =
                      c.query_indexes()
                        .create_index(
                          integration.ctx.bucket, index_name, { "field" }, couchbase::create_query_index_options().build_deferred(true))
                        .get();
                    ec = ctx.ec();
                    return ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(ec);
            }

            {
                auto [ctx, indexes] = c.query_indexes().get_all_indexes(integration.ctx.bucket, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
                auto index = std::find_if(
                  indexes.begin(), indexes.end(), [&index_name](const auto& exp_index) { return exp_index.name == index_name; });
                REQUIRE(index != indexes.end());
                REQUIRE(index->name == index_name);
                REQUIRE(index->state == "deferred");
            }

            {
                auto manager = couchbase::cluster(integration.cluster).query_indexes();
                auto ctx = manager.build_deferred_indexes(integration.ctx.bucket, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
            {
                // now wait till it is online before proceeding
                auto operation_completed = test::utils::wait_until([&integration, &index_name, c]() {
                    auto [ctx, indexes] = c.query_indexes().get_all_indexes(integration.ctx.bucket, {}).get();
                    if (indexes.empty()) {
                        return false;
                    }
                    auto index = std::find_if(
                      indexes.begin(), indexes.end(), [&index_name](const auto& exp_index) { return exp_index.name == index_name; });
                    return index->state == "online";
                });
                REQUIRE(operation_completed);
            }
        }

        SECTION("core API")
        {
            auto index_name = test::utils::uniq_id("index");
            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.index_name = index_name;
                    req.fields = { "field" };
                    req.deferred = true;
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
            {
                couchbase::core::operations::management::query_index_build_deferred_request req{};
                req.bucket_name = integration.ctx.bucket;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                test::utils::wait_until([&integration, &index_name]() {
                    couchbase::core::operations::management::query_index_get_all_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    auto resp = test::utils::execute(integration.cluster, req);
                    if (resp.indexes.empty()) {
                        return false;
                    }
                    auto index = std::find_if(resp.indexes.begin(), resp.indexes.end(), [&index_name](const auto& exp_index) {
                        return exp_index.name == index_name;
                    });
                    return index->state == "online";
                });
            }
        }
    }

    SECTION("create missing bucket")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_create_request req{};
            req.bucket_name = "missing_bucket";
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::bucket_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto ctx = c.query_indexes().create_primary_index("missing_bucket", {}).get();
            REQUIRE(ctx.ec() == couchbase::errc::common::bucket_not_found);
        }
    }

    SECTION("get missing bucket")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_get_all_request req{};
            req.bucket_name = "missing_bucket";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.indexes.empty());
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto [ctx, indexes] = c.query_indexes().get_all_indexes("missing_bucket", {}).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(indexes.empty());
        }
    }

    SECTION("drop missing bucket")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_drop_request req{};
            req.bucket_name = "missing_bucket";
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::bucket_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto ctx = c.query_indexes().drop_primary_index("missing_bucket", {}).get();
            REQUIRE(ctx.ec() == couchbase::errc::common::bucket_not_found);
        }
    }

    SECTION("watch missing index")
    {
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto start = std::chrono::steady_clock::now();
            auto ctx = c.query_indexes()
                         .watch_indexes(integration.ctx.bucket,
                                        { "idontexist", "neitherdoI" },
                                        couchbase::watch_query_indexes_options()
                                          .timeout(std::chrono::milliseconds(10000))
                                          .polling_interval(std::chrono::milliseconds(1000)))
                         .get();
            REQUIRE(ctx.ec() == couchbase::errc::common::ambiguous_timeout);
            REQUIRE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() >= 10000);
        }
    }
    SECTION("watch missing bucket")
    {
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto start = std::chrono::steady_clock::now();
            auto ctx = c.query_indexes()
                         .watch_indexes("missing_buckeet",
                                        { "idontexist", "neitherdoI" },
                                        couchbase::watch_query_indexes_options()
                                          .timeout(std::chrono::milliseconds(10000))
                                          .polling_interval(std::chrono::milliseconds(1000)))
                         .get();
            REQUIRE(ctx.ec() == couchbase::errc::common::ambiguous_timeout);
            REQUIRE(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() >= 10000);
        }
    }
}

TEST_CASE("integration: collections query index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query_index_management()) {
        SKIP("cluster does not support query index management");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    auto index_name = test::utils::uniq_id("collections_index");
    auto scope_name = test::utils::uniq_id("indexscope");
    auto collection_name = test::utils::uniq_id("indexcollection");

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    // create the scope and collection that we'll do index management on.
    {
        couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    auto manager =
      couchbase::cluster(integration.cluster).bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name).query_indexes();

    SECTION("primary index")
    {
        SECTION("core API")
        {
            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &scope_name, &collection_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.scope_name = scope_name;
                    req.collection_name = collection_name;
                    req.is_primary = true;
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.indexes.size() == 1);
                REQUIRE(resp.indexes[0].name == "#primary");
                REQUIRE(resp.indexes[0].is_primary);
            }
        }
        SECTION("public API")
        {
            {
                std::error_code ec;
                bool operation_completed = test::utils::wait_until([&manager, &ec]() {
                    auto ctx = manager.create_primary_index({}).get();
                    ec = ctx.ec();
                    return ctx.ec() != couchbase::errc::common::bucket_not_found;
                });

                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(ec);
            }
            {
                auto [ctx, indexes] = manager.get_all_indexes({}).get();
                REQUIRE_SUCCESS(ctx.ec());
                REQUIRE(indexes.size() == 1);
                REQUIRE(indexes[0].name == "#primary");
                REQUIRE(indexes[0].is_primary);
            }
        }
    }

    SECTION("named primary index")
    {
        SECTION("core API")
        {
            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, &scope_name, &collection_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.scope_name = scope_name;
                    req.collection_name = collection_name;
                    req.index_name = index_name;
                    req.is_primary = true;
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.indexes.size() == 1);
                REQUIRE(resp.indexes[0].name == index_name);
                REQUIRE(resp.indexes[0].is_primary);
            }

            {
                couchbase::core::operations::management::query_index_drop_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.scope_name = scope_name;
                req.is_primary = true;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }
        SECTION("public API")
        {
            {
                std::error_code ec;
                auto operation_completed = test::utils::wait_until([&index_name, &manager, &ec]() {
                    auto ctx = manager.create_primary_index(couchbase::create_primary_query_index_options().index_name(index_name)).get();
                    ec = ctx.ec();
                    return ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(ec);
            }
            {
                auto [ctx, indexes] = manager.get_all_indexes({}).get();
                REQUIRE_SUCCESS(ctx.ec());
                REQUIRE(indexes.size() == 1);
                REQUIRE(indexes[0].name == index_name);
                REQUIRE(indexes[0].is_primary);
            }
            {
                auto ctx = manager.watch_indexes({ index_name }, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
            {
                auto ctx = manager.drop_index(index_name, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
        }
    }

    SECTION("non primary index")
    {
        SECTION("core API")
        {

            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, &scope_name, &collection_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.index_name = index_name;
                    req.scope_name = scope_name;
                    req.collection_name = collection_name;
                    req.fields = { "field" };
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                req.fields = { "field" };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::errc::common::index_exists);
            }

            {
                couchbase::core::operations::management::query_index_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                req.fields = { "field" };
                req.ignore_if_exists = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.indexes.size() == 1);
                REQUIRE(resp.indexes[0].name == index_name);
                REQUIRE_FALSE(resp.indexes[0].is_primary);
                REQUIRE(resp.indexes[0].index_key.size() == 1);
                REQUIRE(resp.indexes[0].index_key[0] == "`field`");
                REQUIRE(resp.indexes[0].collection_name == collection_name);
                REQUIRE(resp.indexes[0].scope_name == scope_name);
                REQUIRE(resp.indexes[0].bucket_name == integration.ctx.bucket);
                REQUIRE(resp.indexes[0].state == "online");
            }

            {
                couchbase::core::operations::management::query_index_drop_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_drop_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.index_name = index_name;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
            }
        }
        SECTION("public API")
        {
            {
                std::error_code ec;
                auto operation_complete = test::utils::wait_until([&manager, &ec, &index_name]() {
                    ec = manager.create_index(index_name, { "field" }, {}).get().ec();
                    return ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_complete);
                REQUIRE_SUCCESS(ec);
            }
            {
                REQUIRE(manager.create_index(index_name, { "field" }, {}).get().ec() == couchbase::errc::common::index_exists);
            }
            {
                REQUIRE_SUCCESS(
                  manager.create_index(index_name, { "field" }, couchbase::create_query_index_options().ignore_if_exists(true)).get().ec());
            }
            {
                REQUIRE_SUCCESS(manager.watch_indexes({ index_name }, {}).get().ec());
                auto [ctx, indexes] = manager.get_all_indexes({}).get();
                REQUIRE_SUCCESS(ctx.ec());
                REQUIRE(indexes.size() == 1);
                REQUIRE(indexes[0].name == index_name);
                REQUIRE_FALSE(indexes[0].is_primary);
                REQUIRE(indexes[0].index_key.size() == 1);
                REQUIRE(indexes[0].index_key[0] == "`field`");
                REQUIRE(indexes[0].collection_name == collection_name);
                REQUIRE(indexes[0].scope_name == scope_name);
                REQUIRE(indexes[0].bucket_name == integration.ctx.bucket);
                REQUIRE(indexes[0].state == "online");
            }
            {
                auto ctx = manager.drop_index(index_name, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
            {
                REQUIRE(manager.drop_index(index_name, {}).get().ec() == couchbase::errc::common::index_not_found);
            }
            {
                REQUIRE_SUCCESS(
                  manager.drop_index(index_name, couchbase::drop_query_index_options().ignore_if_not_exists(true)).get().ec());
            }
        }
    }

    SECTION("deferred index")
    {

        SECTION("public API")
        {
            {
                auto ctx =
                  manager.create_index(index_name, { "field" }, couchbase::create_query_index_options().build_deferred(true)).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
            {
                auto [ctx, indexes] = manager.get_all_indexes({}).get();
                REQUIRE_SUCCESS(ctx.ec());
                REQUIRE(indexes.size() == 1);
                REQUIRE(indexes[0].name == index_name);
                REQUIRE(indexes[0].state == "deferred");
            }
            {
                auto ctx = manager.build_deferred_indexes({}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
            {
                auto ctx = manager.watch_indexes({ index_name }, {}).get();
                REQUIRE_SUCCESS(ctx.ec());
            }
        }

        SECTION("core API")
        {
            {
                couchbase::core::operations::management::query_index_create_response resp;
                bool operation_completed = test::utils::wait_until([&integration, &index_name, &scope_name, &collection_name, &resp]() {
                    couchbase::core::operations::management::query_index_create_request req{};
                    req.bucket_name = integration.ctx.bucket;
                    req.index_name = index_name;
                    req.scope_name = scope_name;
                    req.collection_name = collection_name;
                    req.fields = { "field" };
                    req.deferred = true;
                    resp = test::utils::execute(integration.cluster, req);
                    return resp.ctx.ec != couchbase::errc::common::bucket_not_found;
                });
                REQUIRE(operation_completed);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                REQUIRE(resp.indexes.size() == 1);
                REQUIRE(resp.indexes[0].name == index_name);
                REQUIRE(resp.indexes[0].state == "deferred");
            }
            {
                couchbase::core::operations::management::query_index_build_deferred_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            test::utils::wait_until([&integration, scope_name, collection_name]() {
                couchbase::core::operations::management::query_index_get_all_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                auto resp = test::utils::execute(integration.cluster, req);
                if (resp.indexes.empty()) {
                    return false;
                }
                return resp.indexes[0].state == "online";
            });
        }
    }

    SECTION("create missing collection")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_create_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = scope_name;
            req.collection_name = "missing_collection";
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::collection_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope(scope_name).collection("missing_collection");
            REQUIRE(coll.query_indexes().create_primary_index({}).get().ec() == couchbase::errc::common::collection_not_found);
        }
    }

    SECTION("create missing scope")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_create_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = "missing_scope";
            req.collection_name = collection_name;
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::scope_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope("missing scope").collection(collection_name);
            REQUIRE(coll.query_indexes().create_primary_index({}).get().ec() == couchbase::errc::common::scope_not_found);
        }
    }

    SECTION("get missing collection")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_get_all_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = scope_name;
            req.collection_name = "missing_collection";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.indexes.empty());
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope(scope_name).collection("missing_collection");
            auto [ctx, indexes] = coll.query_indexes().get_all_indexes({}).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(indexes.empty());
        }
    }
    SECTION("get missing scope")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_get_all_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = "missing_scope";
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.indexes.empty());
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope("missing_scope").collection(collection_name);
            auto [ctx, indexes] = coll.query_indexes().get_all_indexes({}).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(indexes.empty());
        }
    }

    SECTION("drop missing collection")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_drop_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = scope_name;
            req.collection_name = "missing_collection";
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::collection_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope(scope_name).collection("missing_collection");
            REQUIRE(coll.query_indexes().drop_index(index_name, {}).get().ec() == couchbase::errc::common::collection_not_found);
        }
    }
    SECTION("drop missing scope")
    {
        SECTION("core API")
        {
            couchbase::core::operations::management::query_index_drop_request req{};
            req.bucket_name = integration.ctx.bucket;
            req.scope_name = "missing_scope";
            req.collection_name = collection_name;
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::scope_not_found);
        }
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope("missing_scope").collection(collection_name);
            REQUIRE(coll.query_indexes().drop_index(index_name, {}).get().ec() == couchbase::errc::common::scope_not_found);
        }
    }
    SECTION("watch missing scope")
    {
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope("missing_scope").collection(collection_name);
            REQUIRE(coll.query_indexes()
                      .watch_indexes({ index_name }, couchbase::watch_query_indexes_options().timeout(std::chrono::seconds(5)))
                      .get()
                      .ec() == couchbase::errc::common::ambiguous_timeout);
        }
    }
    SECTION("watch missing collection")
    {
        SECTION("public API")
        {
            couchbase::cluster c(integration.cluster);
            auto coll = c.bucket(integration.ctx.bucket).scope(scope_name).collection("missing_collection");
            REQUIRE(coll.query_indexes()
                      .watch_indexes({ index_name }, couchbase::watch_query_indexes_options().timeout(std::chrono::seconds(5)))
                      .get()
                      .ec() == couchbase::errc::common::ambiguous_timeout);
        }
    }
}

TEST_CASE("integration: analytics index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics service");
    }
    if (!integration.has_analytics_service()) {
        SKIP("cluster does not have analytics service");
    }
    if (integration.storage_backend() == couchbase::core::management::cluster::bucket_storage_backend::magma) {
        SKIP("analytics does not work with magma storage backend, see MB-47718");
    }

    SECTION("crud")
    {
        auto dataverse_name = test::utils::uniq_id("dataverse");
        auto dataset_name = test::utils::uniq_id("dataset");
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::core::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::analytics::dataverse_exists);
        }

        {
            couchbase::core::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::analytics::dataset_exists);
        }

        {
            couchbase::core::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::index_exists);
        }

        {
            couchbase::core::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_link_connect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataset_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE_FALSE(resp.datasets.empty());
            auto dataset = std::find_if(resp.datasets.begin(), resp.datasets.end(), [&dataset_name](const auto& exp_dataset) {
                return exp_dataset.name == dataset_name;
            });
            REQUIRE(dataset != resp.datasets.end());
            REQUIRE(dataset->dataverse_name == dataverse_name);
            REQUIRE(dataset->link_name == "Local");
            REQUIRE(dataset->bucket_name == integration.ctx.bucket);
        }

        {
            couchbase::core::operations::management::analytics_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());
            auto index = std::find_if(
              resp.indexes.begin(), resp.indexes.end(), [&index_name](const auto& exp_index) { return exp_index.name == index_name; });
            REQUIRE(index != resp.indexes.end());
            REQUIRE(index->dataverse_name == dataverse_name);
            REQUIRE(index->dataset_name == dataset_name);
            REQUIRE_FALSE(index->is_primary);
        }

        if (integration.cluster_version().supports_analytics_pending_mutations()) {
            couchbase::core::operations::management::analytics_get_pending_mutations_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.stats[index_name] == 0);
        }

        {
            couchbase::core::operations::management::analytics_link_disconnect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
        }

        {
            couchbase::core::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::analytics::dataset_not_found);
        }

        {
            couchbase::core::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::analytics::dataverse_not_found);
        }

        {
            couchbase::core::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("compound names")
        {
            auto dataverse_name = fmt::format("{}/{}", test::utils::uniq_id("dataverse"), test::utils::uniq_id("dataverse"));
            auto dataset_name = test::utils::uniq_id("dataset");
            auto index_name = test::utils::uniq_id("index");

            {
                couchbase::core::operations::management::analytics_dataverse_create_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_dataset_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_index_create_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                req.fields["testkey"] = "string";
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_link_connect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_link_disconnect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_index_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_dataset_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::analytics_dataverse_drop_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }
    }
}

void
run_s3_link_test(test::utils::integration_test_guard& integration, const std::string& dataverse_name, const std::string& link_name)
{
    {
        couchbase::core::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::analytics::link_exists);
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].link_name == link_name);
        REQUIRE(resp.s3[0].dataverse == dataverse_name);
        REQUIRE(resp.s3[0].access_key_id == "access_key");
        REQUIRE(resp.s3[0].secret_access_key.empty());
        REQUIRE(resp.s3[0].region == "us-east-1");
        REQUIRE(resp.s3[0].service_endpoint == "service_endpoint");
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.link_type = "s3";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::core::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "eu-west-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_replace_request<couchbase::core::management::analytics::s3_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].region == "eu-west-1");
    }

    {
        couchbase::core::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::analytics::link_not_found);
    }
}

void
run_azure_link_test(test::utils::integration_test_guard& integration, const std::string& dataverse_name, const std::string& link_name)
{
    {
        couchbase::core::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_create_request<
          couchbase::core::management::analytics::azure_blob_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_create_request<
          couchbase::core::management::analytics::azure_blob_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::analytics::link_exists);
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.azure_blob[0].link_name == link_name);
        REQUIRE(resp.azure_blob[0].dataverse == dataverse_name);
        REQUIRE_FALSE(resp.azure_blob[0].connection_string.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].account_name.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].account_key.has_value());
        REQUIRE_FALSE(resp.azure_blob[0].shared_access_signature.has_value());
        REQUIRE(resp.azure_blob[0].blob_endpoint == "blob_endpoint");
        REQUIRE(resp.azure_blob[0].endpoint_suffix == "endpoint_suffix");
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.link_type = "azureblob";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::core::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "new_blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::core::operations::management::analytics_link_replace_request<
          couchbase::core::management::analytics::azure_blob_external_link>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.azure_blob[0].blob_endpoint == "new_blob_endpoint");
    }

    {
        couchbase::core::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::analytics::link_not_found);
    }
}

TEST_CASE("integration: analytics external link management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics service");
    }
    if (!integration.has_analytics_service()) {
        SKIP("cluster does not have analytics service");
    }
    if (!integration.cluster_version().supports_analytics_links()) {
        SKIP("analytics does not support analytics links");
    }
    if (integration.storage_backend() == couchbase::core::management::cluster::bucket_storage_backend::magma) {
        SKIP("analytics does not work with magma storage backend, see MB-47718");
    }
    if (!integration.cluster_version().supports_analytics_links_cert_auth() && integration.origin.credentials().uses_certificate()) {
        SKIP("certificate credentials selected, but analytics service does not support cert auth, see MB-40198");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto link_name = test::utils::uniq_id("link");

    SECTION("missing dataverse")
    {
        couchbase::core::management::analytics::s3_external_link link{};
        link.dataverse = "missing_dataverse";
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;

        {
            couchbase::core::operations::management::analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>
              req{};
            req.link = link;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::analytics::dataverse_not_found);
        }
    }

    SECTION("missing argument")
    {
        couchbase::core::operations::management::analytics_link_create_request<couchbase::core::management::analytics::s3_external_link>
          req{};
        req.link = couchbase::core::management::analytics::s3_external_link{};
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::invalid_argument);
    }

    SECTION("link crud")
    {
        auto dataverse_name = test::utils::uniq_id("dataverse");

        SECTION("s3")
        {
            run_s3_link_test(integration, dataverse_name, link_name);
        }

        if (integration.cluster_version().supports_analytics_link_azure_blob()) {
            SECTION("azure")
            {
                run_azure_link_test(integration, dataverse_name, link_name);
            }
        }
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("link crud scopes")
        {
            auto scope_name = test::utils::uniq_id("scope");

            {
                couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                auto created =
                  test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
                REQUIRE(created);
            }

            auto dataverse_name = fmt::format("{}/{}", integration.ctx.bucket, scope_name);

            SECTION("s3")
            {
                run_s3_link_test(integration, dataverse_name, link_name);
            }

            if (integration.cluster_version().supports_analytics_link_azure_blob()) {
                SECTION("azure")
                {
                    run_azure_link_test(integration, dataverse_name, link_name);
                }
            }
        }
    }
}

// serverless requires 1 partition and 1 replica
const std::string serverless_plan_params = R"({ "indexPartition": 1, "numReplicas": 1 })";

TEST_CASE("integration: search index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_search()) {
        SKIP("cluster does not support search");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("search indexes crud")
    {
        auto index1_name = test::utils::uniq_id("index1");
        auto index2_name = test::utils::uniq_id("index2");
        auto alias_name = test::utils::uniq_id("alias");

        {
            couchbase::core::management::search::index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            if (integration.cluster_version().is_serverless_config_profile()) {
                index.plan_params_json = serverless_plan_params;
            }
            couchbase::core::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::management::search::index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            if (integration.cluster_version().is_serverless_config_profile()) {
                index.plan_params_json = serverless_plan_params;
            }
            couchbase::core::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::index_exists);
        }

        {
            couchbase::core::management::search::index index;
            index.name = index2_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            index.plan_params_json = R"({ "indexPartition": 3 })";
            if (integration.cluster_version().is_serverless_config_profile()) {
                index.plan_params_json = serverless_plan_params;
            }
            index.params_json = R"({ "store": { "indexType": "upside_down", "kvStoreName": "moss" }})";
            couchbase::core::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            if (resp.name != index2_name) {
                // FIXME: server 7.2 might automatically prepend "{scope}.{collection}." in front of the index name
                // to workaround it, we "patch" our variable with the name returned by the server
                index2_name = resp.name;
            }
        }

        {
            couchbase::core::management::search::index index;
            index.name = alias_name;
            index.type = "fulltext-alias";
            index.source_type = "nil";
            index.params_json = couchbase::core::utils::json::generate(
              tao::json::value{ { "targets", { { index1_name, tao::json::empty_object }, { index2_name, tao::json::empty_object } } } });
            if (integration.cluster_version().is_serverless_config_profile()) {
                index.plan_params_json = serverless_plan_params;
            }
            couchbase::core::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::search_index_get_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.index.name == index1_name);
            REQUIRE(resp.index.type == "fulltext-index");
        }

        {
            couchbase::core::operations::management::search_index_get_request req{};
            req.index_name = index2_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.index.name == index2_name);
            REQUIRE(resp.index.type == "fulltext-index");
        }

        {
            couchbase::core::operations::management::search_index_get_request req{};
            req.index_name = alias_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.index.name == alias_name);
            REQUIRE(resp.index.type == "fulltext-alias");
        }

        {
            couchbase::core::operations::management::search_index_get_request req{};
            req.index_name = "missing_index";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
        }

        {
            couchbase::core::operations::management::search_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());

            REQUIRE(1 == std::count_if(
                           resp.indexes.begin(), resp.indexes.end(), [&index1_name](const auto& i) { return i.name == index1_name; }));
            REQUIRE(1 == std::count_if(
                           resp.indexes.begin(), resp.indexes.end(), [&index2_name](const auto& i) { return i.name == index2_name; }));
            REQUIRE(1 ==
                    std::count_if(resp.indexes.begin(), resp.indexes.end(), [&alias_name](const auto& i) { return i.name == alias_name; }));
        }

        {
            couchbase::core::operations::management::search_index_drop_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::search_index_drop_request req{};
            req.index_name = index2_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::search_index_drop_request req{};
            req.index_name = alias_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        couchbase::core::operations::management::search_index_drop_request req{};
        req.index_name = "missing_index";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
    }

    SECTION("upsert index no name")
    {
        couchbase::core::management::search::index index;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        if (integration.cluster_version().is_serverless_config_profile()) {
            index.plan_params_json = serverless_plan_params;
        }
        couchbase::core::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::invalid_argument);
    }

    SECTION("control")
    {
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::core::management::search::index index;
            index.name = index_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            if (integration.cluster_version().is_serverless_config_profile()) {
                index.plan_params_json = serverless_plan_params;
            }
            couchbase::core::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        SECTION("ingest control")
        {
            {
                couchbase::core::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }

        SECTION("query control")
        {
            {
                couchbase::core::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }

        SECTION("partition control")
        {
            {
                couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }

            {
                couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }

        couchbase::core::operations::management::search_index_drop_request req{};
        req.index_name = index_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }
}

bool
wait_for_search_pindexes_ready(test::utils::integration_test_guard& integration, const std::string& index_name)
{
    return test::utils::wait_until(
      [&integration, &index_name]() {
          couchbase::core::operations::management::search_index_stats_request req{};
          auto resp = test::utils::execute(integration.cluster, req);
          if (resp.ctx.ec || resp.stats.empty()) {
              return false;
          }
          auto stats = couchbase::core::utils::json::parse(resp.stats);

          const auto num_pindexes_actual_key = fmt::format("{}:{}:num_pindexes_actual", integration.ctx.bucket, index_name);
          const auto num_pindexes_target_key = fmt::format("{}:{}:num_pindexes_target", integration.ctx.bucket, index_name);
          const auto* num_pindexes_actual = stats.find(num_pindexes_actual_key);
          const auto* num_pindexes_target = stats.find(num_pindexes_target_key);
          CB_LOG_DEBUG(
            "wait_for_search_pindexes_ready: {}={}, {}={}",
            num_pindexes_actual_key,
            (num_pindexes_actual == nullptr || !num_pindexes_actual->is_number()) ? "missing"
                                                                                  : std::to_string(num_pindexes_actual->get_unsigned()),
            num_pindexes_target_key,
            (num_pindexes_target == nullptr || !num_pindexes_target->is_number()) ? "missing"
                                                                                  : std::to_string(num_pindexes_target->get_unsigned()));

          if (num_pindexes_actual == nullptr || !num_pindexes_actual->is_number()) {
              return false;
          }
          if (num_pindexes_target == nullptr || !num_pindexes_target->is_number()) {
              return false;
          }
          return num_pindexes_actual->get_unsigned() == num_pindexes_target->get_unsigned();
      },
      std::chrono::minutes(3));
}

TEST_CASE("integration: search index management analyze document", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_search()) {
        SKIP("cluster does not support search");
    }

    if (!integration.cluster_version().supports_search_analyze()) {
        SKIP("cluster does not support search analyze");
    }

    auto index_name = test::utils::uniq_id("index");

    {
        couchbase::core::management::search::index index;
        index.name = index_name;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        if (integration.cluster_version().is_serverless_config_profile()) {
            index.plan_params_json = serverless_plan_params;
        }
        couchbase::core::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    REQUIRE(test::utils::wait_for_search_pindexes_ready(integration.cluster, integration.ctx.bucket, index_name));

    couchbase::core::operations::management::search_index_analyze_document_response resp;
    bool operation_completed = test::utils::wait_until([&integration, &index_name, &resp]() {
        couchbase::core::operations::management::search_index_analyze_document_request req{};
        req.index_name = index_name;
        req.encoded_document = R"({ "name": "hello world" })";
        resp = test::utils::execute(integration.cluster, req);
        return resp.ctx.ec != couchbase::errc::common::internal_server_failure;
    });
    REQUIRE(operation_completed);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE_FALSE(resp.analysis.empty());
}

TEST_CASE("integration: freeform HTTP request", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("key_value")
    {
        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::key_value;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::invalid_argument);
    }

    SECTION("analytics")
    {
        if (!integration.cluster_version().supports_analytics()) {
            SKIP("cluster does not support analytics");
        }
        if (!integration.has_analytics_service()) {
            SKIP("cluster does not have analytics service");
        }

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::analytics;
        req.method = "GET";
        req.path = "/admin/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        INFO(resp.body);
        auto result = couchbase::core::utils::json::parse(resp.body);
        REQUIRE(result.is_object());
    }

    SECTION("search")
    {
        if (!integration.cluster_version().supports_search()) {
            SKIP("cluster does not support search");
        }

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::search;
        req.method = "GET";
        req.path = "/api/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE(resp.body.empty());
        REQUIRE_FALSE(resp.headers.empty());
        REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
    }

    SECTION("query")
    {
        if (!integration.cluster_version().supports_query()) {
            SKIP("cluster does not support query");
        }

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::query;
        req.method = "GET";
        req.path = "/admin/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        INFO(resp.body);
        auto result = couchbase::core::utils::json::parse(resp.body);
        REQUIRE(result.is_object());
    }

    SECTION("view")
    {
        if (!integration.cluster_version().supports_views()) {
            SKIP("cluster does not support views");
        }

        auto document_name = test::utils::uniq_id("design_document");
        auto view_name = test::utils::uniq_id("view");

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::view;
        req.method = "POST";
        req.path = fmt::format("/{}/_design/{}/_view/{}", integration.ctx.bucket, document_name, view_name);
        req.body = R"({"keys":["foo","bar"]})";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 404);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::core::utils::json::parse(resp.body);
        INFO(resp.body);
        REQUIRE(result["error"].get_string() == "not_found");
    }

    SECTION("management")
    {
        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::management;
        req.method = "GET";
        req.path = "/pools";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::core::utils::json::parse(resp.body);
        INFO(resp.body);
        REQUIRE(result.find("uuid") != nullptr);
    }

    SECTION("create scope")
    {
        if (!integration.cluster_version().supports_collections()) {
            SKIP("cluster does not support collections");
        }

        auto scope_name = test::utils::uniq_id("freeform_scope");

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::management;
        req.method = "POST";
        req.path = fmt::format("/pools/default/buckets/{}/scopes", integration.ctx.bucket);
        req.headers["content-type"] = "application/x-www-form-urlencoded";
        req.body = fmt::format("name={}", couchbase::core::utils::string_codec::form_encode(scope_name));
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.headers.empty());
        if (integration.cluster_version().is_mock()) {
            REQUIRE(resp.headers["content-type"].find("application/json") == std::string::npos);
            REQUIRE(resp.headers["content-type"].find("text/plain") != std::string::npos);
        } else {
            REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
        }
        auto result = couchbase::core::utils::json::parse(resp.body);
        REQUIRE(result.is_object());
        REQUIRE(result.find("uid") != nullptr);
    }

    SECTION("eventing")
    {
        if (!integration.cluster_version().supports_eventing_functions()) {
            SKIP("cluster does not support eventing functions");
        }
        if (!integration.has_eventing_service()) {
            SKIP("cluster does not have eventing service");
        }

        couchbase::core::operations::management::freeform_request req{};
        req.type = couchbase::core::service_type::eventing;
        req.method = "GET";
        req.path = "/api/v1/functions";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::core::utils::json::parse(resp.body);
        INFO(resp.body);
        REQUIRE(result.is_array());
    }
}

static couchbase::core::operations::management::eventing_get_function_response
wait_for_function_created(test::utils::integration_test_guard& integration, const std::string& function_name)
{
    couchbase::core::operations::management::eventing_get_function_response resp{};
    test::utils::wait_until([&integration, &resp, function_name]() {
        couchbase::core::operations::management::eventing_get_function_request req{ function_name };
        resp = test::utils::execute(integration.cluster, req);
        return !resp.ctx.ec;
    });
    return resp;
}

static bool
wait_for_function_reach_status(test::utils::integration_test_guard& integration,
                               const std::string& function_name,
                               couchbase::core::management::eventing::function_status status)
{
    return test::utils::wait_until(
      [&integration, function_name, status]() {
          couchbase::core::operations::management::eventing_get_status_request req{};
          auto resp = test::utils::execute(integration.cluster, req);
          if (resp.ctx.ec) {
              return false;
          }
          auto function = std::find_if(resp.status.functions.begin(), resp.status.functions.end(), [function_name](const auto& fun) {
              return function_name == fun.name;
          });
          if (function == resp.status.functions.end()) {
              return false;
          }
          return function->status == status;
      },
      std::chrono::minutes(3));
}

TEST_CASE("integration: eventing functions management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_eventing_functions()) {
        SKIP("cluster does not support eventing service");
    }
    if (!integration.has_eventing_service()) {
        SKIP("cluster does not have eventing service");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("lifecycle")
    {
        auto function_name = test::utils::uniq_id("name");

        {
            couchbase::core::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            if (integration.cluster_version().is_cheshire_cat()) {
                REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_deployed);
            } else {
                REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
            }
        }

        {
            couchbase::core::operations::management::eventing_get_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
        }

        auto meta_bucket_name = test::utils::uniq_id("meta");
        {

            couchbase::core::management::cluster::bucket_settings bucket_settings;
            bucket_settings.name = meta_bucket_name;
            bucket_settings.ram_quota_mb = 256;

            {
                couchbase::core::operations::management::bucket_create_request req;
                req.bucket = bucket_settings;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
            }
        }

        {
            auto resp = wait_for_bucket_created(integration, meta_bucket_name);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        std::string source_code = R"(
function OnUpdate(doc, meta) {
    log("Doc created/updated", meta.id);
}

function OnDelete(meta, options) {
    log("Doc deleted/expired", meta.id);
}
)";

        {
            couchbase::core::operations::management::eventing_upsert_function_request req{};
            req.function.source_keyspace.bucket = integration.ctx.bucket;
            req.function.metadata_keyspace.bucket = meta_bucket_name;
            req.function.name = function_name;
            req.function.code = source_code;
            req.function.settings.handler_headers = { "// generated by Couchbase C++ SDK" };
            req.function.constant_bindings.emplace_back(couchbase::core::management::eventing::function_constant_binding{ "PI", "3.14" });
            req.function.bucket_bindings.emplace_back(couchbase::core::management::eventing::function_bucket_binding{
              "data", { integration.ctx.bucket }, couchbase::core::management::eventing::function_bucket_access::read_write });
            req.function.url_bindings.emplace_back(
              couchbase::core::management::eventing::function_url_binding{ "home", "https://couchbase.com" });
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            auto resp = wait_for_function_created(integration, function_name);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::eventing_get_all_functions_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            auto function = std::find_if(
              resp.functions.begin(), resp.functions.end(), [&function_name](const auto& fun) { return function_name == fun.name; });
            REQUIRE(function != resp.functions.end());
            REQUIRE(function->code == source_code);
            REQUIRE(function->source_keyspace.bucket == integration.ctx.bucket);
            REQUIRE(function->metadata_keyspace.bucket == meta_bucket_name);
            REQUIRE(function->settings.deployment_status == couchbase::core::management::eventing::function_deployment_status::undeployed);
            REQUIRE(function->settings.processing_status == couchbase::core::management::eventing::function_processing_status::paused);
            REQUIRE(!function->settings.handler_headers.empty());
            REQUIRE(function->settings.handler_headers[0] == "// generated by Couchbase C++ SDK");
            REQUIRE(!function->constant_bindings.empty());
            REQUIRE(function->constant_bindings[0].alias == "PI");
            REQUIRE(function->constant_bindings[0].literal == "3.14");
            REQUIRE(!function->bucket_bindings.empty());
            REQUIRE(function->bucket_bindings[0].alias == "data");
            REQUIRE(function->bucket_bindings[0].name.bucket == "default");
            REQUIRE(function->bucket_bindings[0].access == couchbase::core::management::eventing::function_bucket_access::read_write);
            REQUIRE(!function->url_bindings.empty());
            REQUIRE(function->url_bindings[0].alias == "home");
            REQUIRE(function->url_bindings[0].hostname == "https://couchbase.com");
            REQUIRE(std::holds_alternative<couchbase::core::management::eventing::function_url_no_auth>(function->url_bindings[0].auth));
        }

        {
            couchbase::core::operations::management::eventing_get_status_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            REQUIRE(resp.status.num_eventing_nodes > 0);
            auto function = std::find_if(resp.status.functions.begin(), resp.status.functions.end(), [&function_name](const auto& fun) {
                return function_name == fun.name;
            });
            REQUIRE(function != resp.status.functions.end());
            REQUIRE(function->status == couchbase::core::management::eventing::function_status::undeployed);
            REQUIRE(function->deployment_status == couchbase::core::management::eventing::function_deployment_status::undeployed);
            REQUIRE(function->processing_status == couchbase::core::management::eventing::function_processing_status::paused);
        }

        {
            couchbase::core::operations::management::eventing_undeploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_deployed);
        }

        {
            couchbase::core::operations::management::eventing_deploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        REQUIRE(
          wait_for_function_reach_status(integration, function_name, couchbase::core::management::eventing::function_status::deployed));

        {
            couchbase::core::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_deployed);
        }

        {
            couchbase::core::operations::management::eventing_resume_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_deployed);
        }

        {
            couchbase::core::operations::management::eventing_pause_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        REQUIRE(wait_for_function_reach_status(integration, function_name, couchbase::core::management::eventing::function_status::paused));

        {
            couchbase::core::operations::management::eventing_pause_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_paused);
        }

        {
            couchbase::core::operations::management::eventing_resume_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        REQUIRE(
          wait_for_function_reach_status(integration, function_name, couchbase::core::management::eventing::function_status::deployed));

        {
            couchbase::core::operations::management::eventing_undeploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        REQUIRE(
          wait_for_function_reach_status(integration, function_name, couchbase::core::management::eventing::function_status::undeployed));

        {
            couchbase::core::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }

        {
            couchbase::core::operations::management::eventing_get_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::errc::management::eventing_function_not_found);
        }

        {
            couchbase::core::operations::management::bucket_drop_request req{ meta_bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
        }
    }
}
