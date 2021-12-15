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

#include <couchbase/operations/management/bucket.hxx>
#include <couchbase/operations/management/user.hxx>
#include <couchbase/operations/management/collections.hxx>
#include <couchbase/operations/management/query.hxx>
#include <couchbase/operations/management/analytics.hxx>
#include <couchbase/operations/management/analytics_link.hxx>

static bool
wait_for_bucket_created(test::utils::integration_test_guard& integration, const std::string& bucket_name)
{
    return test::utils::wait_until([&integration, &bucket_name]() {
        couchbase::operations::management::bucket_get_request req{ bucket_name };
        auto resp = test::utils::execute(integration.cluster, req);
        return !resp.ctx.ec;
    });
}

TEST_CASE("integration: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    SECTION("crud")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.ram_quota_mb = 100;
        bucket_settings.num_replicas = 1;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;
        bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::value_only;
        bucket_settings.flush_enabled = true;
        bucket_settings.max_expiry = 10;
        bucket_settings.compression_mode = couchbase::operations::management::bucket_settings::compression_mode::active;
        bucket_settings.replica_indexes = true;
        bucket_settings.conflict_resolution_type =
          couchbase::operations::management::bucket_settings::conflict_resolution_type::sequence_number;

        {
            couchbase::operations::management::bucket_create_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::bucket_get_response resp;
            auto created = test::utils::wait_until([&integration, bucket_name, &resp]() {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                resp = test::utils::execute(integration.cluster, req);
                return !resp.ctx.ec;
            });
            REQUIRE(created);
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

        {
            couchbase::operations::management::bucket_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            bool found = false;
            for (const auto& bucket : resp.buckets) {
                if (bucket.name != bucket_name) {
                    continue;
                }
                found = true;
                REQUIRE(bucket_settings.bucket_type == bucket.bucket_type);
                REQUIRE(bucket_settings.name == bucket.name);
                REQUIRE(bucket_settings.ram_quota_mb == bucket.ram_quota_mb);
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
            bucket_settings.ram_quota_mb += 20;
            couchbase::operations::management::bucket_update_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(Approx(bucket_settings.ram_quota_mb).margin(5) == resp.bucket.ram_quota_mb);
        }

        {
            couchbase::operations::management::bucket_drop_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
        }

        {
            couchbase::operations::management::bucket_get_all_request req;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(!resp.buckets.empty());
            auto known_buckets =
              std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](auto& entry) { return entry.name == bucket_name; });
            REQUIRE(known_buckets == 0);
        }
    }

    SECTION("flush")
    {
        SECTION("flush item")
        {
            couchbase::document_id id{ bucket_name, "_default", "_default", test::utils::uniq_id("foo") };

            {
                couchbase::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name);

            test::utils::open_bucket(integration.cluster, bucket_name);

            {
                const tao::json::value value = {
                    { "a", 1.0 },
                };
                couchbase::operations::insert_request req{ id, couchbase::utils::json::generate(value) };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            auto flushed = test::utils::wait_until([&integration, id]() {
                couchbase::operations::get_request req{ id };
                auto resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec == couchbase::error::key_value_errc::document_not_found;
            });
            REQUIRE(flushed);
        }

        SECTION("no bucket")
        {
            couchbase::operations::management::bucket_flush_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
        }

        SECTION("flush disabled")
        {
            {
                couchbase::operations::management::bucket_create_request req;
                req.bucket.name = bucket_name;
                req.bucket.flush_enabled = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_flush_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE(resp.ctx.ec == couchbase::error::management_errc::bucket_not_flushable);
            }
        }
    }

    SECTION("memcached")
    {
        {
            couchbase::operations::management::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;
            bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::memcached;
            bucket_settings.num_replicas = 0;
            couchbase::operations::management::bucket_create_request req{ bucket_settings };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        REQUIRE(wait_for_bucket_created(integration, bucket_name));

        {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::memcached);
        }
    }

    SECTION("ephemeral")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::ephemeral;

        SECTION("default eviction")
        {
            {
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::no_eviction);
            }
        }

        SECTION("nru eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy ==
                        couchbase::operations::management::bucket_settings::eviction_policy::not_recently_used);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                {
                    bucket_settings.storage_backend = couchbase::operations::management::bucket_settings::storage_backend_type::couchstore;
                    couchbase::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                }

                REQUIRE(wait_for_bucket_created(integration, bucket_name));

                {
                    couchbase::operations::management::bucket_get_request req{ bucket_name };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                    REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::ephemeral);
                    REQUIRE(resp.bucket.storage_backend ==
                            couchbase::operations::management::bucket_settings::storage_backend_type::unknown);
                }
            }
        }
    }

    SECTION("couchbase")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;

        SECTION("default eviction")
        {
            {

                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::value_only);
            }
        }

        SECTION("full eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::operations::management::bucket_settings::eviction_policy::full;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::operations::management::bucket_settings::eviction_policy::full);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                SECTION("couchstore")
                {
                    {
                        bucket_settings.storage_backend =
                          couchbase::operations::management::bucket_settings::storage_backend_type::couchstore;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    REQUIRE(wait_for_bucket_created(integration, bucket_name));

                    {
                        couchbase::operations::management::bucket_get_request req{ bucket_name };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend ==
                                couchbase::operations::management::bucket_settings::storage_backend_type::couchstore);
                    }
                }

                SECTION("magma")
                {
                    {
                        bucket_settings.ram_quota_mb = 256;
                        bucket_settings.storage_backend = couchbase::operations::management::bucket_settings::storage_backend_type::magma;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    REQUIRE(wait_for_bucket_created(integration, bucket_name));

                    {
                        couchbase::operations::management::bucket_get_request req{ bucket_name };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::operations::management::bucket_settings::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend ==
                                couchbase::operations::management::bucket_settings::storage_backend_type::magma);
                    }
                }
            }
        }
    }

    SECTION("update no bucket")
    {

        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        couchbase::operations::management::bucket_update_request req;
        req.bucket = bucket_settings;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
    }

    SECTION("minimum durability level")
    {
        couchbase::operations::management::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;

        SECTION("default")
        {
            {
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::none);
            }
        }

        SECTION("majority")
        {
            if (integration.number_of_nodes() < 2) {
                return;
            }

            {
                bucket_settings.minimum_durability_level = couchbase::protocol::durability_level::majority;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            REQUIRE(wait_for_bucket_created(integration, bucket_name));

            {
                couchbase::operations::management::bucket_get_request req{ bucket_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::majority);
            }
        }
    }

    test::utils::close_bucket(integration.cluster, bucket_name);

    // drop bucket if not already dropped
    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        test::utils::execute(integration.cluster, req);
    }
}

bool
collection_exists(couchbase::cluster& cluster,
                  const std::string& bucket_name,
                  const std::string& scope_name,
                  const std::string& collection_name)
{
    couchbase::operations::management::scope_get_all_request req{ bucket_name };
    auto resp = test::utils::execute(cluster, req);
    if (!resp.ctx.ec) {
        for (const auto& scope : resp.manifest.scopes) {
            if (scope.name == scope_name) {
                for (const auto& collection : scope.collections) {
                    if (collection.name == collection_name) {
                        return true;
                    }
                }
            }
        }
    }
    return false;
}

bool
scope_exists(couchbase::cluster& cluster, const std::string& bucket_name, const std::string& scope_name)
{
    couchbase::operations::management::scope_get_all_request req{ bucket_name };
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

    if (!integration.cluster_version().supports_collections()) {
        return;
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");

    {
        couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto created = test::utils::wait_until([&]() { return scope_exists(integration.cluster, integration.ctx.bucket, scope_name); });
        REQUIRE(created);
    }

    {
        couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::scope_exists);
    }

    {
        couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name, 5 };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto created = test::utils::wait_until(
          [&]() { return collection_exists(integration.cluster, integration.ctx.bucket, scope_name, collection_name); });
        REQUIRE(created);
    }

    // TODO: Check collection was created with correct max expiry when supported

    {
        couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::collection_exists);
    }

    {
        couchbase::operations::management::collection_drop_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until(
          [&]() { return !collection_exists(integration.cluster, integration.ctx.bucket, scope_name, collection_name); });
        REQUIRE(dropped);
    }

    {
        couchbase::operations::management::collection_drop_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::collection_not_found);
    }

    {
        couchbase::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until([&]() { return !scope_exists(integration.cluster, integration.ctx.bucket, scope_name); });
        REQUIRE(dropped);
    }

    {
        couchbase::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::scope_not_found);
    }
}

void
assert_user_and_metadata(const couchbase::operations::management::rbac::user_and_metadata& user,
                         const couchbase::operations::management::rbac::user_and_metadata& expected)
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
        REQUIRE(expected_role != user.effective_roles.end());
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
        return;
    }

    SECTION("group crud")
    {
        auto group_name_1 = test::utils::uniq_id("group");
        auto group_name_2 = test::utils::uniq_id("group");

        couchbase::operations::management::rbac::group group{};
        group.name = group_name_1;
        group.description = "this is a test";
        group.roles = { couchbase::operations::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::operations::management::rbac::role{ "replication_admin" } };
        group.ldap_group_reference = "asda=price";

        {
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::group_get_request req{ group_name_1 };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.group.name == group.name);
            REQUIRE(resp.group.description == group.description);
            REQUIRE(resp.group.ldap_group_reference == group.ldap_group_reference);
        }

        {
            group.description = "this is still a test";
            group.roles.push_back(couchbase::operations::management::rbac::role{ "query_system_catalog" });
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            group.name = group_name_2;
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::group_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            INFO(resp.ctx.ec.message());
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.groups.size() >= 2);
        }

        {
            couchbase::operations::management::role_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.roles.size() > 0);
        }

        {
            couchbase::operations::management::group_drop_request req{ group_name_1 };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::group_drop_request req{ group_name_2 };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }

    SECTION("get missing group")
    {
        couchbase::operations::management::group_get_request req{ test::utils::uniq_id("group") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::group_not_found);
    }

    SECTION("drop missing group")
    {
        couchbase::operations::management::group_drop_request req{ test::utils::uniq_id("group") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::group_not_found);
    }

    SECTION("user and groups crud")
    {
        auto group_name = test::utils::uniq_id("group");
        auto user_name = test::utils::uniq_id("user");

        couchbase::operations::management::rbac::group group{};
        group.name = group_name;
        group.description = "this is a test";
        group.roles = { couchbase::operations::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::operations::management::rbac::role{ "replication_admin" } };

        {
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::operations::management::rbac::user user{ user_name };
        user.display_name = "display_name";
        user.password = "password";
        user.roles = {
            couchbase::operations::management::rbac::role{ "bucket_admin", integration.ctx.bucket },
        };
        user.groups = { group_name };

        {
            couchbase::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::operations::management::rbac::user_and_metadata expected{};
        expected.username = user.username;
        expected.display_name = user.display_name;
        expected.roles = user.roles;
        expected.groups = user.groups;
        expected.domain = couchbase::operations::management::rbac::auth_domain::local;

        couchbase::operations::management::rbac::role_and_origins expected_role_1{};
        expected_role_1.name = "bucket_admin";
        expected_role_1.bucket = integration.ctx.bucket;
        expected_role_1.origins = { couchbase::operations::management::rbac::origin{ "user" } };

        couchbase::operations::management::rbac::role_and_origins expected_role_2{};
        expected_role_2.name = "replication_target";
        expected_role_2.bucket = integration.ctx.bucket;
        expected_role_2.origins = { couchbase::operations::management::rbac::origin{ "group", group_name } };

        couchbase::operations::management::rbac::role_and_origins expected_role_3{};
        expected_role_3.name = "replication_admin";
        expected_role_3.origins = { couchbase::operations::management::rbac::origin{ "group", group_name } };

        expected.effective_roles = { expected_role_1, expected_role_2, expected_role_3 };

        {
            couchbase::operations::management::user_get_request req{ user_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);

            assert_user_and_metadata(resp.user, expected);
        };

        user.display_name = "different_display_name";
        expected.display_name = "different_display_name";

        {
            couchbase::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::user_get_request req{ user_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);

            assert_user_and_metadata(resp.user, expected);
        };

        {
            couchbase::operations::management::user_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.users.empty());
            auto upserted_user =
              std::find_if(resp.users.begin(), resp.users.end(), [&user_name](const auto& u) { return u.username == user_name; });
            REQUIRE(upserted_user != resp.users.end());
            assert_user_and_metadata(*upserted_user, expected);
        }

        {
            couchbase::operations::management::user_drop_request req{ user_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::group_drop_request req{ group_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }
}

TEST_CASE("integration: user management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("get missing user")
    {
        couchbase::operations::management::user_get_request req{ test::utils::uniq_id("user") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::user_not_found);
    }

    SECTION("drop missing user")
    {
        couchbase::operations::management::user_drop_request req{ test::utils::uniq_id("user") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::management_errc::user_not_found);
    }

    SECTION("get roles")
    {
        couchbase::operations::management::role_get_all_request req{};
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.roles.size() > 0);
    }
}

TEST_CASE("integration: user management collections roles", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_collections()) {
        return;
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");
    auto user_name = test::utils::uniq_id("user");

    {
        couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    couchbase::operations::management::rbac::user user{ user_name };
    user.display_name = "display_name";
    user.password = "password";
    user.roles = {
        couchbase::operations::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name },
    };

    {
        couchbase::operations::management::user_upsert_request req{};
        req.user = user;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::user_get_request req{ user_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.user.roles.size() == 1);
        REQUIRE(resp.user.roles[0].name == "data_reader");
        REQUIRE(resp.user.roles[0].bucket == integration.ctx.bucket);
        REQUIRE(resp.user.roles[0].scope == scope_name);
    };

    user.roles = {
        couchbase::operations::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name, collection_name },
    };

    {
        couchbase::operations::management::user_upsert_request req{};
        req.user = user;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::user_get_request req{ user_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.user.roles.size() == 1);
        REQUIRE(resp.user.roles[0].name == "data_reader");
        REQUIRE(resp.user.roles[0].bucket == integration.ctx.bucket);
        REQUIRE(resp.user.roles[0].scope == scope_name);
        REQUIRE(resp.user.roles[0].collection == collection_name);
    };
}

TEST_CASE("integration: query index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query_index_management()) {
        return;
    }

    auto bucket_name = test::utils::uniq_id("bucket");
    auto index_name = test::utils::uniq_id("index");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        req.bucket.bucket_type = couchbase::operations::management::bucket_settings::bucket_type::couchbase;
        req.bucket.num_replicas = 0;
        auto resp = test::utils::execute(integration.cluster, req);
    }

    REQUIRE(wait_for_bucket_created(integration, bucket_name));

    SECTION("primary index")
    {
        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.is_primary = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.indexes.size() == 1);
            REQUIRE(resp.indexes[0].name == "#primary");
            REQUIRE(resp.indexes[0].is_primary);
        }
    }

    SECTION("non primary index")
    {
        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.fields = { "field" };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.fields = { "field" };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.fields = { "field" };
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.indexes.size() == 1);
            REQUIRE(resp.indexes[0].name == index_name);
            REQUIRE_FALSE(resp.indexes[0].is_primary);
            REQUIRE(resp.indexes[0].index_key.size() == 1);
            REQUIRE(resp.indexes[0].index_key[0] == "`field`");
            REQUIRE(resp.indexes[0].keyspace_id == bucket_name);
            REQUIRE(resp.indexes[0].state == "online");
            REQUIRE(resp.indexes[0].namespace_id == "default");
        }

        {
            couchbase::operations::management::query_index_drop_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_drop_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }
    }

    SECTION("deferred index")
    {
        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.fields = { "field" };
            req.deferred = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.indexes.size() == 1);
            REQUIRE(resp.indexes[0].name == index_name);
            REQUIRE(resp.indexes[0].state == "deferred");
        }

        {
            couchbase::operations::management::query_index_build_deferred_request req{};
            req.bucket_name = bucket_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        test::utils::wait_until([&integration, bucket_name]() {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            auto resp = test::utils::execute(integration.cluster, req);
            if (resp.indexes.empty()) {
                return false;
            }
            return resp.indexes[0].state == "online";
        });
    }

    SECTION("create missing bucket")
    {
        couchbase::operations::management::query_index_create_request req{};
        req.bucket_name = "missing_bucket";
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
    }

    SECTION("get missing bucket")
    {
        couchbase::operations::management::query_index_get_all_request req{};
        req.bucket_name = "missing_bucket";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.indexes.empty());
    }

    SECTION("drop missing bucket")
    {
        couchbase::operations::management::query_index_drop_request req{};
        req.bucket_name = "missing_bucket";
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
    }

    // drop any buckets that haven't already been dropped
    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        test::utils::execute(integration.cluster, req);
    }
}

TEST_CASE("integration: analytics index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics_indexes()) {
        return;
    }

    SECTION("crud")
    {
        auto dataverse_name = test::utils::uniq_id("dataverse");
        auto dataset_name = test::utils::uniq_id("dataset");
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_exists);
        }

        {
            couchbase::operations::management::analytics_dataverse_create_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataset_exists);
        }

        {
            couchbase::operations::management::analytics_dataset_create_request req{};
            req.dataset_name = dataset_name;
            req.bucket_name = integration.ctx.bucket;
            req.dataverse_name = dataverse_name;
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::operations::management::analytics_index_create_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.index_name = index_name;
            req.fields["testkey"] = "string";
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_link_connect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.datasets.empty());
            auto dataset = std::find_if(resp.datasets.begin(), resp.datasets.end(), [dataset_name](const auto& exp_dataset) {
                return exp_dataset.name == dataset_name;
            });
            REQUIRE(dataset != resp.datasets.end());
            REQUIRE(dataset->dataverse_name == dataverse_name);
            REQUIRE(dataset->link_name == "Local");
            REQUIRE(dataset->bucket_name == integration.ctx.bucket);
        }

        {
            couchbase::operations::management::analytics_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());
            auto index = std::find_if(
              resp.indexes.begin(), resp.indexes.end(), [index_name](const auto& exp_index) { return exp_index.name == index_name; });
            REQUIRE(index != resp.indexes.end());
            REQUIRE(index->dataverse_name == dataverse_name);
            REQUIRE(index->dataset_name == dataset_name);
            REQUIRE_FALSE(index->is_primary);
        }

        if (integration.cluster_version().supports_analytics_pending_mutations()) {
            couchbase::operations::management::analytics_get_pending_mutations_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.stats[index_name] == 0);
        }

        {
            couchbase::operations::management::analytics_link_disconnect_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }

        {
            couchbase::operations::management::analytics_index_drop_request req{};
            req.index_name = index_name;
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataset_not_found);
        }

        {
            couchbase::operations::management::analytics_dataset_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.dataset_name = dataset_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_not_found);
        }

        {
            couchbase::operations::management::analytics_dataverse_drop_request req{};
            req.dataverse_name = dataverse_name;
            req.ignore_if_does_not_exist = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("compound names")
        {
            auto dataverse_name = fmt::format("{}/{}", test::utils::uniq_id("dataverse"), test::utils::uniq_id("dataverse"));
            auto dataset_name = test::utils::uniq_id("dataset");
            auto index_name = test::utils::uniq_id("index");

            {
                couchbase::operations::management::analytics_dataverse_create_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataset_create_request req{};
                req.bucket_name = integration.ctx.bucket;
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_index_create_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                req.fields["testkey"] = "string";
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_link_connect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_link_disconnect_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_index_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                req.index_name = index_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataset_drop_request req{};
                req.dataverse_name = dataverse_name;
                req.dataset_name = dataset_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::analytics_dataverse_drop_request req{};
                req.dataverse_name = dataverse_name;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }
    }
}

void
run_s3_link_test(test::utils::integration_test_guard& integration, std::string dataverse_name, std::string link_name)
{
    {
        couchbase::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_exists);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].link_name == link_name);
        REQUIRE(resp.s3[0].dataverse == dataverse_name);
        REQUIRE(resp.s3[0].access_key_id == "access_key");
        REQUIRE(resp.s3[0].secret_access_key.empty());
        REQUIRE(resp.s3[0].region == "us-east-1");
        REQUIRE(resp.s3[0].service_endpoint == "service_endpoint");
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "s3";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "eu-west-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.size() == 1);
        REQUIRE(resp.s3[0].region == "eu-west-1");
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_not_found);
    }
}

void
run_azure_link_test(test::utils::integration_test_guard& integration, std::string dataverse_name, std::string link_name)
{
    {
        couchbase::operations::management::analytics_dataverse_create_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_exists);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
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
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "azureblob";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.link_type = "couchbase";
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.s3.empty());
        REQUIRE(resp.azure_blob.empty());
        REQUIRE(resp.couchbase.empty());
    }

    {
        couchbase::operations::management::analytics_link::azure_blob_external link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "new_blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<
          couchbase::operations::management::analytics_link::azure_blob_external>
          req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_get_all_request req{};
        req.dataverse_name = dataverse_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.azure_blob.size() == 1);
        REQUIRE(resp.azure_blob[0].blob_endpoint == "new_blob_endpoint");
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::analytics_link_drop_request req{};
        req.dataverse_name = dataverse_name;
        req.link_name = link_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::link_not_found);
    }
}

TEST_CASE("integration: analytics external link management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics_links()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto link_name = test::utils::uniq_id("link");

    SECTION("missing dataverse")
    {
        couchbase::operations::management::analytics_link::s3_external link{};
        link.dataverse = "missing_dataverse";
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;

        {
            couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
              req{};
            req.link = link;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_not_found);
        }
    }

    SECTION("missing argument")
    {
        couchbase::operations::management::analytics_link_create_request<couchbase::operations::management::analytics_link::s3_external>
          req{};
        req.link = couchbase::operations::management::analytics_link::s3_external{};
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
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
                couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
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