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

#include <couchbase/management/analytics_link.hxx>
#include <couchbase/operations/management/analytics.hxx>
#include <couchbase/operations/management/bucket.hxx>
#include <couchbase/operations/management/collections.hxx>
#include <couchbase/operations/management/eventing.hxx>
#include <couchbase/operations/management/freeform.hxx>
#include <couchbase/operations/management/query.hxx>
#include <couchbase/operations/management/search.hxx>
#include <couchbase/operations/management/user.hxx>

static couchbase::operations::management::bucket_get_response
wait_for_bucket_created(test::utils::integration_test_guard& integration, const std::string& bucket_name)
{
    test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name);
    couchbase::operations::management::bucket_get_request req{ bucket_name };
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

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    SECTION("crud")
    {
        couchbase::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.ram_quota_mb = 100;
        bucket_settings.num_replicas = 1;
        bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
        bucket_settings.eviction_policy = couchbase::management::cluster::bucket_eviction_policy::value_only;
        bucket_settings.flush_enabled = true;
        if (integration.cluster_version().is_enterprise()) {
            bucket_settings.max_expiry = 10;
            bucket_settings.compression_mode = couchbase::management::cluster::bucket_compression::active;
        }
        bucket_settings.replica_indexes = true;
        bucket_settings.conflict_resolution_type = couchbase::management::cluster::bucket_conflict_resolution::sequence_number;

        {
            couchbase::operations::management::bucket_create_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto resp = wait_for_bucket_created(integration, bucket_name);
            REQUIRE_FALSE(resp.ctx.ec);
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
            couchbase::operations::management::bucket_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            INFO(resp.ctx.http_body)
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
            couchbase::operations::management::bucket_update_request req;
            req.bucket = bucket_settings;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        auto ram_quota_updated = test::utils::wait_until([&integration, &bucket_name, old_quota_mb]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return !resp.ctx.ec && resp.bucket.ram_quota_mb > old_quota_mb;
        });
        REQUIRE(ram_quota_updated);

        {
            couchbase::operations::management::bucket_drop_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = retry_on_error(integration, req, {});
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

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

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

            REQUIRE(test::utils::wait_until_bucket_healthy(integration.cluster, bucket_name));

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
            couchbase::management::cluster::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;
            bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::memcached;
            bucket_settings.num_replicas = 0;
            couchbase::operations::management::bucket_create_request req{ bucket_settings };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto resp = wait_for_bucket_created(integration, bucket_name);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::memcached);
        }
    }

    SECTION("ephemeral")
    {
        couchbase::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::ephemeral;

        SECTION("default eviction")
        {
            {
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message)
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::management::cluster::bucket_eviction_policy::no_eviction);
            }
        }

        SECTION("nru eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::management::cluster::bucket_eviction_policy::not_recently_used;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::ephemeral);
                REQUIRE(resp.bucket.eviction_policy == couchbase::management::cluster::bucket_eviction_policy::not_recently_used);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                {
                    bucket_settings.storage_backend = couchbase::management::cluster::bucket_storage_backend::couchstore;
                    couchbase::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                }

                {
                    auto resp = wait_for_bucket_created(integration, bucket_name);
                    REQUIRE_FALSE(resp.ctx.ec);
                    REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::ephemeral);
                    REQUIRE(resp.bucket.storage_backend == couchbase::management::cluster::bucket_storage_backend::unknown);
                }
            }
        }
    }

    SECTION("couchbase")
    {
        couchbase::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::couchbase;

        SECTION("default eviction")
        {
            {

                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                INFO(resp.error_message)
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::management::cluster::bucket_eviction_policy::value_only);
            }
        }

        SECTION("full eviction")
        {
            {
                bucket_settings.eviction_policy = couchbase::management::cluster::bucket_eviction_policy::full;
                couchbase::operations::management::bucket_create_request req{ bucket_settings };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                auto resp = wait_for_bucket_created(integration, bucket_name);
                REQUIRE_FALSE(resp.ctx.ec);
                REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::couchbase);
                REQUIRE(resp.bucket.eviction_policy == couchbase::management::cluster::bucket_eviction_policy::full);
            }
        }

        if (integration.cluster_version().supports_storage_backend()) {
            SECTION("storage backend")
            {
                SECTION("couchstore")
                {
                    {
                        bucket_settings.storage_backend = couchbase::management::cluster::bucket_storage_backend::couchstore;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend == couchbase::management::cluster::bucket_storage_backend::couchstore);
                    }
                }

                SECTION("magma")
                {
                    {
                        bucket_settings.ram_quota_mb = 256;
                        bucket_settings.storage_backend = couchbase::management::cluster::bucket_storage_backend::magma;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.bucket_type == couchbase::management::cluster::bucket_type::couchbase);
                        REQUIRE(resp.bucket.storage_backend == couchbase::management::cluster::bucket_storage_backend::magma);
                    }
                }
            }
        }
    }

    SECTION("update no bucket")
    {

        couchbase::management::cluster::bucket_settings bucket_settings;
        bucket_settings.name = bucket_name;
        couchbase::operations::management::bucket_update_request req;
        req.bucket = bucket_settings;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::bucket_not_found);
    }

    if (integration.cluster_version().supports_minimum_durability_level()) {
        SECTION("minimum durability level")
        {
            couchbase::management::cluster::bucket_settings bucket_settings;
            bucket_settings.name = bucket_name;

            SECTION("default")
            {
                {
                    couchbase::operations::management::bucket_create_request req{ bucket_settings };
                    auto resp = test::utils::execute(integration.cluster, req);
                    REQUIRE_FALSE(resp.ctx.ec);
                }

                {
                    auto resp = wait_for_bucket_created(integration, bucket_name);
                    REQUIRE_FALSE(resp.ctx.ec);
                    REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::none);
                }
            }

            if (integration.number_of_nodes() >= 2) {
                SECTION("majority")
                {
                    {
                        bucket_settings.minimum_durability_level = couchbase::protocol::durability_level::majority;
                        couchbase::operations::management::bucket_create_request req{ bucket_settings };
                        auto resp = test::utils::execute(integration.cluster, req);
                        INFO(resp.error_message)
                        REQUIRE_FALSE(resp.ctx.ec);
                    }

                    {
                        auto resp = wait_for_bucket_created(integration, bucket_name);
                        REQUIRE_FALSE(resp.ctx.ec);
                        REQUIRE(resp.bucket.minimum_durability_level == couchbase::protocol::durability_level::majority);
                    }
                }
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
collection_exists(std::shared_ptr<couchbase::cluster> cluster,
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
scope_exists(std::shared_ptr<couchbase::cluster> cluster, const std::string& bucket_name, const std::string& scope_name)
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
        couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        if (integration.cluster_version().is_enterprise()) {
            req.max_expiry = 5;
        }
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
assert_user_and_metadata(const couchbase::management::rbac::user_and_metadata& user,
                         const couchbase::management::rbac::user_and_metadata& expected)
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
        return;
    }

    SECTION("group crud")
    {
        auto group_name_1 = test::utils::uniq_id("group");
        auto group_name_2 = test::utils::uniq_id("group");

        couchbase::management::rbac::group group{};
        group.name = group_name_1;
        group.description = "this is a test";
        group.roles = { couchbase::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::management::rbac::role{ "replication_admin" } };
        group.ldap_group_reference = "asda=price";

        {
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::group_get_request req{ group_name_1 };
            auto resp = retry_on_error(integration, req, couchbase::error::management_errc::group_not_found);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.group.name == group.name);
            REQUIRE(resp.group.description == group.description);
            REQUIRE(resp.group.ldap_group_reference == group.ldap_group_reference);
        }

        {
            group.description = "this is still a test";
            group.roles.push_back(couchbase::management::rbac::role{ "query_system_catalog" });
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto updated = test::utils::wait_until([&]() {
                couchbase::operations::management::group_get_request req{ group_name_1 };
                auto resp = test::utils::execute(integration.cluster, req);
                return !resp.ctx.ec && resp.group.description == group.description;
            });
            REQUIRE(updated);
        }

        {
            group.name = group_name_2;
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto created = test::utils::wait_until([&]() {
                couchbase::operations::management::group_get_all_request req{ group_name_2 };
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                return resp.groups.size() == 2;
            });
            REQUIRE(created);
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

        couchbase::management::rbac::group group{};
        group.name = group_name;
        group.description = "this is a test";
        group.roles = { couchbase::management::rbac::role{ "replication_target", integration.ctx.bucket },
                        couchbase::management::rbac::role{ "replication_admin" } };

        {
            couchbase::operations::management::group_upsert_request req{ group };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::management::rbac::user user{ user_name };
        user.display_name = "display_name";
        user.password = "password";
        user.roles = {
            couchbase::management::rbac::role{ "bucket_admin", integration.ctx.bucket },
        };
        user.groups = { group_name };

        {
            couchbase::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = retry_on_error(integration, req, couchbase::error::common_errc::invalid_argument);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::management::rbac::user_and_metadata expected{};
        expected.username = user.username;
        expected.display_name = user.display_name;
        expected.roles = user.roles;
        expected.groups = user.groups;
        expected.domain = couchbase::management::rbac::auth_domain::local;

        couchbase::management::rbac::role_and_origins expected_role_1{};
        expected_role_1.name = "bucket_admin";
        expected_role_1.bucket = integration.ctx.bucket;
        expected_role_1.origins = { couchbase::management::rbac::origin{ "user" } };

        couchbase::management::rbac::role_and_origins expected_role_2{};
        expected_role_2.name = "replication_target";
        expected_role_2.bucket = integration.ctx.bucket;
        expected_role_2.origins = { couchbase::management::rbac::origin{ "group", group_name } };

        couchbase::management::rbac::role_and_origins expected_role_3{};
        expected_role_3.name = "replication_admin";
        expected_role_3.origins = { couchbase::management::rbac::origin{ "group", group_name } };

        expected.effective_roles = { expected_role_1, expected_role_2, expected_role_3 };

        {
            couchbase::operations::management::user_get_request req{ user_name };
            auto resp = retry_on_error(integration, req, couchbase::error::management_errc::user_not_found);
            REQUIRE_FALSE(resp.ctx.ec);
            assert_user_and_metadata(resp.user, expected);
        }

        user.display_name = "different_display_name";
        expected.display_name = "different_display_name";

        {
            couchbase::operations::management::user_upsert_request req{};
            req.user = user;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::user_get_response resp;
            test::utils::wait_until([&]() {
                couchbase::operations::management::user_get_request req{ user.username };
                resp = test::utils::execute(integration.cluster, req);
                return !resp.ctx.ec && resp.user.display_name == user.display_name;
            });
            REQUIRE_FALSE(resp.ctx.ec);
            assert_user_and_metadata(resp.user, expected);
        }

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

    if (!integration.cluster_version().supports_collections() || integration.cluster_version().is_community()) {
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

    couchbase::management::rbac::user user{ user_name };
    user.display_name = "display_name";
    user.password = "password";
    user.roles = {
        couchbase::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name },
    };

    {
        couchbase::operations::management::user_upsert_request req{};
        req.user = user;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::user_get_request req{ user_name };
        auto resp = retry_on_error(integration, req, couchbase::error::management_errc::user_not_found);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.user.roles.size() == 1);
        REQUIRE(resp.user.roles[0].name == "data_reader");
        REQUIRE(resp.user.roles[0].bucket == integration.ctx.bucket);
        REQUIRE(resp.user.roles[0].scope == scope_name);
    }

    user.roles = {
        couchbase::management::rbac::role{ "data_reader", integration.ctx.bucket, scope_name, collection_name },
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
    }
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
        req.bucket.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
        req.bucket.num_replicas = 0;
        auto resp = test::utils::execute(integration.cluster, req);
    }

    REQUIRE(!wait_for_bucket_created(integration, bucket_name).ctx.ec);

    SECTION("primary index")
    {
        {
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed = test::utils::wait_until([&integration, &bucket_name, &resp]() {
                couchbase::operations::management::query_index_create_request req{};
                req.bucket_name = bucket_name;
                req.is_primary = true;
                resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
            });
            REQUIRE(operation_completed);
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
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed = test::utils::wait_until([&integration, &bucket_name, &index_name, &resp]() {
                couchbase::operations::management::query_index_create_request req{};
                req.bucket_name = bucket_name;
                req.index_name = index_name;
                req.fields = { "field" };
                resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
            });
            REQUIRE(operation_completed);
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
            REQUIRE(resp.indexes[0].bucket_name == bucket_name);
            REQUIRE(resp.indexes[0].state == "online");
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
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed = test::utils::wait_until([&integration, &bucket_name, &index_name, &resp]() {
                couchbase::operations::management::query_index_create_request req{};
                req.bucket_name = bucket_name;
                req.index_name = index_name;
                req.fields = { "field" };
                req.deferred = true;
                resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
            });
            REQUIRE(operation_completed);
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

TEST_CASE("integration: collections query index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query_index_management()) {
        return;
    }
    if (!integration.cluster_version().supports_collections()) {
        return;
    }

    auto bucket_name = test::utils::uniq_id("collections_bucket");
    auto index_name = test::utils::uniq_id("collections_index");
    auto scope_name = test::utils::uniq_id("indexscope");
    auto collection_name = test::utils::uniq_id("indexcollection");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        req.bucket.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
        req.bucket.num_replicas = 0;
        auto resp = test::utils::execute(integration.cluster, req);
    }

    REQUIRE(!wait_for_bucket_created(integration, bucket_name).ctx.ec);

    // create the scope and collection that we'll do index management on.
    {
        couchbase::operations::management::scope_create_request req{ bucket_name, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::collection_create_request req{ bucket_name, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    SECTION("primary index")
    {
        {
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed = test::utils::wait_until([&integration, &bucket_name, &scope_name, &collection_name, &resp]() {
                couchbase::operations::management::query_index_create_request req{};
                req.bucket_name = bucket_name;
                req.scope_name = scope_name;
                req.collection_name = collection_name;
                req.is_primary = true;
                resp = test::utils::execute(integration.cluster, req);
                return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
            });
            REQUIRE(operation_completed);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
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
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed =
              test::utils::wait_until([&integration, &bucket_name, &index_name, &scope_name, &collection_name, &resp]() {
                  couchbase::operations::management::query_index_create_request req{};
                  req.bucket_name = bucket_name;
                  req.index_name = index_name;
                  req.scope_name = scope_name;
                  req.collection_name = collection_name;
                  req.fields = { "field" };
                  resp = test::utils::execute(integration.cluster, req);
                  return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
              });
            REQUIRE(operation_completed);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            req.fields = { "field" };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::operations::management::query_index_create_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            req.fields = { "field" };
            req.ignore_if_exists = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.indexes.size() == 1);
            REQUIRE(resp.indexes[0].name == index_name);
            REQUIRE_FALSE(resp.indexes[0].is_primary);
            REQUIRE(resp.indexes[0].index_key.size() == 1);
            REQUIRE(resp.indexes[0].index_key[0] == "`field`");
            REQUIRE(resp.indexes[0].collection_name == collection_name);
            REQUIRE(resp.indexes[0].scope_name == scope_name);
            REQUIRE(resp.indexes[0].bucket_name == bucket_name);
            REQUIRE(resp.indexes[0].state == "online");
        }

        {
            couchbase::operations::management::query_index_drop_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_drop_request req{};
            req.bucket_name = bucket_name;
            req.index_name = index_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }
    }

    SECTION("deferred index")
    {
        {
            couchbase::operations::management::query_index_create_response resp;
            bool operation_completed =
              test::utils::wait_until([&integration, &bucket_name, &index_name, &scope_name, &collection_name, &resp]() {
                  couchbase::operations::management::query_index_create_request req{};
                  req.bucket_name = bucket_name;
                  req.index_name = index_name;
                  req.scope_name = scope_name;
                  req.collection_name = collection_name;
                  req.fields = { "field" };
                  req.deferred = true;
                  resp = test::utils::execute(integration.cluster, req);
                  return resp.ctx.ec != couchbase::error::common_errc::bucket_not_found;
              });
            REQUIRE(operation_completed);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.indexes.size() == 1);
            REQUIRE(resp.indexes[0].name == index_name);
            REQUIRE(resp.indexes[0].state == "deferred");
        }

        {
            couchbase::operations::management::query_index_build_deferred_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        test::utils::wait_until([&integration, bucket_name, scope_name, collection_name]() {
            couchbase::operations::management::query_index_get_all_request req{};
            req.bucket_name = bucket_name;
            req.scope_name = scope_name;
            req.collection_name = collection_name;
            auto resp = test::utils::execute(integration.cluster, req);
            if (resp.indexes.empty()) {
                return false;
            }
            return resp.indexes[0].state == "online";
        });
    }

    SECTION("create missing collection")
    {
        couchbase::operations::management::query_index_create_request req{};
        req.bucket_name = bucket_name;
        req.scope_name = scope_name;
        req.collection_name = "missing_collection";
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::collection_not_found);
    }

    SECTION("create missing scope")
    {
        couchbase::operations::management::query_index_create_request req{};
        req.bucket_name = bucket_name;
        req.scope_name = "missing_scope";
        req.collection_name = collection_name;
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::scope_not_found);
    }

    SECTION("get missing collection")
    {
        couchbase::operations::management::query_index_get_all_request req{};
        req.bucket_name = bucket_name;
        req.scope_name = scope_name;
        req.collection_name = "missing_collection";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.indexes.empty());
    }

    SECTION("drop missing collection")
    {
        couchbase::operations::management::query_index_drop_request req{};
        req.bucket_name = bucket_name;
        req.scope_name = scope_name;
        req.collection_name = "missing_collection";
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::collection_not_found);
    }

    SECTION("drop missing scope")
    {
        couchbase::operations::management::query_index_drop_request req{};
        req.bucket_name = bucket_name;
        req.scope_name = "missing_scope";
        req.collection_name = collection_name;
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::scope_not_found);
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

    if (!integration.cluster_version().supports_analytics() || !integration.has_analytics_service()) {
        return;
    }

    // MB-47718
    if (integration.storage_backend() == couchbase::management::cluster::bucket_storage_backend::magma) {
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
        couchbase::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::s3_external_link> req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::s3_external_link> req{};
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
        couchbase::management::analytics::s3_external_link link{};
        link.dataverse = dataverse_name;
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "eu-west-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<couchbase::management::analytics::s3_external_link> req{};
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
        couchbase::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::azure_blob_external_link> req{};
        req.link = link;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::azure_blob_external_link> req{};
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
        couchbase::management::analytics::azure_blob_external_link link{};
        link.dataverse = dataverse_name;
        link.connection_string = "connection_string";
        link.blob_endpoint = "new_blob_endpoint";
        link.endpoint_suffix = "endpoint_suffix";
        link.link_name = link_name;
        couchbase::operations::management::analytics_link_replace_request<couchbase::management::analytics::azure_blob_external_link> req{};
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

    if (!integration.cluster_version().supports_analytics_links() || !integration.has_analytics_service()) {
        return;
    }

    // MB-47718
    if (integration.storage_backend() == couchbase::management::cluster::bucket_storage_backend::magma) {
        return;
    }

    // MB-40198
    if (!integration.cluster_version().supports_analytics_links_cert_auth() && integration.origin.credentials().uses_certificate()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto link_name = test::utils::uniq_id("link");

    SECTION("missing dataverse")
    {
        couchbase::management::analytics::s3_external_link link{};
        link.dataverse = "missing_dataverse";
        link.access_key_id = "access_key";
        link.secret_access_key = "secret_access_key";
        link.region = "us-east-1";
        link.service_endpoint = "service_endpoint";
        link.link_name = link_name;

        {
            couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::s3_external_link> req{};
            req.link = link;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::analytics_errc::dataverse_not_found);
        }
    }

    SECTION("missing argument")
    {
        couchbase::operations::management::analytics_link_create_request<couchbase::management::analytics::s3_external_link> req{};
        req.link = couchbase::management::analytics::s3_external_link{};
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

TEST_CASE("integration: search index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("search indexes crud")
    {
        auto index1_name = test::utils::uniq_id("index");
        auto index2_name = test::utils::uniq_id("index");
        auto alias_name = test::utils::uniq_id("alias");

        {
            couchbase::management::search::index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::management::search::index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::management::search::index index;
            index.name = index2_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            index.plan_params_json = R"({ "indexPartition": 3 })";
            index.params_json = R"({ "store": { "indexType": "upside_down", "kvStoreName": "moss" }})";
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::management::search::index index;
            index.name = alias_name;
            index.type = "fulltext-alias";
            index.source_type = "nil";
            index.params_json = couchbase::utils::json::generate(
              tao::json::value{ { "targets", { { index1_name, tao::json::empty_object }, { index2_name, tao::json::empty_object } } } });
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_get_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.index.name == index1_name);
            REQUIRE(resp.index.type == "fulltext-index");
        }

        {
            couchbase::operations::management::search_index_get_request req{};
            req.index_name = "missing_index";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }

        {
            couchbase::operations::management::search_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = index2_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = alias_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::operations::management::search_index_drop_request req{};
        req.index_name = "missing_index";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
    }

    SECTION("upsert index no name")
    {
        couchbase::management::search::index index;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    SECTION("control")
    {
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::management::search::index index;
            index.name = index_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        SECTION("ingest control")
        {
            {
                couchbase::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        SECTION("query control")
        {
            {
                couchbase::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        SECTION("partition control")
        {
            {
                couchbase::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        couchbase::operations::management::search_index_drop_request req{};
        req.index_name = index_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

bool
wait_for_search_pindexes_ready(test::utils::integration_test_guard& integration, const std::string& index_name)
{
    return test::utils::wait_until(
      [&integration, &index_name]() {
          couchbase::operations::management::search_index_stats_request req{};
          auto resp = test::utils::execute(integration.cluster, req);
          if (resp.ctx.ec || resp.stats.empty()) {
              return false;
          }
          auto stats = couchbase::utils::json::parse(resp.stats);
          const auto* num_pindexes_actual = stats.find(fmt::format("{}:{}:num_pindexes_actual", integration.ctx.bucket, index_name));
          if (num_pindexes_actual == nullptr || !num_pindexes_actual->is_number()) {
              return false;
          }
          const auto* num_pindexes_target = stats.find(fmt::format("{}:{}:num_pindexes_target", integration.ctx.bucket, index_name));
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

    if (!integration.cluster_version().supports_search_analyze()) {
        return;
    }

    auto index_name = test::utils::uniq_id("index");

    {
        couchbase::management::search::index index;
        index.name = index_name;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    REQUIRE(wait_for_search_pindexes_ready(integration, index_name));

    couchbase::operations::management::search_index_analyze_document_response resp;
    bool operation_completed = test::utils::wait_until([&integration, &index_name, &resp]() {
        couchbase::operations::management::search_index_analyze_document_request req{};
        req.index_name = index_name;
        req.encoded_document = R"({ "name": "hello world" })";
        resp = test::utils::execute(integration.cluster, req);
        return resp.ctx.ec != couchbase::error::common_errc::internal_server_failure;
    });
    REQUIRE(operation_completed);
    REQUIRE_FALSE(resp.ctx.ec);
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
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::key_value;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    if (integration.cluster_version().supports_analytics() && integration.has_analytics_service()) {
        SECTION("analytics")
        {
            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::analytics;
            req.method = "GET";
            req.path = "/admin/ping";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.body.empty());
            INFO(resp.body)
            auto result = couchbase::utils::json::parse(resp.body);
            REQUIRE(result.is_object());
        }
    }

    SECTION("search")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::search;
        req.method = "GET";
        req.path = "/api/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE(resp.body.empty());
        REQUIRE_FALSE(resp.headers.empty());
        REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
    }

    SECTION("query")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::query;
        req.method = "GET";
        req.path = "/admin/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        INFO(resp.body)
        auto result = couchbase::utils::json::parse(resp.body);
        REQUIRE(result.is_object());
    }

    SECTION("view")
    {
        auto document_name = test::utils::uniq_id("design_document");
        auto view_name = test::utils::uniq_id("view");

        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::view;
        req.method = "POST";
        req.path = fmt::format("/{}/_design/{}/_view/{}", integration.ctx.bucket, document_name, view_name);
        req.body = R"({"keys":["foo","bar"]})";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 404);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::utils::json::parse(resp.body);
        INFO(resp.body)
        REQUIRE(result["error"].get_string() == "not_found");
    }

    SECTION("management")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::management;
        req.method = "GET";
        req.path = "/pools";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::utils::json::parse(resp.body);
        INFO(resp.body)
        REQUIRE(result.find("uuid") != nullptr);
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("create scope")
        {
            auto scope_name = test::utils::uniq_id("freeform_scope");

            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::management;
            req.method = "POST";
            req.path = fmt::format("/pools/default/buckets/{}/scopes", integration.ctx.bucket);
            req.headers["content-type"] = "application/x-www-form-urlencoded";
            req.body = fmt::format("name={}", couchbase::utils::string_codec::form_encode(scope_name));
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.headers.empty());
            REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
            auto result = couchbase::utils::json::parse(resp.body);
            REQUIRE(result.is_object());
            REQUIRE(result.find("uid") != nullptr);
        }
    }

    if (integration.cluster_version().supports_eventing_functions() && integration.has_eventing_service()) {
        SECTION("eventing")
        {

            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::eventing;
            req.method = "GET";
            req.path = "/api/v1/functions";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.body.empty());
            auto result = couchbase::utils::json::parse(resp.body);
            INFO(resp.body)
            REQUIRE(result.is_array());
        }
    }
}

static couchbase::operations::management::eventing_get_function_response
wait_for_function_created(test::utils::integration_test_guard& integration, const std::string& function_name)
{
    couchbase::operations::management::eventing_get_function_response resp{};
    test::utils::wait_until([&integration, &resp, function_name]() {
        couchbase::operations::management::eventing_get_function_request req{ function_name };
        resp = test::utils::execute(integration.cluster, req);
        return !resp.ctx.ec;
    });
    return resp;
}

static bool
wait_for_function_reach_status(test::utils::integration_test_guard& integration,
                               const std::string& function_name,
                               couchbase::management::eventing::function_status status)
{
    return test::utils::wait_until(
      [&integration, function_name, status]() {
          couchbase::operations::management::eventing_get_status_request req{};
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

    if (!integration.cluster_version().supports_eventing_functions() || !integration.has_eventing_service()) {
        return;
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("lifecycle")
    {
        auto function_name = test::utils::uniq_id("name");

        {
            couchbase::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            if (integration.cluster_version().is_cheshire_cat()) {
                REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_not_deployed);
            } else {
                REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_not_found);
            }
        }

        {
            couchbase::operations::management::eventing_get_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_not_found);
        }

        auto meta_bucket_name = test::utils::uniq_id("meta");
        {

            couchbase::management::cluster::bucket_settings bucket_settings;
            bucket_settings.name = meta_bucket_name;
            bucket_settings.ram_quota_mb = 256;

            {
                couchbase::operations::management::bucket_create_request req;
                req.bucket = bucket_settings;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        {
            auto resp = wait_for_bucket_created(integration, meta_bucket_name);
            REQUIRE_FALSE(resp.ctx.ec);
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
            couchbase::operations::management::eventing_upsert_function_request req{};
            req.function.source_keyspace.bucket = integration.ctx.bucket;
            req.function.metadata_keyspace.bucket = meta_bucket_name;
            req.function.name = function_name;
            req.function.code = source_code;
            req.function.settings.handler_headers = { "// generated by Couchbase C++ SDK" };
            req.function.constant_bindings.emplace_back(couchbase::management::eventing::function_constant_binding{ "PI", "3.14" });
            req.function.bucket_bindings.emplace_back(couchbase::management::eventing::function_bucket_binding{
              "data", { integration.ctx.bucket }, couchbase::management::eventing::function_bucket_access::read_write });
            req.function.url_bindings.emplace_back(
              couchbase::management::eventing::function_url_binding{ "home", "https://couchbase.com" });
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            auto resp = wait_for_function_created(integration, function_name);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::eventing_get_all_functions_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            auto function = std::find_if(
              resp.functions.begin(), resp.functions.end(), [function_name](const auto& fun) { return function_name == fun.name; });
            REQUIRE(function != resp.functions.end());
            REQUIRE(function->code == source_code);
            REQUIRE(function->source_keyspace.bucket == integration.ctx.bucket);
            REQUIRE(function->metadata_keyspace.bucket == meta_bucket_name);
            REQUIRE(function->settings.deployment_status == couchbase::management::eventing::function_deployment_status::undeployed);
            REQUIRE(function->settings.processing_status == couchbase::management::eventing::function_processing_status::paused);
            REQUIRE(!function->settings.handler_headers.empty());
            REQUIRE(function->settings.handler_headers[0] == "// generated by Couchbase C++ SDK");
            REQUIRE(!function->constant_bindings.empty());
            REQUIRE(function->constant_bindings[0].alias == "PI");
            REQUIRE(function->constant_bindings[0].literal == "3.14");
            REQUIRE(!function->bucket_bindings.empty());
            REQUIRE(function->bucket_bindings[0].alias == "data");
            REQUIRE(function->bucket_bindings[0].name.bucket == "default");
            REQUIRE(function->bucket_bindings[0].access == couchbase::management::eventing::function_bucket_access::read_write);
            REQUIRE(!function->url_bindings.empty());
            REQUIRE(function->url_bindings[0].alias == "home");
            REQUIRE(function->url_bindings[0].hostname == "https://couchbase.com");
            REQUIRE(std::holds_alternative<couchbase::management::eventing::function_url_no_auth>(function->url_bindings[0].auth));
        }

        {
            couchbase::operations::management::eventing_get_status_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status.num_eventing_nodes > 0);
            auto function = std::find_if(resp.status.functions.begin(), resp.status.functions.end(), [function_name](const auto& fun) {
                return function_name == fun.name;
            });
            REQUIRE(function != resp.status.functions.end());
            REQUIRE(function->status == couchbase::management::eventing::function_status::undeployed);
            REQUIRE(function->deployment_status == couchbase::management::eventing::function_deployment_status::undeployed);
            REQUIRE(function->processing_status == couchbase::management::eventing::function_processing_status::paused);
        }

        {
            couchbase::operations::management::eventing_undeploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_not_deployed);
        }

        {
            couchbase::operations::management::eventing_deploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        REQUIRE(wait_for_function_reach_status(integration, function_name, couchbase::management::eventing::function_status::deployed));

        {
            couchbase::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_deployed);
        }

        {
            couchbase::operations::management::eventing_resume_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_deployed);
        }

        {
            couchbase::operations::management::eventing_pause_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        REQUIRE(wait_for_function_reach_status(integration, function_name, couchbase::management::eventing::function_status::paused));

        {
            couchbase::operations::management::eventing_pause_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_paused);
        }

        {
            couchbase::operations::management::eventing_resume_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        REQUIRE(wait_for_function_reach_status(integration, function_name, couchbase::management::eventing::function_status::deployed));

        {
            couchbase::operations::management::eventing_undeploy_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        REQUIRE(wait_for_function_reach_status(integration, function_name, couchbase::management::eventing::function_status::undeployed));

        {
            couchbase::operations::management::eventing_drop_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::eventing_get_function_request req{ function_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::management_errc::eventing_function_not_found);
        }

        {
            couchbase::operations::management::bucket_drop_request req{ meta_bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }
}
