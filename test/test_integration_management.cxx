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
#include <couchbase/operations/management/collections.hxx>

TEST_CASE("integration: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto created = test::utils::wait_until([&integration, bucket_name]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return !resp.ctx.ec;
        });
        REQUIRE(created);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets > 0);
    }

    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until([&integration, bucket_name]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return resp.ctx.ec == couchbase::error::common_errc::bucket_not_found;
        });
        REQUIRE(dropped);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets == 0);
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

TEST_CASE("native: collection management", "[native]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_collections()) {
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
