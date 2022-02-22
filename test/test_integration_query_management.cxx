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
#include <couchbase/operations/management/query.hxx>

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

    REQUIRE(!test::utils::wait_for_bucket_created(integration.cluster, bucket_name).ctx.ec);

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
