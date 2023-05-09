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

#include "core/operations/management/collections.hxx"

TEST_CASE("integration: missing scope and collection", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    SECTION("get missing scope")
    {
        couchbase::core::document_id id{ integration.ctx.bucket, "missing_scope", "_default", "key" };
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::scope_not_found);
    }

    SECTION("insert missing scope")
    {
        couchbase::core::document_id id{ integration.ctx.bucket, "missing_scope", "_default", "key" };
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::json::generate_binary(value) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::scope_not_found);
    }

    SECTION("get missing collection")
    {
        couchbase::core::document_id id{ integration.ctx.bucket, "_default", "missing_collection", "key" };
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::unambiguous_timeout);
        REQUIRE(resp.ctx.retried_because_of(couchbase::retry_reason::key_value_collection_outdated));
    }

    SECTION("insert missing collection")
    {
        couchbase::core::document_id id{ integration.ctx.bucket, "_default", "missing_collection", "key" };
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::json::generate_binary(value) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::ambiguous_timeout);
        REQUIRE(resp.ctx.retried_because_of(couchbase::retry_reason::key_value_collection_outdated));
    }
}

TEST_CASE("integration: get and insert non default scope and collection", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("scope");
    auto key = test::utils::uniq_id("foo");
    auto id = couchbase::core::document_id{ integration.ctx.bucket, scope_name, collection_name, key };

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

    {
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary(key) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary(key));
    }
}

TEST_CASE("integration: insert into dropped scope", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("scope");
    auto key = test::utils::uniq_id("foo");
    auto id = couchbase::core::document_id{ integration.ctx.bucket, scope_name, collection_name, key };

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

    {
        couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary(key) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.value == couchbase::core::utils::to_binary(key));
    }

    {
        couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto dropped = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(dropped);
    }

    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES does not generate error when inserting into dropped collection. See "
             "https://github.com/couchbaselabs/gocaves/issues/108");
    }
    {
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(key) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::common::scope_not_found);
    }
}
