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

TEST_CASE("integration: increment", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("counter") };

    SECTION("key exists")
    {
        {
            couchbase::operations::insert_request req{ id, "0" };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        for (uint64_t expected = 2; expected <= 20; expected += 2) {
            couchbase::operations::increment_request req{ id };
            req.delta = 2;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.content == expected);
        }
    }

    SECTION("initial value")
    {
        couchbase::operations::increment_request req{ id };
        req.delta = 2;
        req.initial_value = 10;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.content == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            couchbase::operations::increment_request req{ id };
            req.initial_value = 2;
            req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.content == 2);
        }
    }
}

TEST_CASE("integration: decrement", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("counter") };

    SECTION("key exists")
    {
        {
            couchbase::operations::insert_request req{ id, "20" };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        for (uint64_t expected = 18; expected > 0; expected -= 2) {
            couchbase::operations::decrement_request req{ id };
            req.delta = 2;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.content == expected);
        }
    }

    SECTION("initial value")
    {
        couchbase::operations::decrement_request req{ id };
        req.delta = 2;
        req.initial_value = 10;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.content == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            couchbase::operations::decrement_request req{ id };
            req.initial_value = 2;
            req.durability_level = couchbase::protocol::durability_level::persist_to_majority;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.content == 2);
        }
    }
}
