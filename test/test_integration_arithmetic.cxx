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

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>

TEST_CASE("integration: increment", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("counter") };

    SECTION("key exists")
    {
        {
            couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary("0") };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        for (uint64_t expected = 2; expected <= 20; expected += 2) {
            couchbase::core::operations::increment_request req{ id };
            req.delta = 2;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.content == expected);
        }
    }

    SECTION("initial value")
    {
        couchbase::core::operations::increment_request req{ id };
        req.delta = 2;
        req.initial_value = 10;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.content == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            couchbase::core::operations::increment_request req{ id };
            req.initial_value = 2;
            req.durability_level = couchbase::durability_level::persist_to_majority;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.content == 2);
        }
    }
}

TEST_CASE("integration: increment with public API", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("counter");

    SECTION("key exists")
    {
        {
            const auto ascii_zero = couchbase::core::utils::to_binary("0");
            auto [ctx, resp] = collection.insert<couchbase::codec::raw_binary_transcoder>(id, ascii_zero, {}).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE_FALSE(resp.cas().empty());
        }

        for (uint64_t expected = 2; expected <= 20; expected += 2) {
            auto [ctx, resp] = collection.binary().increment(id, couchbase::increment_options{}.delta(2)).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(resp.content() == expected);
        }
    }

    SECTION("initial value")
    {
        auto [ctx, resp] = collection.binary().increment(id, couchbase::increment_options{}.delta(2).initial(10)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(resp.content() == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            auto [ctx, resp] =
              collection.binary()
                .increment(id, couchbase::increment_options{}.initial(2).durability(couchbase::durability_level::persist_to_majority))
                .get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(resp.content() == 2);
        }
    }
}

TEST_CASE("integration: decrement", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("counter") };

    SECTION("key exists")
    {
        {
            couchbase::core::operations::insert_request req{ id, couchbase::core::utils::to_binary("20") };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
        }

        for (uint64_t expected = 18; expected > 0; expected -= 2) {
            couchbase::core::operations::decrement_request req{ id };
            req.delta = 2;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.content == expected);
        }
    }

    SECTION("initial value")
    {
        couchbase::core::operations::decrement_request req{ id };
        req.delta = 2;
        req.initial_value = 10;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.content == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            couchbase::core::operations::decrement_request req{ id };
            req.initial_value = 2;
            req.durability_level = couchbase::durability_level::persist_to_majority;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec());
            REQUIRE(resp.content == 2);
        }
    }
}

TEST_CASE("integration: decrement with public API", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    auto id = test::utils::uniq_id("counter");

    SECTION("key exists")
    {
        {
            const auto ascii_twenty = couchbase::core::utils::to_binary("20");
            auto [ctx, resp] = collection.insert<couchbase::codec::raw_binary_transcoder>(id, ascii_twenty, {}).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE_FALSE(resp.cas().empty());
        }

        for (uint64_t expected = 18; expected > 0; expected -= 2) {
            auto [ctx, resp] = collection.binary().decrement(id, couchbase::decrement_options{}.delta(2)).get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(resp.content() == expected);
        }
    }

    SECTION("initial value")
    {
        auto [ctx, resp] = collection.binary().decrement(id, couchbase::decrement_options{}.delta(2).initial(10)).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(resp.content() == 10);
    }

    if (integration.cluster_version().supports_enhanced_durability()) {
        SECTION("durability")
        {
            auto [ctx, resp] =
              collection.binary()
                .decrement(id, couchbase::decrement_options{}.initial(2).durability(couchbase::durability_level::persist_to_majority))
                .get();
            REQUIRE_SUCCESS(ctx.ec());
            REQUIRE(resp.content() == 2);
        }
    }
}
