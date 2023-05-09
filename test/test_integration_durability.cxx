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

#include "profile.hxx"

#include <couchbase/cluster.hxx>

TEST_CASE("integration: durable operations", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_enhanced_durability()) {
        SKIP("cluster does not support enhanced durability");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::json::generate_binary(value) };
        req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        const tao::json::value value = {
            { "foo", "bar" },
        };
        couchbase::core::operations::replace_request req{ id, couchbase::core::utils::json::generate_binary(value) };
        req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::mutate_in_request req{ id };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("baz", 42) }.specs();
        req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(couchbase::core::utils::json::parse_binary(resp.value) == couchbase::core::utils::json::parse(R"({"foo":"bar","baz":42})"));
    }
    {
        couchbase::core::operations::remove_request req{ id };
        req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
}

TEST_CASE("integration: legacy durability persist to active and replicate to one", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (integration.number_of_replicas() == 0) {
        SKIP("bucket has zero replicas");
    }
    if (integration.number_of_nodes() <= integration.number_of_replicas()) {
        SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                         integration.number_of_nodes(),
                         integration.number_of_replicas()));
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    std::string key = test::utils::uniq_id("upsert_legacy");

    auto collection = couchbase::cluster(integration.cluster)
                        .bucket(integration.ctx.bucket)
                        .scope(couchbase::scope::default_name)
                        .collection(couchbase::collection::default_name);

    couchbase::cas cas{};

    {
        profile fry{ "fry", "Philip J. Fry", 1974 };
        auto options = couchbase::upsert_options{}.durability(couchbase::persist_to::active, couchbase::replicate_to::one);
        auto [ctx, result] = collection.upsert(key, fry, options).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(result.cas().empty());
        REQUIRE(result.mutation_token().has_value());
        cas = result.cas();
    }

    {
        auto [ctx, result] = collection.get(key, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(result.cas() == cas);
        auto fry = result.content_as<profile>();
        REQUIRE(fry.username == "fry");
    }
}

TEST_CASE("integration: low level legacy durability impossible if number of nodes too high", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (integration.number_of_replicas() == 3) {
        SKIP("bucket has three replicas configured, so the test will not be applicable");
    }

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    const tao::json::value value = {
        { "a", 1.0 },
        { "b", 2.0 },
    };
    {
        couchbase::core::operations::upsert_request_with_legacy_durability req{
            { id, couchbase::core::utils::json::generate_binary(value) },
            couchbase::persist_to::four,
            couchbase::replicate_to::one,
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::durability_impossible);
    }
    {
        couchbase::core::operations::upsert_request_with_legacy_durability req{
            { id, couchbase::core::utils::json::generate_binary(value) },
            couchbase::persist_to::active,
            couchbase::replicate_to::three,
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::durability_impossible);
    }
}

TEST_CASE("integration: low level legacy durability persist to active and replicate to one", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (integration.number_of_replicas() < 1) {
        SKIP("bucket does not have replicas configured");
    }

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::core::operations::upsert_request_with_legacy_durability req{
            { id, couchbase::core::utils::json::generate_binary(value) },
            couchbase::persist_to::active,
            couchbase::replicate_to::one,
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }

    {
        const tao::json::value value = {
            { "foo", "bar" },
        };
        couchbase::core::operations::replace_request_with_legacy_durability req{
            { id, couchbase::core::utils::json::generate_binary(value) },
            couchbase::persist_to::active,
            couchbase::replicate_to::one,
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::mutate_in_request_with_legacy_durability req{
            { id },
            couchbase::persist_to::active,
            couchbase::replicate_to::one,
        };
        req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("baz", 42) }.specs();

        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(couchbase::core::utils::json::parse_binary(resp.value) == couchbase::core::utils::json::parse(R"({"foo":"bar","baz":42})"));
    }
    {
        couchbase::core::operations::remove_request_with_legacy_durability req{
            { id },
            couchbase::persist_to::active,
            couchbase::replicate_to::one,
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
}
