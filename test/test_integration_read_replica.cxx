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

#include "test_helper_integration.hxx"
#include "utils/move_only_context.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/get_any_replica_options.hxx>

static const tao::json::value basic_doc = {
    { "a", 1.0 },
    { "b", 2.0 },
};
static const std::vector<std::byte> basic_doc_json = couchbase::core::utils::json::generate_binary(basic_doc);

//! [smuggling-transcoder]
struct smuggling_transcoder {
    using document_type = std::pair<std::vector<std::byte>, std::uint32_t>;

    static auto decode(const couchbase::codec::encoded_value& encoded) -> document_type
    {
        return { encoded.data, encoded.flags };
    }
};
template<>
struct couchbase::codec::is_transcoder<smuggling_transcoder> : public std::true_type {
};
//! [smuggling-transcoder]

TEST_CASE("unit: get any replica result with custom coder", "[integration]")
{
    couchbase::get_replica_result result{
        couchbase::cas{ 0 },
        true,
        { { std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xaf } }, 0xcafebebe },
    };

    // clang-format off
    //! [smuggling-transcoder-usage]
    auto [data, flags] = result.content_as<smuggling_transcoder>();
    //! [smuggling-transcoder-usage]
    // clang-format on

    REQUIRE(flags == 0xcafebebe);
    REQUIRE(data == std::vector{ std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xaf } });
}

TEST_CASE("integration: get any replica", "[integration]")
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

    std::string scope_name{ "_default" };
    std::string collection_name{ "_default" };
    std::string key = test::utils::uniq_id("get_any_replica");

    {
        couchbase::core::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };

        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        auto collection =
          couchbase::cluster(integration.cluster).bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
        auto [ctx, result] = collection.get_any_replica(key, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(result.content_as<smuggling_transcoder>().first == basic_doc_json);
    }
}

TEST_CASE("integration: get all replicas", "[integration]")
{
    test::utils::integration_test_guard integration;

    auto number_of_replicas = integration.number_of_replicas();
    if (number_of_replicas == 0) {
        SKIP("bucket has zero replicas");
    }
    if (integration.number_of_nodes() <= number_of_replicas) {
        SKIP(fmt::format(
          "number of nodes ({}) is less or equal to number of replicas ({})", integration.number_of_nodes(), number_of_replicas));
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    std::string scope_name{ "_default" };
    std::string collection_name{ "_default" };
    std::string key = test::utils::uniq_id("get_all_replica");

    {
        couchbase::core::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };

        couchbase::core::operations::insert_request req{ id, basic_doc_json };
        req.durability_level = couchbase::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    if (integration.cluster_version().is_mock()) {
        // GOCAVES does not implement syncDurability. See https://github.com/couchbaselabs/gocaves/issues/109
        std::this_thread::sleep_for(std::chrono::seconds{ 1 });
    }

    {
        auto collection =
          couchbase::cluster(integration.cluster).bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
        auto [ctx, result] = collection.get_all_replicas(key, {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE(result.size() == number_of_replicas + 1);
        auto responses_from_active = std::count_if(result.begin(), result.end(), [](const auto& r) { return !r.is_replica(); });
        REQUIRE(responses_from_active == 1);
    }
}

TEST_CASE("integration: get all replicas with missing key", "[integration]")
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

    std::string scope_name{ "_default" };
    std::string collection_name{ "_default" };
    std::string key = test::utils::uniq_id("get_all_replica_missing_key");

    {
        auto collection =
          couchbase::cluster(integration.cluster).bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
        auto [ctx, result] = collection.get_all_replicas(key, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_not_found);
        REQUIRE(result.empty());
    }
}

TEST_CASE("integration: get any replica with missing key", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (integration.number_of_nodes() <= integration.number_of_replicas()) {
        SKIP(fmt::format("number of nodes ({}) is less or equal to number of replicas ({})",
                         integration.number_of_nodes(),
                         integration.number_of_replicas()));
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    std::string scope_name{ "_default" };
    std::string collection_name{ "_default" };
    std::string key = test::utils::uniq_id("get_any_replica_missing_key");

    {
        auto collection =
          couchbase::cluster(integration.cluster).bucket(integration.ctx.bucket).scope(scope_name).collection(collection_name);
        auto [ctx, result] = collection.get_any_replica(key, {}).get();
        REQUIRE(ctx.ec() == couchbase::errc::key_value::document_irretrievable);
    }
}

TEST_CASE("integration: get any replica low-level version", "[integration]")
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

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::json::generate_binary(value) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::get_any_replica_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.value == couchbase::core::utils::to_binary(R"({"a":1.0,"b":2.0})"));
    }
}

TEST_CASE("integration: get all replicas low-level version", "[integration]")
{
    test::utils::integration_test_guard integration;

    auto number_of_replicas = integration.number_of_replicas();
    if (number_of_replicas == 0) {
        SKIP("bucket has zero replicas");
    }
    if (integration.number_of_nodes() <= number_of_replicas) {
        SKIP(fmt::format(
          "number of nodes ({}) is less or equal to number of replicas ({})", integration.number_of_nodes(), number_of_replicas));
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
    }

    if (integration.cluster_version().is_mock()) {
        // GOCAVES does not implement syncDurability. See https://github.com/couchbaselabs/gocaves/issues/109
        std::this_thread::sleep_for(std::chrono::seconds{ 1 });
    }

    {
        couchbase::core::operations::get_all_replicas_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(resp.entries.size() == number_of_replicas + 1);
        auto responses_from_active = std::count_if(resp.entries.begin(), resp.entries.end(), [](const auto& r) { return !r.replica; });
        REQUIRE(responses_from_active == 1);
    }
}
