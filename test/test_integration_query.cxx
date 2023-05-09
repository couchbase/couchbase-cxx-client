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

#include "utils/move_only_context.hxx"

#include "core/operations/management/collections.hxx"
#include "core/operations/management/query.hxx"
#include "couchbase/codec/binary_noop_serializer.hxx"
#include "couchbase/codec/raw_binary_transcoder.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/lookup_in_specs.hxx>

TEST_CASE("integration: trivial non-data query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }
}

TEST_CASE("integration: query with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
        auto barrier = std::make_shared<std::promise<couchbase::core::operations::query_response>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        auto handler = [barrier, ctx = std::move(ctx)](couchbase::core::operations::query_response&& resp) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(std::move(resp));
        };
        integration.cluster->execute(req, std::move(handler));
        auto resp = f.get();
        REQUIRE_SUCCESS(resp.ctx.ec);
    }
}

TEST_CASE("integration: query on a collection", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");
    auto index_name = test::utils::uniq_id("index");
    auto key = test::utils::uniq_id("foo");
    tao::json::value value = {
        { "a", 1.0 },
        { "b", 2.0 },
    };
    auto json = couchbase::core::utils::json::generate_binary(value);

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
        couchbase::core::operations::management::query_index_create_request req{};
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.collection_name = collection_name;
        req.index_name = index_name;
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.http_body);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    couchbase::mutation_token mutation_token;

    {
        couchbase::core::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };
        couchbase::core::operations::insert_request req{ id, json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        mutation_token = resp.token;
    }

    SECTION("correct scope and collection")
    {
        couchbase::core::operations::query_request req{ fmt::format(
          R"(SELECT a, b FROM {} WHERE META().id = "{}")", collection_name, key) };
        req.query_context = fmt::format("default:`{}`.`{}`", integration.ctx.bucket, scope_name);
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
        REQUIRE(value == couchbase::core::utils::json::parse(resp.rows[0]));
    }

    SECTION("missing scope")
    {
        couchbase::core::operations::query_request req{ fmt::format(
          R"(SELECT a, b FROM {} WHERE META().id = "{}")", collection_name, key) };
        req.query_context = fmt::format("default:`{}`.`{}`", integration.ctx.bucket, "missing_scope");
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::query::index_failure);
    }

    SECTION("missing collection")
    {
        couchbase::core::operations::query_request req{ fmt::format(R"(SELECT a, b FROM missing_collection WHERE META().id = "{}")", key) };
        req.query_context = fmt::format("default:`{}`.`{}`", integration.ctx.bucket, scope_name);
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::query::index_failure);
    }

    SECTION("prepared")
    {
        couchbase::core::operations::query_request req{ fmt::format(
          R"(SELECT a, b FROM {} WHERE META().id = "{}")", collection_name, key) };
        req.query_context = fmt::format("default:`{}`.`{}`", integration.ctx.bucket, scope_name);
        req.mutation_state = { mutation_token };
        req.adhoc = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
        REQUIRE(value == couchbase::core::utils::json::parse(resp.rows[0]));
    }
}

TEST_CASE("integration: read only with no results", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{ fmt::format("SELECT * FROM {} LIMIT 0", integration.ctx.bucket) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows.empty());
    }
}

TEST_CASE("integration: invalid query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{ "I'm not n1ql" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::errc::common::parsing_failure);
    }
}

TEST_CASE("integration: preserve expiry for mutation query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_preserve_expiry_for_query()) {
        SKIP("cluster does not support support preserve expiry for query");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{
        integration.ctx.bucket,
        "_default",
        "_default",
        test::utils::uniq_id("preserve_expiry_for_query"),
    };

    uint32_t expiry = std::numeric_limits<uint32_t>::max();

    {
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(R"({"foo":42})") };
        req.expiry = expiry;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::expiry_time).xattr(),
              couchbase::lookup_in_specs::get("foo"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(test::utils::to_string(resp.fields[0].value)));
        REQUIRE(couchbase::core::utils::to_binary("42") == resp.fields[1].value);
    }

    {
        std::string statement = fmt::format("UPDATE {} AS b USE KEYS '{}' SET b.foo = 43", integration.ctx.bucket, id.key());
        couchbase::core::operations::query_request req{ statement };
        req.preserve_expiry = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
        couchbase::core::operations::lookup_in_request req{ id };
        req.specs =
          couchbase::lookup_in_specs{
              couchbase::lookup_in_specs::get(couchbase::subdoc::lookup_in_macro::expiry_time).xattr(),
              couchbase::lookup_in_specs::get("foo"),
          }
            .specs();
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(expiry == std::stoul(test::utils::to_string(resp.fields[0].value)));
        REQUIRE(couchbase::core::utils::to_binary("43") == resp.fields[1].value);
    }
}

TEST_CASE("integration: streaming query results", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
        std::vector<std::string> rows{};
        req.row_callback = [&rows](std::string&& row) {
            rows.emplace_back(std::move(row));
            return couchbase::core::utils::json::stream_control::next_row;
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == R"({"greeting":"ruby rules"})");
    }
}

TEST_CASE("integration: streaming query results with stop in the middle", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::query_request req{
            R"(SELECT * FROM  [{"tech": "C++"}, {"tech": "Ruby"}, {"tech": "Couchbase"}] AS data)"
        };
        std::vector<std::string> rows{};
        req.row_callback = [&rows](std::string&& row) {
            bool should_stop = row.find("Ruby") != std::string::npos;
            rows.emplace_back(std::move(row));
            if (should_stop) {
                return couchbase::core::utils::json::stream_control::stop;
            }
            return couchbase::core::utils::json::stream_control::next_row;
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(rows.size() == 2);
        REQUIRE(rows[0] == R"({"data":{"tech":"C++"}})");
        REQUIRE(rows[1] == R"({"data":{"tech":"Ruby"}})");
    }
}

TEST_CASE("integration: streaming analytics results", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
    }
    if (!integration.has_analytics_service()) {
        SKIP("cluster does not have analytics service");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::core::operations::analytics_request req{
            R"(SELECT * FROM  [{"tech": "C++"}, {"tech": "Ruby"}, {"tech": "Couchbase"}] AS data)"
        };
        std::vector<std::string> rows{};
        req.row_callback = [&rows](std::string&& row) {
            rows.emplace_back(std::move(row));
            return couchbase::core::utils::json::stream_control::next_row;
        };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(rows.size() == 3);
        REQUIRE(rows[0] == R"({ "data": { "tech": "C++" } })");
        REQUIRE(rows[1] == R"({ "data": { "tech": "Ruby" } })");
        REQUIRE(rows[2] == R"({ "data": { "tech": "Couchbase" } })");
    }
}

TEST_CASE("integration: sticking query to the service node", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    std::string node_to_stick_queries;
    {
        couchbase::core::operations::query_request req{ R"(SELECT 42 AS answer)" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
        REQUIRE(resp.rows[0] == R"({"answer":42})");
        REQUIRE_FALSE(resp.served_by_node.empty());
        node_to_stick_queries = resp.served_by_node;
    }

    if (integration.number_of_query_nodes() > 1) {
        std::vector<std::string> used_nodes{};
        std::mutex used_nodes_mutex{};

        std::vector<std::thread> threads;
        threads.reserve(10);
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([i, &cluster = integration.cluster, node_to_stick_queries, &used_nodes, &used_nodes_mutex]() {
                couchbase::core::operations::query_request req{ fmt::format(R"(SELECT {} AS answer)", i) };
                auto resp = test::utils::execute(cluster, req);
                if (resp.ctx.ec || resp.served_by_node.empty() || resp.rows.size() != 1 ||
                    resp.rows[0] != fmt::format(R"({{"answer":{}}})", i)) {
                    return;
                }
                std::scoped_lock lock(used_nodes_mutex);
                used_nodes.push_back(resp.served_by_node);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        REQUIRE(used_nodes.size() == 10);
        REQUIRE(std::set(used_nodes.begin(), used_nodes.end()).size() > 1);

        threads.clear();
        used_nodes.clear();

        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([i, &cluster = integration.cluster, node_to_stick_queries, &used_nodes, &used_nodes_mutex]() {
                couchbase::core::operations::query_request req{ fmt::format(R"(SELECT {} AS answer)", i) };
                req.send_to_node = node_to_stick_queries;
                auto resp = test::utils::execute(cluster, req);
                if (resp.ctx.ec || resp.served_by_node.empty() || resp.rows.size() != 1 ||
                    resp.rows[0] != fmt::format(R"({{"answer":{}}})", i)) {
                    return;
                }
                std::scoped_lock lock(used_nodes_mutex);
                used_nodes.push_back(resp.served_by_node);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        REQUIRE(used_nodes.size() == 10);
        REQUIRE(std::set(used_nodes.begin(), used_nodes.end()).size() == 1);
    }
}

TEST_CASE("analytics create dataset")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }
    if (!integration.has_analytics_service()) {
        SKIP("cluster does not have analytics service");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }
    couchbase::core::operations::analytics_request req{ fmt::format("CREATE DATAVERSE `{}`.`test-scope` IF NOT EXISTS",
                                                                    integration.ctx.bucket) };
    std::vector<std::string> rows{};
    req.row_callback = [&rows](std::string&& row) {
        rows.emplace_back(std::move(row));
        return couchbase::core::utils::json::stream_control::next_row;
    };

    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
}

TEST_CASE("integration: prepared query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    auto key = test::utils::uniq_id("foo");
    tao::json::value value = {
        { "a", 1.0 },
        { "b", 2.0 },
    };
    auto json = couchbase::core::utils::json::generate_binary(value);

    couchbase::mutation_token mutation_token;
    {
        couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", key };
        couchbase::core::operations::insert_request req{ id, json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        mutation_token = resp.token;
    }

    {
        couchbase::core::operations::query_request req{ fmt::format(
          R"(SELECT a, b FROM {} WHERE META().id = "{}")", integration.ctx.bucket, key) };

        req.mutation_state = { mutation_token };
        req.adhoc = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
        REQUIRE(value == couchbase::core::utils::json::parse(resp.rows[0]));
    }
}

TEST_CASE("integration: query with public API", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto cluster = couchbase::cluster(integration.cluster);

    {
        auto [ctx, resp] = cluster.query("SELECT 42 AS the_answer", {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0]["the_answer"] == 42);
    }

    {
        auto [ctx, resp] = cluster.query("SELECT 42 AS the_answer", {}).get();
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as<couchbase::codec::binary_noop_serializer>();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == couchbase::core::utils::to_binary("{\"the_answer\":42}"));
    }

    {
        couchbase::query_options options{};
        options.named_parameters(std::pair{ "a", 2 }, std::pair{ "b", 40 });
        auto [ctx, resp] = cluster.query("SELECT $a + $b AS the_answer", options).get();
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0]["the_answer"] == 42);
    }
}

TEST_CASE("integration: query from scope with public API", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query()) {
        SKIP("cluster does not support query");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto cluster = couchbase::cluster(integration.cluster);
    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("coll");
    auto key = test::utils::uniq_id("foo");
    auto id = couchbase::core::document_id{ integration.ctx.bucket, scope_name, collection_name, key };
    tao::json::value value = {
        { "a", 1.0 },
        { "b", 2.0 },
    };
    auto json = couchbase::core::utils::json::generate_binary(value);
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
        couchbase::core::operations::insert_request req{ id, json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    SECTION("correct scope and collection")
    {
        auto [ctx, resp] = cluster.bucket(integration.ctx.bucket)
                             .scope(scope_name)
                             .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key), {})
                             .get();
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0][collection_name] == value);
    }
    SECTION("missing scope")
    {
        auto [ctx, resp] = cluster.bucket(integration.ctx.bucket)
                             .scope("idontexist")
                             .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key), {})
                             .get();
        REQUIRE(ctx.ec() == couchbase::errc::query::index_failure);
    }
    SECTION("missing collection")
    {
        auto [ctx, resp] = cluster.bucket(integration.ctx.bucket)
                             .scope(scope_name)
                             .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", "idontexist", key), {})
                             .get();
        REQUIRE(ctx.ec() == couchbase::errc::query::index_failure);
    }
    SECTION("prepared")
    {
        auto [ctx, resp] =
          cluster.bucket(integration.ctx.bucket)
            .scope(scope_name)
            .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key), couchbase::query_options().adhoc(true))
            .get();
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0][collection_name] == value);
    }
}
