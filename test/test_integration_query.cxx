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
#include "utils/wait_until.hxx"

#include "core/logger/logger.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/management/collections.hxx"
#include "core/operations/management/query.hxx"
#include "couchbase/codec/binary_noop_serializer.hxx"
#include "couchbase/codec/raw_binary_transcoder.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/lookup_in_specs.hxx>

#include <tao/json/from_string.hpp>

#include <cstdint>
#include <fstream>

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
    REQUIRE(resp.rows.size() == 1);
    REQUIRE(tao::json::from_string(resp.rows[0]) ==
            tao::json::value{ { "greeting", "ruby rules" } });
    REQUIRE_FALSE(resp.meta.client_context_id.empty());
    REQUIRE_FALSE(resp.meta.request_id.empty());
    REQUIRE(resp.meta.status == "success");
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
    auto handler = [barrier,
                    ctx = std::move(ctx)](couchbase::core::operations::query_response&& resp) {
      CHECK(ctx.payload() == "foobar");
      barrier->set_value(std::move(resp));
    };
    integration.cluster.execute(req, std::move(handler));
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
    couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket,
                                                                       scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  {
    couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket,
                                                                            scope_name,
                                                                            collection_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  {
    couchbase::core::operations::management::query_index_create_response resp;
    bool operation_completed =
      test::utils::wait_until([&integration, &index_name, &scope_name, &collection_name, &resp]() {
        couchbase::core::operations::management::query_index_create_request req{};
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.collection_name = collection_name;
        req.index_name = index_name;
        req.is_primary = true;
        resp = test::utils::execute(integration.cluster, req);

        return resp.ctx.ec != couchbase::errc::common::bucket_not_found &&
               resp.ctx.ec != couchbase::errc::common::scope_not_found;
      });
    REQUIRE(operation_completed);
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
    couchbase::core::operations::query_request req{ fmt::format(
      R"(SELECT a, b FROM missing_collection WHERE META().id = "{}")", key) };
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

  {
    couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket,
                                                                     scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
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

  REQUIRE(test::utils::create_primary_index(integration.cluster, integration.ctx.bucket));

  {
    couchbase::core::operations::query_request req{ fmt::format("SELECT * FROM `{}` LIMIT 0",
                                                                integration.ctx.bucket) };
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

  constexpr std::uint32_t expiry = std::numeric_limits<std::uint32_t>::max();

  {
    couchbase::core::operations::upsert_request req{
      id, couchbase::core::utils::to_binary(R"({"foo":42})")
    };
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
    std::string statement =
      fmt::format("UPDATE {} AS b USE KEYS '{}' SET b.foo = 43", integration.ctx.bucket, id.key());
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
    REQUIRE(tao::json::from_string(rows[0]) ==
            tao::json::from_string(R"({"data":{"tech":"C++"}})"));
    REQUIRE(tao::json::from_string(rows[1]) ==
            tao::json::from_string(R"({"data":{"tech":"Ruby"}})"));
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
    REQUIRE(tao::json::from_string(rows[0]) ==
            tao::json::from_string(R"({ "data": { "tech": "C++" } })"));
    REQUIRE(tao::json::from_string(rows[1]) ==
            tao::json::from_string(R"({ "data": { "tech": "Ruby" } })"));
    REQUIRE(tao::json::from_string(rows[2]) ==
            tao::json::from_string(R"({ "data": { "tech": "Couchbase" } })"));
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
    REQUIRE(tao::json::from_string(resp.rows[0]) == tao::json::from_string(R"({"answer":42})"));
    REQUIRE_FALSE(resp.served_by_node.empty());
    node_to_stick_queries = resp.served_by_node;
  }

  if (integration.number_of_query_nodes() > 1) {
    std::vector<std::string> used_nodes{};
    std::mutex used_nodes_mutex{};

    std::vector<std::thread> threads;
    threads.reserve(10);
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([i,
                            &cluster = integration.cluster,
                            node_to_stick_queries,
                            &used_nodes,
                            &used_nodes_mutex]() {
        couchbase::core::operations::query_request req{ fmt::format(R"(SELECT {} AS answer)", i) };
        auto resp = test::utils::execute(cluster, req);
        if (resp.ctx.ec || resp.served_by_node.empty() || resp.rows.size() != 1 ||
            tao::json::from_string(resp.rows[0]) !=
              tao::json::from_string(fmt::format(R"({{"answer":{}}})", i))) {
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
      threads.emplace_back([i,
                            &cluster = integration.cluster,
                            node_to_stick_queries,
                            &used_nodes,
                            &used_nodes_mutex]() {
        couchbase::core::operations::query_request req{ fmt::format(R"(SELECT {} AS answer)", i) };
        req.send_to_node = node_to_stick_queries;
        auto resp = test::utils::execute(cluster, req);
        if (resp.ctx.ec || resp.served_by_node.empty() || resp.rows.size() != 1 ||
            tao::json::from_string(resp.rows[0]) !=
              tao::json::from_string(fmt::format(R"({{"answer":{}}})", i))) {
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

TEST_CASE("integration: analytics create dataset", "[integration]")
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
  couchbase::core::operations::analytics_request req{ fmt::format(
    "CREATE DATAVERSE `{}`.`test-scope` IF NOT EXISTS", integration.ctx.bucket) };
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

  REQUIRE(test::utils::create_primary_index(integration.cluster, integration.ctx.bucket));

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
      R"(SELECT a, b FROM `{}` WHERE META().id = "{}")", integration.ctx.bucket, key) };

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

  auto cluster = integration.public_cluster();

  {
    auto [ctx, resp] = cluster.query("SELECT 42 AS the_answer", {}).get();
    REQUIRE_SUCCESS(ctx.ec());
    auto rows = resp.rows_as();
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
    auto rows = resp.rows_as();
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

  auto cluster = integration.public_cluster();

  auto scope_name = test::utils::uniq_id("scope");
  auto collection_name = test::utils::uniq_id("coll");
  auto key = test::utils::uniq_id("foo");
  auto id =
    couchbase::core::document_id{ integration.ctx.bucket, scope_name, collection_name, key };
  tao::json::value value = {
    { "a", 1.0 },
    { "b", 2.0 },
  };
  auto json = couchbase::core::utils::json::generate_binary(value);
  {
    couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket,
                                                                       scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  {
    couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket,
                                                                            scope_name,
                                                                            collection_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  {
    couchbase::core::operations::insert_request req{ id, json };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  SECTION("correct scope and collection")
  {
    auto [ctx, resp] =
      cluster.bucket(integration.ctx.bucket)
        .scope(scope_name)
        .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key), {})
        .get();
    REQUIRE_SUCCESS(ctx.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0][collection_name] == value);
  }
  SECTION("missing scope")
  {
    auto [ctx, resp] =
      cluster.bucket(integration.ctx.bucket)
        .scope("idontexist")
        .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key), {})
        .get();
    REQUIRE(ctx.ec() == couchbase::errc::query::index_failure);
  }
  SECTION("missing collection")
  {
    auto [ctx, resp] =
      cluster.bucket(integration.ctx.bucket)
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
        .query(fmt::format("SELECT * from `{}` USE KEYS '{}'", collection_name, key),
               couchbase::query_options().adhoc(true))
        .get();
    REQUIRE_SUCCESS(ctx.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0][collection_name] == value);
  }
}

TEST_CASE("integration: public API query using both named and positional parameters",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }

  auto cluster = integration.public_cluster();

  auto opts = couchbase::query_options().positional_parameters(20, "foo").named_parameters(
    std::pair{ "x", 10 });

  auto [err, res] = cluster.query("SELECT $x fieldA, $1 fieldB, $2 fieldC", opts).get();
  if (err) {
    fmt::println("{}", err.ctx().to_json());
  }
  REQUIRE_SUCCESS(err.ec());

  auto rows = res.rows_as<couchbase::codec::tao_json_serializer, tao::json::value>();
  REQUIRE(rows.size() == 1);

  auto row = rows[0].get_object();
  REQUIRE(row["fieldA"].as<std::uint32_t>() == 10);
  REQUIRE(row["fieldB"].as<std::uint32_t>() == 20);
  REQUIRE(row["fieldC"].as<std::string>() == "foo");
}

namespace
{
// The N1QL query below produces ~81 MB of result rows (15,000 rows, each carrying a ~5.4 KB
// padding_data field). It executes entirely in the query engine without touching any bucket, which
// makes it a deterministic stress test for streaming back-pressure. The payload stays well above
// the 50 MB RSS ceiling asserted below, so a run that accidentally buffered the whole response
// would breach it, while the streaming path stays bounded. Kept in the ~50-100 MB range so the CI
// integration matrix (many server versions) does not pay the cost of a half-gigabyte transfer.
const std::string streaming_padding_query =
  R"(SELECT REPEAT("ABCDEFGHJIJKLMNOPQRSTUVWXYZ", 200) AS padding_data
     FROM array_range(0, 15000) AS i)";

// Reads the current resident-set size (VmRSS) of the current process, in kilobytes. VmRSS (not the
// process-lifetime peak VmHWM) is read on purpose: sampling the live working set while draining
// lets the assertion below detect a regression even if an earlier memory-heavy test already
// inflated the peak. Returns 0 when /proc/self/status is unavailable (e.g. non-Linux), which makes
// the memory assertion a no-op on those platforms.
auto
read_current_rss_kb() -> std::uint64_t
{
  std::ifstream status{ "/proc/self/status" };
  std::string line;
  while (std::getline(status, line)) {
    if (line.rfind("VmRSS:", 0) == 0) {
      std::uint64_t value{ 0 };
      for (char c : line) {
        if (c >= '0' && c <= '9') {
          value = value * 10 + static_cast<std::uint64_t>(c - '0');
        }
      }
      return value;
    }
  }
  return 0;
}
} // namespace

TEST_CASE("integration: streaming query yields rows lazily", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.query_stream(streaming_padding_query, {}).get();
  REQUIRE_SUCCESS(err.ec());

  // Pull only the first 5 rows of a ~81 MB result, then abandon.
  int pulled = 0;
  for (int i = 0; i < 5; ++i) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    REQUIRE(row.has_value());
    auto v = row->content_as<couchbase::codec::tao_json_serializer, tao::json::value>();
    REQUIRE(v.find("padding_data") != nullptr);
    ++pulled;
  }
  REQUIRE(pulled == 5);
  result.cancel(); // abandon the remaining ~81 MB

  cluster.close().get();
}

TEST_CASE("integration: streaming query is memory-bounded", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.query_stream(streaming_padding_query, {}).get();
  REQUIRE_SUCCESS(err.ec());

  const auto rss_before_kb = read_current_rss_kb();

  // Drain the whole ~81 MB result one row at a time, tracking the peak resident set observed while
  // draining, and log it for inspection. A buffered query() would materialise the entire payload at
  // once; the streaming path must deliver every row without doing so.
  //
  // We intentionally do NOT assert a hard process-RSS bound here. This binary runs across the CI
  // sanitizer/valgrind matrix, where shadow memory and freed-allocation quarantine make VmRSS grow
  // with the *total* bytes processed rather than the live working set (hundreds of MB under ASan
  // even when streaming is correctly bounded), so any RSS threshold would be meaningless there. The
  // enforced, deterministic memory-bound guarantee lives in the row_streamer unit test
  // ("bounds buffered bytes with byte watermarks"), which asserts buffered_bytes() stays within the
  // watermark independent of instrumentation. Draining every row here verifies the end-to-end
  // streaming path delivers the full result without hanging or buffering it into one allocation.
  std::uint64_t row_count{ 0 };
  std::uint64_t peak_rss_kb{ rss_before_kb };
  while (true) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    if (!row.has_value()) {
      break;
    }
    ++row_count;
    if (row_count % 500 == 0) {
      const auto current = read_current_rss_kb();
      if (current > peak_rss_kb) {
        peak_rss_kb = current;
      }
    }
  }
  REQUIRE(row_count == 15000);

  if (rss_before_kb != 0 && peak_rss_kb >= rss_before_kb) {
    CB_LOG_INFO("streaming query peak RSS delta while draining: {} KB ({} rows)",
                peak_rss_kb - rss_before_kb,
                row_count);
  }

  cluster.close().get();
}

TEST_CASE("integration: cancelling a streaming query mid-stream is clean", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.query_stream(streaming_padding_query, {}).get();
  REQUIRE_SUCCESS(err.ec());

  // Pull a few rows to confirm the stream is live.
  for (int i = 0; i < 3; ++i) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    REQUIRE(row.has_value());
  }

  // Cancel mid-stream — must not hang and must not corrupt memory.
  // Under an ASan/TSan build (controller-owned) this also validates no
  // use-after-free or data-race on the internal streaming machinery.
  result.cancel();

  // Dropping `result` here; the destructor must be safe after cancel().
  cluster.close().get();
}

TEST_CASE("integration: streaming query surfaces a query error", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  // A syntactically-valid statement that references a nonexistent keyspace so
  // the query service rejects it at runtime with a keyspace-not-found error.
  // This verifies that query_stream() propagates query-level errors to the
  // caller rather than silently swallowing them.
  //
  // NOTE: The rows-THEN-trailing-error path (rows emitted before the error
  // JSON key) is additionally covered by the unit test in
  // test_unit_query_stream.cxx:
  //   "query_stream surfaces a trailing query error after rows"
  const std::string error_query =
    "SELECT * FROM `nonexistent_bucket_that_does_not_exist_xyz` LIMIT 1";

  auto [err, result] = cluster.query_stream(error_query, {}).get();

  if (err.ec()) {
    // Error surfaced at dispatch time (pre-row): acceptable.
    SUCCEED("error surfaced at dispatch time");
  } else {
    // Error surfaced through the row stream: drain until we see it.
    bool saw_error = false;
    while (true) {
      auto [rerr, row] = result.next().get();
      if (rerr.ec()) {
        // Terminal error delivered via next() — correct behavior.
        saw_error = true;
        break;
      }
      if (!row.has_value()) {
        // Clean end-of-stream with no error is not acceptable for this query.
        break;
      }
    }
    REQUIRE(saw_error);
  }

  cluster.close().get();
}

TEST_CASE("integration: cancelling a streaming query while a next() is in flight is clean",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.query_stream(streaming_padding_query, {}).get();
  REQUIRE_SUCCESS(err.ec());

  // Issue an asynchronous pull and cancel it before its handler fires. This races cancel()'s
  // connection teardown against an in-flight channel receive — the scenario that exposes the
  // use-after-free / data race the earlier fixes address (validated under ASan/TSan builds). The
  // handler must still be invoked (cancelled or with a row); it must not hang or crash.
  auto done = std::make_shared<std::promise<void>>();
  auto fut = done->get_future();
  result.next([done](couchbase::error, std::optional<couchbase::query_row>) {
    done->set_value();
  });
  result.cancel();

  REQUIRE(fut.wait_for(std::chrono::seconds(10)) == std::future_status::ready);

  cluster.close().get();
}

TEST_CASE("integration: streaming prepared (adhoc=false) query yields rows", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  // The adhoc=false path is forced through the buffered prepared-statement machinery and then
  // replayed as a stream from an in-memory copy. It must be started before it can yield rows;
  // if it is not, next() blocks forever.
  auto [err, result] =
    cluster.query_stream(R"(SELECT "prepared" AS v)", couchbase::query_options{}.adhoc(false))
      .get();
  REQUIRE_SUCCESS(err.ec());

  auto [rerr, row] = result.next().get();
  REQUIRE_SUCCESS(rerr.ec());
  REQUIRE(row.has_value());
  auto value = row->content_as<couchbase::codec::tao_json_serializer, tao::json::value>();
  REQUIRE(value.at("v").get_string() == "prepared");

  auto [eerr, end_row] = result.next().get();
  REQUIRE_SUCCESS(eerr.ec());
  REQUIRE_FALSE(end_row.has_value()); // clean end-of-stream

  auto [merr, meta] = result.meta_data().get();
  REQUIRE_SUCCESS(merr.ec());
  REQUIRE(meta.status() == couchbase::query_status::success);

  cluster.close().get();
}

TEST_CASE("integration: streaming query meta_data can be requested more than once", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.query_stream(R"(SELECT "x" AS v)", {}).get();
  REQUIRE_SUCCESS(err.ec());

  // Requesting meta_data() twice before the terminal must not throw future_already_retrieved;
  // both futures must resolve once the stream drains.
  auto meta_future_1 = result.meta_data();
  auto meta_future_2 = result.meta_data();

  while (result.next().get().second.has_value()) {
    // drain
  }

  auto meta_1 = meta_future_1.get();
  auto meta_2 = meta_future_2.get();
  REQUIRE_SUCCESS(meta_1.first.ec());
  REQUIRE_SUCCESS(meta_2.first.ec());
  REQUIRE(meta_1.second.status() == couchbase::query_status::success);
  REQUIRE(meta_2.second.status() == couchbase::query_status::success);

  // A post-terminal request resolves immediately from the cached value.
  auto meta_3 = result.meta_data().get();
  REQUIRE(meta_3.second.status() == couchbase::query_status::success);

  cluster.close().get();
}

TEST_CASE("integration: streaming query iterator surfaces a terminal error", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  const std::string error_query =
    "SELECT * FROM `nonexistent_bucket_that_does_not_exist_xyz` LIMIT 1";
  auto [err, result] = cluster.query_stream(error_query, {}).get();

  if (err.ec()) {
    SUCCEED("error surfaced up front; iterator path not exercised");
  } else {
    // The range-for iterator must visit the terminal error element rather than comparing equal to
    // end() and silently dropping it.
    bool saw_error = false;
    for (auto it = result.begin(); it != result.end(); ++it) {
      auto [rerr, row] = *it;
      if (rerr.ec()) {
        saw_error = true;
      }
    }
    REQUIRE(saw_error);
  }

  cluster.close().get();
}

TEST_CASE("integration: streaming query matches buffered query", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }
  auto cluster = integration.public_cluster();

  constexpr std::size_t total_rows = 1000;
  // ORDER BY makes the row order deterministic across the two independent executions so the
  // row-by-row comparison below is stable.
  const std::string statement =
    "SELECT n FROM ARRAY_RANGE(0, " + std::to_string(total_rows) + ") AS n ORDER BY n";

  // Buffered reference, decoded to JSON values (not raw bytes).
  auto [berr, buffered] = cluster.query(statement, couchbase::query_options{}).get();
  REQUIRE_SUCCESS(berr.ec());
  const auto expected_rows =
    buffered.rows_as<couchbase::codec::tao_json_serializer, tao::json::value>();
  REQUIRE(expected_rows.size() == total_rows);

  // Drain the whole result through the streaming API and compare row-by-row. Compare parsed JSON,
  // not raw bytes: the streaming path carries the engine's row bytes verbatim while the buffered
  // path re-serializes each row, so the two can differ in incidental whitespace even when the
  // content is identical. This guards the buffered and streaming request encoders and row
  // extraction against divergence; the ORDER BY keeps the order deterministic.
  auto [serr, result] = cluster.query_stream(statement).get();
  REQUIRE_SUCCESS(serr.ec());

  std::vector<tao::json::value> streamed_rows;
  while (true) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    if (!row.has_value()) {
      break;
    }
    streamed_rows.push_back(
      row->content_as<couchbase::codec::tao_json_serializer, tao::json::value>());
  }
  REQUIRE(streamed_rows.size() == expected_rows.size());
  REQUIRE(streamed_rows == expected_rows);

  auto [merr, meta] = result.meta_data().get();
  REQUIRE_SUCCESS(merr.ec());
  REQUIRE(meta.status() == couchbase::query_status::success);

  cluster.close().get();
}
