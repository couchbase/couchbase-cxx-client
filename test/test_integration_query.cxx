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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

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

  auto test_ctx = integration.ctx;
  auto [err, cluster] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(err.ec());

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
