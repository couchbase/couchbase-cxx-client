/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2024. Couchbase, Inc.
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

#include "core/logger/logger.hxx"
#include "test_helper_integration.hxx"

#include "core/operations/management/search.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>

// serverless requires 1 partition and 1 replica
const std::string serverless_plan_params = R"({ "indexPartition": 1, "numReplicas": 1 })";

TEST_CASE("integration: search index management", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (!integration.cluster_version().supports_gcccp()) {
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }

  SECTION("search indexes crud")
  {
    auto index1_base_name = test::utils::uniq_id("index1");
    auto index1_name = index1_base_name;
    auto index2_name = test::utils::uniq_id("index2");
    auto alias_name = test::utils::uniq_id("alias");

    {
      couchbase::core::management::search::index index;
      index.name = index1_name;
      index.type = "fulltext-index";
      index.source_type = "couchbase";
      index.source_name = integration.ctx.bucket;
      if (integration.cluster_version().is_serverless_config_profile()) {
        index.plan_params_json = serverless_plan_params;
      }
      couchbase::core::operations::management::search_index_upsert_request req{};
      req.index = index;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      if (resp.name != index1_name) {
        index1_name = resp.name;
      }
    }

    {
      couchbase::core::management::search::index index;
      index.name = index1_base_name;
      index.type = "fulltext-index";
      index.source_type = "couchbase";
      index.source_name = integration.ctx.bucket;
      if (integration.cluster_version().is_serverless_config_profile()) {
        index.plan_params_json = serverless_plan_params;
      }
      couchbase::core::operations::management::search_index_upsert_request req{};
      req.index = index;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE(resp.ctx.ec == couchbase::errc::common::index_exists);
    }

    {
      couchbase::core::management::search::index index;
      index.name = index2_name;
      index.type = "fulltext-index";
      index.source_type = "couchbase";
      index.source_name = integration.ctx.bucket;
      index.plan_params_json = R"({ "indexPartition": 3 })";
      if (integration.cluster_version().is_serverless_config_profile()) {
        index.plan_params_json = serverless_plan_params;
      }
      index.params_json = R"({ "store": { "kvStoreName": "moss" }})";
      couchbase::core::operations::management::search_index_upsert_request req{};
      req.index = index;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      if (resp.name != index2_name) {
        // FIXME: server 7.2 might automatically prepend "{scope}.{collection}." in front of the
        // index name to workaround it, we "patch" our variable with the name returned by the server
        index2_name = resp.name;
      }
    }

    {
      couchbase::core::management::search::index index;
      index.name = alias_name;
      index.type = "fulltext-alias";
      index.source_type = "nil";
      index.params_json = couchbase::core::utils::json::generate(tao::json::value{
        {
          "targets",
          {
            { index1_name, tao::json::empty_object },
            { index2_name, tao::json::empty_object },
          },
        },
      });
      if (integration.cluster_version().is_serverless_config_profile()) {
        index.plan_params_json = serverless_plan_params;
      }
      couchbase::core::operations::management::search_index_upsert_request req{};
      req.index = index;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      if (resp.name != alias_name) {
        alias_name = resp.name;
      }
    }

    {
      couchbase::core::operations::management::search_index_get_request req{};
      req.index_name = index1_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE(resp.index.name == index1_name);
      REQUIRE(resp.index.type == "fulltext-index");
    }

    {
      couchbase::core::operations::management::search_index_get_request req{};
      req.index_name = index2_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE(resp.index.name == index2_name);
      REQUIRE(resp.index.type == "fulltext-index");
    }

    {
      couchbase::core::operations::management::search_index_get_request req{};
      req.index_name = alias_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE(resp.index.name == alias_name);
      REQUIRE(resp.index.type == "fulltext-alias");
    }

    {
      couchbase::core::operations::management::search_index_get_request req{};
      req.index_name = "missing_index";
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
    }

    {
      couchbase::core::operations::management::search_index_get_all_request req{};
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE_FALSE(resp.indexes.empty());

      REQUIRE(
        1 == std::count_if(resp.indexes.begin(), resp.indexes.end(), [&index1_name](const auto& i) {
          return i.name == index1_name;
        }));
      REQUIRE(
        1 == std::count_if(resp.indexes.begin(), resp.indexes.end(), [&index2_name](const auto& i) {
          return i.name == index2_name;
        }));
      REQUIRE(1 ==
              std::count_if(resp.indexes.begin(), resp.indexes.end(), [&alias_name](const auto& i) {
                return i.name == alias_name;
              }));
    }

    {
      couchbase::core::operations::management::search_index_drop_request req{};
      req.index_name = index1_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      couchbase::core::operations::management::search_index_drop_request req{};
      req.index_name = index2_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    {
      couchbase::core::operations::management::search_index_drop_request req{};
      req.index_name = alias_name;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    couchbase::core::operations::management::search_index_drop_request req{};
    req.index_name = "missing_index";
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::common::index_not_found);
  }

  SECTION("upsert index no name")
  {
    couchbase::core::management::search::index index;
    index.type = "fulltext-index";
    index.source_type = "couchbase";
    index.source_name = integration.ctx.bucket;
    if (integration.cluster_version().is_serverless_config_profile()) {
      index.plan_params_json = serverless_plan_params;
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::common::invalid_argument);
  }

  SECTION("control")
  {
    auto index_name = test::utils::uniq_id("index");

    {
      couchbase::core::management::search::index index;
      index.name = index_name;
      index.type = "fulltext-index";
      index.source_type = "couchbase";
      index.source_name = integration.ctx.bucket;
      if (integration.cluster_version().is_serverless_config_profile()) {
        index.plan_params_json = serverless_plan_params;
      }
      couchbase::core::operations::management::search_index_upsert_request req{};
      req.index = index;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
    }

    SECTION("ingest control")
    {
      {
        couchbase::core::operations::management::search_index_control_ingest_request req{};
        req.index_name = index_name;
        req.pause = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }

      {
        couchbase::core::operations::management::search_index_control_ingest_request req{};
        req.index_name = index_name;
        req.pause = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }
    }

    SECTION("query control")
    {
      {
        couchbase::core::operations::management::search_index_control_query_request req{};
        req.index_name = index_name;
        req.allow = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }

      {
        couchbase::core::operations::management::search_index_control_query_request req{};
        req.index_name = index_name;
        req.allow = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }
    }

    SECTION("partition control")
    {
      {
        couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
        req.index_name = index_name;
        req.freeze = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }

      {
        couchbase::core::operations::management::search_index_control_plan_freeze_request req{};
        req.index_name = index_name;
        req.freeze = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
      }
    }

    couchbase::core::operations::management::search_index_drop_request req{};
    req.index_name = index_name;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }
}

TEST_CASE("integration: search index management public API", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (!integration.cluster_version().supports_gcccp()) {
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }

  auto test_ctx = integration.ctx;
  auto [e, c] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto index_name = test::utils::uniq_id("index");

  SECTION("search indexes crud")
  {
    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;

      auto err = c.search_indexes().upsert_index(index).get();
      REQUIRE_SUCCESS(err.ec());
    }
    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;

      auto err = c.search_indexes().upsert_index(index).get();
      REQUIRE(err.ec() == couchbase::errc::common::index_exists);
    }
    {
      auto [err, index] = c.search_indexes().get_index(index_name).get();
      REQUIRE_SUCCESS(err.ec());
      REQUIRE(index.name == index_name);
      REQUIRE(index.type == "fulltext-index");
    }
    {
      auto [err, index] = c.search_indexes().get_index("missing-index").get();
      REQUIRE(err.ec() == couchbase::errc::common::index_not_found);
    }
    {
      auto [err, indexes] = c.search_indexes().get_all_indexes().get();
      REQUIRE_SUCCESS(err.ec());
      REQUIRE_FALSE(indexes.empty());
      REQUIRE(1 == std::count_if(indexes.begin(), indexes.end(), [&index_name](const auto& i) {
                return i.name == index_name;
              }));
    }
  }
  SECTION("control")
  {
    couchbase::management::search::index index;
    index.name = index_name;
    index.source_name = integration.ctx.bucket;

    auto upsert_err = c.search_indexes().upsert_index(index).get();
    REQUIRE_SUCCESS(upsert_err.ec());
    SECTION("ingest control")
    {
      {
        auto err = c.search_indexes().pause_ingest(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = c.search_indexes().resume_ingest(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
    SECTION("query control")
    {
      {
        auto err = c.search_indexes().allow_querying(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = c.search_indexes().disallow_querying(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
    SECTION("partition control")
    {
      {
        auto err = c.search_indexes().freeze_plan(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = c.search_indexes().unfreeze_plan(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
  }
  auto err = c.search_indexes().drop_index(index_name).get();
  REQUIRE_SUCCESS(err.ec());
}

TEST_CASE("integration: search index management analyze document", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (!integration.cluster_version().supports_search_analyze()) {
    SKIP("cluster does not support search analyze");
  }

  if (integration.cluster_version().is_capella()) {
    SKIP("FIXME: this test on Capella is not very stable.");
  }

  auto index_name = test::utils::uniq_id("index");

  {
    couchbase::core::management::search::index index;
    index.name = index_name;
    index.type = "fulltext-index";
    index.source_type = "couchbase";
    index.source_name = integration.ctx.bucket;
    if (integration.cluster_version().is_serverless_config_profile()) {
      index.plan_params_json = serverless_plan_params;
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    index_name = resp.name;
  }

  REQUIRE(test::utils::wait_for_search_pindexes_ready(
    integration.cluster, integration.ctx.bucket, index_name));

  couchbase::core::operations::management::search_index_analyze_document_response resp;
  bool operation_completed = test::utils::wait_until(
    [&integration, &index_name, &resp]() {
      couchbase::core::operations::management::search_index_analyze_document_request req{};
      req.index_name = index_name;
      req.encoded_document = R"({ "name": "hello world" })";
      resp = test::utils::execute(integration.cluster, req);
      return resp.ctx.ec != couchbase::errc::common::internal_server_failure;
    },
    std::chrono::minutes{ 5 },
    std::chrono::seconds{ 1 });
  REQUIRE(operation_completed);
  REQUIRE_SUCCESS(resp.ctx.ec);
  REQUIRE_FALSE(resp.analysis.empty());
}

TEST_CASE("integration: search index management analyze document public API", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (!integration.cluster_version().supports_search_analyze()) {
    SKIP("cluster does not support search analyze");
  }

  if (integration.cluster_version().is_capella()) {
    SKIP("FIXME: this test on Capella is not very stable.");
  }

  auto index_name = test::utils::uniq_id("index");

  {
    auto test_ctx = integration.ctx;
    auto [e, c] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;
      auto err = c.search_indexes().upsert_index(index).get();
      REQUIRE_SUCCESS(err.ec());
    }
    REQUIRE(test::utils::wait_for_search_pindexes_ready(
      integration.cluster, integration.ctx.bucket, index_name));

    couchbase::error err;
    std::string analysis;
    std::pair<couchbase::error, std::vector<std::string>> result;
    bool operation_completed = test::utils::wait_until([c = c, &index_name, &result]() {
      tao::json::value basic_doc = {
        { "name", "hello world" },
      };
      result = c.search_indexes().analyze_document(index_name, basic_doc).get();
      return result.first.ec() != couchbase::errc::common::internal_server_failure;
    });
    REQUIRE(operation_completed);
    REQUIRE_SUCCESS(result.first.ec());
    REQUIRE_FALSE(result.second.empty());

    auto drop_err = c.search_indexes().drop_index(index_name).get();
    REQUIRE_SUCCESS(drop_err.ec());
  }
}

TEST_CASE("integration: scope search index management public API", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_scope_search()) {
    SKIP("cluster does not support scope search");
  }

  if (!integration.cluster_version().supports_gcccp()) {
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
  }

  auto test_ctx = integration.ctx;
  auto [e, c] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto manager = c.bucket(integration.ctx.bucket).scope("_default").search_indexes();
  auto index_name = test::utils::uniq_id("index");

  SECTION("search indexes crud")
  {
    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;

      auto err = manager.upsert_index(index).get();
      REQUIRE_SUCCESS(err.ec());
    }
    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;

      auto err = manager.upsert_index(index).get();
      REQUIRE(err.ec() == couchbase::errc::common::index_exists);
    }
    {
      auto [err, index] = manager.get_index(index_name).get();
      REQUIRE_SUCCESS(err.ec());
      REQUIRE(index.name == index_name);
      REQUIRE(index.type == "fulltext-index");
    }
    {
      auto [err, index] = manager.get_index("missing-index").get();
      REQUIRE(err.ec() == couchbase::errc::common::index_not_found);
    }
    {
      auto [err, indexes] = manager.get_all_indexes().get();
      REQUIRE_SUCCESS(err.ec());
      REQUIRE_FALSE(indexes.empty());
      REQUIRE(1 == std::count_if(indexes.begin(), indexes.end(), [&index_name](const auto& i) {
                return i.name == index_name;
              }));
    }
  }

  SECTION("control")
  {
    couchbase::management::search::index index;
    index.name = index_name;
    index.source_name = integration.ctx.bucket;

    auto upsert_err = manager.upsert_index(index).get();
    REQUIRE_SUCCESS(upsert_err.ec());
    SECTION("ingest control")
    {
      {
        auto err = manager.pause_ingest(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = manager.resume_ingest(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
    SECTION("query control")
    {
      {
        auto err = manager.allow_querying(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = manager.disallow_querying(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
    SECTION("partition control")
    {
      {
        auto err = manager.freeze_plan(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
      {
        auto err = manager.unfreeze_plan(index_name).get();
        REQUIRE_SUCCESS(err.ec());
      }
    }
  }
  auto err = manager.drop_index(index_name).get();
  REQUIRE_SUCCESS(err.ec());
}

TEST_CASE("integration: scope search index management analyze document public API", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_scope_search_analyze()) {
    SKIP("cluster does not support scoped analyze_document");
  }

  if (integration.cluster_version().is_capella()) {
    SKIP("Wait for search pindexes ready is used in this test, which doesn't work against Capella");
  }

  auto test_ctx = integration.ctx;
  auto [e, c] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto manager = c.bucket(integration.ctx.bucket).scope("_default").search_indexes();
  auto index_name = test::utils::uniq_id("index");
  {
    {
      couchbase::management::search::index index;
      index.name = index_name;
      index.source_name = integration.ctx.bucket;
      auto err = manager.upsert_index(index).get();
      REQUIRE_SUCCESS(err.ec());
    }
    REQUIRE(test::utils::wait_for_search_pindexes_ready(
      integration.cluster, integration.ctx.bucket, index_name));

    couchbase::error err;
    std::string analysis;
    std::pair<couchbase::error, std::vector<std::string>> result;
    bool operation_completed = test::utils::wait_until([&manager, &index_name, &result]() {
      tao::json::value basic_doc = {
        { "name", "hello world" },
      };
      result = manager.analyze_document(index_name, basic_doc).get();
      return result.first.ec() != couchbase::errc::common::internal_server_failure;
    });
    REQUIRE(operation_completed);
    INFO(result.first.ctx().to_json());
    REQUIRE_SUCCESS(result.first.ec());
    REQUIRE_FALSE(result.second.empty());

    auto drop_err = manager.drop_index(index_name).get();
    REQUIRE_SUCCESS(drop_err.ec());
  }
}

TEST_CASE("integration: scope search returns feature not available", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().supports_scope_search()) {
    SKIP("cluster supports scope search");
  }
  auto test_ctx = integration.ctx;
  auto [e, c] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto manager = c.bucket(integration.ctx.bucket).scope("_default").search_indexes();
  auto index_name = test::utils::uniq_id("index");
  {
    couchbase::management::search::index index;
    index.name = index_name;
    index.source_name = integration.ctx.bucket;
    auto err = manager.upsert_index(index).get();
    REQUIRE(err.ec() == couchbase::errc::common::feature_not_available);
  }
}

TEST_CASE("integration: upsert vector index feature not available", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().supports_vector_search()) {
    SKIP("cluster supports vector search");
  }

  auto test_ctx = integration.ctx;
  auto [e, c] =
    couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
  REQUIRE_SUCCESS(e.ec());

  auto manager = c.search_indexes();
  {
    auto index_name = test::utils::uniq_id("index");
    couchbase::management::search::index index;
    index.name = index_name;
    index.params_json = test::utils::read_test_data("sample_vector_index_params.json");
    auto err = manager.upsert_index(index).get();
    REQUIRE(err.ec() == couchbase::errc::common::feature_not_available);
  }
}
