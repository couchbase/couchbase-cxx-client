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
#include <couchbase/operations/management/search.hxx>

TEST_CASE("integration: search index management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("search indexes crud")
    {
        auto index1_name = test::utils::uniq_id("index");
        auto index2_name = test::utils::uniq_id("index");
        auto alias_name = test::utils::uniq_id("alias");

        {
            couchbase::operations::management::search_index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index index;
            index.name = index1_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_exists);
        }

        {
            couchbase::operations::management::search_index index;
            index.name = index2_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            index.plan_params_json = R"({ "indexPartition": 3 })";
            index.params_json = R"({ "store": { "indexType": "upside_down", "kvStoreName": "moss" }})";
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index index;
            index.name = alias_name;
            index.type = "fulltext-alias";
            index.source_type = "nil";
            index.params_json = couchbase::utils::json::generate(
              tao::json::value{ { "targets", { { index1_name, tao::json::empty_object }, { index2_name, tao::json::empty_object } } } });
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_get_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.index.name == index1_name);
            REQUIRE(resp.index.type == "fulltext-index");
        }

        {
            couchbase::operations::management::search_index_get_request req{};
            req.index_name = "missing_index";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
        }

        {
            couchbase::operations::management::search_index_get_all_request req{};
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE_FALSE(resp.indexes.empty());
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = index1_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = index2_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        {
            couchbase::operations::management::search_index_drop_request req{};
            req.index_name = alias_name;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        couchbase::operations::management::search_index_drop_request req{};
        req.index_name = "missing_index";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::index_not_found);
    }

    SECTION("upsert index no name")
    {
        couchbase::operations::management::search_index index;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    SECTION("control")
    {
        auto index_name = test::utils::uniq_id("index");

        {
            couchbase::operations::management::search_index index;
            index.name = index_name;
            index.type = "fulltext-index";
            index.source_type = "couchbase";
            index.source_name = integration.ctx.bucket;
            couchbase::operations::management::search_index_upsert_request req{};
            req.index = index;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }

        SECTION("ingest control")
        {
            {
                couchbase::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_ingest_request req{};
                req.index_name = index_name;
                req.pause = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        SECTION("query control")
        {
            {
                couchbase::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_query_request req{};
                req.index_name = index_name;
                req.allow = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        SECTION("partition control")
        {
            {
                couchbase::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = true;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }

            {
                couchbase::operations::management::search_index_control_plan_freeze_request req{};
                req.index_name = index_name;
                req.freeze = false;
                auto resp = test::utils::execute(integration.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
            }
        }

        couchbase::operations::management::search_index_drop_request req{};
        req.index_name = index_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

bool
wait_for_search_pindexes_ready(test::utils::integration_test_guard& integration, const std::string& index_name)
{
    return test::utils::wait_until(
      [&integration, &index_name]() {
          couchbase::operations::management::search_index_stats_request req{};
          auto resp = test::utils::execute(integration.cluster, req);
          if (resp.ctx.ec || resp.stats.empty()) {
              return false;
          }
          auto stats = couchbase::utils::json::parse(resp.stats);
          const auto* num_pindexes_actual = stats.find(fmt::format("{}:{}:num_pindexes_actual", integration.ctx.bucket, index_name));
          if (num_pindexes_actual == nullptr || !num_pindexes_actual->is_number()) {
              return false;
          }
          const auto* num_pindexes_target = stats.find(fmt::format("{}:{}:num_pindexes_target", integration.ctx.bucket, index_name));
          if (num_pindexes_target == nullptr || !num_pindexes_target->is_number()) {
              return false;
          }
          return num_pindexes_actual->get_unsigned() == num_pindexes_target->get_unsigned();
      },
      std::chrono::minutes(3));
}

TEST_CASE("integration: search index management analyze document", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_search_analyze()) {
        return;
    }

    auto index_name = test::utils::uniq_id("index");

    {
        couchbase::operations::management::search_index index;
        index.name = index_name;
        index.type = "fulltext-index";
        index.source_type = "couchbase";
        index.source_name = integration.ctx.bucket;
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    REQUIRE(wait_for_search_pindexes_ready(integration, index_name));

    couchbase::operations::management::search_index_analyze_document_response resp;
    bool operation_completed = test::utils::wait_until([&integration, &index_name, &resp]() {
        couchbase::operations::management::search_index_analyze_document_request req{};
        req.index_name = index_name;
        req.encoded_document = R"({ "name": "hello world" })";
        resp = test::utils::execute(integration.cluster, req);
        return resp.ctx.ec != couchbase::error::common_errc::internal_server_failure;
    });
    REQUIRE(operation_completed);
    REQUIRE_FALSE(resp.ctx.ec);
    REQUIRE_FALSE(resp.analysis.empty());
}