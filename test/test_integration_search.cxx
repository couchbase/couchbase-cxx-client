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

#include <catch2/matchers/catch_matchers_string.hpp>

#include "core/logger/logger.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_search.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/operations/management/search_index_drop.hxx"
#include "core/operations/management/search_index_upsert.hxx"

#include <couchbase/boolean_query.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/custom_filter_query.hxx>
#include <couchbase/custom_score_query.hxx>
#include <couchbase/match_none_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/search_scoring_none.hxx>
#include <couchbase/search_scoring_reciprocal_rank_fusion.hxx>

#include <tao/json/value.hpp>

#include <optional>
#include <regex>
#include <set>
#include <string>
#include <utility>
#include <vector>

using Catch::Matchers::StartsWith;

TEST_CASE("integration: search query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  {
    auto sample_data = couchbase::core::utils::json::parse(
      couchbase::core::json_string(test::utils::read_test_data("search_beers_dataset.json")));
    const auto& o = sample_data.get_object();
    for (const auto& [key, value] : o) {
      couchbase::core::document_id id(integration.ctx.bucket, "_default", "_default", key);
      couchbase::core::operations::upsert_request req{
        id, couchbase::core::utils::json::generate_binary(value)
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec());
    }
  }

  std::uint64_t beer_sample_doc_count = 5;
  bool completed{};
  std::string index_name = test::utils::uniq_id("beer-search-index");
  const std::string requested_index_name = index_name;
  couchbase::core::operations::management::search_index_drop_request drop_requested_index_req{};
  drop_requested_index_req.index_name = index_name;
  const test::utils::drop_guard drop_requested_index{ integration.cluster,
                                                      "search index " + index_name,
                                                      drop_requested_index_req };

  std::tie(completed, index_name) =
    test::utils::create_search_index(integration,
                                     integration.ctx.bucket,
                                     index_name,
                                     "search_beers_index_params.json",
                                     beer_sample_doc_count);
  // The server may rename the index; a renamed one gets a guard of its own.
  std::optional<
    test::utils::drop_guard<couchbase::core::operations::management::search_index_drop_request>>
    drop_renamed_index;
  if (!index_name.empty() && index_name != requested_index_name) {
    couchbase::core::operations::management::search_index_drop_request drop_index_req{};
    drop_index_req.index_name = index_name;
    drop_renamed_index.emplace(integration.cluster, "search index " + index_name, drop_index_req);
  }
  REQUIRE(completed);

  REQUIRE(test::utils::wait_for_search_pindexes_ready(
    integration.cluster, integration.ctx.bucket, index_name));

  couchbase::core::json_string simple_query(R"({"query": "description:belgian"})");

  // Wait until expected documents are indexed
  {
    auto ok = test::utils::wait_until(
      [&]() {
        couchbase::core::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        return resp.rows.size() == beer_sample_doc_count;
      },
      std::chrono::minutes(5));
    REQUIRE(ok);
  }

  SECTION("simple query")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 5);
    REQUIRE(resp.rows[0].id == "avery_brewing_company-reverend_the");
    REQUIRE(resp.rows[0].score > 0);
    REQUIRE_THAT(resp.rows[0].index, StartsWith(index_name));
    REQUIRE(resp.rows[0].locations.empty());
    REQUIRE(resp.rows[0].explanation.empty());
    REQUIRE(resp.rows[0].fields.empty());
    REQUIRE(resp.rows[0].fragments.empty());
    REQUIRE(resp.meta.metrics.max_score > 0);
    REQUIRE(resp.meta.metrics.total_rows == 5);
    REQUIRE(resp.meta.metrics.took > std::chrono::nanoseconds(0));
  }

  SECTION("limit")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.limit = 1;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 1);
  }

  SECTION("skip")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.skip = 1;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == beer_sample_doc_count - 1);
    REQUIRE(resp.rows[0].id == "bear_republic_brewery-red_rocket_ale");
  }

  SECTION("explain")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.explain = true;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE_FALSE(resp.rows[0].explanation.empty());
  }

  if (integration.cluster_version().supports_search_disable_scoring()) {
    SECTION("disable scoring")
    {
      couchbase::core::operations::search_request req{};
      req.index_name = index_name;
      req.query = simple_query;
      req.disable_scoring = true;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE(resp.rows[0].score == 0);
      REQUIRE(resp.meta.metrics.max_score == 0);
    }
  }

  SECTION("include locations")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
    req.include_locations = true;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0].locations.size() == 1);
    REQUIRE(resp.rows[0].locations[0].field == "description");
    REQUIRE(resp.rows[0].locations[0].term == "belgian");
    REQUIRE(resp.rows[0].locations[0].position == 1);
    REQUIRE(resp.rows[0].locations[0].start_offset == 0);
    REQUIRE(resp.rows[0].locations[0].end_offset == 7);
  }

  SECTION("highlight fields default highlight style")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
    req.highlight_fields = { "description" };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0].fragments["description"][0] == "<mark>Belgian</mark>-Style Quadrupel Ale");
  }

  SECTION("highlight style")
  {
    {
      couchbase::core::operations::search_request req{};
      req.index_name = index_name;
      req.query = simple_query;
      req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
      req.highlight_fields = { "description" };
      req.highlight_style = couchbase::core::search_highlight_style::html;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      REQUIRE(resp.rows[0].fragments["description"][0] ==
              "<mark>Belgian</mark>-Style Quadrupel Ale");
    }

    {
      couchbase::core::operations::search_request req{};
      req.index_name = index_name;
      req.query = simple_query;
      req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
      req.highlight_fields = { "description" };
      req.highlight_style = couchbase::core::search_highlight_style::ansi;
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      // TODO: is there a better way to compare ansi strings?
      std::string snippet = resp.rows[0].fragments["description"][0];
      std::string open = "\x1b[43m";
      std::string close = "\x1b[0m";
      snippet.replace(snippet.find(open), open.size(), "<mark>");
      snippet.replace(snippet.find(close), close.size(), "</mark>");
      REQUIRE(snippet == "<mark>Belgian</mark>-Style Quadrupel Ale");
    }
  }

  SECTION("fields")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_id"));
    req.fields.emplace_back("description");
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto fields = couchbase::core::utils::json::parse(resp.rows[0].fields).get_object();
    REQUIRE(fields.at("description").get_string() == "Belgian-Style Quadrupel Ale");
  }

  SECTION("sort")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.sort_specs.emplace_back(couchbase::core::utils::json::generate("_score"));
    req.timeout = std::chrono::seconds(1);
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0].id == "bear_republic_brewery-red_rocket_ale");
  }

  SECTION("term facet")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.facets.insert(std::make_pair("type", R"({"field": "type", "size": 1})"));
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.facets.size() == 1);
    REQUIRE(resp.facets[0].name == "type");
    REQUIRE(resp.facets[0].field == "type");
    REQUIRE(resp.facets[0].total == 5);
    REQUIRE(resp.facets[0].missing == 0);
    REQUIRE(resp.facets[0].other == 0);
    REQUIRE(resp.facets[0].terms.size() == 1);
    REQUIRE(resp.facets[0].terms[0].term == "beer");
    REQUIRE(resp.facets[0].terms[0].count == 5);
  }

  SECTION("date range facet")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.facets.insert(std::make_pair(
      "updated",
      R"({"field": "updated", "size": 2, "date_ranges": [{"name": "old", "end": "2010-08-01"},{"name": "new", "start": "2010-08-01"}]})"));
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.facets.size() == 1);
    REQUIRE(resp.facets[0].name == "updated");
    REQUIRE(resp.facets[0].field == "updated");
    REQUIRE(resp.facets[0].total == 5);
    REQUIRE(resp.facets[0].missing == 0);
    REQUIRE(resp.facets[0].other == 0);
    REQUIRE(resp.facets[0].date_ranges.size() == 2);
    REQUIRE(resp.facets[0].date_ranges[0].name == "old");
    REQUIRE(resp.facets[0].date_ranges[0].count == 4);
    REQUIRE_FALSE(resp.facets[0].date_ranges[0].start.has_value());
    REQUIRE(resp.facets[0].date_ranges[0].end == "2010-08-01T00:00:00Z");
    REQUIRE(resp.facets[0].date_ranges[1].name == "new");
    REQUIRE(resp.facets[0].date_ranges[1].count == 1);
    REQUIRE(resp.facets[0].date_ranges[1].start == "2010-08-01T00:00:00Z");
    REQUIRE_FALSE(resp.facets[0].date_ranges[1].end.has_value());
  }

  SECTION("numeric range facet")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.facets.insert(std::make_pair(
      "abv",
      R"({"field": "abv", "size": 2, "numeric_ranges": [{"name": "high", "min": 7},{"name": "low", "max": 7}]})"));
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.facets.size() == 1);
    REQUIRE(resp.facets[0].name == "abv");
    REQUIRE(resp.facets[0].field == "abv");
    REQUIRE(resp.facets[0].total == 5);
    REQUIRE(resp.facets[0].missing == 0);
    REQUIRE(resp.facets[0].other == 0);
    REQUIRE(resp.facets[0].numeric_ranges.size() == 2);
    auto high_range = std::find_if(resp.facets[0].numeric_ranges.begin(),
                                   resp.facets[0].numeric_ranges.end(),
                                   [](const auto& range) {
                                     return range.name == "high";
                                   });
    REQUIRE(high_range != resp.facets[0].numeric_ranges.end());
    REQUIRE(high_range->count == 2);
    REQUIRE(std::get<std::uint64_t>(high_range->min) == 7);
    REQUIRE(std::holds_alternative<std::monostate>(high_range->max));
    auto low_range = std::find_if(resp.facets[0].numeric_ranges.begin(),
                                  resp.facets[0].numeric_ranges.end(),
                                  [](const auto& range) {
                                    return range.name == "low";
                                  });
    REQUIRE(low_range != resp.facets[0].numeric_ranges.end());
    REQUIRE(low_range->count == 3);
    REQUIRE(std::holds_alternative<std::monostate>(low_range->min));
    REQUIRE(std::get<std::uint64_t>(low_range->max) == 7);
  }

  SECTION("raw")
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    std::map<std::string, couchbase::core::json_string> raw{};
    raw.insert(std::make_pair("size", couchbase::core::json_string("1")));
    req.raw = raw;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 1);
  }
}

TEST_CASE("integration: search query consistency", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().has_fixed_consistency_check_in_search_engine_MB_55920()) {
    SKIP("consistency checks in search engine has been fixed in 7.2.1, see MB-55920");
  }

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment is incompatible with parts of this test");
  }

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (integration.cluster_version().is_capella()) {
    SKIP("FIXME(SA): this test on Capella is not very stable. Revisit once Capella will use 7.6");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  const std::string params =
    R"(
            {
                "mapping": {
                    "default_mapping": {
                        "enabled": true,
                        "dynamic": true
                    },
                    "default_type": "_default",
                    "default_analyzer": "standard",
                    "default_field": "_all"
                },
                "doc_config": {
                    "mode": "type_field",
                    "type_field": "_type"
                }
            }
        )";

  auto index_name = test::utils::uniq_id("search_index");
  const std::string requested_index_name = index_name;
  couchbase::core::operations::management::search_index_drop_request drop_requested_index_req{};
  drop_requested_index_req.index_name = index_name;
  const test::utils::drop_guard drop_requested_index{ integration.cluster,
                                                      "search index " + index_name,
                                                      drop_requested_index_req };

  {
    couchbase::core::management::search::index index{};
    index.name = index_name;
    index.params_json = params;
    index.type = "fulltext-index";
    index.source_name = integration.ctx.bucket;
    index.source_type = "couchbase";
    if (integration.cluster_version().requires_search_replicas()) {
      index.plan_params_json = couchbase::core::utils::json::generate({
        { "indexPartitions", 1 },
        { "numReplicas", 1 },
      });
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    if (index_name != resp.name) {
      CB_LOG_INFO("update index name \"{}\" -> \"{}\"", index_name, resp.name);
    }
    index_name = resp.name;
  }
  // The server may rename the index; a renamed one gets a guard of its own.
  std::optional<
    test::utils::drop_guard<couchbase::core::operations::management::search_index_drop_request>>
    drop_renamed_index;
  if (!index_name.empty() && index_name != requested_index_name) {
    couchbase::core::operations::management::search_index_drop_request drop_index_req{};
    drop_index_req.index_name = index_name;
    drop_renamed_index.emplace(integration.cluster, "search index " + index_name, drop_index_req);
  }

  REQUIRE(test::utils::wait_for_search_pindexes_ready(
    integration.cluster, integration.ctx.bucket, index_name));

  auto value = test::utils::uniq_id("value");
  auto id = couchbase::core::document_id(
    integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("key"));

  // FIXME: MB-55920, consistency checks is broken in all known versions of the servers at the
  // moment,
  //                  it might return empty results without waiting for the mutation. We cannot
  //                  workaround it in any way. We know that doing a mutation, and then query with
  //                  consistency checks in the loop is not proper test, but at least it will check
  //                  the payload format for now, and later when the issue is fixed, the loop has to
  //                  be removed.
  couchbase::mutation_token token;
  {
    // now update the document and use its mutation token in query later
    auto resp =
      test::utils::execute(integration.cluster,
                           couchbase::core::operations::upsert_request{
                             id,
                             couchbase::core::utils::json::generate_binary(tao::json::value{
                               { "_type", "test_doc" },
                               { "value", value },
                             }),
                           });
    REQUIRE_SUCCESS(resp.ctx.ec());
    token = resp.token;
  }

  /*
   * Retry query with consistency check until it will succeed or reach 20 attempts.
   *
   * FTS might return empty results. See MB-55920.
   */
  int attempt = 0;
  bool done = false;
  while (!done) {
    const tao::json::value query{ { "query", fmt::format("value:{}", value) } };
    auto query_json = couchbase::core::json_string(couchbase::core::utils::json::generate(query));

    {
      couchbase::core::operations::search_request req{};
      req.index_name = index_name;
      req.query = query_json;
      req.mutation_state.emplace_back(token);
      req.timeout = std::chrono::minutes{ 5 };
      auto resp = test::utils::execute(integration.cluster, req);
      if (resp.ctx.ec == couchbase::errc::search::consistency_mismatch) {
        // FIXME(MB-55920): ignore "err: bleve: pindex_consistency mismatched partition"
        CB_LOG_INFO("ignore consistency_mismatch: {}", resp.ctx.http_body);
        continue;
      }
      INFO(resp.ctx.http_body);
      REQUIRE_SUCCESS(resp.ctx.ec);
      switch (resp.rows.size()) {
        case 1:
          done = true;
          break;

        case 0:
          if (attempt > 20) {
            FAIL("Unable to use search query with consistency. Giving up.");
          }
          ++attempt;
          break;

        default:
          REQUIRE(resp.rows.size() == 1);
          break;
      }
    }
  }
}

TEST_CASE("integration: search query collections", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }

  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  auto index_name = test::utils::uniq_id("search_index");
  auto collection1_name = test::utils::uniq_id("collection");
  auto collection2_name = test::utils::uniq_id("collection");
  std::string doc = R"({"name": "test"})";
  const test::utils::drop_guard drop_collection1{
    integration.cluster,
    "collection " + collection1_name,
    couchbase::core::operations::management::collection_drop_request{
      integration.ctx.bucket, "_default", collection1_name },
  };
  const test::utils::drop_guard drop_collection2{
    integration.cluster,
    "collection " + collection2_name,
    couchbase::core::operations::management::collection_drop_request{
      integration.ctx.bucket, "_default", collection2_name },
  };

  for (const auto& collection : { collection1_name, collection2_name }) {
    {
      couchbase::core::operations::management::collection_create_request req{
        integration.ctx.bucket, "_default", collection
      };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      auto created = test::utils::wait_until_collection_manifest_propagated(
        integration.cluster, integration.ctx.bucket, resp.uid);
      REQUIRE(created);
    }

    {
      auto key = test::utils::uniq_id("key");
      auto id = couchbase::core::document_id(integration.ctx.bucket, "_default", collection, key);
      couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(doc) };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec());
    }
  }

  const std::string requested_index_name = index_name;
  couchbase::core::operations::management::search_index_drop_request drop_requested_index_req{};
  drop_requested_index_req.index_name = index_name;
  const test::utils::drop_guard drop_requested_index{ integration.cluster,
                                                      "search index " + index_name,
                                                      drop_requested_index_req };

  {
    // clang-format off
        std::string params =
          R"(
            {
                "mapping": {
                    "types": {
                        "_default.)" + collection1_name + R"(": {
                            "enabled": true,
                            "dynamic": true
                        },
                        "_default.)" + collection2_name + R"(": {
                            "enabled": true,
                            "dynamic": true
                        }
                    },
                    "default_mapping": {
                        "enabled": false
                    },
                    "default_type": "_default",
                    "default_analyzer": "standard",
                    "default_field": "_all"
                },
                "doc_config": {
                    "mode": "scope.collection.type_field"
                }
            }
        )";
    // clang-format on

    couchbase::core::management::search::index index{};
    index.name = index_name;
    index.params_json = params;
    index.type = "fulltext-index";
    index.source_name = integration.ctx.bucket;
    index.source_type = "couchbase";
    if (integration.cluster_version().requires_search_replicas()) {
      index.plan_params_json = couchbase::core::utils::json::generate({
        { "indexPartitions", 1 },
        { "numReplicas", 1 },
      });
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;

    couchbase::core::operations::management::search_index_upsert_response resp;
    bool operation_completed = test::utils::wait_until([&integration, &resp, req]() {
      resp = test::utils::execute(integration.cluster, req);
      std::regex scope_not_found("collection: '.+' doesn't belong to scope");
      return !std::regex_search(resp.error, scope_not_found);
    });
    REQUIRE(operation_completed);
    REQUIRE_SUCCESS(resp.ctx.ec);
    if (index_name != resp.name) {
      CB_LOG_INFO("update index name \"{}\" -> \"{}\"", index_name, resp.name);
    }
    index_name = resp.name;
  }
  // The server may rename the index; a renamed one gets a guard of its own.
  // Declared after the collection guards, so the index is dropped before its collections.
  std::optional<
    test::utils::drop_guard<couchbase::core::operations::management::search_index_drop_request>>
    drop_renamed_index;
  if (!index_name.empty() && index_name != requested_index_name) {
    couchbase::core::operations::management::search_index_drop_request drop_index_req{};
    drop_index_req.index_name = index_name;
    drop_renamed_index.emplace(integration.cluster, "search index " + index_name, drop_index_req);
  }

  REQUIRE(test::utils::wait_until_indexed(integration.cluster, index_name, 2));

  couchbase::core::json_string simple_query(R"({"query": "name:test"})");

  // no collection parameter - both docs returned
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 2);
  }

  // one collection - only docs from that collection returned
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.collections.emplace_back(collection1_name);
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 1);
  }

  // two collections - both docs returned
  {
    couchbase::core::operations::search_request req{};
    req.index_name = index_name;
    req.query = simple_query;
    req.collections.emplace_back(collection1_name);
    req.collections.emplace_back(collection2_name);
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 2);
  }
}

TEST_CASE("integration: scope search returns feature not available", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.cluster_version().supports_scope_search()) {
    SKIP("cluster supports scope search");
  }

  auto c = integration.public_cluster();

  auto search_request = couchbase::search_request(couchbase::match_none_query{});
  auto [error, result] =
    c.bucket(integration.ctx.bucket).default_scope().search("does-not-exist", search_request).get();

  REQUIRE(error.ec() == couchbase::errc::common::feature_not_available);
}

TEST_CASE("integration: score fusion is refused when the cluster does not advertise it",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }
  if (integration.cluster_version().supports_score_fusion()) {
    SKIP("cluster supports score fusion");
  }

  auto c = integration.public_cluster();
  const couchbase::search_request request{ couchbase::match_none_query{} };

  auto [error, result] =
    c.search("does-not-exist",
             request,
             couchbase::search_options{}.scoring(
               couchbase::search_scoring_reciprocal_rank_fusion{}.rank_constant(60)))
      .get();

  REQUIRE(error.ec() == couchbase::errc::common::feature_not_available);
}

TEST_CASE("integration: disabled scoring is not behind the score fusion capability",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }
  if (integration.cluster_version().supports_score_fusion()) {
    SKIP("cluster supports score fusion, so the gate would not turn anything away");
  }

  auto c = integration.public_cluster();
  const couchbase::search_request request{ couchbase::match_none_query{} };

  auto [error, result] =
    c.search("does-not-exist",
             request,
             couchbase::search_options{}.scoring(couchbase::search_scoring_none{}))
      .get();

  // The index does not exist, so this must fail -- but on the server's terms. A capability refusal
  // here would mean "none" had been gated behind score fusion, which it must never be.
  REQUIRE(error.ec());
  REQUIRE(error.ec() != couchbase::errc::common::feature_not_available);
}

TEST_CASE("integration: custom script queries are refused when the cluster does not advertise them",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search()) {
    SKIP("cluster does not support search");
  }
  if (integration.cluster_version().supports_search_custom_script_queries()) {
    SKIP("cluster supports custom script queries");
  }

  auto c = integration.public_cluster();

  SECTION("custom score query")
  {
    const couchbase::search_request request{ couchbase::custom_score_query(
      couchbase::match_none_query{}, "function score(doc, params) { return doc.score; }") };
    auto [error, result] = c.search("does-not-exist", request).get();
    REQUIRE(error.ec() == couchbase::errc::common::feature_not_available);
  }

  SECTION("custom filter query nested in a boolean query")
  {
    const couchbase::search_request request{ couchbase::boolean_query{}.must_not(
      couchbase::custom_filter_query(couchbase::match_none_query{},
                                     "function filter(doc, params) { return true; }")) };
    auto [error, result] = c.search("does-not-exist", request).get();
    REQUIRE(error.ec() == couchbase::errc::common::feature_not_available);
  }
}

TEST_CASE("integration: custom score and filter queries", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_search_custom_script_queries()) {
    SKIP("cluster does not support custom script queries");
  }
  if (integration.cluster_version().is_capella()) {
    SKIP("the feature is enabled through the search service's manager options");
  }

  // The feature is disabled on the server by default, and is switched back off when the test ends.
  auto custom_script_queries_enabled = [](bool enabled) {
    couchbase::core::operations::management::freeform_request req{};
    req.type = couchbase::core::service_type::search;
    req.method = "PUT";
    req.path = "/api/managerOptions";
    req.headers["content-type"] = "application/json";
    req.body = couchbase::core::utils::json::generate(
      tao::json::value{ { "customScriptQueriesEnabled", enabled ? "true" : "false" } });
    return req;
  };
  {
    auto resp = test::utils::execute(integration.cluster, custom_script_queries_enabled(true));
    INFO(resp.body);
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.status == 200);
  }
  const test::utils::drop_guard disable_custom_script_queries{
    integration.cluster, "the custom script queries setting", custom_script_queries_enabled(false)
  };

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  const std::string params = R"(
{
  "mapping": {
    "default_mapping": {"enabled": true, "dynamic": true},
    "default_type": "_default",
    "default_analyzer": "standard",
    "default_field": "_all"
  },
  "doc_config": {"mode": "type_field", "type_field": "_type"}
}
)";

  auto index_name = test::utils::uniq_id("search_index");
  const std::string requested_index_name = index_name;
  couchbase::core::operations::management::search_index_drop_request drop_requested_index_req{};
  drop_requested_index_req.index_name = index_name;
  const test::utils::drop_guard drop_requested_index{ integration.cluster,
                                                      "search index " + index_name,
                                                      drop_requested_index_req };
  {
    couchbase::core::management::search::index index{};
    index.name = index_name;
    index.params_json = params;
    index.type = "fulltext-index";
    index.source_name = integration.ctx.bucket;
    index.source_type = "couchbase";
    if (integration.cluster_version().requires_search_replicas()) {
      index.plan_params_json = couchbase::core::utils::json::generate({
        { "indexPartitions", 1 },
        { "numReplicas", 1 },
      });
    }
    couchbase::core::operations::management::search_index_upsert_request req{};
    req.index = index;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    index_name = resp.name;
  }
  // The server may rename the index; a renamed one gets a guard of its own.
  std::optional<
    test::utils::drop_guard<couchbase::core::operations::management::search_index_drop_request>>
    drop_renamed_index;
  if (!index_name.empty() && index_name != requested_index_name) {
    couchbase::core::operations::management::search_index_drop_request drop_index_req{};
    drop_index_req.index_name = index_name;
    drop_renamed_index.emplace(integration.cluster, "search index " + index_name, drop_index_req);
  }

  REQUIRE(test::utils::wait_for_search_pindexes_ready(
    integration.cluster, integration.ctx.bucket, index_name));

  const auto tag = test::utils::uniq_id("tag");
  const auto cheap_id = test::utils::uniq_id("cheap");
  const auto mid_id = test::utils::uniq_id("mid");
  const auto dear_id = test::utils::uniq_id("dear");
  for (const auto& [key, price] : std::vector<std::pair<std::string, int>>{
         { cheap_id, 50 }, { mid_id, 150 }, { dear_id, 300 } }) {
    const couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", key };
    auto resp =
      test::utils::execute(integration.cluster,
                           couchbase::core::operations::upsert_request{
                             id,
                             couchbase::core::utils::json::generate_binary(tao::json::value{
                               { "_type", "custom_script_query_doc" },
                               { "tag", tag },
                               { "price", price },
                             }),
                           });
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  auto c = integration.public_cluster();
  const couchbase::query_string_query tagged{ "tag:" + tag };

  REQUIRE(test::utils::wait_until(
    [&]() {
      auto [error, result] = c.search(index_name, couchbase::search_request{ tagged }).get();
      return !error.ec() && result.rows().size() == 3;
    },
    std::chrono::minutes{ 1 }));

  SECTION("custom filter query")
  {
    const couchbase::search_request request{
      couchbase::custom_filter_query(
        tagged, "function within_budget(doc, params) { return doc.fields.price <= params.budget; }")
        .fields({ "price" })
        .parameters(std::pair{ "budget", 200 }),
    };
    auto [error, result] = c.search(index_name, request).get();
    INFO(error.message());
    REQUIRE_SUCCESS(error.ec());
    std::set<std::string> ids;
    for (const auto& row : result.rows()) {
      ids.insert(row.id());
    }
    REQUIRE(ids == std::set<std::string>{ cheap_id, mid_id });
  }

  SECTION("custom score query")
  {
    const couchbase::search_request request{
      couchbase::custom_score_query(
        tagged, "function by_price(doc, params) { return doc.fields.price * params.factor; }")
        .fields({ "price" })
        .parameters(std::pair{ "factor", 2 }),
    };
    auto [error, result] = c.search(index_name, request).get();
    INFO(error.message());
    REQUIRE_SUCCESS(error.ec());
    REQUIRE(result.rows().size() == 3);
    REQUIRE(result.rows()[0].id() == dear_id);
    REQUIRE(result.rows()[0].score() == 600);
    REQUIRE(result.rows()[1].id() == mid_id);
    REQUIRE(result.rows()[1].score() == 300);
    REQUIRE(result.rows()[2].id() == cheap_id);
    REQUIRE(result.rows()[2].score() == 100);
  }
}
