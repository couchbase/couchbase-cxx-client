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
#include <couchbase/operations/management/collection_create.hxx>
#include <couchbase/operations/management/search_index_drop.hxx>
#include <couchbase/operations/management/search_index_get_documents_count.hxx>
#include <couchbase/operations/management/search_index_upsert.hxx>

using Catch::Matchers::StartsWith;

void
wait_until_indexed(std::shared_ptr<couchbase::cluster> cluster, const std::string& index_name, std::uint64_t expected_count)
{
    auto indexed = test ::utils::wait_until([cluster = std::move(cluster), &index_name, &expected_count]() {
        couchbase::operations::management::search_index_get_documents_count_request req{};
        req.index_name = index_name;
        req.timeout = std::chrono::seconds{ 1 };
        auto resp = test::utils::execute(cluster, req);
        return resp.count >= expected_count;
    });
    REQUIRE(indexed);
}

TEST_CASE("integration: search query")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        auto sample_data = couchbase::utils::json::parse(couchbase::json_string(test::utils::read_test_data("search_beers_dataset.json")));
        auto const& o = sample_data.get_object();
        for (const auto& [key, value] : o) {
            couchbase::document_id id(integration.ctx.bucket, "_default", "_default", key);
            couchbase::operations::upsert_request req{ id, couchbase::utils::json::generate(value) };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }

    std::string index_name = test::utils::uniq_id("beer-search-index");

    {
        auto params = test::utils::read_test_data("search_beers_index_params.json");

        couchbase::management::search::index index{};
        index.name = index_name;
        index.params_json = params;
        index.type = "fulltext-index";
        index.source_name = integration.ctx.bucket;
        index.source_type = "couchbase";
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;

        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE((!resp.ctx.ec || resp.ctx.ec == couchbase::error::common_errc::index_exists));
    }

    couchbase::json_string simple_query(R"({"query": "description:belgian"})");

    std::uint64_t beer_sample_doc_count = 5;
    // Wait until expected documents are indexed
    {
        wait_until_indexed(integration.cluster, index_name, beer_sample_doc_count);
        auto ok = test::utils::wait_until([&]() {
            couchbase::operations::search_request req{};
            req.index_name = index_name;
            req.query = simple_query;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            return resp.rows.size() == beer_sample_doc_count;
        });
        REQUIRE(ok);
    }

    SECTION("simple query")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 5);
        REQUIRE(resp.rows[0].id == "four_peaks_brewing-abbey_normal_ale");
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
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.limit = 1;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
    }

    SECTION("skip")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.skip = 1;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == beer_sample_doc_count - 1);
        REQUIRE(resp.rows[0].id == "avery_brewing_company-reverend_the");
    }

    SECTION("explain")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.explain = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE_FALSE(resp.rows[0].explanation.empty());
    }

    if (integration.cluster_version().supports_search_disable_scoring()) {
        SECTION("disable scoring")
        {
            couchbase::operations::search_request req{};
            req.index_name = index_name;
            req.query = simple_query;
            req.disable_scoring = true;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.rows[0].score == 0);
            REQUIRE(resp.meta.metrics.max_score == 0);
        }
    }

    SECTION("include locations")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.include_locations = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows[0].locations.size() == 1);
        REQUIRE(resp.rows[0].locations[0].field == "description");
        REQUIRE(resp.rows[0].locations[0].term == "belgian");
        REQUIRE(resp.rows[0].locations[0].position == 2);
        REQUIRE(resp.rows[0].locations[0].start_offset == 2);
        REQUIRE(resp.rows[0].locations[0].end_offset == 9);
    }

    SECTION("highlight fields default highlight style")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.highlight_fields = { "description" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows[0].fragments["description"][0] == "A <mark>Belgian</mark> Double Abbey.");
    }

    SECTION("highlight style")
    {
        {
            couchbase::operations::search_request req{};
            req.index_name = index_name;
            req.query = simple_query;
            req.highlight_fields = { "description" };
            req.highlight_style = couchbase::search_highlight_style::html;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.rows[0].fragments["description"][0] == "A <mark>Belgian</mark> Double Abbey.");
        }

        {
            couchbase::operations::search_request req{};
            req.index_name = index_name;
            req.query = simple_query;
            req.highlight_fields = { "description" };
            req.highlight_style = couchbase::search_highlight_style::ansi;
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            // TODO: is there a better way to compare ansi strings?
            std::string snippet = resp.rows[0].fragments["description"][0];
            std::string open = "\x1b[43m";
            std::string close = "\x1b[0m";
            snippet.replace(snippet.find(open), open.size(), "<mark>");
            snippet.replace(snippet.find(close), close.size(), "</mark>");
            REQUIRE(snippet == "A <mark>Belgian</mark> Double Abbey.");
        }
    }

    SECTION("fields")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.fields.emplace_back("description");
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        auto fields = couchbase::utils::json::parse(resp.rows[0].fields).get_object();
        REQUIRE(fields.at("description").get_string() == "A Belgian Double Abbey.");
    }

    SECTION("sort")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.sort_specs.emplace_back(couchbase::utils::json::generate("_score"));
        req.timeout = std::chrono::seconds(1);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows[0].id == "bear_republic_brewery-red_rocket_ale");
    }

    SECTION("term facet")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.facets.insert(std::make_pair("type", R"({"field": "type", "size": 1})"));
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
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
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.facets.insert(std::make_pair(
          "updated",
          R"({"field": "updated", "size": 2, "date_ranges": [{"name": "old", "end": "2010-08-01"},{"name": "new", "start": "2010-08-01"}]})"));
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
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
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.facets.insert(std::make_pair(
          "abv", R"({"field": "abv", "size": 2, "numeric_ranges": [{"name": "high", "min": 7},{"name": "low", "max": 7}]})"));
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.facets.size() == 1);
        REQUIRE(resp.facets[0].name == "abv");
        REQUIRE(resp.facets[0].field == "abv");
        REQUIRE(resp.facets[0].total == 5);
        REQUIRE(resp.facets[0].missing == 0);
        REQUIRE(resp.facets[0].other == 0);
        REQUIRE(resp.facets[0].numeric_ranges.size() == 2);
        auto high_range = std::find_if(resp.facets[0].numeric_ranges.begin(), resp.facets[0].numeric_ranges.end(), [](const auto& range) {
            return range.name == "high";
        });
        REQUIRE(high_range != resp.facets[0].numeric_ranges.end());
        REQUIRE(high_range->count == 2);
        REQUIRE(std::get<std::uint64_t>(high_range->min) == 7);
        REQUIRE(std::holds_alternative<std::monostate>(high_range->max));
        auto low_range = std::find_if(resp.facets[0].numeric_ranges.begin(), resp.facets[0].numeric_ranges.end(), [](const auto& range) {
            return range.name == "low";
        });
        REQUIRE(low_range != resp.facets[0].numeric_ranges.end());
        REQUIRE(low_range->count == 3);
        REQUIRE(std::holds_alternative<std::monostate>(low_range->min));
        REQUIRE(std::get<std::uint64_t>(low_range->max) == 7);
    }

    SECTION("raw")
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        std::map<std::string, couchbase::json_string> raw{};
        raw.insert(std::make_pair("size", couchbase::json_string("1")));
        req.raw = raw;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
    }
}

TEST_CASE("integration: search query consistency", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    std::string params =
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
                    "mode": "type_field"
                }
            }
        )";

    auto index_name = test::utils::uniq_id("search_index");

    {
        couchbase::management::search::index index{};
        index.name = index_name;
        index.params_json = params;
        index.type = "fulltext-index";
        index.source_name = integration.ctx.bucket;
        index.source_type = "couchbase";
        // TODO: there seems to be a server bug related to default number of partitions
        // remove when it is identified and fixed
        index.plan_params_json = R"({"indexPartitions": 1})";
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    REQUIRE(test::utils::wait_for_search_pindexes_ready(integration.cluster, integration.ctx.bucket, index_name));

    auto key = test::utils::uniq_id("key");
    auto id = couchbase::document_id(integration.ctx.bucket, "_default", "_default", key);
    auto value = test::utils::uniq_id("value");
    auto doc = couchbase::utils::json::generate(tao::json::value{
      { "value", value },
    });

    couchbase::mutation_token token;

    {
        couchbase::operations::upsert_request req{ id, doc };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        token = resp.token;
    }

    tao::json::value query{ { "query", fmt::format("value:{}", value) } };
    auto query_json = couchbase::json_string(couchbase::utils::json::generate(query));

    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = query_json;
        req.mutation_state.emplace_back(token);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
    }

    {
        couchbase::operations::management::search_index_drop_request req{};
        req.index_name = index_name;
        test::utils::execute(integration.cluster, req);
    }
}

TEST_CASE("integration: search query collections")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_collections()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto index_name = test::utils::uniq_id("search_index");
    auto collection1_name = test::utils::uniq_id("collection");
    auto collection2_name = test::utils::uniq_id("collection");
    std::string doc = R"({"name": "test"})";

    for (const auto& collection : { collection1_name, collection2_name }) {
        {
            couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, "_default", collection };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
            REQUIRE(created);
        }

        {
            auto key = test::utils::uniq_id("key");
            auto id = couchbase::document_id(integration.ctx.bucket, "_default", collection, key);
            couchbase::operations::upsert_request req{ id, doc };
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
        }
    }

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

        couchbase::management::search::index index{};
        index.name = index_name;
        index.params_json = params;
        index.type = "fulltext-index";
        index.source_name = integration.ctx.bucket;
        index.source_type = "couchbase";
        couchbase::operations::management::search_index_upsert_request req{};
        req.index = index;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    wait_until_indexed(integration.cluster, index_name, 2);

    couchbase::json_string simple_query(R"({"query": "name:test"})");

    // no collections parameter - both docs returned
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 2);
    }

    // one collection - only docs from that collection returned
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.collections.emplace_back(collection1_name);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 1);
    }

    // two collections - both docs returned
    {
        couchbase::operations::search_request req{};
        req.index_name = index_name;
        req.query = simple_query;
        req.collections.emplace_back(collection1_name);
        req.collections.emplace_back(collection2_name);
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.rows.size() == 2);
    }
}