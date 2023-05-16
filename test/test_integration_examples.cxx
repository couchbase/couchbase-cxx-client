/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_get_all.hxx"

#include <couchbase/boolean_query.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/term_facet.hxx>

#include <tao/json.hpp>

namespace start_using
{
//! [start-using]
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>

#include <tao/json.hpp>

int
main(int argc, const char* argv[])
{
    if (argc != 4) {
        fmt::print("USAGE: ./start_using couchbase://127.0.0.1 Administrator password\n");
        return 1;
    }

    std::string connection_string{ argv[1] }; // "couchbase://127.0.0.1"
    std::string username{ argv[2] };          // "Administrator"
    std::string password{ argv[3] };          // "password"
    std::string bucket_name{ "travel-sample" };

    // run IO context on separate thread
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    // [1] connect to cluster using the given connection string and the options
    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    if (ec) {
        fmt::print("unable to connect to the cluster: {}\n", ec.message());
        return 1;
    }

    // get a bucket reference
    auto bucket = cluster.bucket(bucket_name);

    // get a user-defined collection reference
    auto scope = bucket.scope("tenant_agent_00");
    auto collection = scope.collection("users");

    { // [2] upsert document
        auto [ctx, upsert_result] = collection.upsert("my-document", tao::json::value{ { "name", "mike" } }).get();
        if (ctx.ec()) {
            fmt::print("unable to upsert the document \"{}\": {}\n", ctx.id(), ctx.ec().message());
            return 1;
        }
        fmt::print("saved document \"{}\", cas={}, token={}\n", ctx.id(), upsert_result.cas(), upsert_result.mutation_token().value());
    }

    { // [3] get document
        auto [ctx, get_result] = collection.get("my-document").get();
        if (ctx.ec()) {
            fmt::print("unable to get the document \"{}\": {}\n", ctx.id(), ctx.ec().message());
            return 1;
        }
        auto name = get_result.content_as<tao::json::value>()["name"].get_string();
        fmt::print("retrieved document \"{}\", name=\"{}\"\n", ctx.id(), name);
    }

    { // [4] N1QL query
        auto inventory_scope = bucket.scope("inventory");
        auto [ctx, query_result] = inventory_scope.query("SELECT * FROM airline WHERE id = 10").get();
        if (ctx.ec()) {
            fmt::print("unable to perform query: {}, ({}, {})\n", ctx.ec().message(), ctx.first_error_code(), ctx.first_error_message());
            return 1;
        }
        for (const auto& row : query_result.rows_as_json()) {
            fmt::print("row: {}\n", tao::json::to_string(row));
        }
    }

    // [5] close cluster connection
    cluster.close();
    guard.reset();

    io_thread.join();
    return 0;
}

/*

$ ./start_using couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17486a1722b20000
retrieved document "my-document", name="mike"
row: {"airline":{"callsign":"MILE-AIR","country":"United States","iata":"Q5","icao":"MLA","id":10,"name":"40-Mile Air","type":"airline"}}

 */
//! [start-using]

} // namespace start_using

TEST_CASE("example: start using", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }
    if (integration.cluster_version().is_mock()) {
        SKIP("GOCAVES cannot load travel-sample bucket. See https://github.com/couchbaselabs/gocaves/issues/101");
    }

    {
        couchbase::core::operations::management::query_index_create_request req{};
        req.index_name = "def_inventory_airline_primary";
        req.bucket_name = "travel-sample";
        req.scope_name = "inventory";
        req.collection_name = "airline";
        req.is_primary = true;
        req.ignore_if_exists = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::core::operations::management::query_index_build_request req{};
        req.index_names = { "def_inventory_airline_primary" };
        req.bucket_name = "travel-sample";
        req.scope_name = "inventory";
        req.collection_name = "airline";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    CHECK(test::utils::wait_until(
      [&integration]() {
          couchbase::core::operations::management::query_index_get_all_request req{};
          req.bucket_name = "travel-sample";
          req.scope_name = "inventory";
          auto resp = test::utils::execute(integration.cluster, req);
          if (resp.ctx.ec) {
              return false;
          }
          return std::any_of(resp.indexes.begin(), resp.indexes.end(), [](const auto& index) {
              return index.collection_name == "airline" && index.is_primary && index.state == "online";
          });
      },
      std::chrono::minutes{ 5 }));

    const auto env = test::utils::test_context::load_from_environment();
    const char* argv[] = {
        "start_using", // name of the "executable"
        env.connection_string.c_str(),
        env.username.c_str(),
        env.password.c_str(),
    };

    REQUIRE(start_using::main(4, argv) == 0);
}

namespace example_search
{
//! [example-search]
#include <couchbase/cluster.hxx>

#include <couchbase/boolean_query.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/query_string_query.hxx>

#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>

#include <tao/json.hpp>

int
main(int argc, const char* argv[])
{
    if (argc != 4) {
        fmt::print("USAGE: ./example_search couchbase://127.0.0.1 Administrator password\n");
        return 1;
    }

    std::string connection_string{ argv[1] }; // "couchbase://127.0.0.1"
    std::string username{ argv[2] };          // "Administrator"
    std::string password{ argv[3] };          // "password"
    std::string bucket_name{ "travel-sample" };

    // run IO context on separate thread
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    auto [cluster, ec] = couchbase::cluster::connect(io, connection_string, options).get();
    if (ec) {
        fmt::print("unable to connect to the cluster: {}\n", ec.message());
        return 1;
    }

    {
        fmt::print("--- simple query\n");
        auto [ctx, result] = cluster.search_query("travel-sample-index", couchbase::query_string_query("nice bar")).get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with fields\n");
        auto [ctx, result] = cluster
                               .search_query("travel-sample-index",
                                             couchbase::query_string_query("nice bar"),
                                             couchbase::search_options{}.fields({ "description" }))
                               .get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            auto fields = row.fields_as<couchbase::codec::tao_json_serializer>();
            fmt::print("id: {}, score: {}, description: {}\n", row.id(), row.score(), fields["description"].as<std::string>());
        }
    }

    {
        fmt::print("--- simple query with limit\n");
        auto [ctx, result] =
          cluster
            .search_query("travel-sample-index", couchbase::query_string_query("nice bar"), couchbase::search_options{}.skip(3).limit(4))
            .get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with highlight\n");
        auto [ctx, result] =
          cluster
            .search_query("travel-sample-index",
                          couchbase::query_string_query("nice bar"),
                          couchbase::search_options{}.highlight(couchbase::highlight_style::html, { "description", "title" }))
            .get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
            for (const auto& [field, fragments] : row.fragments()) {
                fmt::print("- {}:\n", field);
                for (const auto& fragment : fragments) {
                    fmt::print("-- {}\n", fragment);
                }
            }
        }
    }

    {
        fmt::print("--- simple query with collections\n");
        auto [ctx, result] = cluster
                               .search_query("travel-sample-index",
                                             couchbase::query_string_query("west"),
                                             couchbase::search_options{}.collections({ "airline" }))
                               .get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- query with consistency requirements\n");

        auto bucket = cluster.bucket(bucket_name);
        auto collection = bucket.scope("inventory").collection("hotel");

        couchbase::mutation_state state;

        {
            auto [ctx, upsert_result] =
              collection
                .upsert(
                  "prancing-pony",
                  tao::json::value{
                    { "title", "The Prancing Pony" },
                    { "type", "hotel" },
                    { "description",
                      "The inn was located just where the East Road bent round the foot of Bree-hill, within the dike that stretched "
                      "around the town. The building was three stories tall with many windows. Its front faced the Road and it had two "
                      "wings that ran back towards the elevated ground of the hill, such that in the rear the second floor was at ground "
                      "level. " } })
                .get();
            if (ctx.ec()) {
                fmt::print("unable to upsert the document \"{}\": {}\n", ctx.id(), ctx.ec().message());
                return 1;
            }
            fmt::print("saved document \"{}\", cas={}, token={}\n",
                       ctx.id(),
                       upsert_result.cas(),
                       upsert_result.mutation_token().value_or(couchbase::mutation_token{}));
            state.add(upsert_result);
        }

        auto [ctx, result] =
          cluster
            .search_query("travel-sample-index", couchbase::query_string_query("bree"), couchbase::search_options{}.consistent_with(state))
            .get();

        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- complex query\n");
        auto [ctx, result] = cluster
                               .search_query("travel-sample-index",
                                             couchbase::boolean_query()
                                               .must(couchbase::match_query("honeymoon").field("reviews.content"),
                                                     couchbase::numeric_range_query().field("reviews.ratings.Overall").min(4))
                                               .must_not(couchbase::match_query("San Francisco").field("city")),
                                             couchbase::search_options{}.collections({ "hotel" }).highlight())
                               .get();
        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with facets\n");
        auto [ctx, result] =
          cluster
            .search_query("travel-sample-index",
                          couchbase::query_string_query("honeymoon"),
                          couchbase::search_options{}.collections({ "hotel" }).facet("by_country", couchbase::term_facet("country", 3)))
            .get();
        if (ctx.ec()) {
            fmt::print("unable to perform search query: {}, ({}, {})\n", ctx.ec().message(), ctx.status(), ctx.error());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& [name, facet] : result.facets()) {
            fmt::print("{} facet: total={}, missing={}\n", name, facet->total(), facet->missing());
            if (name == "by_country") {
                auto term_facet = std::static_pointer_cast<couchbase::term_facet_result>(facet);
                for (const auto& group : term_facet->terms()) {
                    fmt::print("* {}: {}\n", group.name(), group.count());
                }
            }
        }
    }

    // close cluster connection
    cluster.close();
    guard.reset();

    io_thread.join();
    return 0;
}

/*

$ ./example_search couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17486a1722b20000
retrieved document "my-document", name="mike"
row: {"airline":{"callsign":"MILE-AIR","country":"United States","iata":"Q5","icao":"MLA","id":10,"name":"40-Mile Air","type":"airline"}}

 */
//! [example-search]
} // namespace example_search

TEST_CASE("example: search", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_collections()) {
        return;
    }

    const auto env = test::utils::test_context::load_from_environment();
    const char* argv[] = {
        "example_search", // name of the "executable"
        env.connection_string.c_str(),
        env.username.c_str(),
        env.password.c_str(),
    };

    REQUIRE(example_search::main(4, argv) == 0);
}
