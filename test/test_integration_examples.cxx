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
#include "utils/logger.hxx"

#include <couchbase/boolean_query.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/mutation_token.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/term_facet.hxx>

#include <tao/json.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#endif

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
    std::thread io_thread([&io]() {
        io.run();
    });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    // [1] connect to cluster using the given connection string and the options
    auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
    if (connect_err) {
        fmt::print("unable to connect to the cluster: {}\n", connect_err.message());
        return 1;
    }

    // get a bucket reference
    auto bucket = cluster.bucket(bucket_name);

    // get a user-defined collection reference
    auto scope = bucket.scope("tenant_agent_00");
    auto collection = scope.collection("users");

    { // [2] upsert document
        std::string doc_id = "my-document";
        auto [err, upsert_result] = collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
        if (err.ec()) {
            fmt::print("unable to upsert the document \"{}\": {}\n", doc_id, err);
            return 1;
        }
        fmt::print("saved document \"{}\", cas={}, token={}\n", doc_id, upsert_result.cas(), upsert_result.mutation_token().value());
    }

    { // [3] get document
        std::string doc_id = "my-document";
        auto [err, get_result] = collection.get(doc_id).get();
        if (err.ec()) {
            fmt::print("unable to get the document \"{}\": {}\n", doc_id, err);
            return 1;
        }
        auto name = get_result.content_as<tao::json::value>()["name"].get_string();
        fmt::print("retrieved document \"{}\", name=\"{}\"\n", doc_id, name);
    }

    { // [4] N1QL query
        auto inventory_scope = bucket.scope("inventory");
        auto [error, query_result] = inventory_scope.query("SELECT * FROM airline WHERE id = 10").get();
        if (error) {
            fmt::print("unable to perform query: {}\n", error.ctx().to_json());
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
    if (integration.cluster_version().is_capella()) {
        SKIP("Capella does not allow to use REST API to load sample buckets");
    }
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
    std::thread io_thread([&io]() {
        io.run();
    });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
    if (connect_err) {
        fmt::print("unable to connect to the cluster: {}\n", connect_err.message());
        return 1;
    }

    {
        fmt::print("--- simple query\n");
        auto [error, result] =
          cluster.search("travel-sample-index", couchbase::search_request(couchbase::query_string_query("nice bar"))).get();

        if (error.ec()) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with fields\n");
        auto [error, result] = cluster
                                 .search("travel-sample-index",
                                         couchbase::search_request(couchbase::query_string_query("nice bar")),
                                         couchbase::search_options{}.fields({ "description" }))
                                 .get();

        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
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
        auto [error, result] = cluster
                                 .search("travel-sample-index",
                                         couchbase::search_request(couchbase::query_string_query("nice bar")),
                                         couchbase::search_options{}.skip(3).limit(4))
                                 .get();

        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with highlight\n");
        auto [error, result] =
          cluster
            .search("travel-sample-index",
                    couchbase::search_request(couchbase::query_string_query("nice bar")),
                    couchbase::search_options{}.highlight(couchbase::highlight_style::html, { "description", "title" }))
            .get();

        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
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
        auto [error, result] = cluster
                                 .search("travel-sample-index",
                                         couchbase::search_request(couchbase::query_string_query("west")),
                                         couchbase::search_options{}.collections({ "airline" }))
                                 .get();

        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
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
            std::string doc_id = "prancing-pony";
            auto [err, upsert_result] =
              collection
                .upsert(
                  doc_id,
                  tao::json::value{
                    { "title", "The Prancing Pony" },
                    { "type", "hotel" },
                    { "description",
                      "The inn was located just where the East Road bent round the foot of Bree-hill, within the dike that stretched "
                      "around the town. The building was three stories tall with many windows. Its front faced the Road and it had two "
                      "wings that ran back towards the elevated ground of the hill, such that in the rear the second floor was at ground "
                      "level. " } })
                .get();
            if (err.ec()) {
                fmt::print("unable to upsert the document \"{}\": {}\n", doc_id, err);
                return 1;
            }
            fmt::print("saved document \"{}\", cas={}, token={}\n",
                       doc_id,
                       upsert_result.cas(),
                       upsert_result.mutation_token().value_or(couchbase::mutation_token{}));
            state.add(upsert_result);
        }

        auto [error, result] = cluster
                                 .search("travel-sample-index",
                                         couchbase::search_request(couchbase::query_string_query("bree")),
                                         couchbase::search_options{}.consistent_with(state))
                                 .get();

        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- complex query\n");
        auto [error, result] =
          cluster
            .search("travel-sample-index",
                    couchbase::search_request(couchbase::boolean_query()
                                                .must(couchbase::match_query("honeymoon").field("reviews.content"),
                                                      couchbase::numeric_range_query().field("reviews.ratings.Overall").min(4))
                                                .must_not(couchbase::match_query("San Francisco").field("city"))),
                    couchbase::search_options{}.collections({ "hotel" }).highlight())
            .get();
        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
            return 1;
        }
        fmt::print("{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
        for (const auto& row : result.rows()) {
            fmt::print("id: {}, score: {}\n", row.id(), row.score());
        }
    }

    {
        fmt::print("--- simple query with facets\n");
        auto [error, result] =
          cluster
            .search("travel-sample-index",
                    couchbase::search_request(couchbase::query_string_query("honeymoon")),
                    couchbase::search_options{}.collections({ "hotel" }).facet("by_country", couchbase::term_facet("country", 3)))
            .get();
        if (error) {
            fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
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

    if (integration.cluster_version().is_capella()) {
        SKIP("Capella does not allow to use REST API to load sample buckets");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
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

namespace example_buckets
{
//! [example-buckets]
#include <couchbase/cluster.hxx>

int
main(int argc, const char* argv[])
{
    if (argc != 4) {
        fmt::print("USAGE: ./example_buckets couchbase://127.0.0.1 Administrator password\n");
        return 1;
    }

    std::string connection_string{ argv[1] }; // "couchbase://127.0.0.1"
    std::string username{ argv[2] };          // "Administrator"
    std::string password{ argv[3] };          // "password"
    std::string bucket_name{ "travel-sample" };

    // run IO context on separate thread
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() {
        io.run();
    });

    auto options = couchbase::cluster_options(username, password);
    // customize through the 'options'.
    // For example, optimize timeouts for WAN
    options.apply_profile("wan_development");

    auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
    if (connect_err) {
        fmt::print("unable to connect to the cluster: {}\n", connect_err.message());
        return 1;
    }
    auto manager = cluster.buckets();

    couchbase::management::cluster::bucket_settings bucket_settings{};
    std::string test_bucket_name = "cxx_test_integration_examples_bucket";
    bucket_settings.name = test_bucket_name;
    bucket_settings.ram_quota_mb = 150;
    bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
    bucket_settings.eviction_policy = couchbase::management::cluster::bucket_eviction_policy::value_only;
    bucket_settings.flush_enabled = true;
    bucket_settings.replica_indexes = true;
    bucket_settings.conflict_resolution_type = couchbase::management::cluster::bucket_conflict_resolution::sequence_number;
    {
        fmt::print("--- create bucket\n");
        auto err = manager.create_bucket(bucket_settings).get();
        if (err) {
            if (err.ec() == couchbase::errc::common::invalid_argument) {
                fmt::print("bucket already exists\n");
            } else {
                fmt::print("unable to create the bucket: {}\n", err.ec().message());
                return 1;
            }
        } else {
            fmt::print("--- bucket has been successfully created\n");
        }
    }
    {
        fmt::print("--- get bucket\n");
        auto [ctx, bucket] = manager.get_bucket(bucket_name).get();
        if (ctx.ec()) {
            fmt::print("unable to get the bucket: {}\n", ctx.ec().message());
            return 1;
        }
        fmt::print("name of fetched bucket: {}\n", bucket.name);
    }
    {
        fmt::print("--- get all buckets\n");
        auto [ctx, buckets] = manager.get_all_buckets().get();
        if (ctx.ec()) {
            fmt::print("unable to get all buckets: {}\n", ctx.ec().message());
            return 1;
        }
        for (const auto& fetched_bucket : buckets) {
            fmt::print("Bucket name: {}\n", fetched_bucket.name);
        }
    }
    {
        fmt::print("--- update bucket\n");
        bucket_settings.ram_quota_mb = 150;
        auto ctx = manager.update_bucket(bucket_settings).get();
        if (ctx.ec()) {
            fmt::print("unable to update the bucket: {}\n", ctx.ec().message());
            return 1;
        }
        fmt::print("bucket has been updated\n");
    }
    {
        fmt::print("--- drop bucket\n");
        auto ctx = manager.drop_bucket(test_bucket_name).get();
        if (ctx.ec()) {
            fmt::print("unable to drop the bucket: {}\n", ctx.ec().message());
            return 1;
        }
        fmt::print("bucket has been dropped\n");
    }

    // close cluster connection
    cluster.close();
    guard.reset();

    io_thread.join();
    return 0;
}

//! [example-buckets]
} // namespace example_buckets

TEST_CASE("example: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_capella()) {
        SKIP("Capella does not allow to use REST API to load sample buckets");
    }

    const auto env = test::utils::test_context::load_from_environment();

    const char* argv[] = {
        "example_buckets", // name of the "executable"
        env.connection_string.c_str(),
        env.username.c_str(),
        env.password.c_str(),
    };

    REQUIRE(example_buckets::main(4, argv) == 0);
}

#ifndef _WIN32
namespace example_fork
{
//! [fork-for-scaling]
#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>

#include <tao/json.hpp>

#include <sys/wait.h>

int
main(int argc, const char* argv[])
{
    if (argc != 4) {
        fmt::print("USAGE: ./example_fork couchbase://127.0.0.1 Administrator password\n");
        return 1;
    }

    std::string connection_string{ argv[1] }; // "couchbase://127.0.0.1"
    std::string username{ argv[2] };          // "Administrator"
    std::string password{ argv[3] };          // "password"
    std::string bucket_name{ "travel-sample" };

    // run IO context on separate thread
    asio::io_context io;
    auto guard = asio::make_work_guard(io);
    std::thread io_thread([&io]() {
        fmt::print("PARENT(pid={}): start IO thread\n", getpid());
        io.run();
        fmt::print("PARENT(pid={}): stop IO thread\n", getpid());
    });

    auto options = couchbase::cluster_options(username, password);
    options.apply_profile("wan_development");

    auto [connect_err, cluster] = couchbase::cluster::connect(io, connection_string, options).get();
    if (connect_err) {
        fmt::print("PARENT(pid={}): sunable to connect to the cluster: {}\n", getpid(), connect_err.message());
        return 1;
    }

    auto bucket = cluster.bucket(bucket_name);

    cluster.notify_fork(couchbase::fork_event::prepare);
    guard.reset();
    io.stop();
    io_thread.join();
    io.notify_fork(asio::execution_context::fork_prepare);
    auto child_pid = fork();
    if (child_pid == 0) {
        io.notify_fork(asio::execution_context::fork_child);
        cluster.notify_fork(couchbase::fork_event::child);
        io.restart();
        fmt::print("CHILD(pid={}): restarting IO thread\n", getpid());
        auto child_guard = asio::make_work_guard(io);
        io_thread = std::thread([&io]() {
            fmt::print("CHILD(pid={}): start new IO thread\n", getpid());
            io.run();
            fmt::print("CHILD(pid={}): stop new IO thread\n", getpid());
        });

        fmt::print("CHILD(pid={}): continue after fork()\n", getpid());
        auto collection = bucket.scope("tenant_agent_00").collection("users");

        {
            fmt::print("CHILD(pid={}): upsert into collection\n", getpid());
            std::string doc_id = "child-document";
            auto [err, upsert_result] = collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
            if (err.ec()) {
                fmt::print("CHILD(pid={}): unable to upsert the document \"{}\": {}\n", getpid(), doc_id, err);
                return 1;
            }
            fmt::print("CHILD(pid={}): saved document \"{}\", cas={}, token={}\n",
                       getpid(),
                       doc_id,
                       upsert_result.cas(),
                       upsert_result.mutation_token().value());
        }

        {
            fmt::print("CHILD(pid={}): get from collection\n", getpid());
            std::string doc_id = "parent-document";
            auto [err, get_result] = collection.get(doc_id).get();
            if (err.ec()) {
                fmt::print("CHILD(pid={}): unable to get the document \"{}\": {}\n", getpid(), doc_id, err);
                return 1;
            }
            auto name = get_result.content_as<tao::json::value>()["name"].get_string();
            fmt::print("CHILD(pid={}): retrieved document \"{}\", name=\"{}\"\n", getpid(), doc_id, name);
        }

        child_guard.reset();
    } else {
        io.notify_fork(asio::execution_context::fork_parent);
        cluster.notify_fork(couchbase::fork_event::parent);
        io.restart();
        auto parent_guard = asio::make_work_guard(io);
        fmt::print("PARENT(pid={}): restarting IO thread\n", getpid());
        io_thread = std::thread([&io]() {
            fmt::print("PARENT(pid={}): start IO new thread\n", getpid());
            io.run();
            fmt::print("PARENT(pid={}): stop IO new thread\n", getpid());
        });
        fmt::print("PARENT(pid={}): continue after fork() child_pid={}\n", getpid(), child_pid);

        {
            auto collection = bucket.scope("tenant_agent_00").collection("users");
            std::string doc_id = "tenant_agent_00";
            auto [err, upsert_result] = collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
            if (err.ec()) {
                fmt::print("unable to upsert the document \"{}\": {}\n", doc_id, err);
                return 1;
            }
            fmt::print("saved document \"{}\", cas={}, token={}\n", doc_id, upsert_result.cas(), upsert_result.mutation_token().value());
        }
        {
            auto inventory_scope = bucket.scope("inventory");
            auto [error, query_result] = inventory_scope.query("SELECT * FROM airline WHERE id = 10").get();
            if (error) {
                fmt::print("PARENT(pid={}): unable to perform query: {}\n", getpid(), error.ctx().to_json());
                return 1;
            }
            for (const auto& row : query_result.rows_as_json()) {
                fmt::print("PARENT(pid={}): row: {}\n", getpid(), tao::json::to_string(row));
            }
        }
        parent_guard.reset();

        int status{};
        fmt::print("PARENT(pid={}): waiting for child pid={}...\n", getpid(), child_pid);
        const auto rc = waitpid(child_pid, &status, 0);

        if (rc == -1) {
            fmt::print("PARENT(pid={}): unable to wait for child pid={} (rc={})\n", getpid(), child_pid, rc);
            return 1;
        }
        auto pretty_status = [](int status) {
            std::vector<std::string> flags{};
            if (WIFCONTINUED(status)) {
                flags.emplace_back("continued");
            }
            if (WIFSTOPPED(status)) {
                flags.emplace_back("stopped");
            }
            if (WIFEXITED(status)) {
                flags.emplace_back("exited");
            }
            if (WIFSIGNALED(status)) {
                flags.emplace_back("signaled");
            }
            if (const auto signal = WSTOPSIG(status); signal > 0) {
                flags.emplace_back(fmt::format("stopsig={}", signal));
            }
            if (const auto signal = WTERMSIG(status); signal > 0) {
                flags.emplace_back(fmt::format("termsig={}", signal));
            }
            return fmt::format("status=0x{:02x} ({})", status, fmt::join(flags, ", "));
        };
        fmt::print("PARENT(pid={}): Child pid={} returned {}, {}\n", getpid(), child_pid, WEXITSTATUS(status), pretty_status(status));
    }

    fmt::print("COMMON(pid={}): close cluster\n", getpid());
    cluster.close();

    fmt::print("COMMON(pid={}): join thread\n", getpid());
    io_thread.join();
    return 0;
}

/*

$ ./example_fork couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17486a1722b20000
retrieved document "my-document", name="mike"
row: {"airline":{"callsign":"MILE-AIR","country":"United States","iata":"Q5","icao":"MLA","id":10,"name":"40-Mile Air","type":"airline"}}

 */
//! [fork-for-scaling]

} // namespace example_fork

TEST_CASE("example: using fork() for scaling", "[integration]")
{
    {
        test::utils::integration_test_guard integration;
        if (integration.cluster_version().is_capella()) {
            SKIP("Capella does not allow to use REST API to load sample buckets");
        }
        if (!integration.cluster_version().supports_collections()) {
            SKIP("cluster does not support collections");
        }
    }

    setbuf(stdout, nullptr); // disable buffering for output
    test::utils::init_logger();

    const auto env = test::utils::test_context::load_from_environment();

    const char* argv[] = {
        "example_fork", // name of the "executable"
        env.connection_string.c_str(),
        env.username.c_str(),
        env.password.c_str(),
    };

    REQUIRE(example_fork::main(4, argv) == 0);
}
#endif
