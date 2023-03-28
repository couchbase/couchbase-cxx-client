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

#include <couchbase/cluster.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>

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

    std::string connection_string{ argv[1] };
    std::string username{ argv[2] };
    std::string password{ argv[3] };
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

    // get a bucket reference
    auto bucket = cluster.bucket(bucket_name);

    // get a user-defined collection reference
    auto scope = bucket.scope("tenant_agent_00");
    auto collection = scope.collection("users");

    {
        // upsert document
        auto [ctx, upsert_result] = collection.upsert("my-document", tao::json::value{ { "name", "mike" } }).get();
        if (ctx.ec()) {
            fmt::print("unable to upsert the document \"{}\": {}\n", ctx.id(), ctx.ec().message());
            return 1;
        }
        fmt::print("saved document \"{}\", cas={}, token={}\n", ctx.id(), upsert_result.cas(), upsert_result.mutation_token().value());
    }

    {
        // get document
        auto [ctx, get_result] = collection.get("my-document").get();
        if (ctx.ec()) {
            fmt::print("unable to get the document \"{}\": {}\n", ctx.id(), ctx.ec().message());
            return 1;
        }
        auto name = get_result.content_as<tao::json::value>()["name"].get_string();
        fmt::print("retrieved document \"{}\", name=\"{}\"\n", ctx.id(), name);
    }

    {
        // N1QL query
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

    // close cluster connection
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
        return;
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
