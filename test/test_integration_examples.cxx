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

#include "couchbase/configuration_profiles_registry.hxx"
#include "test_helper_integration.hxx"

#include "core/operations/management/query_index_build.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_get_all.hxx"
#include "utils/logger.hxx"

#include <couchbase/boolean_query.hxx>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/error.hxx>
#include <couchbase/fmt/mutation_token.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/term_facet.hxx>

#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/ranges.h>
#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

#ifndef _WIN32
#include <sys/wait.h>
#endif

//! [start-using-hotel]
// [6] definition of the custom type and its decoder
struct hotel {
  std::string id{};
  std::string name{};
  std::string country{};
  double average_rating{};
};

template<>
struct tao::json::traits<hotel> {
  template<template<typename...> class Traits>
  static auto as(const tao::json::basic_value<Traits>& v) -> hotel
  {
    hotel result;
    auto object = v.get_object();
    result.id = object["id"].template as<std::string>();
    result.average_rating = object["avg_rating"].template as<double>();
    result.name = object["doc"]["title"].template as<std::string>();
    result.country = object["doc"]["country"].template as<std::string>();
    return result;
  }
};
//! [start-using-hotel]

namespace start_using
{
//! [start-using]
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/fmt/cas.hxx>
#include <couchbase/fmt/mutation_token.hxx>

#include <tao/json.hpp>
#include <tao/json/contrib/traits.hpp>

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

  auto options = couchbase::cluster_options(username, password);
  // customize through the 'options'.
  // For example, optimize timeouts for WAN
  options.apply_profile("wan_development");

  // [1] connect to cluster using the given connection string and the options
  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  // get a bucket reference
  auto bucket = cluster.bucket(bucket_name);

  // get a user-defined collection reference
  auto scope = bucket.scope("tenant_agent_00");
  auto collection = scope.collection("users");

  { // [2] upsert document
    std::string doc_id = "my-document";
    auto [err, upsert_result] =
      collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
    if (err.ec()) {
      fmt::print("unable to upsert the document \"{}\": {}\n", doc_id, err);
      return 1;
    }
    fmt::print("saved document \"{}\", cas={}, token={}\n",
               doc_id,
               upsert_result.cas(),
               upsert_result.mutation_token().value());
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
    // Select first 5 hotels from US or UK, that describe themselves as cheap
    // and order them by overall rating.
    std::string query{ R"(
        SELECT META(h).id, h AS doc,
               AVG(r.ratings.Overall) AS avg_rating
        FROM hotel h
        UNNEST h.reviews r
        WHERE h.country IN $1 AND h.description LIKE "%cheap%"
        GROUP BY META(h).id, h
        ORDER BY avg_rating DESC
        LIMIT 5;
    )" };
    auto query_options = couchbase::query_options{}.positional_parameters(
      std::vector{ "United States", "United Kingdom" });
    auto [error, query_result] = inventory_scope.query(query, query_options).get();
    if (error) {
      fmt::print("unable to perform query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::println("{:<15} {:<15} {:>10} {:<30}", "ID", "Country", "Rating", "Hotel");
    for (auto& row : query_result.rows_as()) {
      fmt::println("{:<15} {:<15} {:>10.2f} {:<30}",
                   row["id"].as<std::string>(),
                   row["doc"]["country"].as<std::string>(),
                   row["avg_rating"].as<double>(),
                   row["doc"]["title"].as<std::string>());
    }

    // [5] iterate over results using custom type
    fmt::println("{:<15} {:<15} {:>10} {:<30}", "ID", "Country", "Rating", "Hotel");
    for (const auto& row : query_result.rows_as<couchbase::codec::tao_json_serializer, hotel>()) {
      fmt::println(
        "{:<15} {:<15} {:>10.2f} {:<30}", row.id, row.country, row.average_rating, row.name);
    }
  }

  // [7] close cluster connection
  cluster.close().get();
  return 0;
}

/*

$ ./start_using couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17ed9f687ee90000, token=travel-sample:110:101634532779186:101
retrieved document "my-document", name="mike"
ID              Country             Rating Hotel
hotel_26169     United States         4.75 San Francisco/Twin Peaks-Lake Merced
hotel_26499     United States         4.60 Santa Monica
hotel_3616      United Kingdom        4.57 Birmingham (England)
hotel_7387      United States         4.50 Death Valley National Park
hotel_25588     United States         4.44 San Francisco/Civic Center-Tenderloin

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
    SKIP("GOCAVES cannot load travel-sample bucket. See "
         "https://github.com/couchbaselabs/gocaves/issues/101");
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

class github_actions_configuration_profile : public couchbase::configuration_profile
{
public:
  void apply(couchbase::cluster_options& options) override
  {
    options.timeouts()
      .search_timeout(std::chrono::minutes(5))
      .management_timeout(std::chrono::minutes(5));
  }
};

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

  auto options = couchbase::cluster_options(username, password);
  // customize through the 'options'.
  // For example, optimize timeouts for WAN
  couchbase::configuration_profiles_registry::register_profile(
    "github_actions", std::make_shared<github_actions_configuration_profile>());
  options.apply_profile("github_actions");

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }

  {
    fmt::print("--- simple query\n");
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(couchbase::query_string_query("nice bar")))
        .get();

    if (error.ec()) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
    for (const auto& row : result.rows()) {
      fmt::print("id: {}, score: {}\n", row.id(), row.score());
    }
  }

  {
    fmt::print("--- simple query with fields\n");
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(couchbase::query_string_query("nice bar")),
                couchbase::search_options{}.fields({ "description" }))
        .get();

    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
    for (const auto& row : result.rows()) {
      auto fields = row.fields_as<couchbase::codec::tao_json_serializer>();
      fmt::print("id: {}, score: {}, description: {}\n",
                 row.id(),
                 row.score(),
                 fields["description"].as<std::string>());
    }
  }

  {
    fmt::print("--- simple query with limit\n");
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(couchbase::query_string_query("nice bar")),
                couchbase::search_options{}.skip(3).limit(4))
        .get();

    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
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
                couchbase::search_options{}.highlight(couchbase::highlight_style::html,
                                                      { "description", "title" }))
        .get();

    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
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
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(couchbase::query_string_query("west")),
                couchbase::search_options{}.collections({ "airline" }))
        .get();

    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
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
                "The inn was located just where the East Road bent round the foot of Bree-hill, "
                "within the dike that stretched "
                "around the town. The building was three stories tall with many windows. Its "
                "front faced the Road and it had two "
                "wings that ran back towards the elevated ground of the hill, such that in the "
                "rear the second floor was at ground "
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

    auto start = std::chrono::system_clock::now();
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(couchbase::query_string_query("bree")),
                couchbase::search_options{}.consistent_with(state))
        .get();
    auto stop = std::chrono::system_clock::now();

    if (error) {
      fmt::print("unable to perform search query: {} ({}), time: {} or {}, context: {}\n",
                 error.ec().message(),
                 error.message(),
                 std::chrono::duration_cast<std::chrono::milliseconds>(stop - start),
                 std::chrono::duration_cast<std::chrono::seconds>(stop - start),
                 error.ctx().to_json());
      return 1;
    }
    fmt::print("{} hits, total: {}, time: {} or {} (server reported {})\n",
               result.rows().size(),
               result.meta_data().metrics().total_rows(),
               std::chrono::duration_cast<std::chrono::milliseconds>(stop - start),
               std::chrono::duration_cast<std::chrono::seconds>(stop - start),
               result.meta_data().metrics().took());
    for (const auto& row : result.rows()) {
      fmt::print("id: {}, score: {}\n", row.id(), row.score());
    }
  }

  {
    fmt::print("--- complex query\n");
    auto [error, result] =
      cluster
        .search("travel-sample-index",
                couchbase::search_request(
                  couchbase::boolean_query()
                    .must(couchbase::match_query("honeymoon").field("reviews.content"),
                          couchbase::numeric_range_query().field("reviews.ratings.Overall").min(4))
                    .must_not(couchbase::match_query("San Francisco").field("city"))),
                couchbase::search_options{}.collections({ "hotel" }).highlight())
        .get();
    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
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
                couchbase::search_options{}
                  .collections({ "hotel" })
                  .facet("by_country", couchbase::term_facet("country", 3)))
        .get();
    if (error) {
      fmt::print("unable to perform search query: {}\n", error.ctx().to_json());
      return 1;
    }
    fmt::print(
      "{} hits, total: {}\n", result.rows().size(), result.meta_data().metrics().total_rows());
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
  cluster.close().get();
  return 0;
}

/*

$ ./example_search couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17486a1722b20000
retrieved document "my-document", name="mike"
row: {"airline":{"callsign":"MILE-AIR","country":"United
States","iata":"Q5","icao":"MLA","id":10,"name":"40-Mile Air","type":"airline"}}

 */
//! [example-search]
} // namespace example_search

TEST_CASE("example: search", "[integration]")
{
  {
    test::utils::integration_test_guard integration;

    if (integration.cluster_version().is_capella()) {
      SKIP("Capella does not allow to use REST API to load sample buckets");
    }
    if (!integration.cluster_version().supports_collections()) {
      SKIP("cluster does not support collections");
    }

    test::utils::create_search_index(integration,
                                     "travel-sample",
                                     "travel-sample-index",
                                     integration.cluster_version().is_mad_hatter()
                                       ? "travel_sample_index_params_v6.json"
                                       : "travel_sample_index_params.json");
  }

  const auto env = test::utils::test_context::load_from_environment();
  const char* argv[] = {
    "example_search", // name of the "executable"
    env.connection_string.c_str(),
    env.username.c_str(),
    env.password.c_str(),
  };

  REQUIRE(example_search::main(4, argv) == 0);

  {
    test::utils::integration_test_guard integration;
    test::utils::drop_search_index(integration, "travel-sample-index");
  }
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

  auto options = couchbase::cluster_options(username, password);
  // customize through the 'options'.
  // For example, optimize timeouts for WAN
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("unable to connect to the cluster: {}\n", connect_err);
    return 1;
  }
  auto manager = cluster.buckets();

  couchbase::management::cluster::bucket_settings bucket_settings{};
  std::string test_bucket_name = "cxx_test_integration_examples_bucket";
  bucket_settings.name = test_bucket_name;
  bucket_settings.ram_quota_mb = 150;
  bucket_settings.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
  bucket_settings.eviction_policy =
    couchbase::management::cluster::bucket_eviction_policy::value_only;
  bucket_settings.flush_enabled = true;
  bucket_settings.replica_indexes = true;
  bucket_settings.conflict_resolution_type =
    couchbase::management::cluster::bucket_conflict_resolution::sequence_number;
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
  fmt::print("--- wait for couple of seconds (in highly distributed deployment, bucket creation "
             "might take few moments)\n");
  std::this_thread::sleep_for(std::chrono::seconds{ 2 });
  {
    auto [ctx, bucket] = manager.get_bucket(test_bucket_name).get();
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
  cluster.close().get();
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

  auto options = couchbase::cluster_options(username, password);
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(connection_string, options).get();
  if (connect_err) {
    fmt::print("PARENT(pid={}): sunable to connect to the cluster: {}\n", getpid(), connect_err);
    return 1;
  }

  auto bucket = cluster.bucket(bucket_name);

  cluster.notify_fork(couchbase::fork_event::prepare);
  auto child_pid = fork();
  if (child_pid == 0) {
    cluster.notify_fork(couchbase::fork_event::child);

    fmt::print("CHILD(pid={}): continue after fork()\n", getpid());
    auto collection = bucket.scope("tenant_agent_00").collection("users");

    {
      fmt::print("CHILD(pid={}): upsert into collection\n", getpid());
      std::string doc_id = "child-document";
      auto [err, upsert_result] =
        collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
      if (err.ec()) {
        fmt::print(
          "CHILD(pid={}): unable to upsert the document \"{}\": {}\n", getpid(), doc_id, err);
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
      std::string doc_id = "child-document";
      auto [err, get_result] = collection.get(doc_id).get();
      if (err.ec()) {
        fmt::print("CHILD(pid={}): unable to get the document \"{}\": {}\n", getpid(), doc_id, err);
        return 1;
      }
      auto name = get_result.content_as<tao::json::value>()["name"].get_string();
      fmt::print("CHILD(pid={}): retrieved document \"{}\", name=\"{}\"\n", getpid(), doc_id, name);
    }

  } else {
    cluster.notify_fork(couchbase::fork_event::parent);
    fmt::print("PARENT(pid={}): continue after fork() child_pid={}\n", getpid(), child_pid);

    {
      auto collection = bucket.scope("tenant_agent_00").collection("users");
      std::string doc_id = "tenant_agent_00";
      auto [err, upsert_result] =
        collection.upsert(doc_id, tao::json::value{ { "name", "mike" } }).get();
      if (err.ec()) {
        fmt::print("unable to upsert the document \"{}\": {}\n", doc_id, err);
        return 1;
      }
      fmt::print("saved document \"{}\", cas={}, token={}\n",
                 doc_id,
                 upsert_result.cas(),
                 upsert_result.mutation_token().value());
    }
    {
      auto inventory_scope = bucket.scope("inventory");
      auto [error, query_result] =
        inventory_scope.query("SELECT * FROM airline WHERE id = 10").get();
      if (error) {
        fmt::print(
          "PARENT(pid={}): unable to perform query: {}\n", getpid(), error.ctx().to_json());
        return 1;
      }
      for (const auto& row : query_result.rows_as()) {
        fmt::print("PARENT(pid={}): row: {}\n", getpid(), tao::json::to_string(row));
      }
    }

    int status{};
    fmt::print("PARENT(pid={}): waiting for child pid={}...\n", getpid(), child_pid);
    const auto rc = waitpid(child_pid, &status, 0);

    if (rc == -1) {
      fmt::print(
        "PARENT(pid={}): unable to wait for child pid={} (rc={})\n", getpid(), child_pid, rc);
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
    fmt::print("PARENT(pid={}): Child pid={} returned {}, {}\n",
               getpid(),
               child_pid,
               WEXITSTATUS(status),
               pretty_status(status));
  }

  fmt::print("COMMON(pid={}): close cluster\n", getpid());
  cluster.close().get();
  return 0;
}

/*

$ ./example_fork couchbase://127.0.0.1 Administrator password
saved document "my-document", cas=17486a1722b20000
retrieved document "my-document", name="mike"
row: {"airline":{"callsign":"MILE-AIR","country":"United
States","iata":"Q5","icao":"MLA","id":10,"name":"40-Mile Air","type":"airline"}}

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
