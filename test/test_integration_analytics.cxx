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

#include <couchbase/codec/tao_json_serializer.hxx>

#include "core/error_context/analytics_json.hxx"
#include "core/impl/internal_error_context.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/management/analytics.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collections.hxx"

TEST_CASE("integration: analytics query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }

  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  auto dataset_name = test::utils::uniq_id("dataset");

  {
    couchbase::core::operations::management::analytics_dataset_create_request req{};
    req.dataset_name = dataset_name;
    req.bucket_name = integration.ctx.bucket;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  {
    couchbase::core::operations::management::analytics_link_connect_request req{};
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  auto key = test::utils::uniq_id("key");
  auto test_value = test::utils::uniq_id("value");
  auto value = couchbase::core::utils::json::generate({ { "testkey", test_value } });
  {
    auto id = couchbase::core::document_id(integration.ctx.bucket, "_default", "_default", key);
    couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(value) };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  SECTION("simple query")
  {
    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement = fmt::format(
        R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value);
      resp = test::utils::execute(integration.cluster, req);
      return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);
    REQUIRE_FALSE(resp.meta.request_id.empty());
    REQUIRE_FALSE(resp.meta.client_context_id.empty());
    REQUIRE(resp.meta.status ==
            couchbase::core::operations::analytics_response::analytics_status::success);
  }

  SECTION("positional params")
  {
    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement =
        fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = ?)", dataset_name);
      req.positional_parameters.emplace_back(couchbase::core::utils::json::generate(test_value));
      resp = test::utils::execute(integration.cluster, req);
      return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);
  }

  SECTION("named params")
  {
    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement =
        fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
      req.named_parameters["testkey"] =
        couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
      resp = test::utils::execute(integration.cluster, req);
      return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);
  }

  SECTION("named params preformatted")
  {
    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement =
        fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
      req.named_parameters["$testkey"] =
        couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
      resp = test::utils::execute(integration.cluster, req);
      return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);
  }

  SECTION("raw")
  {
    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement =
        fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
      req.raw["$testkey"] =
        couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
      resp = test::utils::execute(integration.cluster, req);
      return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);
  }

  SECTION("consistency")
  {
    couchbase::core::operations::analytics_response resp{};
    CHECK(test::utils::wait_until([&]() {
      /*
       * In consistency test, always do fresh mutation
       */
      test_value = test::utils::uniq_id("value");
      value = couchbase::core::utils::json::generate({ { "testkey", test_value } });
      {
        auto id = couchbase::core::document_id(integration.ctx.bucket, "_default", "_default", key);
        couchbase::core::operations::upsert_request req{ id,
                                                         couchbase::core::utils::to_binary(value) };
        REQUIRE_SUCCESS(test::utils::execute(integration.cluster, req).ctx.ec());
      }

      couchbase::core::operations::analytics_request req{};
      req.statement = fmt::format(
        R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value);
      req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
      resp = test::utils::execute(integration.cluster, req);
      /* Analytics might give us code 23027, ignore it here
       *
       * "errors": [{"code": 23027, "msg": "Bucket default on link Default.Local is not connected"}
       * ],
       */
      return resp.ctx.first_error_code != 23027;
    }));

    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows.size() == 1);
    REQUIRE(resp.rows[0] == value);
  }

  SECTION("readonly")
  {
    couchbase::core::operations::analytics_request req{};
    req.statement = fmt::format("DROP DATASET Default.`{}`", dataset_name);
    req.readonly = true;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE(resp.ctx.ec == couchbase::errc::common::internal_server_failure);
    REQUIRE(resp.meta.status ==
            couchbase::core::operations::analytics_response::analytics_status::fatal);
  }

  {
    couchbase::core::operations::management::analytics_dataset_drop_request req{};
    req.dataset_name = dataset_name;
    test::utils::execute(integration.cluster, req);
  }
}

TEST_CASE("integration: analytics scope query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }

  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  auto scope_name = test::utils::uniq_id("scope");
  auto collection_name = test::utils::uniq_id("collection");

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

  CHECK(test::utils::wait_until(
    [&]() {
      couchbase::core::operations::analytics_request req{};
      req.statement = fmt::format("ALTER COLLECTION `{}`.`{}`.`{}` ENABLE ANALYTICS",
                                  integration.ctx.bucket,
                                  scope_name,
                                  collection_name);
      auto resp = test::utils::execute(integration.cluster, req);
      return !resp.ctx.ec;
    },
    std::chrono::minutes{ 5 }));

  auto key = test::utils::uniq_id("key");
  auto test_value = test::utils::uniq_id("value");
  auto value = couchbase::core::utils::json::generate({ { "testkey", test_value } });
  {
    auto id =
      couchbase::core::document_id(integration.ctx.bucket, scope_name, collection_name, key);
    couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(value) };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec());
  }

  couchbase::core::operations::analytics_response resp{};
  REQUIRE(test::utils::wait_until([&]() {
    couchbase::core::operations::analytics_request req{};
    req.statement =
      fmt::format(R"(SELECT testkey FROM `{}` WHERE testkey = "{}")", collection_name, test_value);
    req.bucket_name = integration.ctx.bucket;
    req.scope_name = scope_name;
    resp = test::utils::execute(integration.cluster, req);
    return resp.rows.size() == 1;
  }));
  REQUIRE_SUCCESS(resp.ctx.ec);
  REQUIRE(resp.rows[0] == value);

  {
    couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket,
                                                                     scope_name };
    test::utils::execute(integration.cluster, req);
  }
}

couchbase::core::http_context
make_http_context()
{
  static couchbase::core::topology::configuration config{};
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  std::string canonical_hostname{};
  std::uint16_t canonical_port{};
  couchbase::core::http_context ctx{
    config, cluster_options, query_cache, hostname, port, canonical_hostname, canonical_port,
  };
  return ctx;
}

TEST_CASE("unit: analytics query", "[unit]")
{
  SECTION("priority true")
  {
    couchbase::core::io::http_request http_req;
    auto ctx = make_http_context();
    couchbase::core::operations::analytics_request req{};
    req.priority = true;
    auto ec = req.encode_to(http_req, ctx);
    REQUIRE_SUCCESS(ec);
    auto priority_header = http_req.headers.find("analytics-priority");
    REQUIRE(priority_header != http_req.headers.end());
    REQUIRE(priority_header->second == "-1");
  }

  SECTION("priority false")
  {
    couchbase::core::io::http_request http_req;
    auto ctx = make_http_context();
    couchbase::core::operations::analytics_request req{};
    req.priority = false;
    auto ec = req.encode_to(http_req, ctx);
    REQUIRE_SUCCESS(ec);
    REQUIRE(http_req.headers.find("analytics-priority") == http_req.headers.end());
  }
}

TEST_CASE("integration: public API analytics query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }

  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto dataset_name = test::utils::uniq_id("dataset");

  {
    couchbase::core::operations::management::analytics_dataset_create_request req{};
    req.dataset_name = dataset_name;
    req.bucket_name = integration.ctx.bucket;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  {
    couchbase::core::operations::management::analytics_link_connect_request req{};
    req.force = true;
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
  }

  auto key = test::utils::uniq_id("key");
  auto test_value = test::utils::uniq_id("value");
  tao::json::value document = {
    { "testkey", test_value },
  };
  {
    auto [ctx, resp] = collection.upsert(key, document).get();
    REQUIRE_SUCCESS(ctx.ec());
  }

  SECTION("simple query")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      std::tie(error, resp) =
        cluster
          .analytics_query(fmt::format(
            R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value))
          .get();
      return !error && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(error.ec());
    REQUIRE_FALSE(resp.meta_data().request_id().empty());
    REQUIRE_FALSE(resp.meta_data().client_context_id().empty());
    REQUIRE(resp.meta_data().status() == couchbase::analytics_status::success);
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("positional params")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      std::tie(error, resp) =
        cluster
          .analytics_query(
            fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = ?)", dataset_name),
            couchbase::analytics_options{}.positional_parameters(test_value))
          .get();
      return !error && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(error.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("named params")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      std::tie(error, resp) =
        cluster
          .analytics_query(
            fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)",
                        dataset_name),
            couchbase::analytics_options{}.named_parameters(std::pair{ "testkey", test_value }))
          .get();
      return !error && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(error.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("named params preformatted")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      std::tie(error, resp) =
        cluster
          .analytics_query(
            fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)",
                        dataset_name),
            couchbase::analytics_options{}.encoded_named_parameters(
              { { "testkey", couchbase::core::utils::json::generate_binary(test_value) } }))
          .get();
      return !error && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(error.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("raw")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      std::tie(error, resp) =
        cluster
          .analytics_query(
            fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)",
                        dataset_name),
            couchbase::analytics_options{}.raw("$testkey", test_value))
          .get();
      return !error && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(error.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("consistency")
  {
    couchbase::analytics_result resp{};
    couchbase::error error{};
    CHECK(test::utils::wait_until([&, cluster = cluster]() {
      /*
       * In consistency test, always do fresh mutation
       */
      test_value = test::utils::uniq_id("value");
      document = {
        { "testkey", test_value },
      };
      {
        auto [ctx2, _] = collection.upsert(key, document).get();
        REQUIRE_SUCCESS(ctx2.ec());
      }

      std::tie(error, resp) =
        cluster
          .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")",
                                       dataset_name,
                                       test_value),
                           couchbase::analytics_options{}.scan_consistency(
                             couchbase::analytics_scan_consistency::request_plus))
          .get();
      /* Analytics might give us code 23027, ignore it here
       *
       * "errors": [{"code": 23027, "msg": "Bucket default on link Default.Local is not connected"}
       * ],
       */

      if (!error.ec()) {
        return true;
      }
      return error.ctx().impl()->as<couchbase::core::error_context::analytics>().first_error_code !=
             23027;
    }));

    REQUIRE_SUCCESS(error.ec());
    auto rows = resp.rows_as();
    REQUIRE(rows.size() == 1);
    REQUIRE(rows[0] == document);
  }

  SECTION("readonly")
  {
    auto [error, resp] = cluster
                           .analytics_query(fmt::format("DROP DATASET Default.`{}`", dataset_name),
                                            couchbase::analytics_options{}.readonly(true))
                           .get();

    REQUIRE(error.ec() == couchbase::errc::common::internal_server_failure);
    REQUIRE(resp.meta_data().status() == couchbase::analytics_status::fatal);
  }

  {
    couchbase::core::operations::management::analytics_dataset_drop_request req{};
    req.dataset_name = dataset_name;
    test::utils::execute(integration.cluster, req);
  }
}

TEST_CASE("integration: public API analytics scope query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }

  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }
  if (!integration.cluster_version().supports_collections()) {
    SKIP("cluster does not support collections");
  }

  auto cluster = integration.public_cluster();
  auto bucket = cluster.bucket(integration.ctx.bucket);

  auto scope_name = test::utils::uniq_id("scope");
  auto collection_name = test::utils::uniq_id("collection");

  {
    const couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket,
                                                                             scope_name };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  {
    const couchbase::core::operations::management::collection_create_request req{
      integration.ctx.bucket, scope_name, collection_name
    };
    auto resp = test::utils::execute(integration.cluster, req);
    REQUIRE_SUCCESS(resp.ctx.ec);
    auto created = test::utils::wait_until_collection_manifest_propagated(
      integration.cluster, integration.ctx.bucket, resp.uid);
    REQUIRE(created);
  }

  CHECK(test::utils::wait_until(
    [&, cluster = cluster]() {
      auto [error, resp] =
        cluster
          .analytics_query(fmt::format("ALTER COLLECTION `{}`.`{}`.`{}` ENABLE ANALYTICS",
                                       integration.ctx.bucket,
                                       scope_name,
                                       collection_name))
          .get();
      return !error;
    },
    std::chrono::minutes{ 5 }));

  auto scope = bucket.scope(scope_name);
  auto collection = scope.collection(collection_name);

  auto key = test::utils::uniq_id("key");
  auto test_value = test::utils::uniq_id("value");
  const tao::json::value document = {
    { "testkey", test_value },
  };
  {
    auto [ctx, resp] = collection.upsert(key, document).get();
    REQUIRE_SUCCESS(ctx.ec());
  }

  couchbase::analytics_result resp{};
  couchbase::error error{};
  CHECK(test::utils::wait_until([&]() {
    std::tie(error, resp) =
      scope
        .analytics_query(fmt::format(
          R"(SELECT testkey FROM `{}` WHERE testkey = "{}")", collection_name, test_value))
        .get();
    return !error && resp.meta_data().metrics().result_count() == 1;
  }));
  REQUIRE_SUCCESS(error.ec());
  REQUIRE(resp.rows_as()[0] == document);
  REQUIRE_FALSE(resp.meta_data().request_id().empty());
  REQUIRE_FALSE(resp.meta_data().client_context_id().empty());
  REQUIRE(resp.meta_data().status() == couchbase::analytics_status::success);

  {
    const couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket,
                                                                           scope_name };
    test::utils::execute(integration.cluster, req);
  }
}

TEST_CASE("integration: public API analytics query using both named and positional parameters",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  auto cluster = integration.public_cluster();

  auto opts = couchbase::analytics_options().positional_parameters(20, "foo").named_parameters(
    std::pair{ "x", 10 });

  auto [err, res] = cluster.analytics_query("SELECT $x fieldA, $1 fieldB, $2 fieldC", opts).get();
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
// Generates `count` rows entirely inside the analytics engine without touching any dataset, which
// makes it a deterministic source for the streaming tests below.
auto
streaming_analytics_statement(std::size_t count) -> std::string
{
  // ORDER BY makes the row order deterministic: analytics does not otherwise guarantee ordering
  // across independent executions, and the "matches buffered" test compares the two row-by-row.
  return fmt::format("SELECT i AS n FROM array_range(0, {}) AS i ORDER BY i", count);
}
} // namespace

TEST_CASE("integration: streaming analytics yields rows lazily", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  auto cluster = integration.public_cluster();

  constexpr std::size_t total_rows = 2000;
  auto [err, result] =
    cluster.analytics_query_stream(streaming_analytics_statement(total_rows)).get();
  REQUIRE_SUCCESS(err.ec());

  // Pull only the first 5 rows, then abandon the rest.
  int pulled = 0;
  for (int i = 0; i < 5; ++i) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    REQUIRE(row.has_value());
    auto v = row->content_as<couchbase::codec::tao_json_serializer, tao::json::value>();
    REQUIRE(v.find("n") != nullptr);
    ++pulled;
  }
  REQUIRE(pulled == 5);
  result.cancel(); // abandon the remaining rows

  cluster.close().get();
}

TEST_CASE("integration: streaming analytics matches buffered analytics", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }

  auto cluster = integration.public_cluster();

  constexpr std::size_t total_rows = 1000;
  const auto statement = streaming_analytics_statement(total_rows);

  // Buffered reference, decoded to JSON values (not raw bytes).
  auto [berr, buffered] = cluster.analytics_query(statement).get();
  REQUIRE_SUCCESS(berr.ec());
  const auto expected_rows =
    buffered.rows_as<couchbase::codec::tao_json_serializer, tao::json::value>();
  REQUIRE(expected_rows.size() == total_rows);

  // Drain the entire result through the streaming API and compare row-by-row.
  auto [serr, result] = cluster.analytics_query_stream(statement).get();
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

  // Compare parsed JSON, not raw bytes: the streaming path carries the engine's row bytes verbatim
  // while the buffered path re-serializes each row, so on a service that pretty-prints (analytics
  // on some server versions) the raw bytes differ even though the content is identical. The
  // statement's ORDER BY fixes the row order across the two independent executions.
  REQUIRE(streamed_rows == expected_rows);

  // Metadata resolves once the stream has been fully drained.
  auto [merr, meta] = result.meta_data().get();
  REQUIRE_SUCCESS(merr.ec());
  REQUIRE(meta.status() == couchbase::analytics_status::success);

  cluster.close().get();
}

TEST_CASE("integration: cancelling a streaming analytics query mid-stream is clean",
          "[integration]")
{
  test::utils::integration_test_guard integration;
  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.analytics_query_stream(streaming_analytics_statement(2000)).get();
  REQUIRE_SUCCESS(err.ec());

  for (int i = 0; i < 3; ++i) {
    auto [rerr, row] = result.next().get();
    REQUIRE_SUCCESS(rerr.ec());
    REQUIRE(row.has_value());
  }

  // Cancel mid-stream — must not hang or corrupt memory (validated under ASan/TSan).
  result.cancel();

  cluster.close().get();
}

TEST_CASE(
  "integration: cancelling a streaming analytics query while a next() is in flight is clean",
  "[integration]")
{
  test::utils::integration_test_guard integration;
  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }
  auto cluster = integration.public_cluster();

  auto [err, result] = cluster.analytics_query_stream(streaming_analytics_statement(2000)).get();
  REQUIRE_SUCCESS(err.ec());

  // Issue an asynchronous pull and cancel it before its handler fires: this races cancel()'s
  // teardown against an in-flight channel receive. The handler must still be invoked (cancelled or
  // with a row); it must not hang or crash.
  auto done = std::make_shared<std::promise<void>>();
  auto fut = done->get_future();
  result.next([done](couchbase::error, std::optional<couchbase::analytics_row>) {
    done->set_value();
  });
  result.cancel();

  REQUIRE(fut.wait_for(std::chrono::seconds(10)) == std::future_status::ready);

  cluster.close().get();
}

TEST_CASE("integration: streaming analytics surfaces a query error", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("elixir deployment does not support analytics");
  }
  if (!integration.has_analytics_service()) {
    SKIP("cluster does not have analytics service");
  }
  auto cluster = integration.public_cluster();

  // References a dataset that does not exist, so the analytics engine rejects it.
  const std::string error_statement =
    "SELECT * FROM nonexistent_dataset_that_does_not_exist_xyz LIMIT 1";
  auto [err, result] = cluster.analytics_query_stream(error_statement).get();

  if (err.ec()) {
    // Error surfaced up front (before any row): acceptable.
    SUCCEED("error surfaced up front");
  } else {
    bool saw_error = false;
    while (true) {
      auto [rerr, row] = result.next().get();
      if (rerr.ec()) {
        saw_error = true;
        break;
      }
      if (!row.has_value()) {
        break;
      }
    }
    REQUIRE(saw_error);
  }

  cluster.close().get();
}
