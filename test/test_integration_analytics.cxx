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

#include "core/operations/management/analytics.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collections.hxx"
#include "test_helper_integration.hxx"

TEST_CASE("integration: analytics query")
{
    test::utils::integration_test_guard integration;

    if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
        SKIP("elixir deployment does not support analytics");
    }

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
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
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value);
            resp = test::utils::execute(integration.cluster, req);
            return resp.rows.size() == 1;
        }));
        REQUIRE_SUCCESS(resp.ctx.ec);
        REQUIRE(resp.rows[0] == value);
        REQUIRE_FALSE(resp.meta.request_id.empty());
        REQUIRE_FALSE(resp.meta.client_context_id.empty());
        REQUIRE(resp.meta.status == couchbase::core::operations::analytics_response::analytics_status::success);
    }

    SECTION("positional params")
    {
        couchbase::core::operations::analytics_response resp{};
        REQUIRE(test::utils::wait_until([&]() {
            couchbase::core::operations::analytics_request req{};
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = ?)", dataset_name);
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
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
            req.named_parameters["testkey"] = couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
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
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
            req.named_parameters["$testkey"] = couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
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
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name);
            req.raw["$testkey"] = couchbase::core::json_string(couchbase::core::utils::json::generate(test_value));
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
                couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(value) };
                REQUIRE_SUCCESS(test::utils::execute(integration.cluster, req).ctx.ec());
            }

            couchbase::core::operations::analytics_request req{};
            req.statement = fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value);
            req.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
            resp = test::utils::execute(integration.cluster, req);
            /* Analytics might give us code 23027, ignore it here
             *
             * "errors": [{"code": 23027, "msg": "Bucket default on link Default.Local is not connected"} ],
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
        REQUIRE(resp.meta.status == couchbase::core::operations::analytics_response::analytics_status::fatal);
    }

    {
        couchbase::core::operations::management::analytics_dataset_drop_request req{};
        req.dataset_name = dataset_name;
        test::utils::execute(integration.cluster, req);
    }
}

TEST_CASE("integration: analytics scope query")
{
    test::utils::integration_test_guard integration;

    if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
        SKIP("elixir deployment does not support analytics");
    }

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");

    {
        couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    CHECK(test::utils::wait_until(
      [&]() {
          couchbase::core::operations::analytics_request req{};
          req.statement =
            fmt::format("ALTER COLLECTION `{}`.`{}`.`{}` ENABLE ANALYTICS", integration.ctx.bucket, scope_name, collection_name);
          auto resp = test::utils::execute(integration.cluster, req);
          return !resp.ctx.ec;
      },
      std::chrono::minutes{ 5 }));

    auto key = test::utils::uniq_id("key");
    auto test_value = test::utils::uniq_id("value");
    auto value = couchbase::core::utils::json::generate({ { "testkey", test_value } });
    {
        auto id = couchbase::core::document_id(integration.ctx.bucket, scope_name, collection_name, key);
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary(value) };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
    }

    couchbase::core::operations::analytics_response resp{};
    REQUIRE(test::utils::wait_until([&]() {
        couchbase::core::operations::analytics_request req{};
        req.statement = fmt::format(R"(SELECT testkey FROM `{}` WHERE testkey = "{}")", collection_name, test_value);
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        resp = test::utils::execute(integration.cluster, req);
        return resp.rows.size() == 1;
    }));
    REQUIRE_SUCCESS(resp.ctx.ec);
    REQUIRE(resp.rows[0] == value);

    {
        couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
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
    couchbase::core::http_context ctx{ config, cluster_options, query_cache, hostname, port };
    return ctx;
}

TEST_CASE("unit: analytics query")
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

TEST_CASE("integration: public API analytics query")
{
    test::utils::integration_test_guard integration;

    if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
        SKIP("elixir deployment does not support analytics");
    }

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
    }

    auto cluster = couchbase::cluster(integration.cluster);
    auto bucket = cluster.bucket(integration.ctx.bucket);
    auto collection = bucket.default_collection();

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
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
            std::tie(ctx, resp) =
              cluster.analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value))
                .get();
            return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
        }));
        REQUIRE_SUCCESS(ctx.ec());
        REQUIRE_FALSE(resp.meta_data().request_id().empty());
        REQUIRE_FALSE(resp.meta_data().client_context_id().empty());
        REQUIRE(resp.meta_data().status() == couchbase::analytics_status::success);
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("positional params")
    {
        couchbase::analytics_result resp{};
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
            std::tie(ctx, resp) = cluster
                                    .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = ?)", dataset_name),
                                                     couchbase::analytics_options{}.positional_parameters(test_value))
                                    .get();
            return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
        }));
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("named params")
    {
        couchbase::analytics_result resp{};
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
            std::tie(ctx, resp) =
              cluster
                .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name),
                                 couchbase::analytics_options{}.named_parameters(std::pair{ "testkey", test_value }))
                .get();
            return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
        }));
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("named params preformatted")
    {
        couchbase::analytics_result resp{};
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
            std::tie(ctx, resp) =
              cluster
                .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name),
                                 couchbase::analytics_options{}.encoded_named_parameters(
                                   { { "testkey", couchbase::core::utils::json::generate_binary(test_value) } }))
                .get();
            return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
        }));
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("raw")
    {
        couchbase::analytics_result resp{};
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
            std::tie(ctx, resp) =
              cluster
                .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = $testkey)", dataset_name),
                                 couchbase::analytics_options{}.raw("$testkey", test_value))
                .get();
            return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
        }));
        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("consistency")
    {
        couchbase::analytics_result resp{};
        couchbase::analytics_error_context ctx{};
        CHECK(test::utils::wait_until([&]() {
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

            std::tie(ctx, resp) =
              cluster
                .analytics_query(fmt::format(R"(SELECT testkey FROM `Default`.`{}` WHERE testkey = "{}")", dataset_name, test_value),
                                 couchbase::analytics_options{}.scan_consistency(couchbase::analytics_scan_consistency::request_plus))
                .get();
            /* Analytics might give us code 23027, ignore it here
             *
             * "errors": [{"code": 23027, "msg": "Bucket default on link Default.Local is not connected"} ],
             */
            return ctx.first_error_code() != 23027;
        }));

        REQUIRE_SUCCESS(ctx.ec());
        auto rows = resp.rows_as_json();
        REQUIRE(rows.size() == 1);
        REQUIRE(rows[0] == document);
    }

    SECTION("readonly")
    {
        auto [ctx, resp] =
          cluster.analytics_query(fmt::format("DROP DATASET Default.`{}`", dataset_name), couchbase::analytics_options{}.readonly(true))
            .get();

        REQUIRE(ctx.ec() == couchbase::errc::common::internal_server_failure);
        REQUIRE(resp.meta_data().status() == couchbase::analytics_status::fatal);
    }

    {
        couchbase::core::operations::management::analytics_dataset_drop_request req{};
        req.dataset_name = dataset_name;
        test::utils::execute(integration.cluster, req);
    }
}

TEST_CASE("integration: public API analytics scope query")
{
    test::utils::integration_test_guard integration;

    if (integration.ctx.deployment == test::utils::deployment_type::elixir) {
        SKIP("elixir deployment does not support analytics");
    }

    if (!integration.cluster_version().supports_analytics()) {
        SKIP("cluster does not support analytics");
    }
    if (!integration.cluster_version().supports_collections()) {
        SKIP("cluster does not support collections");
    }

    auto cluster = couchbase::cluster(integration.cluster);
    auto bucket = cluster.bucket(integration.ctx.bucket);

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");

    {
        const couchbase::core::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    {
        const couchbase::core::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec);
        auto created = test::utils::wait_until_collection_manifest_propagated(integration.cluster, integration.ctx.bucket, resp.uid);
        REQUIRE(created);
    }

    CHECK(test::utils::wait_until(
      [&]() {
          auto [ctx, resp] = cluster
                               .analytics_query(fmt::format(
                                 "ALTER COLLECTION `{}`.`{}`.`{}` ENABLE ANALYTICS", integration.ctx.bucket, scope_name, collection_name))
                               .get();
          return !ctx.ec();
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
    couchbase::analytics_error_context ctx{};
    CHECK(test::utils::wait_until([&]() {
        std::tie(ctx, resp) =
          scope.analytics_query(fmt::format(R"(SELECT testkey FROM `{}` WHERE testkey = "{}")", collection_name, test_value)).get();
        return !ctx.ec() && resp.meta_data().metrics().result_count() == 1;
    }));
    REQUIRE_SUCCESS(ctx.ec());
    REQUIRE(resp.rows_as_json()[0] == document);
    REQUIRE_FALSE(resp.meta_data().request_id().empty());
    REQUIRE_FALSE(resp.meta_data().client_context_id().empty());
    REQUIRE(resp.meta_data().status() == couchbase::analytics_status::success);

    {
        const couchbase::core::operations::management::scope_drop_request req{ integration.ctx.bucket, scope_name };
        test::utils::execute(integration.cluster, req);
    }
}
