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
#include <couchbase/operations/management/collections.hxx>
#include <couchbase/operations/management/query.hxx>

TEST_CASE("integration: trivial non-data query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: query with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        struct move_only_context {
          public:
            explicit move_only_context(std::string input)
              : payload_(std::move(input))
            {
            }
            move_only_context(move_only_context&& other) = default;
            move_only_context& operator=(move_only_context&& other) = default;
            ~move_only_context() = default;

            move_only_context(const move_only_context& other) = delete;
            move_only_context& operator=(const move_only_context& other) = delete;

            [[nodiscard]] const std::string& payload() const
            {
                return payload_;
            }

          private:
            std::string payload_;
        };

        couchbase::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
        auto barrier = std::make_shared<std::promise<couchbase::operations::query_response>>();
        auto f = barrier->get_future();
        move_only_context ctx("foobar");
        auto handler = [barrier, ctx = std::move(ctx)](couchbase::operations::query_response&& resp) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(std::move(resp));
        };
        integration.cluster.execute(req, std::move(handler));
        auto resp = f.get();
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
    }
}

TEST_CASE("integration: query on a collection", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.ctx.version.supports_collections()) {
        return;
    }
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    auto scope_name = test::utils::uniq_id("scope");
    auto collection_name = test::utils::uniq_id("collection");
    auto index_name = test::utils::uniq_id("index");
    auto key = test::utils::uniq_id("foo");
    tao::json::value value = {
        { "a", 1.0 },
        { "b", 2.0 },
    };
    auto json = couchbase::utils::json::generate(value);

    uint64_t scope_uid;
    uint64_t collection_uid;

    {
        couchbase::operations::management::scope_create_request req{ integration.ctx.bucket, scope_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        scope_uid = resp.uid;
    }

    {
        couchbase::operations::management::collection_create_request req{ integration.ctx.bucket, scope_name, collection_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        collection_uid = resp.uid;
    }

    // wait until scope and collection have propagated
    test::utils::wait_until([&]() {
        couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", "" };
        couchbase::operations::management::collections_manifest_get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        return resp.manifest.uid >= scope_uid && resp.manifest.uid >= collection_uid;
    });

    {
        couchbase::operations::management::query_index_create_request req{};
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.collection_name = collection_name;
        req.index_name = index_name;
        req.is_primary = true;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    couchbase::mutation_token mutation_token;

    {
        couchbase::document_id id{ integration.ctx.bucket, scope_name, collection_name, key };
        couchbase::operations::insert_request req{ id, json };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        mutation_token = resp.token;
    }

    SECTION("correct scope and collection")
    {
        couchbase::operations::query_request req{ "SELECT a, b FROM " + collection_name + " WHERE META().id = \"" + key + "\"" };
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.payload.rows.size() == 1);
        REQUIRE(value == couchbase::utils::json::parse(resp.payload.rows[0]));
    }

    SECTION("missing scope")
    {
        couchbase::operations::query_request req{ "SELECT a, b FROM " + collection_name + " WHERE META().id = \"" + key + "\"" };
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = "missing_scope";
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::query_errc::index_failure);
    }

    SECTION("missing collection")
    {
        couchbase::operations::query_request req{ "SELECT a, b FROM missing_collection WHERE META().id = \"" + key + "\"" };
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.mutation_state = { mutation_token };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::query_errc::index_failure);
    }

    SECTION("prepared")
    {
        couchbase::operations::query_request req{ "SELECT a, b FROM " + collection_name + " WHERE META().id = \"" + key + "\"" };
        req.bucket_name = integration.ctx.bucket;
        req.scope_name = scope_name;
        req.mutation_state = { mutation_token };
        req.adhoc = false;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.payload.rows.size() == 1);
        REQUIRE(value == couchbase::utils::json::parse(resp.payload.rows[0]));
    }
}

TEST_CASE("integration: read only with no results", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::operations::query_request req{ "SELECT * FROM " + integration.ctx.bucket + " LIMIT 0" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.payload.rows.empty());
    }
}

TEST_CASE("integration: invalid query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    {
        couchbase::operations::query_request req{ "I'm not n1ql" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message());
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::parsing_failure);
    }
}