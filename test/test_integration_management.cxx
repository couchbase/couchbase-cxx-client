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
#include <couchbase/operations/management/freeform.hxx>
#include <couchbase/utils/url_codec.hxx>

TEST_CASE("integration: freeform HTTP request", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    SECTION("key_value")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::key_value;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::common_errc::invalid_argument);
    }

    if (integration.cluster_version().supports_analytics()) {
        SECTION("analytics")
        {
            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::analytics;
            req.method = "GET";
            req.path = "/admin/ping";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.body.empty());
            INFO(resp.body)
            auto result = couchbase::utils::json::parse(resp.body);
            REQUIRE(result.is_object());
        }
    }

    SECTION("search")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::search;
        req.method = "GET";
        req.path = "/api/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE(resp.body.empty());
        REQUIRE_FALSE(resp.headers.empty());
        REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
    }

    SECTION("query")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::query;
        req.method = "GET";
        req.path = "/admin/ping";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        INFO(resp.body)
        auto result = couchbase::utils::json::parse(resp.body);
        REQUIRE(result.is_object());
    }

    SECTION("view")
    {
        auto document_name = test::utils::uniq_id("design_document");
        auto view_name = test::utils::uniq_id("view");

        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::view;
        req.method = "POST";
        req.path = fmt::format("/{}/_design/{}/_view/{}", integration.ctx.bucket, document_name, view_name);
        req.body = R"({"keys":["foo","bar"]})";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 404);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::utils::json::parse(resp.body);
        INFO(resp.body)
        REQUIRE(result["error"].get_string() == "not_found");
    }

    SECTION("management")
    {
        couchbase::operations::management::freeform_request req{};
        req.type = couchbase::service_type::management;
        req.method = "GET";
        req.path = "/pools";
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.status == 200);
        REQUIRE_FALSE(resp.body.empty());
        auto result = couchbase::utils::json::parse(resp.body);
        INFO(resp.body)
        REQUIRE(result.find("uuid") != nullptr);
    }

    if (integration.cluster_version().supports_collections()) {
        SECTION("create scope")
        {
            auto scope_name = test::utils::uniq_id("freeform_scope");

            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::management;
            req.method = "POST";
            req.path = fmt::format("/pools/default/buckets/{}/scopes", integration.ctx.bucket);
            req.headers["content-type"] = "application/x-www-form-urlencoded";
            req.body = fmt::format("name={}", couchbase::utils::string_codec::form_encode(scope_name));
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.headers.empty());
            REQUIRE(resp.headers["content-type"].find("application/json") != std::string::npos);
            auto result = couchbase::utils::json::parse(resp.body);
            REQUIRE(result.is_object());
            REQUIRE(result.find("uid") != nullptr);
        }
    }

    if (integration.cluster_version().supports_eventing_functions() && integration.has_eventing_service()) {
        SECTION("eventing")
        {

            couchbase::operations::management::freeform_request req{};
            req.type = couchbase::service_type::eventing;
            req.method = "GET";
            req.path = "/api/v1/functions";
            auto resp = test::utils::execute(integration.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            REQUIRE(resp.status == 200);
            REQUIRE_FALSE(resp.body.empty());
            auto result = couchbase::utils::json::parse(resp.body);
            INFO(resp.body)
            REQUIRE(result.is_array());
        }
    }
}
