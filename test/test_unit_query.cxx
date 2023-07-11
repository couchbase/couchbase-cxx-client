/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023 Couchbase, Inc.
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

#include "utils/move_only_context.hxx"

#include "core/operations/document_query.hxx"

couchbase::core::http_context
make_http_context(couchbase::core::topology::configuration& config)
{
    static couchbase::core::query_cache query_cache{};
    static couchbase::core::cluster_options cluster_options{};
    std::string hostname{};
    std::uint16_t port{};
    couchbase::core::http_context ctx{ config, cluster_options, query_cache, hostname, port };
    return ctx;
}

TEST_CASE("unit: query with read from replica", "[unit]")
{
    couchbase::core::topology::configuration config{};
    config.cluster_capabilities.insert(couchbase::core::cluster_capability::n1ql_read_from_replica);
    auto ctx = make_http_context(config);

    SECTION("use_replica true")
    {
        couchbase::core::io::http_request http_req;
        couchbase::core::operations::query_request req{};
        req.use_replica = true;
        auto ec = req.encode_to(http_req, ctx);
        REQUIRE_SUCCESS(ec);
        auto body = couchbase::core::utils::json::parse(http_req.body);
        REQUIRE(body.is_object());
        REQUIRE(body.get_object().at("use_replica").get_string() == "on");
    }

    SECTION("use_replica false")
    {
        couchbase::core::io::http_request http_req;
        couchbase::core::operations::query_request req{};
        req.use_replica = false;
        auto ec = req.encode_to(http_req, ctx);
        REQUIRE_SUCCESS(ec);
        auto body = couchbase::core::utils::json::parse(http_req.body);
        REQUIRE(body.is_object());
        REQUIRE(body.get_object().at("use_replica").get_string() == "off");
    }

    SECTION("use_replica not set")
    {
        couchbase::core::io::http_request http_req;
        couchbase::core::operations::query_request req{};
        auto ec = req.encode_to(http_req, ctx);
        REQUIRE_SUCCESS(ec);
        auto body = couchbase::core::utils::json::parse(http_req.body);
        REQUIRE(body.is_object());
        REQUIRE_FALSE(body.get_object().count("use_replica"));
    }
}
