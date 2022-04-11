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

#include <couchbase/cas_fmt.hxx>

TEST_CASE("integration: append", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        couchbase::operations::upsert_request req{ id, "world" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        INFO("cas=" << fmt::to_string(resp.cas))
        REQUIRE(!resp.cas.empty());
        INFO("seqno=" << resp.token.sequence_number)
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::append_request req{ id, "!" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("seqno=" << resp.token.sequence_number)
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("value=" << resp.value)
        REQUIRE(resp.value == "world!");
    }
}

TEST_CASE("integration: prepend", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        couchbase::operations::upsert_request req{ id, "world" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("seqno=" << resp.token.sequence_number)
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::prepend_request req{ id, "Hello, " };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("seqno=" << resp.token.sequence_number)
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        INFO("value=" << resp.value)
        REQUIRE(resp.value == "Hello, world");
    }
}

TEST_CASE("integration: binary ops on missing document", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

    SECTION("append")
    {
        couchbase::operations::append_request req{ id, "" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }

    SECTION("prepend")
    {
        couchbase::operations::prepend_request req{ id, "" };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec == couchbase::error::key_value_errc::document_not_found);
    }
}
