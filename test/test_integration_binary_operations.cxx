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

#include "core/utils/binary.hxx"

TEST_CASE("integration: append", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary("world") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::append_request req{ id, couchbase::core::utils::to_binary("!") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.value == couchbase::core::utils::to_binary("world!"));
    }
}

TEST_CASE("integration: prepend", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        couchbase::core::operations::upsert_request req{ id, couchbase::core::utils::to_binary("world") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::prepend_request req{ id, couchbase::core::utils::to_binary("Hello, ") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number() != 0);
    }
    {
        couchbase::core::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_SUCCESS(resp.ctx.ec());
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.value == couchbase::core::utils::to_binary("Hello, world"));
    }
}

TEST_CASE("integration: binary ops on missing document", "[integration]")
{
    test::utils::integration_test_guard integration;
    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::core::document_id id{ integration.ctx.bucket, "_default", "_default", "missing_key" };

    SECTION("append")
    {
        couchbase::core::operations::append_request req{ id, couchbase::core::utils::to_binary("") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }

    SECTION("prepend")
    {
        couchbase::core::operations::prepend_request req{ id, couchbase::core::utils::to_binary("") };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
    }
}
