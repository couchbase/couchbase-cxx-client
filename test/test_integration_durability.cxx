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

TEST_CASE("integration: durable operations", "[integration]")
{
    test::utils::integration_test_guard integration;
    if (!integration.cluster_version().supports_enhanced_durability()) {
        return;
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    couchbase::document_id id{ integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
    {
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::operations::upsert_request req{ id, couchbase::utils::json::generate(value) };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        const tao::json::value value = {
            { "foo", "bar" },
        };
        couchbase::operations::replace_request req{ id, couchbase::utils::json::generate(value) };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "baz", "42");
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::get_request req{ id };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.value == R"({"foo":"bar","baz":42})");
    }
    {
        couchbase::operations::remove_request req{ id };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.cas.empty());
        REQUIRE(resp.token.sequence_number != 0);
    }
}
