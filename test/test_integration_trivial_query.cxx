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

TEST_CASE("integration: trivial non-data query", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

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
