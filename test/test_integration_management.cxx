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

#include <couchbase/operations/management/bucket.hxx>

TEST_CASE("integration: bucket management", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.ctx.version.supports_gcccp()) {
        test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    }

    auto bucket_name = test::utils::uniq_id("bucket");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto created = test::utils::wait_until([&integration, bucket_name]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return !resp.ctx.ec;
        });
        REQUIRE(created);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets > 0);
    }

    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        auto dropped = test::utils::wait_until([&integration, bucket_name]() {
            couchbase::operations::management::bucket_get_request req{ bucket_name };
            auto resp = test::utils::execute(integration.cluster, req);
            return resp.ctx.ec == couchbase::error::common_errc::bucket_not_found;
        });
        REQUIRE(dropped);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = test::utils::execute(integration.cluster, req);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets == 0);
    }
}
