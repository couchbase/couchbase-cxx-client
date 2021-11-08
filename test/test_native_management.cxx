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

#include "test_helper_native.hxx"

#include <couchbase/operations/management/bucket.hxx>

TEST_CASE("native: bucket management", "[native]")
{
    auto ctx = test_context::load_from_environment();
    native_init_logger();

    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;

    asio::io_context io;

    couchbase::cluster cluster(io);
    auto io_thread = std::thread([&io]() { io.run(); });

    open_cluster(cluster, couchbase::origin(auth, connstr));
    if (!ctx.version.supports_gcccp()) {
        open_bucket(cluster, ctx.bucket);
    }

    auto bucket_name = uniq_id("bucket");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        auto resp = execute(cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = execute(cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets > 0);
    }

    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        auto resp = execute(cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto resp = execute(cluster, req);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](const auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets == 0);
    }

    close_cluster(cluster);

    io_thread.join();
}
