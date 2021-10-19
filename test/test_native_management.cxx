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

std::string
uniq_id(const std::string& prefix)
{
    return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

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

    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open_bucket(ctx.bucket, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        auto rc = f.get();
        INFO(rc.message());
        REQUIRE_FALSE(rc);
    }

    auto bucket_name = uniq_id("bucket");

    {
        couchbase::operations::management::bucket_create_request req;
        req.bucket.name = bucket_name;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_create_response>>();
        auto f = barrier->get_future();
        cluster.execute_http(
          req, [barrier](couchbase::operations::management::bucket_create_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_get_all_response>>();
        auto f = barrier->get_future();
        cluster.execute_http(
          req, [barrier](couchbase::operations::management::bucket_get_all_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets > 0);
    }

    {
        couchbase::operations::management::bucket_drop_request req{ bucket_name };
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_drop_response>>();
        auto f = barrier->get_future();
        cluster.execute_http(
          req, [barrier](couchbase::operations::management::bucket_drop_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        REQUIRE_FALSE(resp.ctx.ec);
    }

    {
        couchbase::operations::management::bucket_get_all_request req;
        auto barrier = std::make_shared<std::promise<couchbase::operations::management::bucket_get_all_response>>();
        auto f = barrier->get_future();
        cluster.execute_http(
          req, [barrier](couchbase::operations::management::bucket_get_all_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(!resp.buckets.empty());
        auto known_buckets =
          std::count_if(resp.buckets.begin(), resp.buckets.end(), [bucket_name](auto& entry) { return entry.name == bucket_name; });
        REQUIRE(known_buckets == 0);
    }

    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        cluster.close([barrier]() { barrier->set_value(); });
        f.get();
    }

    io_thread.join();
}
