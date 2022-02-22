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

#include "test/utils/logger.hxx"
#include "utils/move_only_context.hxx"

TEST_CASE("integration: connecting with empty bootstrap nodes list", "[integration]")
{
    asio::io_context io{};
    auto connstr = couchbase::utils::parse_connection_string("couchbase://");
    REQUIRE(connstr.bootstrap_nodes.empty());
    auto origin = couchbase::origin({}, connstr);
    auto cluster = couchbase::cluster::create(io);
    auto io_thread = std::thread([&io]() { io.run(); });
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster->open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    REQUIRE(rc == couchbase::error::common_errc::invalid_argument);
    test::utils::close_cluster(cluster);
    io_thread.join();
}

TEST_CASE("integration: connecting with unresponsive first node in bootstrap nodes list", "[integration]")
{
    test::utils::init_logger();
    asio::io_context io{};
    auto ctx = test::utils::test_context::load_from_environment();
    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    REQUIRE_FALSE(connstr.bootstrap_nodes.empty());
    connstr.bootstrap_nodes.insert(connstr.bootstrap_nodes.begin(),
                                   couchbase::utils::connection_string::node{
                                     "example.com",
                                     11210,
                                     couchbase::utils::connection_string::address_type::dns,
                                     couchbase::utils::connection_string::bootstrap_mode::gcccp,
                                   });
    auto origin = couchbase::origin(ctx.build_auth(), connstr);
    auto cluster = couchbase::cluster::create(io);
    auto io_thread = std::thread([&io]() { io.run(); });
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster->open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    REQUIRE_FALSE(rc);
    test::utils::close_cluster(cluster);
    io_thread.join();
}

TEST_CASE("integration: can connect with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    auto cluster = couchbase::cluster::create(integration.io);

    // test connecting
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        cluster->open(integration.origin, [barrier, ctx = std::move(ctx)](std::error_code ec) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(ec);
        });
        auto rc = f.get();
        REQUIRE(!rc);
    }

    // test opening a bucket
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        cluster->open_bucket(integration.ctx.bucket, [barrier, ctx = std::move(ctx)](std::error_code ec) {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(ec);
        });
        auto rc = f.get();
        REQUIRE(!rc);
    }

    // test disconnecting
    {
        auto barrier = std::make_shared<std::promise<bool>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        cluster->close([barrier, ctx = std::move(ctx)]() mutable {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(false);
        });

        auto rc = f.get();
        REQUIRE(!rc);
    }
}

TEST_CASE("integration: destroy cluster without waiting for close completion", "[integration]")
{
    test::utils::init_logger();
    auto ctx = test::utils::test_context::load_from_environment();

    asio::io_context io{};

    auto cluster = couchbase::cluster::create(io);
    auto io_thread = std::thread([&io]() { io.run(); });

    auto origin = couchbase::origin(ctx.build_auth(), couchbase::utils::parse_connection_string(ctx.connection_string));
    test::utils::open_cluster(cluster, origin);
    test::utils::open_bucket(cluster, ctx.bucket);

    // hit KV
    {
        couchbase::document_id id{ ctx.bucket, "_default", "_default", test::utils::uniq_id("foo") };
        couchbase::operations::upsert_request req{ id, "{{}}" };
        auto resp = test::utils::execute(cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // hit Query
    {
        couchbase::operations::query_request req{ R"(SELECT 42 AS the_answer)" };
        auto resp = test::utils::execute(cluster, req);
        REQUIRE_FALSE(resp.ctx.ec);
    }

    // close but do not explicitly wait for callback
    std::atomic_bool closed{ false };
    cluster->close([&closed]() { closed = true; });
    cluster.reset();
    io_thread.join();
    REQUIRE(closed);
}
