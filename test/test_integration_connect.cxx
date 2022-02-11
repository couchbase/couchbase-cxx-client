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

#include "utils/move_only_context.hxx"

TEST_CASE("integration: connecting with empty bootstrap nodes list", "[integration]")
{
    asio::io_context io{};
    auto io_thread = std::thread([&io]() { io.run(); });
    auto connstr = couchbase::utils::parse_connection_string("couchbase://");
    REQUIRE(connstr.bootstrap_nodes.empty());
    auto origin = couchbase::origin({}, connstr);
    couchbase::cluster cluster{ io };
    auto barrier = std::make_shared<std::promise<std::error_code>>();
    auto f = barrier->get_future();
    cluster.open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
    auto rc = f.get();
    REQUIRE(rc == couchbase::error::common_errc::invalid_argument);
    test::utils::close_cluster(cluster);
    io_thread.join();
}

TEST_CASE("integration: can connect with handler capturing non-copyable object", "[integration]")
{
    test::utils::integration_test_guard integration;

    couchbase::cluster cluster{ integration.io };

    // test connecting
    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        test::utils::move_only_context ctx("foobar");
        cluster.open(integration.origin, [barrier, ctx = std::move(ctx)](std::error_code ec) {
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
        cluster.open_bucket(integration.ctx.bucket, [barrier, ctx = std::move(ctx)](std::error_code ec) {
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
        cluster.close([barrier, ctx = std::move(ctx)]() mutable {
            CHECK(ctx.payload() == "foobar");
            barrier->set_value(false);
        });

        auto rc = f.get();
        REQUIRE(!rc);
    }
}
