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

std::string
uniq_id(const std::string& prefix)
{
    return fmt::format("{}_{}", prefix, std::chrono::steady_clock::now().time_since_epoch().count());
}

TEST_CASE("native: upsert document into default collection", "[native]")
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
    open_bucket(cluster, ctx.bucket);

    {
        couchbase::document_id id{ ctx.bucket, "_default._default", uniq_id("foo") };
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::operations::upsert_request req{ id, couchbase::utils::json::generate(value) };
        auto resp = execute(cluster, req);
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        INFO("rc=" << resp.cas);
        REQUIRE(resp.cas != 0);
        INFO("seqno=" << resp.token.sequence_number);
        REQUIRE(resp.token.sequence_number != 0);
    }

    close_cluster(cluster);

    io_thread.join();
}
