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

TEST_CASE("native: durable operations", "[native]")
{
    auto ctx = test_context::load_from_environment();
    native_init_logger();

    if (!ctx.version.supports_enhanced_durability()) {
        return;
    }

    auto connstr = couchbase::utils::parse_connection_string(ctx.connection_string);
    couchbase::cluster_credentials auth{};
    auth.username = ctx.username;
    auth.password = ctx.password;

    asio::io_context io;

    couchbase::cluster cluster(io);
    auto io_thread = std::thread([&io]() { io.run(); });

    open_cluster(cluster, couchbase::origin(auth, connstr));
    open_bucket(cluster, ctx.bucket);

    couchbase::document_id id{ ctx.bucket, "_default._default", uniq_id("foo") };
    {
        const tao::json::value value = {
            { "a", 1.0 },
            { "b", 2.0 },
        };
        couchbase::operations::upsert_request req{ id, tao::json::to_string(value) };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto barrier = std::make_shared<std::promise<couchbase::operations::upsert_response>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::upsert_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.cas != 0);
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        const tao::json::value value = {
            { "foo", "bar" },
        };
        couchbase::operations::replace_request req{ id, tao::json::to_string(value) };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto barrier = std::make_shared<std::promise<couchbase::operations::replace_response>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::replace_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.cas != 0);
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::mutate_in_request req{ id };
        req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "baz", "42");
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto barrier = std::make_shared<std::promise<couchbase::operations::mutate_in_response>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.cas != 0);
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        couchbase::operations::get_request req{ id };
        auto barrier = std::make_shared<std::promise<couchbase::operations::get_response>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::get_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.cas != 0);
        REQUIRE(resp.value == R"({"foo":"bar","baz":42})");
    }
    {
        couchbase::operations::remove_request req{ id };
        req.durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
        auto barrier = std::make_shared<std::promise<couchbase::operations::remove_response>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::remove_response resp) mutable { barrier->set_value(std::move(resp)); });
        auto resp = f.get();
        INFO(resp.ctx.ec.message());
        REQUIRE_FALSE(resp.ctx.ec);
        REQUIRE(resp.cas != 0);
        REQUIRE(resp.token.sequence_number != 0);
    }
    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        cluster.close([barrier]() { barrier->set_value(); });
        f.get();
    }

    io_thread.join();
}
