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
#include <couchbase/platform/uuid.h>

class test_span : public couchbase::tracing::request_span
{
  public:
    test_span(const std::string& name)
      : test_span(name, nullptr)
    {
    }
    test_span(const std::string& name, std::shared_ptr<couchbase::tracing::request_span> parent)
      : request_span(name, parent)
    {
        start_ = std::chrono::steady_clock::now();
        id_ = test::utils::uniq_id("span");
    }
    void add_tag(const std::string& name, std::uint64_t value) override
    {
        int_tags_[name] = value;
    }
    void add_tag(const std::string& name, const std::string& value) override
    {
        string_tags_[name] = value;
    }
    void end() override
    {
        duration_ = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_);
    }
    std::map<std::string, std::string> string_tags()
    {
        return string_tags_;
    }
    std::map<std::string, std::uint64_t> int_tags()
    {
        return int_tags_;
    }
    std::chrono::nanoseconds duration()
    {
        return duration_;
    }
    std::chrono::time_point<std::chrono::steady_clock> start()
    {
        return start_;
    }
    std::string id()
    {
        return id_;
    }

  private:
    std::string id_;
    std::chrono::time_point<std::chrono::steady_clock> start_;
    std::chrono::nanoseconds duration_{ 0 };
    std::map<std::string, std::string> string_tags_;
    std::map<std::string, std::uint64_t> int_tags_;
};

class test_tracer : public couchbase::tracing::request_tracer
{
  public:
    std::shared_ptr<couchbase::tracing::request_span> start_span(std::string name,
                                                                 std::shared_ptr<couchbase::tracing::request_span> parent = {})
    {
        std::lock_guard<std::mutex> lock(mutex_);
        spans_.push_back(std::make_shared<test_span>(name, parent));
        return spans_.back();
    }
    std::vector<std::shared_ptr<test_span>> spans()
    {
        return spans_;
    }
    void reset()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        spans_.clear();
    }

  private:
    std::vector<std::shared_ptr<test_span>> spans_;
    std::mutex mutex_;
};

class tracer_test_guard
{
  public:
    tracer_test_guard()
    {
        tracer = std::make_shared<test_tracer>();
        ctx = test::utils::test_context::load_from_environment();
        auto auth = ctx.build_auth();
        couchbase::cluster_options opts{};
        opts.tracer = tracer;
        auto conn_str = couchbase::utils::parse_connection_string(ctx.connection_string);
        auto addr = conn_str.bootstrap_nodes.front().address;
        auto port = conn_str.default_port;
        couchbase::origin orig(auth, addr, port, opts);
        cluster = couchbase::cluster::create(io);
        io_thread = std::thread([this]() { io.run(); });
        test::utils::open_cluster(cluster, orig);
        test::utils::open_bucket(cluster, ctx.bucket);
    }
    ~tracer_test_guard()
    {
        // close cluster
        {
            auto barrier = std::make_shared<std::promise<void>>();
            auto f = barrier->get_future();
            cluster->close([barrier]() { barrier->set_value(); });
            f.get();
        }
        // now shutdown io and iothread
        io.stop();
        if (io_thread.joinable()) {
            io_thread.join();
        }
    }

    couchbase::document_id make_id(std::string key = "")
    {
        if (key.empty()) {
            key = test::utils::uniq_id("tracer");
        }
        return couchbase::document_id{ ctx.bucket, "_default", "_default", key };
    }

    test::utils::test_context ctx;
    std::thread io_thread{};
    asio::io_context io{};
    std::shared_ptr<couchbase::cluster> cluster;
    std::shared_ptr<test_tracer> tracer;
};
void
assert_kv_op_span_ok(tracer_test_guard& guard,
                     std::shared_ptr<test_span> span,
                     const std::string& op,
                     std::shared_ptr<test_span> parent = nullptr)
{
    auto server_duration = span->int_tags()["cb.server_duration"];
    REQUIRE(op == span->name());
    REQUIRE(static_cast<uint64_t>(span->duration().count()) >= server_duration);
    REQUIRE(span->string_tags()["cb.service"] == "kv");
    REQUIRE_FALSE(span->string_tags()["cb.local_id"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.local_socket"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.remote_socket"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.operation_id"].empty());
    REQUIRE(span->string_tags()["db.instance"] == guard.ctx.bucket);
    REQUIRE(span->parent() == parent);
    if (parent) {
        // the parent span should not be closed yet
        REQUIRE(parent->duration().count() == 0);
    }
}

void
assert_http_op_span_ok(std::shared_ptr<test_span> span, const std::string& op, std::shared_ptr<test_span> parent = nullptr)
{
    REQUIRE(span->name().find(op) != std::string::npos);
    REQUIRE_FALSE(span->string_tags()["cb.local_id"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.local_socket"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.operation_id"].empty());
    REQUIRE_FALSE(span->string_tags()["cb.remote_socket"].empty());
    REQUIRE(span->string_tags()["cb.service"] == op);
    REQUIRE(span->parent() == parent);
    REQUIRE(span->duration().count() > 0);
    if (parent) {
        // the parent span should not be closed yet
        REQUIRE(parent->duration().count() == 0);
    }
    // spec has some specific fields for query, analytics, etc...
}

TEST_CASE("integration: enable external tracer", "[integration]")
{
    tracer_test_guard guard;
    auto parent_span = GENERATE(std::shared_ptr<test_span>{ nullptr }, std::make_shared<test_span>("parent"));
    auto value = couchbase::utils::to_binary(R"({"some":"thing"})");
    auto existing_id = guard.make_id();
    SECTION("upsert doc 'foo'")
    {
        couchbase::operations::upsert_request r{ existing_id, value };
        auto response = test::utils::execute(guard.cluster, r);
        REQUIRE_FALSE(response.ctx.ec);

        SECTION("test some KV ops:")
        {
            SECTION("upsert")
            {
                guard.tracer->reset();
                couchbase::operations::upsert_request req{ guard.make_id(), value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.upsert", parent_span);
            }
            SECTION("insert")
            {
                guard.tracer->reset();
                couchbase::operations::insert_request req{ guard.make_id(), value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.insert", parent_span);
                guard.tracer->reset();
            }

            SECTION("get")
            {
                guard.tracer->reset();
                couchbase::operations::get_request req{ existing_id };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.get", parent_span);
            }
            SECTION("replace")
            {
                guard.tracer->reset();
                auto new_value = couchbase::utils::to_binary(R"({"some": "thing else")");
                couchbase::operations::replace_request req{ existing_id, new_value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.replace", parent_span);
            }
            SECTION("lookup_in")
            {
                guard.tracer->reset();
                couchbase::operations::lookup_in_request req{};
                req.parent_span = parent_span;
                req.id = existing_id;
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::get, false, "some");
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.lookup_in", parent_span);
            }
            SECTION("mutate_in")
            {
                guard.tracer->reset();
                couchbase::operations::mutate_in_request req{};
                req.parent_span = parent_span;
                req.id = existing_id;
                req.specs.add_spec(couchbase::protocol::subdoc_opcode::dict_upsert, false, false, false, "another", R"("field")");
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_FALSE(resp.ctx.ec);
                auto spans = guard.tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.mutate_in", parent_span);
            }
        }
    }
    SECTION("test http ops:")
    {
        SECTION("query")
        {
            guard.tracer->reset();
            couchbase::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
            req.parent_span = parent_span;
            auto resp = test::utils::execute(guard.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            auto spans = guard.tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "query", parent_span);
        }
        SECTION("search")
        {
            guard.tracer->reset();
            couchbase::operations::search_request req{};
            req.parent_span = parent_span;
            req.index_name = "idontexist";
            req.query = R"("foo")";
            auto resp = test::utils::execute(guard.cluster, req);
            // we didn't create an index, so this will fail
            REQUIRE(resp.ctx.ec);
            auto spans = guard.tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "search", parent_span);
        }
        SECTION("analytics")
        {
            guard.tracer->reset();
            couchbase::operations::analytics_request req{};
            req.parent_span = parent_span;
            req.bucket_name = guard.ctx.bucket;
            req.statement = R"(SELECT "ruby rules" AS greeting)";
            auto resp = test::utils::execute(guard.cluster, req);
            REQUIRE_FALSE(resp.ctx.ec);
            auto spans = guard.tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "analytics", parent_span);
        }
        SECTION("view")
        {
            guard.tracer->reset();
            couchbase::operations::document_view_request req{};
            req.parent_span = parent_span;
            req.bucket_name = guard.ctx.bucket;
            req.view_name = "idontexist";
            req.document_name = "nordoi";
            auto resp = test::utils::execute(guard.cluster, req);
            // we didn't setup a view, so this will fail.
            REQUIRE(resp.ctx.ec);
            auto spans = guard.tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "views", parent_span);
        }
    }
}