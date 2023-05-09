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

#include <catch2/generators/catch_generators.hpp>

#include "core/platform/uuid.h"

#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/tracing/request_tracer.hxx>

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

couchbase::core::document_id
make_id(const test::utils::test_context& ctx, std::string key = "")
{
    if (key.empty()) {
        key = test::utils::uniq_id("tracer");
    }
    return couchbase::core::document_id{ ctx.bucket, "_default", "_default", key };
}

void
assert_kv_op_span_ok(test::utils::integration_test_guard& guard,
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
    couchbase::core::cluster_options opts{};
    auto tracer = std::make_shared<test_tracer>();
    opts.tracer = tracer;
    test::utils::integration_test_guard guard(opts);
    test::utils::open_bucket(guard.cluster, guard.ctx.bucket);
    auto parent_span = GENERATE(std::shared_ptr<test_span>{ nullptr }, std::make_shared<test_span>("parent"));
    auto value = couchbase::core::utils::to_binary(R"({"some":"thing"})");
    auto existing_id = make_id(guard.ctx);
    SECTION("upsert doc 'foo'")
    {
        couchbase::core::operations::upsert_request r{ existing_id, value };
        auto response = test::utils::execute(guard.cluster, r);
        REQUIRE_FALSE(response.ctx.ec());

        SECTION("test some KV ops:")
        {
            SECTION("upsert")
            {
                tracer->reset();
                couchbase::core::operations::upsert_request req{ make_id(guard.ctx), value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.upsert", parent_span);
            }
            SECTION("insert")
            {
                tracer->reset();
                couchbase::core::operations::insert_request req{ make_id(guard.ctx), value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.insert", parent_span);
            }

            SECTION("get")
            {
                tracer->reset();
                couchbase::core::operations::get_request req{ existing_id };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.get", parent_span);
            }
            SECTION("replace")
            {
                tracer->reset();
                auto new_value = couchbase::core::utils::to_binary(R"({"some": "thing else")");
                couchbase::core::operations::replace_request req{ existing_id, new_value };
                req.parent_span = parent_span;
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.replace", parent_span);
            }
            SECTION("lookup_in")
            {
                tracer->reset();
                couchbase::core::operations::lookup_in_request req{};
                req.parent_span = parent_span;
                req.id = existing_id;
                req.specs = couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("some") }.specs();
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.lookup_in", parent_span);
            }
            SECTION("mutate_in")
            {
                tracer->reset();
                couchbase::core::operations::mutate_in_request req{};
                req.parent_span = parent_span;
                req.id = existing_id;
                req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("another", "field") }.specs();
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec());
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_kv_op_span_ok(guard, spans.front(), "cb.mutate_in", parent_span);
            }
        }
    }
    SECTION("test http ops:")
    {
        SECTION("query")
        {
            if (!guard.cluster_version().supports_query()) {
                SKIP("cluster does not support query");
            }
            tracer->reset();
            couchbase::core::operations::query_request req{ R"(SELECT "ruby rules" AS greeting)" };
            req.parent_span = parent_span;
            auto resp = test::utils::execute(guard.cluster, req);
            REQUIRE_SUCCESS(resp.ctx.ec);
            auto spans = tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "query", parent_span);
        }
        SECTION("search")
        {
            tracer->reset();
            couchbase::core::operations::search_request req{};
            req.parent_span = parent_span;
            req.index_name = "idontexist";
            req.query = R"("foo")";
            auto resp = test::utils::execute(guard.cluster, req);
            // we didn't create an index, so this will fail
            REQUIRE(resp.ctx.ec);
            auto spans = tracer->spans();
            REQUIRE_FALSE(spans.empty());
            assert_http_op_span_ok(spans.front(), "search", parent_span);
        }
        if (guard.cluster_version().supports_analytics()) {
            SECTION("analytics")
            {
                tracer->reset();
                couchbase::core::operations::analytics_request req{};
                req.parent_span = parent_span;
                req.bucket_name = guard.ctx.bucket;
                req.statement = R"(SELECT "ruby rules" AS greeting)";
                auto resp = test::utils::execute(guard.cluster, req);
                REQUIRE_SUCCESS(resp.ctx.ec);
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_http_op_span_ok(spans.front(), "analytics", parent_span);
            }
        }
        if (guard.cluster_version().supports_views()) {
            SECTION("view")
            {
                tracer->reset();
                couchbase::core::operations::document_view_request req{};
                req.parent_span = parent_span;
                req.bucket_name = guard.ctx.bucket;
                req.view_name = "idontexist";
                req.document_name = "nordoi";
                auto resp = test::utils::execute(guard.cluster, req);
                // we didn't setup a view, so this will fail.
                REQUIRE(resp.ctx.ec);
                auto spans = tracer->spans();
                REQUIRE_FALSE(spans.empty());
                assert_http_op_span_ok(spans.front(), "views", parent_span);
            }
        }
    }
}
