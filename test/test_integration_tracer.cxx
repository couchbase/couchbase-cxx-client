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

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/match_all_query.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/tracing/request_tracer.hxx>

#include <catch2/catch_message.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <spdlog/fmt/bundled/ranges.h>

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
    duration_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start_);
  }

  void add_child_span(const std::shared_ptr<test_span>& child)
  {
    const auto child_span_name = child->name();
    if (child_spans_.count(child_span_name) == 0) {
      child_spans_.insert({ child_span_name, {} });
    }
    child_spans_[child_span_name].emplace_back(child);
  }

  auto child_spans() -> const std::map<std::string, std::vector<std::weak_ptr<test_span>>>&
  {
    return child_spans_;
  }

  auto string_tags() -> std::map<std::string, std::string>
  {
    return string_tags_;
  }

  auto int_tags() -> std::map<std::string, std::uint64_t>
  {
    return int_tags_;
  }

  auto duration() -> std::chrono::nanoseconds
  {
    return duration_;
  }

  auto start() -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return start_;
  }

  auto id() -> std::string
  {
    return id_;
  }

private:
  std::string id_;
  std::chrono::time_point<std::chrono::steady_clock> start_;
  std::chrono::nanoseconds duration_{ 0 };
  std::map<std::string, std::string> string_tags_;
  std::map<std::string, std::uint64_t> int_tags_;
  std::map<std::string, std::vector<std::weak_ptr<test_span>>> child_spans_{};
};

class test_tracer : public couchbase::tracing::request_tracer
{
public:
  auto start_span(std::string name, std::shared_ptr<couchbase::tracing::request_span> parent = {})
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
    fmt::println("Creating span {} with parent {}", name, parent ? parent->name() : "<none>");

    const std::lock_guard<std::mutex> lock(mutex_);
    spans_.push_back(std::make_shared<test_span>(name, parent));

    if (parent != nullptr) {
      const auto parent_test_span = std::dynamic_pointer_cast<test_span>(parent);
      parent_test_span->add_child_span(spans_.back());
    }

    return spans_.back();
  }

  auto spans() -> std::vector<std::shared_ptr<test_span>>
  {
    return spans_;
  }

  void reset()
  {
    const std::lock_guard<std::mutex> lock(mutex_);
    spans_.clear();
  }

private:
  std::vector<std::shared_ptr<test_span>> spans_;
  std::mutex mutex_;
};

auto
make_id(const test::utils::test_context& ctx, std::string key = "") -> couchbase::core::document_id
{
  if (key.empty()) {
    key = test::utils::uniq_id("tracer");
  }
  return couchbase::core::document_id{ ctx.bucket, "_default", "_default", key };
}

void
assert_span_ok(test::utils::integration_test_guard& guard,
               const std::shared_ptr<test_span>& span,
               bool is_top_level_op_span,
               const std::shared_ptr<test_span>& expected_parent = nullptr)
{
  fmt::println("TEST SPAN `{}`,\n  Parent: `{}`,\n  Tags: `[string] {}, [int] {}`",
               span->name(),
               span->parent() ? span->parent()->name() : "<none>",
               fmt::join(span->string_tags(), ", "),
               fmt::join(span->int_tags(), ", "));

  auto parent_test_span = std::dynamic_pointer_cast<test_span>(span->parent());
  REQUIRE(parent_test_span == expected_parent);
  if (parent_test_span && is_top_level_op_span) {
    // the parent span that was given to the operation's options should not be closed yet
    REQUIRE(parent_test_span->duration().count() == 0);
  }

  // Span should be closed
  REQUIRE(span->duration().count() > 0);

  const auto& tags = span->string_tags();

  REQUIRE(tags.at("db.system.name") == "couchbase");
  if (guard.cluster_version().supports_cluster_labels()) {
    REQUIRE_FALSE(tags.at("couchbase.cluster.uuid").empty());
    REQUIRE_FALSE(tags.at("couchbase.cluster.name").empty());
  } else {
    REQUIRE(tags.find("couchbase.cluster.uuid") == tags.end());
    REQUIRE(tags.find("couchbase.cluster.name") == tags.end());
  }
}

void
assert_dispatch_span_ok(test::utils::integration_test_guard& guard,
                        const std::shared_ptr<test_span>& span,
                        std::shared_ptr<test_span> parent)
{
  assert_span_ok(guard, span, false, parent);

  REQUIRE("dispatch_to_server" == span->name());

  REQUIRE_FALSE(span->string_tags()["couchbase.local_id"].empty());
  REQUIRE_FALSE(span->string_tags()["server.address"].empty());
  REQUIRE(span->int_tags()["server.port"] != 0);
  REQUIRE_FALSE(span->string_tags()["network.peer.address"].empty());
  REQUIRE(span->int_tags()["network.peer.port"] != 0);
  REQUIRE(span->string_tags()["network.transport"] == "tcp");
  REQUIRE_FALSE(span->string_tags()["couchbase.operation_id"].empty());
}

void
assert_kv_dispatch_span_ok(test::utils::integration_test_guard& guard,
                           const std::shared_ptr<test_span>& span,
                           std::shared_ptr<test_span> parent)
{
  assert_dispatch_span_ok(guard, span, parent);

  const std::size_t expected_tag_count =
    (guard.cluster_version().supports_cluster_labels()) ? 11 : 9;
  REQUIRE(span->string_tags().size() + span->int_tags().size() == expected_tag_count);

  REQUIRE(static_cast<uint64_t>(span->duration().count()) >=
          span->int_tags()["couchbase.server_duration"]);
}

void
assert_kv_op_span_ok(test::utils::integration_test_guard& guard,
                     const std::shared_ptr<test_span>& span,
                     const std::string& op,
                     std::shared_ptr<test_span> parent = nullptr)
{
  assert_span_ok(guard, span, true, parent);

  const std::size_t expected_tag_count =
    (guard.cluster_version().supports_cluster_labels()) ? 8 : 6;
  REQUIRE(span->string_tags().size() + span->int_tags().size() == expected_tag_count);

  REQUIRE(op == span->name());
  REQUIRE(span->string_tags()["couchbase.service"] == "kv");
  REQUIRE(span->string_tags()["db.namespace"] == guard.ctx.bucket);
  REQUIRE(span->string_tags()["couchbase.scope.name"] == "_default");
  REQUIRE(span->string_tags()["couchbase.collection.name"] == "_default");
  REQUIRE(span->string_tags()["db.operation.name"] == op);

  // There must be at least one dispatch span
  auto dispatch_spans = span->child_spans().find("dispatch_to_server");
  REQUIRE(dispatch_spans != span->child_spans().end());
  REQUIRE_FALSE(dispatch_spans->second.empty());

  for (const auto& dispatch_span : dispatch_spans->second) {
    assert_kv_dispatch_span_ok(guard, dispatch_span.lock(), span);
  }
}

void
assert_kv_op_span_has_request_encoding(test::utils::integration_test_guard& guard,
                                       const std::shared_ptr<test_span>& op_span)
{
  std::shared_ptr<test_span> request_encoding_span;
  {
    const auto request_encoding_spans = op_span->child_spans().find("request_encoding");
    REQUIRE(request_encoding_spans != op_span->child_spans().end());
    REQUIRE(request_encoding_spans->second.size() == 1);
    request_encoding_span = request_encoding_spans->second.at(0).lock();
  }
  assert_span_ok(guard, request_encoding_span, false, op_span);
}

void
assert_http_dispatch_span_ok(test::utils::integration_test_guard& guard,
                             const std::shared_ptr<test_span>& span,
                             std::shared_ptr<test_span> parent)
{
  assert_dispatch_span_ok(guard, span, parent);

  const std::size_t expected_tag_count =
    (guard.cluster_version().supports_cluster_labels()) ? 10 : 8;
  REQUIRE(span->string_tags().size() + span->int_tags().size() == expected_tag_count);

  // server_duration is only available for KV operations
  REQUIRE_FALSE(span->int_tags().count("couchbase.server_duration"));
}

void
assert_http_op_span_ok(test::utils::integration_test_guard& guard,
                       const std::shared_ptr<test_span>& span,
                       const std::string& op,
                       const std::optional<std::string>& expected_service,
                       const std::optional<std::string>& expected_bucket_name,
                       const std::optional<std::string>& expected_scope_name,
                       const std::optional<std::string>& expected_collection_name,
                       std::shared_ptr<test_span> parent = nullptr,
                       bool is_top_level_op_span = true)
{
  assert_span_ok(guard, span, is_top_level_op_span, std::move(parent));

  REQUIRE(span->name().find(op) != std::string::npos);
  REQUIRE(span->string_tags()["db.operation.name"] == op);
  REQUIRE(span->duration().count() > 0);
  if (expected_service.has_value()) {
    REQUIRE(span->string_tags()["couchbase.service"] == expected_service.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.service") == 0);
  }
  if (expected_bucket_name.has_value()) {
    REQUIRE(span->string_tags()["db.namespace"] == expected_bucket_name.value());
  } else {
    REQUIRE(span->string_tags().count("db.namespace") == 0);
  }
  if (expected_scope_name.has_value()) {
    REQUIRE(span->string_tags()["couchbase.scope.name"] == expected_scope_name.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.scope.name") == 0);
  }
  if (expected_collection_name.has_value()) {
    REQUIRE(span->string_tags()["couchbase.collection.name"] == expected_collection_name.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.collection.name") == 0);
  }

  // There must be at least one dispatch span
  auto dispatch_spans = span->child_spans().find("dispatch_to_server");
  REQUIRE(dispatch_spans != span->child_spans().end());
  REQUIRE_FALSE(dispatch_spans->second.empty());

  for (const auto& dispatch_span : dispatch_spans->second) {
    assert_http_dispatch_span_ok(guard, dispatch_span.lock(), span);
  }
}

void
assert_compound_http_op_span_ok(
  test::utils::integration_test_guard& guard,
  const std::shared_ptr<test_span>& span,
  const std::string& op,
  const std::vector<std::pair<std::string, std::size_t>>& expected_sub_ops,
  const std::optional<std::string>& expected_service,
  const std::optional<std::string>& expected_bucket_name,
  const std::optional<std::string>& expected_scope_name,
  const std::optional<std::string>& expected_collection_name,
  std::shared_ptr<test_span> parent = nullptr)
{
  assert_span_ok(guard, span, true, parent);

  REQUIRE(span->name().find(op) != std::string::npos);
  REQUIRE(span->string_tags()["db.operation.name"] == op);
  REQUIRE(span->duration().count() > 0);
  if (expected_service.has_value()) {
    REQUIRE(span->string_tags()["couchbase.service"] == expected_service.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.service") == 0);
  }
  if (expected_bucket_name.has_value()) {
    REQUIRE(span->string_tags()["db.namespace"] == expected_bucket_name.value());
  } else {
    REQUIRE(span->string_tags().count("db.namespace") == 0);
  }
  if (expected_scope_name.has_value()) {
    REQUIRE(span->string_tags()["couchbase.scope.name"] == expected_scope_name.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.scope.name") == 0);
  }
  if (expected_collection_name.has_value()) {
    REQUIRE(span->string_tags()["couchbase.collection.name"] == expected_collection_name.value());
  } else {
    REQUIRE(span->string_tags().count("couchbase.collection.name") == 0);
  }

  auto sub_op_spans = span->child_spans();
  for (const auto& [expected_sub_op, expected_sub_op_count] : expected_sub_ops) {
    auto it = sub_op_spans.find(expected_sub_op);
    REQUIRE(it != sub_op_spans.end());

    if (expected_sub_op_count > 0) {
      REQUIRE(it->second.size() == expected_sub_op_count);
    } else {
      // We don't expect a specific number of sub-operations. For example, in watch_indexes there
      // can be any number of get_all_indexes calls.
      REQUIRE(it->second.size() > 0);
    }

    for (const auto& sub_op_span : it->second) {
      assert_http_op_span_ok(guard,
                             sub_op_span.lock(),
                             expected_sub_op,
                             expected_service,
                             expected_bucket_name,
                             expected_scope_name,
                             expected_collection_name,
                             span,
                             false);
    }
  }
}

TEST_CASE("integration: enable external tracer - KV operations", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto tracer = std::make_shared<test_tracer>();
  auto cluster = integration.public_cluster([tracer](couchbase::cluster_options& opts) {
    opts.tracing().tracer(tracer);
  });

  auto parent_span =
    GENERATE(std::shared_ptr<test_span>{ nullptr }, std::make_shared<test_span>("parent"));

  auto value = couchbase::core::utils::json::parse(R"({"some":"thing"})");
  auto existing_key = test::utils::uniq_id("tracer");
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  {
    auto [err, res] = collection.upsert(existing_key, value, {}).get();
    REQUIRE_SUCCESS(err.ec());
  }

  tracer->reset();

  SECTION("upsert")
  {
    auto [err, res] = collection
                        .upsert(test::utils::uniq_id("tracer"),
                                value,
                                couchbase::upsert_options().parent_span(parent_span))
                        .get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "upsert", parent_span);
    assert_kv_op_span_has_request_encoding(integration, spans.front());
  }

  SECTION("insert")
  {
    auto [err, res] = collection
                        .insert(test::utils::uniq_id("tracer"),
                                value,
                                couchbase::insert_options().parent_span(parent_span))
                        .get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "insert", parent_span);
    assert_kv_op_span_has_request_encoding(integration, spans.front());
  }

  SECTION("get")
  {
    auto [err, res] =
      collection.get(existing_key, couchbase::get_options().parent_span(parent_span)).get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "get", parent_span);
  }

  SECTION("replace")
  {
    auto new_value = couchbase::core::utils::json::parse(R"({"some": "thing else"})");
    auto [err, res] =
      collection
        .replace(existing_key, new_value, couchbase::replace_options().parent_span(parent_span))
        .get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "replace", parent_span);
    assert_kv_op_span_has_request_encoding(integration, spans.front());
  }

  SECTION("lookup_in")
  {
    auto [err, res] =
      collection
        .lookup_in(existing_key,
                   couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("some") },
                   couchbase::lookup_in_options().parent_span(parent_span))
        .get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "lookup_in", parent_span);
  }

  SECTION("mutate_in")
  {
    auto [err, res] = collection
                        .mutate_in(existing_key,
                                   couchbase::mutate_in_specs{
                                     couchbase::mutate_in_specs::upsert("another", "field") },
                                   couchbase::mutate_in_options().parent_span(parent_span))
                        .get();
    REQUIRE_SUCCESS(err.ec());
    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_kv_op_span_ok(integration, spans.front(), "mutate_in", parent_span);
  }

  tracer->reset();
}

TEST_CASE("integration: enable external tracer - HTTP operations", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto tracer = std::make_shared<test_tracer>();
  auto cluster = integration.public_cluster([tracer](couchbase::cluster_options& opts) {
    opts.tracing().tracer(tracer);
  });

  auto scope = cluster.bucket(integration.ctx.bucket).default_scope();

  auto parent_span =
    GENERATE(std::shared_ptr<test_span>{ nullptr }, std::make_shared<test_span>("parent"));

  tracer->reset();

  SECTION("search")
  {
    if (integration.cluster_version().supports_scope_search()) {
      SECTION("scope-level")
      {
        auto [err, _] = scope
                          .search("does-not-exist",
                                  couchbase::search_request(couchbase::match_all_query()),
                                  couchbase::search_options().parent_span(parent_span))
                          .get();
        REQUIRE(err.ec() == couchbase::errc::common::index_not_found);
        auto spans = tracer->spans();
        REQUIRE_FALSE(spans.empty());
        assert_http_op_span_ok(integration,
                               spans.front(),
                               "search",
                               "search",
                               integration.ctx.bucket,
                               "_default",
                               {},
                               parent_span);
      }
    }

    SECTION("cluster-level")
    {
      auto [err, _] = cluster
                        .search("does-not-exist",
                                couchbase::search_request(couchbase::match_all_query()),
                                couchbase::search_options().parent_span(parent_span))
                        .get();
      REQUIRE(err.ec() == couchbase::errc::common::index_not_found);
      auto spans = tracer->spans();
      REQUIRE_FALSE(spans.empty());
      assert_http_op_span_ok(integration,
                             spans.front(),
                             "search",
                             "search",
                             std::nullopt,
                             std::nullopt,
                             std::nullopt,
                             parent_span);
    }
  }

  SECTION("query")
  {
    if (integration.cluster_version().supports_scoped_queries()) {
      SECTION("scope-level")
      {
        std::optional<std::string> expected_statement{};

        SECTION("no parameters")
        {
          auto [err, _] =
            scope.query("SELECT 1=1", couchbase::query_options().parent_span(parent_span)).get();
          expected_statement = std::nullopt;
        }

        SECTION("positional parameters")
        {
          const auto statement = "SELECT $1=$2";
          auto [err, _] =
            scope
              .query(
                statement,
                couchbase::query_options().positional_parameters(1, 1).parent_span(parent_span))
              .get();
          REQUIRE_SUCCESS(err.ec());
          expected_statement = statement;
        }

        SECTION("named parameters")
        {
          const auto statement = "SELECT $a=$b";
          expected_statement = statement;
          auto [err, _] =
            scope
              .query(statement,
                     couchbase::query_options()
                       .named_parameters(std::make_pair("a", 1), std::make_pair("b", 1))
                       .parent_span(parent_span))
              .get();
          REQUIRE_SUCCESS(err.ec());
          expected_statement = statement;
        }

        auto spans = tracer->spans();
        REQUIRE_FALSE(spans.empty());
        const auto span = spans.front();
        if (expected_statement.has_value()) {
          REQUIRE(span->string_tags()["db.query.text"] == expected_statement.value());
        } else {
          REQUIRE(span->string_tags().count("db.query.text") == 0);
        }
        assert_http_op_span_ok(integration,
                               spans.front(),
                               "query",
                               "query",
                               integration.ctx.bucket,
                               "_default",
                               {},
                               parent_span);
      }
    }

    SECTION("cluster-level")
    {
      std::optional<std::string> expected_statement{};

      SECTION("no parameters")
      {
        auto [err, _] =
          cluster.query("SELECT 1=1", couchbase::query_options().parent_span(parent_span)).get();
        expected_statement = std::nullopt;
      }

      SECTION("positional parameters")
      {
        const auto statement = "SELECT $1=$2";
        auto [err, _] =
          cluster
            .query(statement,
                   couchbase::query_options().positional_parameters(1, 1).parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());
        expected_statement = statement;
      }

      SECTION("named parameters")
      {
        const auto statement = "SELECT $a=$b";
        expected_statement = statement;
        auto [err, _] = cluster
                          .query(statement,
                                 couchbase::query_options()
                                   .named_parameters(std::make_pair("a", 1), std::make_pair("b", 1))
                                   .parent_span(parent_span))
                          .get();
        REQUIRE_SUCCESS(err.ec());
        expected_statement = statement;
      }

      auto spans = tracer->spans();
      REQUIRE_FALSE(spans.empty());
      const auto span = spans.front();
      if (expected_statement.has_value()) {
        REQUIRE(span->string_tags()["db.query.text"] == expected_statement.value());
      } else {
        REQUIRE(span->string_tags().count("db.query.text") == 0);
      }
      assert_http_op_span_ok(integration,
                             spans.front(),
                             "query",
                             "query",
                             std::nullopt,
                             std::nullopt,
                             std::nullopt,
                             parent_span);
    }
  }

  SECTION("analytics query")
  {
    if (integration.cluster_version().supports_scoped_queries()) {
      SECTION("scope-level")
      {
        std::optional<std::string> expected_statement{};

        SECTION("no parameters")
        {
          auto [err, _] = scope
                            .analytics_query(
                              "SELECT 1=1", couchbase::analytics_options().parent_span(parent_span))
                            .get();
          REQUIRE(err.ec() == couchbase::errc::analytics::dataverse_not_found);
          expected_statement = std::nullopt;
        }

        SECTION("positional parameters")
        {
          const auto statement = "SELECT $1=$2";
          auto [err, _] =
            scope
              .analytics_query(
                statement,
                couchbase::analytics_options().positional_parameters(1, 1).parent_span(parent_span))
              .get();
          REQUIRE(err.ec() == couchbase::errc::analytics::dataverse_not_found);
          expected_statement = statement;
        }

        SECTION("named parameters")
        {
          const auto statement = "SELECT $a=$b";
          expected_statement = statement;
          auto [err, _] =
            scope
              .analytics_query(statement,
                               couchbase::analytics_options()
                                 .named_parameters(std::make_pair("a", 1), std::make_pair("b", 1))
                                 .parent_span(parent_span))
              .get();
          REQUIRE(err.ec() == couchbase::errc::analytics::dataverse_not_found);
          expected_statement = statement;
        }

        auto spans = tracer->spans();
        REQUIRE_FALSE(spans.empty());
        const auto span = spans.front();
        if (expected_statement.has_value()) {
          REQUIRE(span->string_tags()["db.query.text"] == expected_statement.value());
        } else {
          REQUIRE(span->string_tags().count("db.query.text") == 0);
        }
        assert_http_op_span_ok(integration,
                               spans.front(),
                               "analytics",
                               "analytics",
                               integration.ctx.bucket,
                               "_default",
                               std::nullopt,
                               parent_span);
      }
    }

    SECTION("cluster-level")
    {
      std::optional<std::string> expected_statement{};

      SECTION("no parameters")
      {
        auto [err, _] =
          cluster
            .analytics_query("SELECT 1=1", couchbase::analytics_options().parent_span(parent_span))
            .get();
        expected_statement = std::nullopt;
      }

      SECTION("positional parameters")
      {
        const auto statement = "SELECT $1=$2";
        auto [err, _] =
          cluster
            .analytics_query(
              statement,
              couchbase::analytics_options().positional_parameters(1, 1).parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());
        expected_statement = statement;
      }

      SECTION("named parameters")
      {
        const auto statement = "SELECT $a=$b";
        expected_statement = statement;
        auto [err, _] =
          cluster
            .analytics_query(statement,
                             couchbase::analytics_options()
                               .named_parameters(std::make_pair("a", 1), std::make_pair("b", 1))
                               .parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());
        expected_statement = statement;
      }

      auto spans = tracer->spans();
      REQUIRE_FALSE(spans.empty());
      const auto span = spans.front();
      if (expected_statement.has_value()) {
        REQUIRE(span->string_tags()["db.query.text"] == expected_statement.value());
      } else {
        REQUIRE(span->string_tags().count("db.query.text") == 0);
      }
      assert_http_op_span_ok(integration,
                             spans.front(),
                             "analytics",
                             "analytics",
                             std::nullopt,
                             std::nullopt,
                             std::nullopt,
                             parent_span);
    }
  }

  if (integration.cluster_version().supports_collections()) {
    SECTION("collections management - get all scopes")
    {
      auto mgr = cluster.bucket(integration.ctx.bucket).collections();
      auto [err, _] =
        mgr.get_all_scopes(couchbase::get_all_scopes_options().parent_span(parent_span)).get();
      REQUIRE_SUCCESS(err.ec());
      auto spans = tracer->spans();
      REQUIRE_FALSE(spans.empty());
      assert_http_op_span_ok(integration,
                             spans.front(),
                             "manager_collections_get_all_scopes",
                             "management",
                             integration.ctx.bucket,
                             std::nullopt,
                             std::nullopt,
                             parent_span);
    }
  }

  if (integration.cluster_version().supports_collections()) {
    SECTION("collection query index management - create, watch and drop index")
    {
      const auto mgr = cluster.bucket(integration.ctx.bucket).default_collection().query_indexes();
      const auto index_name = test::utils::uniq_id("tracer_idx");

      {
        auto err =
          mgr
            .create_index(
              index_name,
              { "field" },
              couchbase::create_query_index_options().build_deferred(true).parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_http_op_span_ok(integration,
                               tracer->spans().front(),
                               "manager_query_create_index",
                               "query",
                               integration.ctx.bucket,
                               "_default",
                               "_default",
                               parent_span);
        tracer->reset();
      }
      {
        auto err =
          mgr
            .build_deferred_indexes(couchbase::build_query_index_options().parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_compound_http_op_span_ok(
          integration,
          tracer->spans().front(),
          "manager_query_build_deferred_indexes",
          { { "manager_query_get_all_deferred_indexes", 1 }, { "manager_query_build_indexes", 1 } },
          "query",
          integration.ctx.bucket,
          "_default",
          "_default",
          parent_span);
        tracer->reset();
      }
      {
        auto err = mgr
                     .drop_primary_index(couchbase::drop_primary_query_index_options()
                                           .parent_span(parent_span)
                                           .ignore_if_not_exists(true))
                     .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_http_op_span_ok(integration,
                               tracer->spans().front(),
                               "manager_query_drop_primary_index",
                               "query",
                               integration.ctx.bucket,
                               "_default",
                               "_default",
                               parent_span);
        tracer->reset();
      }
      {
        auto err =
          mgr
            .watch_indexes({ index_name },
                           couchbase::watch_query_indexes_options().parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_compound_http_op_span_ok(integration,
                                        tracer->spans().front(),
                                        "manager_query_watch_indexes",
                                        { { "manager_query_get_all_indexes", {} } },
                                        "query",
                                        integration.ctx.bucket,
                                        "_default",
                                        "_default",
                                        parent_span);
        tracer->reset();
      }
      {
        auto [err, _] =
          mgr.get_all_indexes(couchbase::get_all_query_indexes_options().parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_http_op_span_ok(integration,
                               tracer->spans().front(),
                               "manager_query_get_all_indexes",
                               "query",
                               integration.ctx.bucket,
                               "_default",
                               "_default",
                               parent_span);
        tracer->reset();
      }
      {
        auto err =
          mgr.drop_index(index_name, couchbase::drop_query_index_options().parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        REQUIRE_FALSE(tracer->spans().empty());
        assert_http_op_span_ok(integration,
                               tracer->spans().front(),
                               "manager_query_drop_index",
                               "query",
                               integration.ctx.bucket,
                               "_default",
                               "_default",
                               parent_span);
        tracer->reset();
      }
    }
  }

  SECTION("search index management - get all indexes")
  {
    SECTION("cluster-level")
    {
      auto mgr = cluster.search_indexes();
      auto [err, _] =
        mgr.get_all_indexes(couchbase::get_all_search_indexes_options().parent_span(parent_span))
          .get();
      REQUIRE_SUCCESS(err.ec());

      auto spans = tracer->spans();
      REQUIRE_FALSE(spans.empty());
      assert_http_op_span_ok(integration,
                             spans.front(),
                             "manager_search_get_all_indexes",
                             "search",
                             std::nullopt,
                             std::nullopt,
                             std::nullopt,
                             parent_span);
    }

    if (integration.cluster_version().supports_scope_search()) {
      SECTION("scope-level")
      {
        auto mgr = cluster.bucket(integration.ctx.bucket).default_scope().search_indexes();
        auto [err, _] =
          mgr.get_all_indexes(couchbase::get_all_search_indexes_options().parent_span(parent_span))
            .get();
        REQUIRE_SUCCESS(err.ec());

        auto spans = tracer->spans();
        REQUIRE_FALSE(spans.empty());
        assert_http_op_span_ok(integration,
                               spans.front(),
                               "manager_search_get_all_indexes",
                               "search",
                               integration.ctx.bucket,
                               "_default",
                               std::nullopt,
                               parent_span);
      }
    }
  }

  SECTION("analytics index management - get all indexes")
  {
    auto mgr = cluster.analytics_indexes();
    auto [err, _] =
      mgr.get_all_indexes(couchbase::get_all_indexes_analytics_options().parent_span(parent_span))
        .get();
    REQUIRE_SUCCESS(err.ec());

    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_http_op_span_ok(integration,
                           spans.front(),
                           "manager_analytics_get_all_indexes",
                           "analytics",
                           std::nullopt,
                           std::nullopt,
                           std::nullopt,
                           parent_span);
  }

  SECTION("bucket management - get all buckets")
  {
    auto mgr = cluster.buckets();
    auto [err, _] =
      mgr.get_all_buckets(couchbase::get_all_buckets_options().parent_span(parent_span)).get();
    REQUIRE_SUCCESS(err.ec());

    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_http_op_span_ok(integration,
                           spans.front(),
                           "manager_buckets_get_all_buckets",
                           "management",
                           std::nullopt,
                           std::nullopt,
                           std::nullopt,
                           parent_span);
  }

  SECTION("bucket management - get bucket")
  {
    auto mgr = cluster.buckets();
    auto [err, _] = mgr
                      .get_bucket(integration.ctx.bucket,
                                  couchbase::get_bucket_options().parent_span(parent_span))
                      .get();
    REQUIRE_SUCCESS(err.ec());

    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_http_op_span_ok(integration,
                           spans.front(),
                           "manager_buckets_get_bucket",
                           "management",
                           integration.ctx.bucket,
                           std::nullopt,
                           std::nullopt,
                           parent_span);
  }

  SECTION("bucket management - drop bucket")
  {
    auto mgr = cluster.buckets();
    auto err =
      mgr.drop_bucket("does_not_exist", couchbase::drop_bucket_options().parent_span(parent_span))
        .get();
    REQUIRE(err.ec() == couchbase::errc::common::bucket_not_found);

    auto spans = tracer->spans();
    REQUIRE_FALSE(spans.empty());
    assert_http_op_span_ok(integration,
                           spans.front(),
                           "manager_buckets_drop_bucket",
                           "management",
                           "does_not_exist",
                           std::nullopt,
                           std::nullopt,
                           parent_span);
  }

  tracer->reset();
}
