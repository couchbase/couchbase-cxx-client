/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "test_helper.hxx"

#include "core/metrics/constants.hxx"
#include "core/metrics/logging_meter.hxx"
#include "core/metrics/logging_meter_options.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/constants.hxx"

#include <couchbase/error_codes.hxx>
#include <spdlog/fmt/bundled/printf.h>

#include <asio/io_context.hpp>

TEST_CASE("unit: metric attributes encoding", "[unit]")
{
  SECTION("all attributes set")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      "test-bucket",
      "test-scope",
      "test-collection",
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 10);
    REQUIRE(tags.at("couchbase.service") == "kv");
    REQUIRE(tags.at("db.operation.name") == "get");
    REQUIRE(tags.at("db.namespace") == "test-bucket");
    REQUIRE(tags.at("couchbase.scope.name") == "test-scope");
    REQUIRE(tags.at("couchbase.collection.name") == "test-collection");
    REQUIRE(tags.at("error.type") == "DocumentNotFound");
    REQUIRE(tags.at("couchbase.cluster.name") == "test-cluster");
    REQUIRE(tags.at("couchbase.cluster.uuid") == "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251");
    REQUIRE(tags.at("db.system.name") == "couchbase");
    REQUIRE(tags.at("__unit") == "s");
  }

  SECTION("successful operation")
  {
    couchbase::core::metrics::metric_attributes attrs{ couchbase::core::tracing::service::key_value,
                                                       "get",
                                                       {},
                                                       "test-bucket",
                                                       "test-scope",
                                                       "test-collection",
                                                       { "test-cluster",
                                                         "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" } };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 9);
    REQUIRE(tags.find("error.type") == tags.end());
  }

  SECTION("cluster labels missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      {},
      "test-bucket",
      "test-scope",
      "test-collection",
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 7);
    REQUIRE(tags.find("couchbase.cluster.uuid") == tags.end());
    REQUIRE(tags.find("couchbase.cluster.name") == tags.end());
  }

  SECTION("bucket/scope/collection names missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      {},
      {},
      {},
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 7);
    REQUIRE(tags.find("db.namespace") == tags.end());
    REQUIRE(tags.find("couchbase.scope.name") == tags.end());
    REQUIRE(tags.find("couchbase.collection.name") == tags.end());
  }
}

TEST_CASE("unit: logging_meter get_value_recorder", "[unit]")
{
  asio::io_context ctx{};
  couchbase::core::metrics::logging_meter_options options{};
  auto meter = std::make_shared<couchbase::core::metrics::logging_meter>(ctx, options);

  const std::map<std::string, std::string> kv_get_tags{
    { couchbase::core::tracing::attributes::op::service,
      couchbase::core::tracing::service::key_value },
    { couchbase::core::tracing::attributes::op::operation_name, "get" },
  };
  const std::map<std::string, std::string> kv_upsert_tags{
    { couchbase::core::tracing::attributes::op::service,
      couchbase::core::tracing::service::key_value },
    { couchbase::core::tracing::attributes::op::operation_name, "upsert" },
  };
  const std::map<std::string, std::string> query_tags{
    { couchbase::core::tracing::attributes::op::service, couchbase::core::tracing::service::query },
    { couchbase::core::tracing::attributes::op::operation_name, "n1ql_query" },
  };

  SECTION("returns noop recorder for unknown meter name")
  {
    auto recorder = meter->get_value_recorder("unknown.meter", kv_get_tags);
    REQUIRE(recorder != nullptr);
    // noop recorder is returned — recording should not throw
    recorder->record_value(42);
  }

  SECTION("returns noop recorder when service tag is missing")
  {
    const std::map<std::string, std::string> no_service_tags{
      { couchbase::core::tracing::attributes::op::operation_name, "get" },
    };
    auto recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, no_service_tags);
    REQUIRE(recorder != nullptr);
    recorder->record_value(42);
  }

  SECTION("returns noop recorder when operation tag is missing")
  {
    const std::map<std::string, std::string> no_op_tags{
      { couchbase::core::tracing::attributes::op::service,
        couchbase::core::tracing::service::key_value },
    };
    auto recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, no_op_tags);
    REQUIRE(recorder != nullptr);
    recorder->record_value(42);
  }

  SECTION("returns a valid recorder for a known operation")
  {
    auto recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_get_tags);
    REQUIRE(recorder != nullptr);
    recorder->record_value(100);
  }

  SECTION("repeated calls with the same tags return the same recorder instance")
  {
    auto recorder1 =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_get_tags);
    auto recorder2 =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_get_tags);
    REQUIRE(recorder1 != nullptr);
    REQUIRE(recorder1 == recorder2);
  }

  SECTION("different operations under the same service return distinct recorders")
  {
    auto get_recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_get_tags);
    auto upsert_recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_upsert_tags);
    REQUIRE(get_recorder != nullptr);
    REQUIRE(upsert_recorder != nullptr);
    REQUIRE(get_recorder != upsert_recorder);
  }

  SECTION("different services return distinct recorders")
  {
    auto kv_recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, kv_get_tags);
    auto query_recorder =
      meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, query_tags);
    REQUIRE(kv_recorder != nullptr);
    REQUIRE(query_recorder != nullptr);
    REQUIRE(kv_recorder != query_recorder);
  }
}
