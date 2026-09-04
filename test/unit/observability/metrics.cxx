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

#include "framework/test_registry.hxx"

#include "core/metrics/constants.hxx"
#include "core/metrics/logging_meter.hxx"
#include "core/metrics/logging_meter_options.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/constants.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <map>
#include <memory>
#include <string>

namespace couchbase::test
{
namespace
{
void
metric_attributes_encode_every_field_that_is_set([[maybe_unused]] context& ctx)
{
  couchbase::core::metrics::metric_attributes attrs{
    couchbase::core::tracing::service::key_value,
    "get",
    couchbase::core::metrics::standardized_error_type(
      couchbase::errc::key_value::document_not_found),
    "test-bucket",
    "test-scope",
    "test-collection",
    { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
  };

  auto tags = attrs.encode();

  assert_eq(tags.size(), std::size_t{ 10 }, "one tag per attribute, plus the unit");
  assert_eq(tags.at("couchbase.service"), std::string{ "kv" }, "service");
  assert_eq(tags.at("db.operation.name"), std::string{ "get" }, "operation name");
  assert_eq(tags.at("db.namespace"), std::string{ "test-bucket" }, "bucket name");
  assert_eq(tags.at("couchbase.scope.name"), std::string{ "test-scope" }, "scope name");
  assert_eq(
    tags.at("couchbase.collection.name"), std::string{ "test-collection" }, "collection name");
  assert_eq(tags.at("error.type"), std::string{ "DocumentNotFound" }, "error type");
  assert_eq(tags.at("couchbase.cluster.name"), std::string{ "test-cluster" }, "cluster name");
  assert_eq(tags.at("couchbase.cluster.uuid"),
            std::string{ "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" },
            "cluster uuid");
  assert_eq(tags.at("db.system.name"), std::string{ "couchbase" }, "system name");
  assert_eq(tags.at("__unit"), std::string{ "s" }, "the unit the value is recorded in");
}

void
metric_attributes_omit_the_error_type_when_there_is_no_error([[maybe_unused]] context& ctx)
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

  assert_eq(tags.size(), std::size_t{ 9 }, "every tag but the error type");
  assert_true(tags.find("error.type") == tags.end(),
              "a successful operation carries no error type at all");
}

void
metric_attributes_omit_the_cluster_labels_when_they_are_absent([[maybe_unused]] context& ctx)
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

  assert_eq(tags.size(), std::size_t{ 7 }, "every tag but the error type and the cluster labels");
  assert_true(tags.find("couchbase.cluster.uuid") == tags.end(), "cluster uuid");
  assert_true(tags.find("couchbase.cluster.name") == tags.end(), "cluster name");
}

void
metric_attributes_omit_the_keyspace_names_when_they_are_absent([[maybe_unused]] context& ctx)
{
  couchbase::core::metrics::metric_attributes attrs{
    couchbase::core::tracing::service::key_value,
    "get",
    couchbase::core::metrics::standardized_error_type(
      couchbase::errc::key_value::document_not_found),
    {},
    {},
    {},
    { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
  };

  auto tags = attrs.encode();

  assert_eq(tags.size(), std::size_t{ 7 }, "every tag but the bucket, scope and collection names");
  assert_true(tags.find("db.namespace") == tags.end(), "bucket name");
  assert_true(tags.find("couchbase.scope.name") == tags.end(), "scope name");
  assert_true(tags.find("couchbase.collection.name") == tags.end(), "collection name");
}

// The meter owns a timer on the io_context, so the two are constructed and destroyed together.
struct logging_meter_fixture {
  asio::io_context io{};
  std::shared_ptr<couchbase::core::metrics::logging_meter> meter{
    std::make_shared<couchbase::core::metrics::logging_meter>(
      io,
      couchbase::core::metrics::logging_meter_options{})
  };
};

auto
kv_tags(const std::string& operation) -> std::map<std::string, std::string>
{
  return {
    { couchbase::core::tracing::attributes::op::service,
      couchbase::core::tracing::service::key_value },
    { couchbase::core::tracing::attributes::op::operation_name, operation },
  };
}

void
logging_meter_returns_a_noop_recorder_for_an_unknown_meter_name([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  auto recorder = fixture.meter->get_value_recorder("unknown.meter", kv_tags("get"));
  assert_true(recorder != nullptr, "an unknown meter name still yields a recorder");
  recorder->record_value(42);
}

void
logging_meter_returns_a_noop_recorder_when_the_service_tag_is_missing([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  const std::map<std::string, std::string> no_service_tags{
    { couchbase::core::tracing::attributes::op::operation_name, "get" },
  };
  auto recorder = fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name,
                                                    no_service_tags);
  assert_true(recorder != nullptr, "a tag set without a service still yields a recorder");
  recorder->record_value(42);
}

void
logging_meter_returns_a_noop_recorder_when_the_operation_tag_is_missing(
  [[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  const std::map<std::string, std::string> no_op_tags{
    { couchbase::core::tracing::attributes::op::service,
      couchbase::core::tracing::service::key_value },
  };
  auto recorder =
    fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, no_op_tags);
  assert_true(recorder != nullptr, "a tag set without an operation still yields a recorder");
  recorder->record_value(42);
}

void
logging_meter_returns_a_recorder_for_a_known_operation([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  auto recorder = fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name,
                                                    kv_tags("get"));
  assert_true(recorder != nullptr, "a known operation yields a recorder");
  recorder->record_value(100);
}

void
logging_meter_returns_the_same_recorder_for_repeated_identical_tags([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  auto first = fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name,
                                                 kv_tags("get"));
  auto second = fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name,
                                                  kv_tags("get"));
  assert_true(first != nullptr, "a known operation yields a recorder");
  assert_true(first == second, "the histogram for one operation is shared across lookups");
}

void
logging_meter_returns_distinct_recorders_for_different_operations([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  auto get_recorder = fixture.meter->get_value_recorder(
    couchbase::core::metrics::operation_meter_name, kv_tags("get"));
  auto upsert_recorder = fixture.meter->get_value_recorder(
    couchbase::core::metrics::operation_meter_name, kv_tags("upsert"));
  assert_true(get_recorder != nullptr, "get yields a recorder");
  assert_true(upsert_recorder != nullptr, "upsert yields a recorder");
  assert_true(get_recorder != upsert_recorder,
              "two operations of one service keep separate histograms");
}

void
logging_meter_returns_distinct_recorders_for_different_services([[maybe_unused]] context& ctx)
{
  logging_meter_fixture fixture;

  const std::map<std::string, std::string> query_tags{
    { couchbase::core::tracing::attributes::op::service, couchbase::core::tracing::service::query },
    { couchbase::core::tracing::attributes::op::operation_name, "n1ql_query" },
  };
  auto kv_recorder = fixture.meter->get_value_recorder(
    couchbase::core::metrics::operation_meter_name, kv_tags("get"));
  auto query_recorder =
    fixture.meter->get_value_recorder(couchbase::core::metrics::operation_meter_name, query_tags);
  assert_true(kv_recorder != nullptr, "key/value yields a recorder");
  assert_true(query_recorder != nullptr, "query yields a recorder");
  assert_true(kv_recorder != query_recorder, "two services keep separate histograms");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(metric_attributes_encode_every_field_that_is_set) },
      { CASE(metric_attributes_omit_the_error_type_when_there_is_no_error) },
      { CASE(metric_attributes_omit_the_cluster_labels_when_they_are_absent) },
      { CASE(metric_attributes_omit_the_keyspace_names_when_they_are_absent) },
      { CASE(logging_meter_returns_a_noop_recorder_for_an_unknown_meter_name) },
      { CASE(logging_meter_returns_a_noop_recorder_when_the_service_tag_is_missing) },
      { CASE(logging_meter_returns_a_noop_recorder_when_the_operation_tag_is_missing) },
      { CASE(logging_meter_returns_a_recorder_for_a_known_operation) },
      { CASE(logging_meter_returns_the_same_recorder_for_repeated_identical_tags) },
      { CASE(logging_meter_returns_distinct_recorders_for_different_operations) },
      { CASE(logging_meter_returns_distinct_recorders_for_different_services) },
    },
  };
}

} // namespace couchbase::test
