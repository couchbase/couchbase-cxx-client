/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025. Couchbase, Inc.
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

#include "core/cluster_label_listener.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/query_stream.hxx"
#include "core/tracing/wrapper_sdk_tracer.hxx"

#include <chrono>
#include <future>

TEST_CASE("integration: wrappers can get dispatch spans using a parent wrapper span",
          "[integration]")
{
  couchbase::core::cluster_options opts{};
  opts.tracer = std::make_shared<couchbase::core::tracing::wrapper_sdk_tracer>();

  const test::utils::integration_test_guard integration(opts);

  const auto root_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>();
  couchbase::core::operations::get_request request{ couchbase::core::document_id{
    integration.ctx.bucket, "_default", "_default", test::utils::uniq_id("wrapper_tracer_test") } };
  request.parent_span = root_span;

  auto resp = test::utils::execute(integration.cluster, request);
  REQUIRE(resp.ctx.ec() == couchbase::errc::key_value::document_not_found);
  REQUIRE(root_span->children().size() == 1);
  REQUIRE(root_span->children().front()->name() == "dispatch_to_server");
}

TEST_CASE("integration: wrappers can get dispatch spans for a streaming query", "[integration]")
{
  couchbase::core::cluster_options opts{};
  opts.tracer = std::make_shared<couchbase::core::tracing::wrapper_sdk_tracer>();

  test::utils::integration_test_guard integration(opts);

  if (!integration.cluster_version().supports_query()) {
    SKIP("cluster does not support query");
  }

  const auto root_span = std::make_shared<couchbase::core::tracing::wrapper_sdk_span>();
  couchbase::core::operations::query_request request{ R"(SELECT "wrapper tracer" AS greeting)" };
  request.parent_span = root_span;
  request.client_context_id = test::utils::uniq_id("wrapper_tracer");

  auto barrier = std::make_shared<std::promise<std::error_code>>();
  auto f = barrier->get_future();
  integration.cluster.query_stream(
    request,
    [barrier](couchbase::core::query_stream stream, couchbase::core::error_context::query ctx) {
      if (!ctx.ec) {
        stream.cancel();
      }
      barrier->set_value(ctx.ec);
    });
  REQUIRE_SUCCESS(f.get());

  // The dispatch span ends once the response headers arrive, before the rows are read.
  const auto children = root_span->children();
  REQUIRE(children.size() == 1);
  const auto& dispatch_span = children.front();
  REQUIRE(dispatch_span->name() == "dispatch_to_server");
  // A child is registered when it is created, so check the end time to prove it was ended.
  REQUIRE(dispatch_span->end_time() != std::chrono::system_clock::time_point{});
  REQUIRE(dispatch_span->string_tags().at("couchbase.operation_id") ==
          request.client_context_id.value());
}

TEST_CASE("integration: cluster label listener can be used to get cluster labels", "[integration]")
{
  test::utils::integration_test_guard integration{};

  const auto [cluster_name, cluster_uuid] =
    integration.cluster.cluster_label_listener()->cluster_labels();

  if (integration.cluster_version().supports_cluster_labels()) {
    REQUIRE(cluster_name.has_value());
    REQUIRE(cluster_uuid.has_value());

    couchbase::core::operations::management::freeform_request bucket_cfg_req{
      couchbase::core::service_type::management,
      "GET",
      "/pools/default/b/" + integration.ctx.bucket,
    };
    const auto bucket_cfg_resp = test::utils::execute(integration.cluster, bucket_cfg_req);

    REQUIRE_SUCCESS(bucket_cfg_resp.ctx.ec);

    const auto bucket_cfg = couchbase::core::utils::json::parse(bucket_cfg_resp.body);

    REQUIRE(bucket_cfg.at("clusterName").get_string() == cluster_name.value());
    REQUIRE(bucket_cfg.at("clusterUUID").get_string() == cluster_uuid.value());
  } else {
    REQUIRE_FALSE(cluster_name.has_value());
    REQUIRE_FALSE(cluster_uuid.has_value());
  }
}
