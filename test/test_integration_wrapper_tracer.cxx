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

#include "core/operations/document_get.hxx"
#include "core/tracing/wrapper_sdk_tracer.hxx"

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
