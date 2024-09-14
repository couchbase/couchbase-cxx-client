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

#include "core/diagnostics.hxx"
#include "core/operations/document_query.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>

using namespace std::literals::chrono_literals;

TEST_CASE("integration: fetch diagnostics after N1QL query", "[integration]")
{
  test::utils::integration_test_guard integration;

  if (!integration.cluster_version().supports_query() ||
      integration.ctx.deployment == test::utils::deployment_type::elixir) {
    SKIP("cluster does not support query or cluster level query");
  }

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  SECTION("Core API")
  {
    {
      couchbase::core::operations::query_request req{ "SELECT 'hello, couchbase' AS greetings" };
      auto resp = test::utils::execute(integration.cluster, req);
      REQUIRE_SUCCESS(resp.ctx.ec);
      INFO("rows.size() =" << resp.rows.size());
      REQUIRE(resp.rows.size() == 1);
      INFO("row=" << resp.rows[0]);
      REQUIRE(resp.rows[0] == R"({"greetings":"hello, couchbase"})");
    }
    {
      auto barrier = std::make_shared<std::promise<couchbase::core::diag::diagnostics_result>>();
      auto f = barrier->get_future();
      integration.cluster.diagnostics(
        "my_report_id", [barrier](couchbase::core::diag::diagnostics_result&& resp) mutable {
          barrier->set_value(std::move(resp));
        });
      auto res = f.get();
      REQUIRE(res.id == "my_report_id");
      REQUIRE(res.sdk.find("cxx/") == 0);
      REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);
      REQUIRE(res.services[couchbase::core::service_type::query].size() == 1);
      REQUIRE(res.services[couchbase::core::service_type::query][0].state ==
              couchbase::core::diag::endpoint_state::connected);
    }
  }

  SECTION("Public API")
  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    {
      auto [ctx, res] = cluster.query("SELECT 'hello, couchbase' AS greetings", {}).get();
      REQUIRE_SUCCESS(ctx.ec());
      INFO("rows.size() =" << res.rows_as_binary().size());
      REQUIRE(res.rows_as_binary().size() == 1);
      INFO("row=" << couchbase::core::utils::json::generate(res.rows_as()[0]));
      REQUIRE(res.rows_as()[0] ==
              couchbase::core::utils::json::parse(R"({"greetings":"hello, couchbase"})"));
    }
    {
      auto [err, res] =
        cluster.diagnostics(couchbase::diagnostics_options().report_id("my_report_id")).get();
      REQUIRE(res.id() == "my_report_id");
      REQUIRE(res.sdk().find("cxx/") == 0);
      REQUIRE(res.endpoints()[couchbase::service_type::key_value].size() > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::query].size() == 1);
      REQUIRE(res.endpoints()[couchbase::service_type::query][0].state() ==
              couchbase::endpoint_state::connected);
    }
  }
}

TEST_CASE("integration: ping", "[integration]")
{
  test::utils::integration_test_guard integration;

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  SECTION("Core API")
  {
    auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
    auto f = barrier->get_future();
    integration.cluster.ping(
      "my_report_id", {}, {}, {}, [barrier](couchbase::core::diag::ping_result&& resp) mutable {
        barrier->set_value(std::move(resp));
      });
    auto res = f.get();
    REQUIRE(res.services.size() > 0);

    REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);

    REQUIRE(res.services.count(couchbase::core::service_type::management) > 0);
    REQUIRE(res.services[couchbase::core::service_type::management].size() > 0);

    if (integration.ctx.deployment != test::utils::deployment_type::elixir) {
      REQUIRE(res.services.count(couchbase::core::service_type::view) > 0);
      REQUIRE(res.services[couchbase::core::service_type::view].size() > 0);
    }

    REQUIRE(res.services.count(couchbase::core::service_type::query) > 0);
    REQUIRE(res.services[couchbase::core::service_type::query].size() > 0);

    REQUIRE(res.services.count(couchbase::core::service_type::search) > 0);
    REQUIRE(res.services[couchbase::core::service_type::search].size() > 0);

    if (integration.ctx.version.supports_analytics()) {
      REQUIRE(res.services.count(couchbase::core::service_type::analytics) > 0);
      REQUIRE(res.services[couchbase::core::service_type::analytics].size() > 0);
    }

    if (integration.ctx.version.supports_eventing_functions()) {
      REQUIRE(res.services.count(couchbase::core::service_type::eventing) > 0);
      REQUIRE(res.services[couchbase::core::service_type::eventing].size() > 0);
    }

    REQUIRE(res.id == "my_report_id");
    INFO(res.sdk);
    REQUIRE(res.sdk.find("cxx/") == 0);
  }

  SECTION("Public API")
  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto [err, res] = cluster.ping(couchbase::ping_options().report_id("my_report_id")).get();
    REQUIRE(res.endpoints().size() > 0);

    REQUIRE(res.endpoints().count(couchbase::service_type::key_value) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::key_value].size() > 0);

    REQUIRE(res.endpoints().count(couchbase::service_type::management) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::management].size() > 0);

    if (integration.ctx.deployment != test::utils::deployment_type::elixir) {
      REQUIRE(res.endpoints().count(couchbase::service_type::view) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::view].size() > 0);
    }

    REQUIRE(res.endpoints().count(couchbase::service_type::query) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::query].size() > 0);

    REQUIRE(res.endpoints().count(couchbase::service_type::search) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::search].size() > 0);

    if (integration.ctx.version.supports_analytics()) {
      REQUIRE(res.endpoints().count(couchbase::service_type::analytics) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::analytics].size() > 0);
    }

    if (integration.ctx.version.supports_eventing_functions()) {
      REQUIRE(res.endpoints().count(couchbase::service_type::eventing) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::eventing].size() > 0);
    }

    REQUIRE(res.id() == "my_report_id");
    INFO(res.sdk());
    REQUIRE(res.sdk().find("cxx/") == 0);
  }
}

TEST_CASE("integration: ping allows to select services", "[integration]")
{
  test::utils::integration_test_guard integration;

  test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

  SECTION("Core API")
  {
    auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
    auto f = barrier->get_future();
    integration.cluster.ping(
      {},
      {},
      { couchbase::core::service_type::key_value, couchbase::core::service_type::query },
      {},
      [barrier](couchbase::core::diag::ping_result&& resp) mutable {
        barrier->set_value(std::move(resp));
      });
    auto res = f.get();
    REQUIRE(res.services.size() == 2);

    REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);

    REQUIRE(res.services.count(couchbase::core::service_type::query) > 0);
    REQUIRE(res.services[couchbase::core::service_type::query].size() > 0);
  }

  SECTION("Public API")
  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto opts = couchbase::ping_options().service_types(
      { couchbase::service_type::key_value, couchbase::service_type::query });
    auto [err, res] = cluster.ping(opts).get();

    REQUIRE(res.endpoints().size() == 2);

    REQUIRE(res.endpoints().count(couchbase::service_type::key_value) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::key_value].size() > 0);

    REQUIRE(res.endpoints().count(couchbase::service_type::query) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::query].size() > 0);
  }
}

TEST_CASE("integration: ping allows to select bucket and opens it automatically", "[integration]")
{
  test::utils::integration_test_guard integration;

  SECTION("Core API")
  {
    auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
    auto f = barrier->get_future();
    integration.cluster.ping({},
                             integration.ctx.bucket,
                             { couchbase::core::service_type::key_value },
                             {},
                             [barrier](couchbase::core::diag::ping_result&& resp) mutable {
                               barrier->set_value(std::move(resp));
                             });
    auto res = f.get();

    REQUIRE(res.services.size() == 1);
    REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value][0].bucket.has_value());
    REQUIRE(res.services[couchbase::core::service_type::key_value][0].bucket.value() ==
            integration.ctx.bucket);
  }

  SECTION("Public API")
  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto bucket = cluster.bucket(integration.ctx.bucket);

    auto [err, res] =
      bucket.ping(couchbase::ping_options().service_types({ couchbase::service_type::key_value }))
        .get();

    REQUIRE(res.endpoints().size() == 1);
    REQUIRE(res.endpoints().count(couchbase::service_type::key_value) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::key_value].size() > 0);
    REQUIRE(
      res.endpoints()[couchbase::service_type::key_value][0].endpoint_namespace().has_value());
    REQUIRE(res.endpoints()[couchbase::service_type::key_value][0].endpoint_namespace().value() ==
            integration.ctx.bucket);
  }
}

TEST_CASE("integration: ping allows setting timeout", "[integration]")
{
  test::utils::integration_test_guard integration;

  SECTION("Core API")
  {
    auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
    auto f = barrier->get_future();
    integration.cluster.ping({},
                             {},
                             {},
                             std::chrono::milliseconds(0),
                             [barrier](couchbase::core::diag::ping_result&& resp) mutable {
                               barrier->set_value(std::move(resp));
                             });
    auto res = f.get();
    REQUIRE(res.services.size() > 0);

    REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);
    REQUIRE(res.services[couchbase::core::service_type::key_value][0].error.has_value());
    REQUIRE(res.services[couchbase::core::service_type::key_value][0].state ==
            couchbase::core::diag::ping_state::timeout);

    REQUIRE(res.services.count(couchbase::core::service_type::management) > 0);
    REQUIRE(res.services[couchbase::core::service_type::management].size() > 0);
    REQUIRE(res.services[couchbase::core::service_type::management][0].error.has_value());
    REQUIRE(res.services[couchbase::core::service_type::management][0].state ==
            couchbase::core::diag::ping_state::timeout);

    if (integration.ctx.deployment != test::utils::deployment_type::elixir) {
      REQUIRE(res.services.count(couchbase::core::service_type::view) > 0);
      REQUIRE(res.services[couchbase::core::service_type::view].size() > 0);
      REQUIRE(res.services[couchbase::core::service_type::view][0].error.has_value());
      REQUIRE(res.services[couchbase::core::service_type::view][0].state ==
              couchbase::core::diag::ping_state::timeout);
    }

    REQUIRE(res.services.count(couchbase::core::service_type::query) > 0);
    REQUIRE(res.services[couchbase::core::service_type::query].size() > 0);
    REQUIRE(res.services[couchbase::core::service_type::query][0].error.has_value());
    REQUIRE(res.services[couchbase::core::service_type::query][0].state ==
            couchbase::core::diag::ping_state::timeout);

    REQUIRE(res.services.count(couchbase::core::service_type::search) > 0);
    REQUIRE(res.services[couchbase::core::service_type::search].size() > 0);
    REQUIRE(res.services[couchbase::core::service_type::search][0].error.has_value());
    REQUIRE(res.services[couchbase::core::service_type::search][0].state ==
            couchbase::core::diag::ping_state::timeout);

    if (integration.ctx.version.supports_analytics()) {
      REQUIRE(res.services.count(couchbase::core::service_type::analytics) > 0);
      REQUIRE(res.services[couchbase::core::service_type::analytics].size() > 0);
      REQUIRE(res.services[couchbase::core::service_type::analytics][0].error.has_value());
      REQUIRE(res.services[couchbase::core::service_type::analytics][0].state ==
              couchbase::core::diag::ping_state::timeout);
    }

    if (integration.ctx.version.supports_eventing_functions()) {
      REQUIRE(res.services.count(couchbase::core::service_type::eventing) > 0);
      REQUIRE(res.services[couchbase::core::service_type::eventing].size() > 0);
      REQUIRE(res.services[couchbase::core::service_type::eventing][0].error.has_value());
      REQUIRE(res.services[couchbase::core::service_type::eventing][0].state ==
              couchbase::core::diag::ping_state::timeout);
    }
  }

  SECTION("Public API")
  {
    auto test_ctx = integration.ctx;
    auto [e, cluster] =
      couchbase::cluster::connect(test_ctx.connection_string, test_ctx.build_options()).get();
    REQUIRE_SUCCESS(e.ec());

    auto [err, res] =
      cluster.ping(couchbase::ping_options().timeout(std::chrono::milliseconds(0))).get();

    REQUIRE(res.endpoints().size() > 0);

    REQUIRE(res.endpoints().count(couchbase::service_type::key_value) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::key_value].size() > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::key_value][0].error().has_value());
    REQUIRE(res.endpoints()[couchbase::service_type::key_value][0].state() ==
            couchbase::ping_state::timeout);

    REQUIRE(res.endpoints().count(couchbase::service_type::management) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::management].size() > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::management][0].error().has_value());
    REQUIRE(res.endpoints()[couchbase::service_type::management][0].state() ==
            couchbase::ping_state::timeout);

    if (integration.ctx.deployment != test::utils::deployment_type::elixir) {
      REQUIRE(res.endpoints().count(couchbase::service_type::view) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::view].size() > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::view][0].error().has_value());
      REQUIRE(res.endpoints()[couchbase::service_type::view][0].state() ==
              couchbase::ping_state::timeout);
    }

    REQUIRE(res.endpoints().count(couchbase::service_type::query) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::query].size() > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::query][0].error().has_value());
    REQUIRE(res.endpoints()[couchbase::service_type::query][0].state() ==
            couchbase::ping_state::timeout);

    REQUIRE(res.endpoints().count(couchbase::service_type::search) > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::search].size() > 0);
    REQUIRE(res.endpoints()[couchbase::service_type::search][0].error().has_value());
    REQUIRE(res.endpoints()[couchbase::service_type::search][0].state() ==
            couchbase::ping_state::timeout);

    if (integration.ctx.version.supports_analytics()) {
      REQUIRE(res.endpoints().count(couchbase::service_type::analytics) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::analytics].size() > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::analytics][0].error().has_value());
      REQUIRE(res.endpoints()[couchbase::service_type::analytics][0].state() ==
              couchbase::ping_state::timeout);
    }

    if (integration.ctx.version.supports_eventing_functions()) {
      REQUIRE(res.endpoints().count(couchbase::service_type::eventing) > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::eventing].size() > 0);
      REQUIRE(res.endpoints()[couchbase::service_type::eventing][0].error().has_value());
      REQUIRE(res.endpoints()[couchbase::service_type::eventing][0].state() ==
              couchbase::ping_state::timeout);
    }
  }
}
