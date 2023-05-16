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
#include "core/diagnostics_json.hxx"

using namespace std::literals::chrono_literals;

TEST_CASE("unit: serializing diagnostics report", "[unit]")
{
    couchbase::core::diag::diagnostics_result res{
        "0xdeadbeef",
        "cxx/1.0.0",
        {
          {
            {
              couchbase::core::service_type::search,
              {
                {
                  couchbase::core::service_type::search,
                  "0x1415F11",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::core::diag::endpoint_state::connecting,
                  std::nullopt,
                  "RECONNECTING, backoff for 4096ms from Fri Sep  1 00:03:44 PDT 2017",
                },
              },
            },
            {
              couchbase::core::service_type::key_value,
              {
                {
                  couchbase::core::service_type::key_value,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::core::diag::endpoint_state::connected,
                  "bucketname",
                },
              },
            },
            {
              couchbase::core::service_type::query,
              {
                {
                  couchbase::core::service_type::query,
                  "0x1415F13",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8093",
                  "127.0.0.1:54671",
                  couchbase::core::diag::endpoint_state::connected,
                },
                {
                  couchbase::core::service_type::query,
                  "0x1415F14",
                  1182000us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::core::diag::endpoint_state::disconnected,
                },
              },
            },
            {
              couchbase::core::service_type::analytics,
              {
                {
                  couchbase::core::service_type::analytics,
                  "0x1415F15",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::core::diag::endpoint_state::connected,
                },
              },
            },
            {
              couchbase::core::service_type::view,
              {
                {
                  couchbase::core::service_type::view,
                  "0x1415F16",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::core::diag::endpoint_state::connected,
                },
              },
            },
          },
        },
    };

    auto expected = couchbase::core::utils::json::parse(R"(
{
  "version": 2,
  "id": "0xdeadbeef",
  "sdk": "cxx/1.0.0",
  "services": {
    "kv": [
      {
        "id": "0x1415F12",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:11210",
        "local": "127.0.0.1:54670",
        "state": "connected",
        "namespace": "bucketname"
      }
    ],
    "search": [
      {
        "id": "0x1415F11",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8094",
        "local": "127.0.0.1:54669",
        "state": "connecting",
        "details": "RECONNECTING, backoff for 4096ms from Fri Sep  1 00:03:44 PDT 2017"
      }
    ],
    "query": [
      {
        "id": "0x1415F13",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8093",
        "local": "127.0.0.1:54671",
        "state": "connected"
      },
      {
        "id": "0x1415F14",
        "last_activity_us": 1182000,
        "remote": "centos7-lx2.home.ingenthron.org:8095",
        "local": "127.0.0.1:54682",
        "state": "disconnected"
      }
    ],
    "analytics": [
      {
        "id": "0x1415F15",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8095",
        "local": "127.0.0.1:54675",
        "state": "connected"
      }
    ],
    "views": [
      {
        "id": "0x1415F16",
        "last_activity_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:8092",
        "local": "127.0.0.1:54672",
        "state": "connected"
      }
    ]
  }
}
)");
    auto report = tao::json::value(res);
    REQUIRE(report == expected);
}

TEST_CASE("integration: serializing ping report", "[integration]")
{
    test::utils::integration_test_guard integration;

    couchbase::core::diag::ping_result res{
        "0xdeadbeef",
        "cxx/1.0.0",
        {
          {
            {
              couchbase::core::service_type::search,
              {
                {
                  couchbase::core::service_type::search,
                  "0x1415F11",
                  877909us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::core::diag::ping_state::ok,
                },
              },
            },
            {
              couchbase::core::service_type::key_value,
              {
                {
                  couchbase::core::service_type::key_value,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::core::diag::ping_state::ok,
                  "bucketname",
                },
              },
            },
            {
              couchbase::core::service_type::query,
              {
                {
                  couchbase::core::service_type::query,
                  "0x1415F14",
                  2213us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::core::diag::ping_state::timeout,
                },
              },
            },
            {
              couchbase::core::service_type::analytics,
              {
                {
                  couchbase::core::service_type::analytics,
                  "0x1415F15",
                  2213us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::core::diag::ping_state::error,
                  std::nullopt,
                  "endpoint returned HTTP code 500!",
                },
              },
            },
            {
              couchbase::core::service_type::view,
              {
                {
                  couchbase::core::service_type::view,
                  "0x1415F16",
                  45585us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::core::diag::ping_state::ok,
                },
              },
            },
          },
        },
    };

    auto expected = couchbase::core::utils::json::parse(R"(
{
  "version": 2,
  "id": "0xdeadbeef",
  "sdk": "cxx/1.0.0",
  "services": {
    "search": [
      {
        "id": "0x1415F11",
        "latency_us": 877909,
        "remote": "centos7-lx1.home.ingenthron.org:8094",
        "local": "127.0.0.1:54669",
        "state": "ok"
      }
    ],
    "kv": [
      {
        "id": "0x1415F12",
        "latency_us": 1182000,
        "remote": "centos7-lx1.home.ingenthron.org:11210",
        "local": "127.0.0.1:54670",
        "state": "ok",
        "namespace": "bucketname"
      }
    ],
    "query": [
      {
        "id": "0x1415F14",
        "latency_us": 2213,
        "remote": "centos7-lx2.home.ingenthron.org:8095",
        "local": "127.0.0.1:54682",
        "state": "timeout"
      }
    ],
    "analytics": [
      {
        "id": "0x1415F15",
        "latency_us": 2213,
        "remote": "centos7-lx1.home.ingenthron.org:8095",
        "local": "127.0.0.1:54675",
        "state": "error",
        "error": "endpoint returned HTTP code 500!"
      }
    ],
    "views": [
      {
        "id": "0x1415F16",
        "latency_us": 45585,
        "remote": "centos7-lx1.home.ingenthron.org:8092",
        "local": "127.0.0.1:54672",
        "state": "ok"
      }
    ]
  }
}
)");
    auto report = tao::json::value(res);
    REQUIRE(report == expected);
}

TEST_CASE("integration: fetch diagnostics after N1QL query", "[integration]")
{
    test::utils::integration_test_guard integration;

    if (!integration.cluster_version().supports_query() || integration.ctx.deployment == test::utils::deployment_type::elixir) {
        SKIP("cluster does not support query or cluster level query");
    }

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
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
        integration.cluster->diagnostics(
          "my_report_id", [barrier](couchbase::core::diag::diagnostics_result&& resp) mutable { barrier->set_value(std::move(resp)); });
        auto res = f.get();
        REQUIRE(res.id == "my_report_id");
        REQUIRE(res.sdk.find("cxx/") == 0);
        REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 1);
        REQUIRE(res.services[couchbase::core::service_type::query].size() == 1);
        REQUIRE(res.services[couchbase::core::service_type::query][0].state == couchbase::core::diag::endpoint_state::connected);
    }
}

TEST_CASE("integration: ping", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
        auto f = barrier->get_future();
        integration.cluster->ping(
          "my_report_id", {}, {}, [barrier](couchbase::core::diag::ping_result&& resp) mutable { barrier->set_value(std::move(resp)); });
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
}

TEST_CASE("integration: ping allows to select services", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
        auto f = barrier->get_future();
        integration.cluster->ping({},
                                  {},
                                  { couchbase::core::service_type::key_value, couchbase::core::service_type::query },
                                  [barrier](couchbase::core::diag::ping_result&& resp) mutable { barrier->set_value(std::move(resp)); });
        auto res = f.get();
        REQUIRE(res.services.size() == 2);

        REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
        REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);

        REQUIRE(res.services.count(couchbase::core::service_type::query) > 0);
        REQUIRE(res.services[couchbase::core::service_type::query].size() > 0);
    }
}

TEST_CASE("integration: ping allows to select bucket and opens it automatically", "[integration]")
{
    test::utils::integration_test_guard integration;

    {
        auto barrier = std::make_shared<std::promise<couchbase::core::diag::ping_result>>();
        auto f = barrier->get_future();
        integration.cluster->ping({},
                                  integration.ctx.bucket,
                                  { couchbase::core::service_type::key_value },
                                  [barrier](couchbase::core::diag::ping_result&& resp) mutable { barrier->set_value(std::move(resp)); });
        auto res = f.get();
        REQUIRE(res.services.size() == 1);

        REQUIRE(res.services.count(couchbase::core::service_type::key_value) > 0);
        REQUIRE(res.services[couchbase::core::service_type::key_value].size() > 0);
        REQUIRE(res.services[couchbase::core::service_type::key_value][0].bucket.has_value());
        REQUIRE(res.services[couchbase::core::service_type::key_value][0].bucket.value() == integration.ctx.bucket);
    }
}
