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

#include <couchbase/diagnostics.hxx>
#include <couchbase/diagnostics_json.hxx>

using namespace std::literals::chrono_literals;

TEST_CASE("unit: serializing diagnostics report", "[unit]")
{
    couchbase::diag::diagnostics_result res{
        "0xdeadbeef",
        "cxx/1.0.0",
        {
          {
            {
              couchbase::service_type::search,
              {
                {
                  couchbase::service_type::search,
                  "0x1415F11",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::diag::endpoint_state::connecting,
                  std::nullopt,
                  "RECONNECTING, backoff for 4096ms from Fri Sep  1 00:03:44 PDT 2017",
                },
              },
            },
            {
              couchbase::service_type::key_value,
              {
                {
                  couchbase::service_type::key_value,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::diag::endpoint_state::connected,
                  "bucketname",
                },
              },
            },
            {
              couchbase::service_type::query,
              {
                {
                  couchbase::service_type::query,
                  "0x1415F13",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8093",
                  "127.0.0.1:54671",
                  couchbase::diag::endpoint_state::connected,
                },
                {
                  couchbase::service_type::query,
                  "0x1415F14",
                  1182000us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::diag::endpoint_state::disconnected,
                },
              },
            },
            {
              couchbase::service_type::analytics,
              {
                {
                  couchbase::service_type::analytics,
                  "0x1415F15",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::diag::endpoint_state::connected,
                },
              },
            },
            {
              couchbase::service_type::view,
              {
                {
                  couchbase::service_type::view,
                  "0x1415F16",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::diag::endpoint_state::connected,
                },
              },
            },
          },
        },
    };

    auto expected = couchbase::utils::json::parse(R"(
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

    couchbase::diag::ping_result res{
        "0xdeadbeef",
        "cxx/1.0.0",
        {
          {
            {
              couchbase::service_type::search,
              {
                {
                  couchbase::service_type::search,
                  "0x1415F11",
                  877909us,
                  "centos7-lx1.home.ingenthron.org:8094",
                  "127.0.0.1:54669",
                  couchbase::diag::ping_state::ok,
                },
              },
            },
            {
              couchbase::service_type::key_value,
              {
                {
                  couchbase::service_type::key_value,
                  "0x1415F12",
                  1182000us,
                  "centos7-lx1.home.ingenthron.org:11210",
                  "127.0.0.1:54670",
                  couchbase::diag::ping_state::ok,
                  "bucketname",
                },
              },
            },
            {
              couchbase::service_type::query,
              {
                {
                  couchbase::service_type::query,
                  "0x1415F14",
                  2213us,
                  "centos7-lx2.home.ingenthron.org:8095",
                  "127.0.0.1:54682",
                  couchbase::diag::ping_state::timeout,
                },
              },
            },
            {
              couchbase::service_type::analytics,
              {
                {
                  couchbase::service_type::analytics,
                  "0x1415F15",
                  2213us,
                  "centos7-lx1.home.ingenthron.org:8095",
                  "127.0.0.1:54675",
                  couchbase::diag::ping_state::error,
                  std::nullopt,
                  "endpoint returned HTTP code 500!",
                },
              },
            },
            {
              couchbase::service_type::view,
              {
                {
                  couchbase::service_type::view,
                  "0x1415F16",
                  45585us,
                  "centos7-lx1.home.ingenthron.org:8092",
                  "127.0.0.1:54672",
                  couchbase::diag::ping_state::ok,
                },
              },
            },
          },
        },
    };

    auto expected = couchbase::utils::json::parse(R"(
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

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);
    {
        couchbase::operations::query_request req{ "SELECT 'hello, couchbase' AS greetings" };
        auto resp = test::utils::execute(integration.cluster, req);
        INFO(resp.ctx.ec.message())
        REQUIRE_FALSE(resp.ctx.ec);
        INFO("rows.size() =" << resp.rows.size())
        REQUIRE(resp.rows.size() == 1);
        INFO("row=" << resp.rows[0])
        REQUIRE(resp.rows[0] == R"({"greetings":"hello, couchbase"})");
    }
    {
        auto barrier = std::make_shared<std::promise<couchbase::diag::diagnostics_result>>();
        auto f = barrier->get_future();
        integration.cluster->diagnostics(
          "my_report_id", [barrier](couchbase::diag::diagnostics_result&& resp) mutable { barrier->set_value(std::move(resp)); });
        auto res = f.get();
        REQUIRE(res.id == "my_report_id");
        REQUIRE(res.sdk.find("cxx/") == 0);
        REQUIRE(res.services[couchbase::service_type::key_value].size() > 1);
        REQUIRE(res.services[couchbase::service_type::query].size() == 1);
        REQUIRE(res.services[couchbase::service_type::query][0].state == couchbase::diag::endpoint_state::connected);
    }
}

TEST_CASE("integration: ping", "[integration]")
{
    test::utils::integration_test_guard integration;

    test::utils::open_bucket(integration.cluster, integration.ctx.bucket);

    {
        auto barrier = std::make_shared<std::promise<couchbase::diag::ping_result>>();
        auto f = barrier->get_future();
        integration.cluster->ping(
          "my_report_id", {}, {}, [barrier](couchbase::diag::ping_result&& resp) mutable { barrier->set_value(std::move(resp)); });
        auto res = f.get();
        REQUIRE(res.id == "my_report_id");
        INFO(res.sdk)
        REQUIRE(res.sdk.find("cxx/") == 0);
    }
}
