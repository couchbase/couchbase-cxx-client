/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include <couchbase/diagnostics_result.hxx>
#include <couchbase/ping_result.hxx>

#include <tao/json.hpp>

using namespace std::literals::chrono_literals;

TEST_CASE("unit: serializing diagnostics report", "[unit]")
{
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

  SECTION("Core API")
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

    auto report = tao::json::value(res);
    REQUIRE(report == expected);
  }

  SECTION("Public API")
  {
    couchbase::diagnostics_result res{
      "0xdeadbeef",
      2,
      "cxx/1.0.0",
      {
        {
          couchbase::service_type::search,
          {
            {
              couchbase::service_type::search,
              "0x1415F11",
              1182000us,
              "127.0.0.1:54669",
              "centos7-lx1.home.ingenthron.org:8094",
              std::nullopt,
              couchbase::endpoint_state::connecting,
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
              "127.0.0.1:54670",
              "centos7-lx1.home.ingenthron.org:11210",
              "bucketname",
              couchbase::endpoint_state::connected,
              std::nullopt,
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
              "127.0.0.1:54671",
              "centos7-lx1.home.ingenthron.org:8093",
              std::nullopt,
              couchbase::endpoint_state::connected,
              std::nullopt,
            },
            {
              couchbase::service_type::query,
              "0x1415F14",
              1182000us,
              "127.0.0.1:54682",
              "centos7-lx2.home.ingenthron.org:8095",
              std::nullopt,
              couchbase::endpoint_state::disconnected,
              std::nullopt,
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
              "127.0.0.1:54675",
              "centos7-lx1.home.ingenthron.org:8095",
              std::nullopt,
              couchbase::endpoint_state::connected,
              std::nullopt,
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
              "127.0.0.1:54672",
              "centos7-lx1.home.ingenthron.org:8092",
              std::nullopt,
              couchbase::endpoint_state::connected,
              std::nullopt,
            },
          },
        },
      },
    };

    auto report = tao::json::from_string(res.as_json());
    REQUIRE(report == expected);
  }
}

TEST_CASE("unit: serializing ping report", "[integration]")
{
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

  SECTION("Core API")
  {
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

    auto report = tao::json::value(res);
    REQUIRE(report == expected);
  }

  SECTION("Public API")
  {
    couchbase::ping_result res{
      "0xdeadbeef",
      2,
      "cxx/1.0.0",
      {
        {
          couchbase::service_type::search,
          {
            {
              couchbase::service_type::search,
              "0x1415F11",
              "127.0.0.1:54669",
              "centos7-lx1.home.ingenthron.org:8094",
              couchbase::ping_state::ok,
              std::nullopt,
              std::nullopt,
              877909us,
            },
          },
        },
        {
          couchbase::service_type::key_value,
          {
            {
              couchbase::service_type::key_value,
              "0x1415F12",
              "127.0.0.1:54670",
              "centos7-lx1.home.ingenthron.org:11210",
              couchbase::ping_state::ok,
              std::nullopt,
              "bucketname",
              1182000us,
            },
          },
        },
        {
          couchbase::service_type::query,
          {
            {
              couchbase::service_type::query,
              "0x1415F14",
              "127.0.0.1:54682",
              "centos7-lx2.home.ingenthron.org:8095",
              couchbase::ping_state::timeout,
              std::nullopt,
              std::nullopt,
              2213us,
            },
          },
        },
        {
          couchbase::service_type::analytics,
          {
            {
              couchbase::service_type::analytics,
              "0x1415F15",
              "127.0.0.1:54675",
              "centos7-lx1.home.ingenthron.org:8095",
              couchbase::ping_state::error,
              "endpoint returned HTTP code 500!",
              std::nullopt,
              2213us,
            },
          },
        },
        {
          couchbase::service_type::view,
          {
            {
              couchbase::service_type::view,
              "0x1415F16",
              "127.0.0.1:54672",
              "centos7-lx1.home.ingenthron.org:8092",
              couchbase::ping_state::ok,
              std::nullopt,
              std::nullopt,
              45585us,
            },
          },
        },
      },
    };

    auto report = tao::json::from_string(res.as_json());
    REQUIRE(report == expected);
  }
}
