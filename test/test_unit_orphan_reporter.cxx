/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025 Couchbase, Inc.
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

#include <couchbase/build_info.hxx>

#include "core/orphan_reporter.hxx"

#include <tao/json/from_string.hpp>

TEST_CASE("unit: orphan reporter output", "[unit]")
{
  auto opts = couchbase::core::orphan_reporter_options{};
  opts.sample_size = 4;
  asio::io_context io_context;
  auto reporter = couchbase::core::orphan_reporter{ io_context, opts };

  SECTION("No orphaned responses")
  {
    const auto out = reporter.flush_and_create_output();
    REQUIRE(!out.has_value());
  }

  SECTION("More oprhaned responses than the sample size")
  {
    reporter.add_orphan({ /* .connection_id = */ "conn2",
                          /* .operation_id = */ "0x24",
                          /* .last_remote_socket = */ "remote2",
                          /* .last_local_socket = */ "local2",
                          /* .total_duration = */ std::chrono::microseconds{ 200 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 40 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 80 },
                          /* .operation_name = */ "upsert" });
    reporter.add_orphan({ /* .connection_id = */ "conn1",
                          /* .operation_id = */ "0x23",
                          /* .last_remote_socket = */ "remote1",
                          /* .last_local_socket = */ "local1",
                          /* .total_duration = */ std::chrono::microseconds{ 100 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 30 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .operation_name = */ "get" });
    reporter.add_orphan({ /* .connection_id = */ "conn4",
                          /* .operation_id = */ "0x26",
                          /* .last_remote_socket = */ "remote4",
                          /* .last_local_socket = */ "local4",
                          /* .total_duration = */ std::chrono::microseconds{ 400 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 120 },
                          /* .operation_name = */ "replace" });
    reporter.add_orphan({ /* .connection_id = */ "conn3",
                          /* .operation_id = */ "0x25",
                          /* .last_remote_socket = */ "remote3",
                          /* .last_local_socket = */ "local3",
                          /* .total_duration = */ std::chrono::microseconds{ 300 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 50 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 100 },
                          /* .operation_name = */ "remove" });
    reporter.add_orphan({ /* .connection_id = */ "conn6",
                          /* .operation_id = */ "0x28",
                          /* .last_remote_socket = */ "remote6",
                          /* .last_local_socket = */ "local6",
                          /* .total_duration = */ std::chrono::microseconds{ 600 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 80 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 160 },
                          /* .operation_name = */ "unlock" });
    reporter.add_orphan({ /* .connection_id = */ "conn5",
                          /* .operation_id = */ "0x27",
                          /* .last_remote_socket = */ "remote5",
                          /* .last_local_socket = */ "local5",
                          /* .total_duration = */ std::chrono::microseconds{ 500 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 70 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 140 },
                          /* .operation_name = */ "insert" });

    const auto out = reporter.flush_and_create_output();
    REQUIRE(out.has_value());

    tao::json::value expected = tao::json::from_string(R"({
  "kv": {
    "total_count": 6,
    "top_requests": [
      {
        "total_duration_us": 600,
        "last_server_duration_us": 80,
        "total_server_duration_us": 160,
        "operation_name": "unlock",
        "last_local_id": "conn6",
        "operation_id": "0x28",
        "last_local_socket": "local6",
        "last_remote_socket": "remote6"
      },
      {
        "total_duration_us": 500,
        "last_server_duration_us": 70,
        "total_server_duration_us": 140,
        "operation_name": "insert",
        "last_local_id": "conn5",
        "operation_id": "0x27",
        "last_local_socket": "local5",
        "last_remote_socket": "remote5"
      },
      {
        "total_duration_us": 400,
        "last_server_duration_us": 60,
        "total_server_duration_us": 120,
        "operation_name": "replace",
        "last_local_id": "conn4",
        "operation_id": "0x26",
        "last_local_socket": "local4",
        "last_remote_socket": "remote4"
      },
      {
        "total_duration_us": 300,
        "last_server_duration_us": 50,
        "total_server_duration_us": 100,
        "operation_name": "remove",
        "last_local_id": "conn3",
        "operation_id": "0x25",
        "last_local_socket": "local3",
        "last_remote_socket": "remote3"
      }
    ]
  }
})");

#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
    expected["emit_interval_ms"] = 10000;
    expected["sample_size"] = 4;
#endif

    REQUIRE(expected == tao::json::from_string(out.value()));
  }

  SECTION("As many orphaned responses as the sample size")
  {
    reporter.add_orphan({ /* .connection_id = */ "conn2",
                          /* .operation_id = */ "0x24",
                          /* .last_remote_socket = */ "remote2",
                          /* .last_local_socket = */ "local2",
                          /* .total_duration = */ std::chrono::microseconds{ 200 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 40 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 80 },
                          /* .operation_name = */ "upsert" });
    reporter.add_orphan({ /* .connection_id = */ "conn1",
                          /* .operation_id = */ "0x23",
                          /* .last_remote_socket = */ "remote1",
                          /* .last_local_socket = */ "local1",
                          /* .total_duration = */ std::chrono::microseconds{ 100 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 30 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .operation_name = */ "get" });
    reporter.add_orphan({ /* .connection_id = */ "conn4",
                          /* .operation_id = */ "0x26",
                          /* .last_remote_socket = */ "remote4",
                          /* .last_local_socket = */ "local4",
                          /* .total_duration = */ std::chrono::microseconds{ 400 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 120 },
                          /* .operation_name = */ "replace" });
    reporter.add_orphan({ /* .connection_id = */ "conn3",
                          /* .operation_id = */ "0x25",
                          /* .last_remote_socket = */ "remote3",
                          /* .last_local_socket = */ "local3",
                          /* .total_duration = */ std::chrono::microseconds{ 300 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 50 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 100 },
                          /* .operation_name = */ "remove" });

    const auto out = reporter.flush_and_create_output();
    REQUIRE(out.has_value());

    tao::json::value expected = tao::json::from_string(R"({
  "kv": {
    "total_count": 4,
    "top_requests": [
      {
        "total_duration_us": 400,
        "last_server_duration_us": 60,
        "total_server_duration_us": 120,
        "operation_name": "replace",
        "last_local_id": "conn4",
        "operation_id": "0x26",
        "last_local_socket": "local4",
        "last_remote_socket": "remote4"
      },
      {
        "total_duration_us": 300,
        "last_server_duration_us": 50,
        "total_server_duration_us": 100,
        "operation_name": "remove",
        "last_local_id": "conn3",
        "operation_id": "0x25",
        "last_local_socket": "local3",
        "last_remote_socket": "remote3"
      },
      {
        "total_duration_us": 200,
        "last_server_duration_us": 40,
        "total_server_duration_us": 80,
        "operation_name": "upsert",
        "last_local_id": "conn2",
        "operation_id": "0x24",
        "last_local_socket": "local2",
        "last_remote_socket": "remote2"
      },
      {
        "total_duration_us": 100,
        "last_server_duration_us": 30,
        "total_server_duration_us": 60,
        "operation_name": "get",
        "last_local_id": "conn1",
        "operation_id": "0x23",
        "last_local_socket": "local1",
        "last_remote_socket": "remote1"
      }
    ]
  }
})");

#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
    expected["emit_interval_ms"] = 10000;
    expected["sample_size"] = 4;
#endif

    REQUIRE(expected == tao::json::from_string(out.value()));
  }

  SECTION("Fewer orphaned responses than sample size")
  {
    reporter.add_orphan({ /* .connection_id = */ "conn2",
                          /* .operation_id = */ "0x24",
                          /* .last_remote_socket = */ "remote2",
                          /* .last_local_socket = */ "local2",
                          /* .total_duration = */ std::chrono::microseconds{ 200 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 40 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 80 },
                          /* .operation_name = */ "upsert" });
    reporter.add_orphan({ /* .connection_id = */ "conn1",
                          /* .operation_id = */ "0x23",
                          /* .last_remote_socket = */ "remote1",
                          /* .last_local_socket = */ "local1",
                          /* .total_duration = */ std::chrono::microseconds{ 100 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 30 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .operation_name = */ "get" });

    const auto out = reporter.flush_and_create_output();
    REQUIRE(out.has_value());

    tao::json::value expected = tao::json::from_string(R"({
  "kv": {
    "total_count": 2,
    "top_requests": [
      {
        "total_duration_us": 200,
        "last_server_duration_us": 40,
        "total_server_duration_us": 80,
        "operation_name": "upsert",
        "last_local_id": "conn2",
        "operation_id": "0x24",
        "last_local_socket": "local2",
        "last_remote_socket": "remote2"
      },
      {
        "total_duration_us": 100,
        "last_server_duration_us": 30,
        "total_server_duration_us": 60,
        "operation_name": "get",
        "last_local_id": "conn1",
        "operation_id": "0x23",
        "last_local_socket": "local1",
        "last_remote_socket": "remote1"
      }
    ]
  }
})");

#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
    expected["emit_interval_ms"] = 10000;
    expected["sample_size"] = 4;
#endif

    REQUIRE(expected == tao::json::from_string(out.value()));
  }

  SECTION("Flushing & getting the output clears existing orphaned responses")
  {
    reporter.add_orphan({ /* .connection_id = */ "conn2",
                          /* .operation_id = */ "0x24",
                          /* .last_remote_socket = */ "remote2",
                          /* .last_local_socket = */ "local2",
                          /* .total_duration = */ std::chrono::microseconds{ 200 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 40 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 80 },
                          /* .operation_name = */ "upsert" });
    reporter.add_orphan({ /* .connection_id = */ "conn1",
                          /* .operation_id = */ "0x23",
                          /* .last_remote_socket = */ "remote1",
                          /* .last_local_socket = */ "local1",
                          /* .total_duration = */ std::chrono::microseconds{ 100 },
                          /* .last_server_duration = */ std::chrono::microseconds{ 30 },
                          /* .total_server_duration = */ std::chrono::microseconds{ 60 },
                          /* .operation_name = */ "get" });

    REQUIRE(reporter.flush_and_create_output().has_value());
    REQUIRE_FALSE(reporter.flush_and_create_output().has_value());
  }
}
