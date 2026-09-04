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

#include "framework/test_registry.hxx"

#include <couchbase/build_info.hxx>

#include "core/orphan_reporter.hxx"

#include <asio/io_context.hpp>
#include <tao/json/from_string.hpp>
#include <tao/json/to_string.hpp>
#include <tao/json/value.hpp>

#include <chrono>
#include <string>

namespace couchbase::test
{
// Without this a mismatch reports only that the two reports differed. The report is the whole
// contract of this file, so the failure has to carry it.
template<>
struct operand_printer<tao::json::value> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const tao::json::value& value) -> std::string
  {
    return tao::json::to_string(value);
  }
};

namespace
{
using namespace std::literals::chrono_literals;

auto
options_with_sample_size_4() -> couchbase::core::orphan_reporter_options
{
  auto opts = couchbase::core::orphan_reporter_options{};
  opts.sample_size = 4;
  return opts;
}

// The reporter holds a timer on the io_context, so the two are constructed and destroyed together.
struct reporter_fixture {
  asio::io_context io{};
  couchbase::core::orphan_reporter reporter{ io, options_with_sample_size_4() };
};

// Six orphans whose durations increase with the index, so the sample the reporter keeps is
// determined by the index alone.
auto
get_100us() -> couchbase::core::orphan_attributes
{
  return { "conn1", "0x23", "remote1", "local1", 100us, 30us, 60us, "get" };
}

auto
upsert_200us() -> couchbase::core::orphan_attributes
{
  return { "conn2", "0x24", "remote2", "local2", 200us, 40us, 80us, "upsert" };
}

auto
remove_300us() -> couchbase::core::orphan_attributes
{
  return { "conn3", "0x25", "remote3", "local3", 300us, 50us, 100us, "remove" };
}

auto
replace_400us() -> couchbase::core::orphan_attributes
{
  return { "conn4", "0x26", "remote4", "local4", 400us, 60us, 120us, "replace" };
}

auto
insert_500us() -> couchbase::core::orphan_attributes
{
  return { "conn5", "0x27", "remote5", "local5", 500us, 70us, 140us, "insert" };
}

auto
unlock_600us() -> couchbase::core::orphan_attributes
{
  return { "conn6", "0x28", "remote6", "local6", 600us, 80us, 160us, "unlock" };
}

// A debug build reports the emit interval and sample size alongside the sample.
auto
with_debug_fields(tao::json::value expected) -> tao::json::value
{
#if COUCHBASE_CXX_CLIENT_DEBUG_BUILD
  expected["emit_interval_ms"] = 10000;
  expected["sample_size"] = 4;
#endif
  return expected;
}

void
no_orphans_produces_no_output([[maybe_unused]] context& ctx)
{
  reporter_fixture fixture;

  assert_false(fixture.reporter.flush_and_create_output().has_value(),
               "nothing is emitted while no response has been orphaned");
}

void
more_orphans_than_the_sample_size_reports_the_slowest_ones([[maybe_unused]] context& ctx)
{
  reporter_fixture fixture;

  fixture.reporter.add_orphan(upsert_200us());
  fixture.reporter.add_orphan(get_100us());
  fixture.reporter.add_orphan(replace_400us());
  fixture.reporter.add_orphan(remove_300us());
  fixture.reporter.add_orphan(unlock_600us());
  fixture.reporter.add_orphan(insert_500us());

  const auto out = fixture.reporter.flush_and_create_output();
  assert_true(out.has_value(), "the recorded orphans are emitted");

  const auto expected = with_debug_fields(tao::json::from_string(R"({
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
})"));

  assert_eq(tao::json::from_string(out.value()),
            expected,
            "the sample holds the slowest four, ordered by total duration");
}

void
as_many_orphans_as_the_sample_size_reports_all_of_them([[maybe_unused]] context& ctx)
{
  reporter_fixture fixture;

  fixture.reporter.add_orphan(upsert_200us());
  fixture.reporter.add_orphan(get_100us());
  fixture.reporter.add_orphan(replace_400us());
  fixture.reporter.add_orphan(remove_300us());

  const auto out = fixture.reporter.flush_and_create_output();
  assert_true(out.has_value(), "the recorded orphans are emitted");

  const auto expected = with_debug_fields(tao::json::from_string(R"({
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
})"));

  assert_eq(tao::json::from_string(out.value()),
            expected,
            "every orphan is reported, ordered by total duration");
}

void
fewer_orphans_than_the_sample_size_reports_all_of_them([[maybe_unused]] context& ctx)
{
  reporter_fixture fixture;

  fixture.reporter.add_orphan(upsert_200us());
  fixture.reporter.add_orphan(get_100us());

  const auto out = fixture.reporter.flush_and_create_output();
  assert_true(out.has_value(), "the recorded orphans are emitted");

  const auto expected = with_debug_fields(tao::json::from_string(R"({
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
})"));

  assert_eq(tao::json::from_string(out.value()),
            expected,
            "a sample shorter than the limit is reported whole");
}

void
flushing_the_output_clears_the_recorded_orphans([[maybe_unused]] context& ctx)
{
  reporter_fixture fixture;

  fixture.reporter.add_orphan(upsert_200us());
  fixture.reporter.add_orphan(get_100us());

  assert_true(fixture.reporter.flush_and_create_output().has_value(),
              "the first flush emits what was recorded");
  assert_false(fixture.reporter.flush_and_create_output().has_value(),
               "a second flush finds nothing left to emit");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(no_orphans_produces_no_output) },
      { CASE(more_orphans_than_the_sample_size_reports_the_slowest_ones) },
      { CASE(as_many_orphans_as_the_sample_size_reports_all_of_them) },
      { CASE(fewer_orphans_than_the_sample_size_reports_all_of_them) },
      { CASE(flushing_the_output_clears_the_recorded_orphans) },
    },
  };
}

} // namespace couchbase::test
