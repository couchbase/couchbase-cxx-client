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

#include "test_helper.hxx"

#include "core/chrono_utils.hxx"

#include <spdlog/fmt/bundled/format.h>

// NOLINTBEGIN(bugprone-chained-comparison, misc-use-anonymous-namespace)

using namespace couchbase::core;

TEST_CASE("to_iso8601_utc with time_t and microseconds", "[unit][chrono_utils]")
{
  SECTION("epoch time with zero microseconds")
  {
    std::time_t time = 0;
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "1970-01-01T00:00:00.000000Z");
  }

  SECTION("epoch time with microseconds")
  {
    std::time_t time = 0;
    std::int64_t micros = 123456;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "1970-01-01T00:00:00.123456Z");
  }

  SECTION("known timestamp - 2025-10-20 22:56:00 UTC")
  {
    std::time_t time = 1761000960; // 2025-10-20 22:56:00 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2025-10-20T22:56:00.000000Z");
  }

  SECTION("known timestamp with microseconds")
  {
    std::time_t time = 1761000960; // 2025-10-20 22:56:00 UTC
    std::int64_t micros = 987654;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2025-10-20T22:56:00.987654Z");
  }

  SECTION("single digit microseconds with leading zeros")
  {
    std::time_t time = 1000000000; // 2001-09-09 01:46:40 UTC
    std::int64_t micros = 1;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2001-09-09T01:46:40.000001Z");
  }

  SECTION("maximum microseconds value")
  {
    std::time_t time = 1000000000;
    std::int64_t micros = 999999;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2001-09-09T01:46:40.999999Z");
  }

  SECTION("year 2000 timestamp")
  {
    std::time_t time = 946684800; // 2000-01-01 00:00:00 UTC
    std::int64_t micros = 500000;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2000-01-01T00:00:00.500000Z");
  }

  SECTION("leap second boundary - 2015-06-30 23:59:60")
  {
    // Note: std::time_t typically cannot represent leap seconds,
    // but testing nearby values
    std::time_t time = 1435708799; // 2015-06-30 23:59:59 UTC
    std::int64_t micros = 999999;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2015-06-30T23:59:59.999999Z");
  }

  SECTION("negative microseconds should be handled")
  {
    std::time_t time = 1000000000;
    std::int64_t micros = -1;
    auto result = to_iso8601_utc(time, micros);
    // Verify format is maintained (actual behavior may vary)
    REQUIRE(result.size() == 27);
    REQUIRE(result.back() == 'Z');
  }
}

TEST_CASE("to_iso8601_utc with system_clock::time_point", "[unit][chrono_utils]")
{
  SECTION("epoch time point")
  {
    auto time_point = std::chrono::system_clock::time_point{};
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "1970-01-01T00:00:00.000000Z");
  }

  SECTION("time point with whole seconds")
  {
    auto time_point = std::chrono::system_clock::time_point{ std::chrono::seconds(1000000000) };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2001-09-09T01:46:40.000000Z");
  }

  SECTION("time point with microseconds precision")
  {
    auto duration = std::chrono::seconds(1000000000) + std::chrono::microseconds(123456);
    auto time_point = std::chrono::system_clock::time_point{ duration };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2001-09-09T01:46:40.123456Z");
  }

  SECTION("time point with milliseconds converted to microseconds")
  {
    auto duration = std::chrono::seconds(1000000000) + std::chrono::milliseconds(123);
    auto time_point = std::chrono::system_clock::time_point{ duration };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2001-09-09T01:46:40.123000Z");
  }

  SECTION("time point with nanoseconds rounded to microseconds")
  {
    // Nanoseconds will be truncated to microseconds
    auto duration = std::chrono::seconds(1000000000) + std::chrono::nanoseconds(123456789);
    auto time_point = std::chrono::system_clock::time_point{
      std::chrono::duration_cast<std::chrono::system_clock::duration>(duration)
    };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2001-09-09T01:46:40.123456Z");
  }

  SECTION("current time produces valid ISO8601 format")
  {
    auto now = std::chrono::system_clock::now();
    auto result = to_iso8601_utc(now);

    // Verify format structure
    REQUIRE(result.size() == 27);
    REQUIRE(result[4] == '-');
    REQUIRE(result[7] == '-');
    REQUIRE(result[10] == 'T');
    REQUIRE(result[13] == ':');
    REQUIRE(result[16] == ':');
    REQUIRE(result[19] == '.');
    REQUIRE(result[26] == 'Z');
  }

  SECTION("year 2038 boundary (32-bit time_t edge case)")
  {
    // 2038-01-19 03:14:07 UTC (near Int32 max)
    auto time_point = std::chrono::system_clock::time_point{ std::chrono::seconds(2147483647) };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2038-01-19T03:14:07.000000Z");
  }

  SECTION("far future date - year 2100")
  {
    auto time_point = std::chrono::system_clock::time_point{
      std::chrono::seconds(4102444800) // 2100-01-01 00:00:00 UTC
    };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2100-01-01T00:00:00.000000Z");
  }

  SECTION("sub-second precision edge case - 999999 microseconds")
  {
    auto duration = std::chrono::seconds(1000000000) + std::chrono::microseconds(999999);
    auto time_point = std::chrono::system_clock::time_point{ duration };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result == "2001-09-09T01:46:40.999999Z");
  }
}

TEST_CASE("to_iso8601_utc format validation", "[unit][chrono_utils]")
{
  SECTION("output string length is always 27 characters")
  {
    std::time_t time = 1234567890;
    std::int64_t micros = 12345;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.size() == 27);
  }

  SECTION("year padding for years < 1000")
  {
    std::time_t time = -62167219200; // Year 0001-01-01 (if supported)
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    // Verify 4-digit year formatting
    REQUIRE(result.size() == 27);
  }

  SECTION("microseconds always padded to 6 digits")
  {
    std::time_t time = 1000000000;

    auto result1 = to_iso8601_utc(time, 1);
    REQUIRE(result1.substr(20, 6) == "000001");

    auto result2 = to_iso8601_utc(time, 10);
    REQUIRE(result2.substr(20, 6) == "000010");

    auto result3 = to_iso8601_utc(time, 100);
    REQUIRE(result3.substr(20, 6) == "000100");

    auto result4 = to_iso8601_utc(time, 1000);
    REQUIRE(result4.substr(20, 6) == "001000");
  }
}

TEST_CASE("to_iso8601_utc error handling", "[unit][chrono_utils]")
{
  SECTION("extremely large year values")
  {
    // Test with year that would require > 4 digits
    std::time_t time = 253402300799; // 9999-12-31 23:59:59 UTC
    std::int64_t micros = 999999;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "9999-12-31T23:59:59.999999Z");
    REQUIRE(result.size() == 27);
  }

  SECTION("very large microseconds that could overflow format")
  {
    std::time_t time = 1000000000;
    std::int64_t micros = 999999999999LL; // Way beyond valid range
    // Should still format but with unexpected microsecond value
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.size() >= 27); // May be longer due to overflow
    REQUIRE(result.back() == 'Z');
  }

  SECTION("maximum safe time_t value on 64-bit systems")
  {
    // Far future date that should still be valid
    std::time_t time = 32503680000; // 3000-01-01 00:00:00 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "3000-01-01T00:00:00.000000Z");
  }

  SECTION("buffer size validation with normal input")
  {
    // Verify that normal inputs don't trigger buffer overflow
    std::time_t time = 1729468560; // 2024-10-20 22:56:00 UTC
    std::int64_t micros = 123456;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.size() == 27);
    REQUIRE_NOTHROW(to_iso8601_utc(time, micros));
  }
}

TEST_CASE("to_iso8601_utc return value length precision", "[unit][chrono_utils]")
{
  SECTION("verify exact byte count for standard dates")
  {
    std::time_t time = 1234567890;
    std::int64_t micros = 123456;
    auto result = to_iso8601_utc(time, micros);
    // ISO8601 format: YYYY-MM-DDTHH:MM:SS.FFFFFFZ = 27 chars
    REQUIRE(result.size() == 27);
    REQUIRE(result.length() == 27);
  }

  SECTION("no null terminator in returned string content")
  {
    std::time_t time = 1000000000;
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    // Verify no embedded nulls before the end
    REQUIRE(result.find('\0') == std::string::npos);
  }

  SECTION("consistent length across different dates")
  {
    std::vector<std::time_t> times = {
      0,          // 1970-01-01
      946684800,  // 2000-01-01
      1000000000, // 2001-09-09
      1234567890, // 2009-02-13
      1729468560  // 2024-10-20
    };

    for (auto time : times) {
      auto result = to_iso8601_utc(time, 123456);
      REQUIRE(result.size() == 27);
    }
  }
}

TEST_CASE("to_iso8601_utc cross-platform consistency", "[unit][chrono_utils]")
{
  SECTION("gmtime conversion produces UTC")
  {
    std::time_t time = 1609459200; // 2021-01-01 00:00:00 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    // Should always be UTC regardless of local timezone
    REQUIRE(result == "2021-01-01T00:00:00.000000Z");
  }

  SECTION("midnight times are correctly formatted")
  {
    std::time_t time = 1704067200; // 2024-01-01 00:00:00 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.substr(11, 8) == "00:00:00");
  }

  SECTION("end of day times are correctly formatted")
  {
    std::time_t time = 1704153599; // 2023-12-31 23:59:59 UTC
    std::int64_t micros = 999999;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.substr(11, 8) == "23:59:59");
    REQUIRE(result.substr(20, 6) == "999999");
  }
}

TEST_CASE("to_iso8601_utc edge cases for month and day", "[unit][chrono_utils]")
{
  SECTION("february 29 on leap year")
  {
    std::time_t time = 1709164800; // 2024-02-29 00:00:00 UTC (leap year)
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result == "2024-02-29T00:00:00.000000Z");
  }

  SECTION("january first")
  {
    std::time_t time = 1704067200; // 2024-01-01 00:00:00 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.substr(5, 5) == "01-01");
  }

  SECTION("december 31st")
  {
    std::time_t time = 1735689599; // 2024-12-31 23:59:59 UTC
    std::int64_t micros = 0;
    auto result = to_iso8601_utc(time, micros);
    REQUIRE(result.substr(5, 5) == "12-31");
  }

  SECTION("single digit months are zero-padded")
  {
    std::time_t time = 1704067200; // 2024-01-01
    auto result = to_iso8601_utc(time, 0);
    REQUIRE(result[5] == '0');
    REQUIRE(result[6] == '1');
  }

  SECTION("single digit days are zero-padded")
  {
    std::time_t time = 1704067200; // 2024-01-01
    auto result = to_iso8601_utc(time, 0);
    REQUIRE(result[8] == '0');
    REQUIRE(result[9] == '1');
  }
}

TEST_CASE("to_iso8601_utc with chrono time_point edge cases", "[unit][chrono_utils]")
{
  SECTION("time_point with only nanosecond precision below microsecond threshold")
  {

    auto duration =
      std::chrono::seconds(1000000000) + std::chrono::nanoseconds(999); // Less than 1 microsecond
    auto time_point = std::chrono::system_clock::time_point{
      std::chrono::duration_cast<std::chrono::system_clock::duration>(duration)
    };

    auto result = to_iso8601_utc(time_point);
    // Should truncate to 0 microseconds
    REQUIRE(result == "2001-09-09T01:46:40.000000Z");
  }

  SECTION("time_point constructed from duration_cast")
  {
    auto millis = std::chrono::milliseconds(1000000000000LL);
    auto time_point = std::chrono::system_clock::time_point{
      std::chrono::duration_cast<std::chrono::system_clock::duration>(millis)
    };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result.back() == 'Z');
    REQUIRE(result[10] == 'T');
  }

  SECTION("time_point with mixed duration units")
  {
    auto duration = std::chrono::hours(24) + std::chrono::minutes(60) + std::chrono::seconds(60) +
                    std::chrono::microseconds(500000);
    auto time_point = std::chrono::system_clock::time_point{ duration };
    auto result = to_iso8601_utc(time_point);
    // 24h + 1h + 1m + 0.5s = 25:01:00.500000
    REQUIRE(result.size() == 27);
    REQUIRE(result.substr(19, 8) == ".500000Z");
  }

  SECTION("subtraction of seconds leaves exact microsecond remainder")
  {
    auto total_duration = std::chrono::microseconds(1000000123456LL); // 1M seconds + 123456 µs
    auto time_point = std::chrono::system_clock::time_point{ total_duration };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result.substr(20, 6) == "123456");
  }
}

TEST_CASE("to_iso8601_utc microsecond arithmetic validation", "[unit][chrono_utils]")
{
  SECTION("duration subtraction preserves microsecond precision")
  {
    // Verify that (duration - seconds) correctly isolates microseconds
    auto time_point = std::chrono::system_clock::time_point{ std::chrono::seconds(100) +
                                                             std::chrono::microseconds(999999) };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result.substr(20, 6) == "999999");
  }

  SECTION("zero microseconds after second boundary")
  {
    auto time_point = std::chrono::system_clock::time_point{
      std::chrono::seconds(12345) // Exactly on second boundary
    };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result.substr(20, 6) == "000000");
  }

  SECTION("half second represented as microseconds")
  {
    auto time_point = std::chrono::system_clock::time_point{ std::chrono::seconds(100) +
                                                             std::chrono::microseconds(500000) };
    auto result = to_iso8601_utc(time_point);
    REQUIRE(result.substr(20, 6) == "500000");
  }
}

// NOLINTEND(bugprone-chained-comparison, misc-use-anonymous-namespace)
