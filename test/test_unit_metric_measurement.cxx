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

#include "core/metric_measurement.hxx"

// NOLINTBEGIN(bugprone-chained-comparison, misc-use-anonymous-namespace)

using namespace couchbase::core;

TEST_CASE("metric_measurement constructors", "[unit][metric_measurement]")
{
  SECTION("construct with double value")
  {
    metric_measurement m("temperature", 23.5);
    REQUIRE(m.is_double());
    REQUIRE(m.as_double() == 23.5);
    REQUIRE_FALSE(m.is_int64());
  }

  SECTION("construct with int64_t value")
  {
    metric_measurement m("count", std::int64_t{ 42 });
    REQUIRE(m.is_int64());
    REQUIRE(m.as_int64() == 42);
    REQUIRE_FALSE(m.is_double());
  }
}

TEST_CASE("metric_measurement copy operations", "[unit][metric_measurement]")
{
  SECTION("copy constructor with double")
  {
    metric_measurement m1("pressure", 101.325);
    metric_measurement m2(m1); // NOLINT(performance-unnecessary-copy-initialization)
    REQUIRE(m2.is_double());
    REQUIRE(m2.as_double() == 101.325);
  }

  SECTION("copy assignment with int64")
  {
    metric_measurement m1("iterations", std::int64_t{ 1000 });
    metric_measurement m2("dummy", 0.0);
    m2 = m1;
    REQUIRE(m2.is_int64());
    REQUIRE(m2.as_int64() == 1000);
  }
}

TEST_CASE("metric_measurement move operations", "[unit][metric_measurement]")
{
  SECTION("move constructor")
  {
    metric_measurement m1("velocity", 299792458.0);
    metric_measurement m2(std::move(m1));
    REQUIRE(m2.is_double());
    REQUIRE(m2.as_double() == 299792458.0);
  }

  SECTION("move assignment")
  {
    metric_measurement m1("requests", std::int64_t{ 50000 });
    metric_measurement m2("dummy", 0.0);
    m2 = std::move(m1);
    REQUIRE(m2.is_int64());
    REQUIRE(m2.as_int64() == 50000);
  }
}

TEST_CASE("metric_measurement type checking", "[unit][metric_measurement]")
{
  SECTION("is_double returns correct value")
  {
    metric_measurement double_metric("ratio", 1.618);
    metric_measurement int_metric("size", std::int64_t{ 256 });

    REQUIRE(double_metric.is_double());
    REQUIRE_FALSE(int_metric.is_double());
  }

  SECTION("is_int64 returns correct value")
  {
    metric_measurement double_metric("pi", 3.14159);
    metric_measurement int_metric("timeout", std::int64_t{ 5000 });

    REQUIRE_FALSE(double_metric.is_int64());
    REQUIRE(int_metric.is_int64());
  }
}

TEST_CASE("metric_measurement value retrieval", "[unit][metric_measurement]")
{
  SECTION("as_double returns correct value")
  {
    metric_measurement m("humidity", 68.5);
    REQUIRE(m.as_double() == 68.5);
  }

  SECTION("as_int64 returns correct value")
  {
    metric_measurement m("errors", std::int64_t{ 7 });
    REQUIRE(m.as_int64() == 7);
  }

  SECTION("as_double throws on wrong type")
  {
    metric_measurement m("count", std::int64_t{ 100 });
    REQUIRE_THROWS_AS(m.as_double(), std::bad_variant_access);
  }

  SECTION("as_int64 throws on wrong type")
  {
    metric_measurement m("rate", 9.81);
    REQUIRE_THROWS_AS(m.as_int64(), std::bad_variant_access);
  }
}

TEST_CASE("metric_measurement try_as methods", "[unit][metric_measurement]")
{
  SECTION("try_as_double returns value when double")
  {
    metric_measurement m("latitude", 37.7749);
    auto result = std::move(m).try_as_double();
    REQUIRE(result.has_value());
    REQUIRE(*result == 37.7749);
  }

  SECTION("try_as_double returns nullopt when int64")
  {
    metric_measurement m("port", std::int64_t{ 8080 });
    auto result = std::move(m).try_as_double();
    REQUIRE_FALSE(result.has_value());
  }

  SECTION("try_as_int64 returns value when int64")
  {
    metric_measurement m("connections", std::int64_t{ 42 });
    auto result = std::move(m).try_as_int64();
    REQUIRE(result.has_value());
    REQUIRE(*result == 42);
  }

  SECTION("try_as_int64 returns nullopt when double")
  {
    metric_measurement m("voltage", 3.3);
    auto result = std::move(m).try_as_int64();
    REQUIRE_FALSE(result.has_value());
  }
}

TEST_CASE("metric_measurement explicit conversions", "[unit][metric_measurement]")
{
  SECTION("explicit cast to double")
  {
    metric_measurement m("frequency", 440.0);
    auto value = static_cast<double>(m);
    REQUIRE(value == 440.0);
  }

  SECTION("explicit cast to int64_t")
  {
    metric_measurement m("buffer_size", std::int64_t{ 4096 });
    auto value = static_cast<std::int64_t>(m);
    REQUIRE(value == 4096);
  }

  SECTION("explicit cast to double throws on wrong type")
  {
    metric_measurement m("retry_count", std::int64_t{ 3 });
    REQUIRE_THROWS_AS(static_cast<double>(m), std::bad_variant_access);
  }
}

TEST_CASE("metric_measurement equality operator", "[unit][metric_measurement]")
{
  SECTION("equal double measurements")
  {
    metric_measurement m1("cpu_usage", 75.5);
    metric_measurement m2("cpu_usage", 75.5);
    REQUIRE(m1 == m2);
  }

  SECTION("equal int64 measurements")
  {
    metric_measurement m1("packets", std::int64_t{ 1024 });
    metric_measurement m2("packets", std::int64_t{ 1024 });
    REQUIRE(m1 == m2);
  }

  SECTION("different names are not equal")
  {
    metric_measurement m1("metric_a", 100.0);
    metric_measurement m2("metric_b", 100.0);
    REQUIRE_FALSE(m1 == m2);
  }

  SECTION("different double values are not equal")
  {
    metric_measurement m1("latency", 10.5);
    metric_measurement m2("latency", 10.6);
    REQUIRE_FALSE(m1 == m2);
  }

  SECTION("different int64 values are not equal")
  {
    metric_measurement m1("requests", std::int64_t{ 500 });
    metric_measurement m2("requests", std::int64_t{ 501 });
    REQUIRE_FALSE(m1 == m2);
  }

  SECTION("different types are not equal")
  {
    metric_measurement m1("value", 42.0);
    metric_measurement m2("value", std::int64_t{ 42 });
    REQUIRE_FALSE(m1 == m2);
  }
}

TEST_CASE("metric_measurement edge cases", "[unit][metric_measurement]")
{
  SECTION("zero values")
  {
    metric_measurement double_zero("dbl", 0.0);
    metric_measurement int_zero("int", std::int64_t{ 0 });

    REQUIRE(double_zero.as_double() == 0.0);
    REQUIRE(int_zero.as_int64() == 0);
  }

  SECTION("negative values")
  {
    metric_measurement double_neg("temperature", -273.15);
    metric_measurement int_neg("offset", std::int64_t{ -100 });

    REQUIRE(double_neg.as_double() == -273.15);
    REQUIRE(int_neg.as_int64() == -100);
  }

  SECTION("large values")
  {
    metric_measurement large_int("timestamp", std::int64_t{ 1729468800 });
    REQUIRE(large_int.as_int64() == 1729468800);
  }

  SECTION("empty name")
  {
    metric_measurement m("", 42.0);
    metric_measurement m2("", 42.0);
    REQUIRE(m == m2);
  }
}
// NOLINTEND(bugprone-chained-comparison, misc-use-anonymous-namespace)
