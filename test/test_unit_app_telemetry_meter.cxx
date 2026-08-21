/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2024-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "test_helper.hxx"

#include "core/app_telemetry_meter.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

using couchbase::core::app_telemetry_counter;
using couchbase::core::app_telemetry_latency;
using couchbase::core::app_telemetry_meter;
using couchbase::core::app_telemetry_recorder_cache;

namespace
{
auto
generate_report_text(app_telemetry_meter& meter) -> std::string
{
  std::vector<std::byte> buffer{};
  meter.generate_report(buffer);
  return { reinterpret_cast<const char*>(buffer.data()), buffer.size() };
}

/**
 * Value of the named series in a report, e.g. "sdk_query_duration_milliseconds_sum". Matches at the
 * start of a line so that a series name which is a suffix of another cannot be picked up.
 */
auto
series_value(const std::string& report, const std::string& series) -> std::optional<std::uint64_t>
{
  std::istringstream lines{ report };
  for (std::string line; std::getline(lines, line);) {
    if (line.rfind(series + "{", 0) == 0) {
      return std::stoull(line.substr(line.find("} ") + 2));
    }
  }
  return {};
}
} // namespace

TEST_CASE("unit: app_telemetry_recorder_cache resolves once and stays valid within a generation",
          "[unit]")
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto generation = meter.generation();
  const auto recorder = cache.value_recorder(meter, "node-1", "bucket-1");
  REQUIRE(recorder != nullptr);

  // valid for the same key and generation -> no re-resolution
  REQUIRE(cache.is_valid_for(generation, "node-1", "bucket-1"));
  // invalidated by a different node, bucket, or generation
  REQUIRE_FALSE(cache.is_valid_for(generation, "node-2", "bucket-1"));
  REQUIRE_FALSE(cache.is_valid_for(generation, "node-1", "bucket-2"));
  REQUIRE_FALSE(cache.is_valid_for(generation + 1, "node-1", "bucket-1"));

  // a second resolve for the same key returns the same recorder
  REQUIRE(cache.value_recorder(meter, "node-1", "bucket-1") == recorder);
}

TEST_CASE("unit: app_telemetry_recorder_cache is never valid before first use", "[unit]")
{
  const app_telemetry_recorder_cache cache;
  REQUIRE_FALSE(cache.is_valid_for(0, "node-1", "bucket-1"));
}

TEST_CASE("unit: app_telemetry_recorder_cache re-resolves after a report swaps recorders", "[unit]")
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto recorder = cache.value_recorder(meter, "node-1", "bucket-1");
  recorder->update_counter(app_telemetry_counter::kv_r_total);

  // generate_report replaces the underlying recorders and bumps the generation, so the cached
  // recorder is now stale and must not be reused.
  std::vector<std::byte> buffer{};
  meter.generate_report(buffer);

  REQUIRE(cache.value_recorder(meter, "node-1", "bucket-1") != recorder);
}

TEST_CASE("unit: app_telemetry_recorder_cache re-resolves when the key changes", "[unit]")
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto node1 = cache.value_recorder(meter, "node-1", "bucket-1");
  // A different node (or bucket) within the same generation must resolve a distinct recorder rather
  // than return the cached one.
  const auto node2 = cache.value_recorder(meter, "node-2", "bucket-1");
  REQUIRE(node2 != node1);

  // Switching back re-resolves again (the single slot now holds node-2's recorder).
  REQUIRE(cache.value_recorder(meter, "node-1", "bucket-1") != node2);
}

TEST_CASE("unit: app_telemetry_meter reports histogram sums in milliseconds", "[unit]")
{
  struct sample {
    app_telemetry_latency latency;
    const char* metric;
    std::uint64_t millis;
  };

  // One observation per histogram, each a distinct duration so that a sample routed to the wrong
  // family shows up in both series checked below. 250ms and 1500ms straddle a second: reporting the
  // sum in seconds would give 0 and 1.
  const std::vector<sample> samples{
    { app_telemetry_latency::kv_retrieval, "sdk_kv_retrieval_duration_milliseconds", 3 },
    { app_telemetry_latency::kv_mutation_nondurable,
      "sdk_kv_mutation_nondurable_duration_milliseconds",
      7 },
    { app_telemetry_latency::kv_mutation_durable,
      "sdk_kv_mutation_durable_duration_milliseconds",
      40 },
    { app_telemetry_latency::query, "sdk_query_duration_milliseconds", 1500 },
    { app_telemetry_latency::search, "sdk_search_duration_milliseconds", 250 },
    { app_telemetry_latency::analytics, "sdk_analytics_duration_milliseconds", 30000 },
    { app_telemetry_latency::management, "sdk_management_duration_milliseconds", 120 },
    { app_telemetry_latency::eventing, "sdk_eventing_duration_milliseconds", 900 },
  };

  app_telemetry_meter meter;
  const auto recorder = meter.value_recorder("node-1", "bucket-1");
  for (const auto& s : samples) {
    recorder->record_latency(s.latency, std::chrono::milliseconds{ s.millis });
  }

  const auto report = generate_report_text(meter);

  for (const auto& s : samples) {
    const std::string metric{ s.metric };
    CAPTURE(metric);
    const auto sum = series_value(report, metric + "_sum");
    const auto count = series_value(report, metric + "_count");
    REQUIRE(sum.has_value());
    REQUIRE(count.has_value());
    REQUIRE(*sum == s.millis);
    REQUIRE(*count == std::uint64_t{ 1 });
  }
}
