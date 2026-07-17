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

#include <cstddef>
#include <vector>

using couchbase::core::app_telemetry_counter;
using couchbase::core::app_telemetry_meter;
using couchbase::core::app_telemetry_recorder_cache;

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
