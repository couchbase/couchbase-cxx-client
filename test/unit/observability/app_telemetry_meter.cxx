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

#include "framework/test_registry.hxx"

// The per-sample messages below name the metric family, which no operand can say; the framework
// headers deliberately carry no formatting library.
#include <spdlog/fmt/fmt.h>

#include "core/app_telemetry_meter.hxx"
#include "core/topology/configuration.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::app_telemetry_counter;
using couchbase::core::app_telemetry_latency;
using couchbase::core::app_telemetry_meter;
using couchbase::core::app_telemetry_recorder_cache;

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

void
recorder_cache_resolves_once_and_stays_valid_within_a_generation([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto generation = meter.generation();
  const auto recorder = cache.value_recorder(meter, "node-1", "bucket-1");
  assert_true(recorder != nullptr, "the first resolve yields a recorder");

  assert_true(cache.is_valid_for(generation, "node-1", "bucket-1"),
              "the same key in the same generation needs no re-resolution");
  assert_false(cache.is_valid_for(generation, "node-2", "bucket-1"), "a different node");
  assert_false(cache.is_valid_for(generation, "node-1", "bucket-2"), "a different bucket");
  assert_false(cache.is_valid_for(generation + 1, "node-1", "bucket-1"), "a later generation");

  assert_true(cache.value_recorder(meter, "node-1", "bucket-1") == recorder,
              "a second resolve of the same key returns the cached recorder");
}

void
recorder_cache_is_never_valid_before_first_use([[maybe_unused]] context& ctx)
{
  const app_telemetry_recorder_cache cache;
  assert_false(cache.is_valid_for(0, "node-1", "bucket-1"),
               "an unused cache holds no recorder to reuse");
}

void
recorder_cache_re_resolves_after_a_report_swaps_recorders([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto recorder = cache.value_recorder(meter, "node-1", "bucket-1");
  recorder->update_counter(app_telemetry_counter::kv_r_total);

  // generate_report replaces the underlying recorders and bumps the generation, so the cached
  // recorder is now stale and must not be reused.
  std::vector<std::byte> buffer{};
  meter.generate_report(buffer);

  assert_true(cache.value_recorder(meter, "node-1", "bucket-1") != recorder,
              "a recorder from a superseded generation is not handed out again");
}

void
recorder_cache_re_resolves_when_the_key_changes([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  app_telemetry_recorder_cache cache;

  const auto node1 = cache.value_recorder(meter, "node-1", "bucket-1");
  const auto node2 = cache.value_recorder(meter, "node-2", "bucket-1");
  assert_true(node2 != node1, "a different node within one generation resolves its own recorder");

  // The single slot now holds node-2's recorder.
  assert_true(cache.value_recorder(meter, "node-1", "bucket-1") != node2,
              "switching back re-resolves rather than returning the cached recorder");
}

void
histogram_sums_are_reported_in_milliseconds([[maybe_unused]] context& ctx)
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
    const auto sum = series_value(report, metric + "_sum");
    const auto count = series_value(report, metric + "_count");
    assert_true(sum.has_value(), fmt::format("{} reports a sum", metric));
    assert_true(count.has_value(), fmt::format("{} reports a count", metric));
    assert_eq(*sum, s.millis, fmt::format("{} sums the observation in milliseconds", metric));
    assert_eq(*count, std::uint64_t{ 1 }, fmt::format("{} counts one observation", metric));
  }
}
void
label_values_are_escaped([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.node_uuid = "u\"1";
  node.hostname = "h\n1";
  couchbase::core::topology::configuration::alternate_address alt{};
  alt.hostname = R"(a\1)";
  node.alt.emplace("external", alt);
  config.nodes.push_back(node);
  meter.update_config(config);

  const auto recorder = meter.value_recorder(node.node_uuid, "b\"1");
  recorder->update_counter(app_telemetry_counter::kv_r_total);
  recorder->record_latency(app_telemetry_latency::kv_retrieval, std::chrono::milliseconds{ 5 });

  std::istringstream lines{ generate_report_text(meter) };
  std::size_t count = 0;
  for (std::string line; std::getline(lines, line); ++count) {
    assert_true(line.rfind("sdk_", 0) == 0, fmt::format("a metric line: {}", line));
    assert_true(line.find(R"(node_uuid="u\"1",node="h\n1",alt_node="a\\1",bucket="b\"1")") !=
                  std::string::npos,
                fmt::format("escaped labels: {}", line));
  }
  assert_true(count > 0, "the report has metric lines");
}

void
config_and_agent_updates_do_not_race_report_generation([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  couchbase::core::topology::configuration config{};
  for (std::size_t i = 0; i < 8; ++i) {
    couchbase::core::topology::configuration::node node{};
    node.node_uuid = fmt::format("uuid-{}", i);
    node.hostname = fmt::format("host-{}", i);
    config.nodes.push_back(std::move(node));
  }

  std::vector<std::thread> threads;
  for (std::size_t t = 0; t < 4; ++t) {
    threads.emplace_back([&meter, &config, t]() {
      for (std::size_t i = 0; i < 200; ++i) {
        meter.update_config(config);
        meter.update_agent(fmt::format("app-{}", t));
        meter.value_recorder(fmt::format("uuid-{}", i % 8), "default")
          ->update_counter(app_telemetry_counter::kv_r_total);
        std::vector<std::byte> buffer{};
        meter.generate_report(buffer);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(recorder_cache_resolves_once_and_stays_valid_within_a_generation) },
      { CASE(recorder_cache_is_never_valid_before_first_use) },
      { CASE(recorder_cache_re_resolves_after_a_report_swaps_recorders) },
      { CASE(recorder_cache_re_resolves_when_the_key_changes) },
      { CASE(histogram_sums_are_reported_in_milliseconds) },
      { CASE(label_values_are_escaped) },
      { CASE(config_and_agent_updates_do_not_race_report_generation) },
    },
  };
}

} // namespace couchbase::test
