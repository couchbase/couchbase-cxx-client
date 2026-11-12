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
#include "core/app_telemetry_reporter.hxx"
#include "core/origin.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"

#include <asio/io_context.hpp>

#include <atomic>
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
report_escapes_special_characters_in_metric_labels([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;

  couchbase::core::topology::configuration config{};
  couchbase::core::topology::configuration::node node{};
  node.node_uuid = "uuid\"1\n\\x";
  node.hostname = "host\"name\n\\1";
  couchbase::core::topology::configuration::alternate_address alt{};
  alt.hostname = "alt\"host\n\\2";
  node.alt.emplace("external", alt);
  config.nodes.push_back(node);
  meter.update_config(config);

  const std::string bucket = "bkt\"name\n\\3";
  const auto recorder = meter.value_recorder(node.node_uuid, bucket);
  recorder->update_counter(app_telemetry_counter::kv_r_total);
  recorder->record_latency(app_telemetry_latency::kv_retrieval, std::chrono::milliseconds{ 5 });
  recorder->record_latency(app_telemetry_latency::kv_mutation_durable,
                           std::chrono::milliseconds{ 15 });
  recorder->record_latency(app_telemetry_latency::query, std::chrono::milliseconds{ 25 });

  const auto report = generate_report_text(meter);
  assert_false(report.empty(), "report is generated");

  std::istringstream lines{ report };
  std::size_t line_count = 0;
  for (std::string line; std::getline(lines, line);) {
    if (line.empty()) {
      continue;
    }
    ++line_count;
    assert_true(line.rfind("sdk_", 0) == 0,
                "every report line starts with an sdk_ metric name without newline splitting");
    assert_true(line.find(R"(node_uuid="uuid\"1\n\\x")") != std::string::npos,
                "node_uuid escapes quotes, newlines, and backslashes");
    assert_true(line.find(R"(node="host\"name\n\\1")") != std::string::npos,
                "node label escapes quotes, newlines, and backslashes");
    assert_true(line.find(R"(alt_node="alt\"host\n\\2")") != std::string::npos,
                "alt_node label escapes quotes, newlines, and backslashes");
    assert_true(line.find(R"(bucket="bkt\"name\n\\3")") != std::string::npos,
                "bucket label escapes quotes, newlines, and backslashes");
  }
  assert_true(line_count > 0, "at least one metric line was verified");
}

void
concurrent_update_config_and_report_generation_is_thread_safe([[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;

  couchbase::core::topology::configuration config{};
  for (std::size_t i = 0; i < 32; ++i) {
    couchbase::core::topology::configuration::node node{};
    node.node_uuid = fmt::format("node-uuid-{}", i);
    node.hostname = fmt::format("host-{}.example.com", i);
    config.nodes.push_back(std::move(node));
  }

  std::atomic_bool start{ false };
  std::vector<std::thread> threads;
  threads.reserve(4);
  for (std::size_t t = 0; t < 4; ++t) {
    threads.emplace_back([&meter, &config, &start, t]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::size_t iter = 0; iter < 200; ++iter) {
        meter.update_config(config);
        meter.update_agent(fmt::format("app-{}", t));
        auto rec = meter.value_recorder(fmt::format("node-uuid-{}", iter % 32), "default");
        rec->update_counter(app_telemetry_counter::kv_r_total);
        if (iter % 25 == 0) {
          std::vector<std::byte> buf{};
          meter.generate_report(buf);
        }
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& th : threads) {
    th.join();
  }

  auto rec = meter.value_recorder("node-uuid-0", "default");
  rec->update_counter(app_telemetry_counter::kv_r_total);
  const auto report = generate_report_text(meter);
  assert_true(report.find(R"(node="host-0.example.com")") != std::string::npos,
              "node labels remain intact after concurrent config updates");
}

void
meter_preserves_labels_across_disable_and_enable_and_skips_empty_node_uuid(
  [[maybe_unused]] context& ctx)
{
  app_telemetry_meter meter;
  meter.disable();

  couchbase::core::topology::configuration stale_config{};
  {
    couchbase::core::topology::configuration::node stale_node{};
    stale_node.node_uuid = "uuid-stale";
    stale_node.hostname = "stale.example.com";
    stale_config.nodes.push_back(std::move(stale_node));
  }
  meter.update_config(stale_config);

  couchbase::core::topology::configuration config{};
  {
    couchbase::core::topology::configuration::node empty_uuid_node{};
    empty_uuid_node.node_uuid = "";
    empty_uuid_node.hostname = "should-not-pollute-empty-uuid.example.com";
    config.nodes.push_back(std::move(empty_uuid_node));
  }
  {
    couchbase::core::topology::configuration::node valid_node{};
    valid_node.node_uuid = "uuid-1";
    valid_node.hostname = "host\r1.example.com";
    config.nodes.push_back(std::move(valid_node));
  }

  meter.update_config(config);
  meter.enable();
  meter.disable();
  meter.enable();
  meter.update_agent("app\tname\r");

  meter.value_recorder("", "default")->update_counter(app_telemetry_counter::kv_r_total);
  meter.value_recorder("uuid-stale", "default")->update_counter(app_telemetry_counter::kv_r_total);
  meter.value_recorder("uuid-1", "default")->update_counter(app_telemetry_counter::kv_r_total);

  const auto report = generate_report_text(meter);
  assert_true(report.find("node=\"host\r1.example.com\"") != std::string::npos,
              "labels updated while disabled are preserved on enable and retain literal CR");
  assert_true(report.find(";app\tname\r)\"") != std::string::npos,
              "agent label retains literal tab and CR without JSON escape sequences");
  assert_true(report.find("stale.example.com") == std::string::npos,
              "disabled meter clears departed node labels on subsequent config updates");
  assert_true(report.find("should-not-pollute-empty-uuid") == std::string::npos,
              "nodes with an empty node_uuid do not attach a node label to unrouted metrics");
}

void
reporter_rejects_wss_override_when_cluster_tls_is_disabled([[maybe_unused]] context& ctx)
{
  asio::io_context io{};
  couchbase::core::tls_context_provider tls{};

  {
    auto meter = std::make_shared<app_telemetry_meter>();
    couchbase::core::origin origin{};
    origin.options().enable_app_telemetry = true;
    origin.options().enable_tls = false;
    origin.options().app_telemetry_endpoint = "wss://collector.example.com";

    couchbase::core::app_telemetry_reporter reporter{ meter, origin, io, tls };
    meter->value_recorder("uuid-1", "default")->update_counter(app_telemetry_counter::kv_r_total);
    assert_true(generate_report_text(*meter).empty(),
                "wss:// override on a non-TLS cluster disables the meter");
    reporter.stop();
  }
  {
    auto meter = std::make_shared<app_telemetry_meter>();
    couchbase::core::origin origin{};
    origin.options().enable_app_telemetry = true;
    origin.options().enable_tls = true;
    origin.options().app_telemetry_endpoint = "ws://collector.example.com";

    couchbase::core::app_telemetry_reporter reporter{ meter, origin, io, tls };
    meter->value_recorder("uuid-1", "default")->update_counter(app_telemetry_counter::kv_r_total);
    assert_false(generate_report_text(*meter).empty(),
                 "ws:// override on a TLS cluster keeps the meter enabled");
    reporter.stop();
  }
  {
    auto meter = std::make_shared<app_telemetry_meter>();
    couchbase::core::origin origin{};
    origin.options().enable_app_telemetry = true;
    origin.options().enable_tls = true;
    origin.options().app_telemetry_endpoint = "wss://collector.example.com";

    couchbase::core::app_telemetry_reporter reporter{ meter, origin, io, tls };
    meter->value_recorder("uuid-1", "default")->update_counter(app_telemetry_counter::kv_r_total);
    assert_false(generate_report_text(*meter).empty(),
                 "wss:// override on a TLS cluster keeps the meter enabled");
    reporter.stop();
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
      { CASE(report_escapes_special_characters_in_metric_labels) },
      { CASE(concurrent_update_config_and_report_generation_is_thread_safe) },
      { CASE(meter_preserves_labels_across_disable_and_enable_and_skips_empty_node_uuid) },
      { CASE(reporter_rejects_wss_override_when_cluster_tls_is_disabled) },
    },
  };
}

} // namespace couchbase::test
