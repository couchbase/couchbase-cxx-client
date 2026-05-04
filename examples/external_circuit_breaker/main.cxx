/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include "circuit_breaker.hxx"
#include "demo_admin.hxx"
#include "demo_failed_probe.hxx"
#include "demo_healthy_traffic.hxx"
#include "demo_per_node_isolation.hxx"
#include "demo_slow_calls.hxx"
#include "demo_state_transitions.hxx"
#include "demo_topology_sweep.hxx"
#include "ui.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/logger.hxx>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>

namespace
{

constexpr auto default_connection_string{ "couchbase://127.0.0.1" };
constexpr auto default_username{ "Administrator" };
constexpr auto default_password{ "password" };
constexpr auto default_bucket_name{ "default" };

auto
safe_getenv(const std::string& name) noexcept -> std::optional<std::string>
{
  if (name.empty()) {
    return std::nullopt;
  }
#if defined(_WIN32)
  char* buf = nullptr;
  size_t len = 0;
  if (_dupenv_s(&buf, &len, name.c_str()) == 0 && buf != nullptr) {
    std::string value(buf);
    free(buf);
    if (!value.empty()) {
      return value;
    }
  }
  return std::nullopt;
#else
  if (const char* val = std::getenv(name.c_str())) { // NOLINT(concurrency-mt-unsafe)
    if (val[0] != '\0') {
      return std::string(val);
    }
  }
  return std::nullopt;
#endif
}

auto
env_or(const char* var, const char* fallback) -> std::string
{
  return safe_getenv(var).value_or(fallback);
}

} // namespace

auto
main() -> int
{
  example::ui::style::initialize();

  auto conn_str = env_or("CB_CONNECTION_STRING", default_connection_string);
  auto username = env_or("CB_USERNAME", default_username);
  auto password = env_or("CB_PASSWORD", default_password);
  auto bucket = env_or("CB_BUCKET", default_bucket_name);

  couchbase::logger::initialize_console_logger();
  couchbase::logger::set_level(couchbase::logger::log_level::warn);

  auto options = couchbase::cluster_options(username, password);
  options.apply_profile("wan_development");

  auto [connect_err, cluster] = couchbase::cluster::connect(conn_str, options).get();
  if (connect_err) {
    std::cerr << "connect failed: " << connect_err.ec().message() << "\n";
    return 1;
  }
  auto collection = cluster.bucket(bucket).default_collection();

  example::cb::circuit_breaker_config config;
  config.with_failure_rate_threshold_percent(50)
    .with_slow_call_rate_threshold_percent(80)
    .with_slow_call_duration_threshold(std::chrono::milliseconds{ 2000 })
    .with_minimum_number_of_calls(20)
    .with_sliding_window(std::chrono::seconds{ 10 })
    .with_number_of_buckets(10)
    .with_wait_duration_in_open_state(std::chrono::seconds{ 5 })
    .with_permitted_calls_in_half_open_state(3);

  example::cb::circuit_breaker breaker(config);
  breaker.set_transition_callback([](example::cb::circuit_state from,
                                     example::cb::circuit_state to,
                                     const std::string& why) -> void {
    example::ui::print_transition(from, to, why);
  });

  example::ui::print_intro(
    "External Circuit Breaker — Couchbase C++ SDK",
    "Seven demos: healthy traffic, state transitions, failed probe, operator overrides, "
    "slow-call tripping, per-node isolation, topology sweep.");
  example::ui::print_legend();

  example::demo::demo_healthy_traffic(collection, breaker);

  // Discover a real node_id from the cluster to use as the victim in the
  // failure-injection demos.
  auto [nid_err, victim] = collection.node_id_for("cb-demo-0").get();
  if (nid_err) {
    example::ui::print_error("failed to resolve a victim node: " +
                             std::string{ nid_err.ec().message() });
  }

  example::demo::demo_state_transitions(breaker, victim);
  example::demo::demo_failed_probe(breaker, victim);
  example::demo::demo_admin(breaker, victim);
  example::demo::demo_slow_calls(breaker, victim);
  example::demo::demo_per_node_isolation(collection, breaker);
  example::demo::demo_topology_sweep(collection, breaker);

  std::cout << "\n"
            << example::ui::paint("All demos complete.", example::ui::style::bold) << "\n\n";

  cluster.close().get();
  return 0;
}
