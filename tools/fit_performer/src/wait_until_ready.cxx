/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "wait_until_ready.hxx"

#include <couchbase/bucket.hxx>
#include <couchbase/endpoint_ping_report.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/ping_options.hxx>
#include <couchbase/ping_result.hxx>

#include "core/cluster.hxx"
#include "core/topology/configuration.hxx"

#include <algorithm>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <utility>

namespace fit_cxx::wait_until_ready
{
namespace
{
// Polling cadence.  Note the SDK refreshes the cluster map on its own interval
// (default 2.5s), so a freshly created bucket's vbucket map will not update
// faster than that regardless; a 100ms poll just keeps latency low once it does.
constexpr std::chrono::milliseconds poll_interval{ 100 };

// Evaluate the desired-state predicate against a ping report.
//
// online   : every requested service has at least one endpoint and ALL of them ok.
// degraded : every requested service has at least one endpoint that is ok.
//
// When the caller did not name any services we consider whatever the report
// contains; an empty report (nothing pinged back yet) is treated as not-ready.
auto
ping_predicate_satisfied(const couchbase::ping_result& report,
                         cluster_state state,
                         const std::set<couchbase::service_type>& requested) -> bool
{
  const auto& endpoints = report.endpoints();

  std::set<couchbase::service_type> services = requested;
  if (services.empty()) {
    for (const auto& [service, reports] : endpoints) {
      services.insert(service);
    }
    if (services.empty()) {
      return false;
    }
  }

  for (const auto service : services) {
    const auto it = endpoints.find(service);
    if (it == endpoints.end() || it->second.empty()) {
      // A requested service has no endpoint yet -> not ready, for both online and
      // degraded (we cannot claim even a degraded service is up with no endpoint).
      return false;
    }
    const auto& reports = it->second;
    const auto is_ok = [](const couchbase::endpoint_ping_report& r) {
      return r.state() == couchbase::ping_state::ok;
    };
    const bool ok = (state == cluster_state::online)
                      ? std::all_of(reports.begin(), reports.end(), is_ok)
                      : std::any_of(reports.begin(), reports.end(), is_ok);
    if (!ok) {
      return false;
    }
  }
  return true;
}

// Fetch the bucket's current topology configuration, waiting no longer than
// the deadline.  with_bucket_configuration() defers its handler until the
// bucket is configured, so an unbounded wait here could hang past opts.timeout
// when the config never arrives (e.g. a stalled open).  On timeout we report
// "not ready" (nullptr) and let the caller's deadline check surface the
// unambiguous_timeout.
auto
fetch_bucket_configuration(const couchbase::cluster& cluster,
                           const std::string& bucket_name,
                           std::chrono::steady_clock::time_point deadline)
  -> std::shared_ptr<couchbase::core::topology::configuration>
{
  auto core = couchbase::core::get_core_cluster(cluster);
  auto barrier = std::make_shared<std::promise<
    std::pair<std::error_code, std::shared_ptr<couchbase::core::topology::configuration>>>>();
  auto future = barrier->get_future();
  core.with_bucket_configuration(
    bucket_name,
    [barrier](std::error_code ec,
              std::shared_ptr<couchbase::core::topology::configuration> config) mutable {
      barrier->set_value({ ec, std::move(config) });
    });
  if (future.wait_until(deadline) != std::future_status::ready) {
    return nullptr;
  }
  auto [ec, config] = future.get();
  if (ec) {
    return nullptr;
  }
  return config;
}

// True once every vbucket has its active and all replica copies assigned to a
// node.  A freshly created bucket reports an empty/partial vbucket map (or
// replica slots set to -1) until the server finishes placing replicas; durable
// (MAJORITY) writes are ambiguous until then.
auto
bucket_kv_ready(const couchbase::cluster& cluster,
                const std::string& bucket_name,
                std::chrono::steady_clock::time_point deadline) -> bool
{
  const auto config = fetch_bucket_configuration(cluster, bucket_name, deadline);
  if (!config || !config->vbmap.has_value()) {
    return false;
  }
  const auto& vbmap = config->vbmap.value();
  if (vbmap.empty()) {
    return false;
  }
  const auto copies = config->num_replicas.value_or(0) + 1; // active + replicas
  for (const auto& chain : vbmap) {
    if (chain.size() < copies) {
      return false;
    }
    for (std::size_t i = 0; i < copies; ++i) {
      if (chain[i] < 0) { // -1 => copy not yet assigned to a node
        return false;
      }
    }
  }
  return true;
}

// Bound each ping to the time left before the deadline.  ping() blocks on its
// future, so without a per-operation timeout a single slow ping could run well
// past opts.timeout; capping it at the remaining budget keeps the overall wait
// bounded.
auto
make_ping_options(const options& opts, std::chrono::milliseconds remaining)
  -> couchbase::ping_options
{
  couchbase::ping_options ping_opts{};
  ping_opts.timeout(remaining);
  if (!opts.service_types.empty()) {
    ping_opts.service_types(opts.service_types);
  }
  return ping_opts;
}

// Time left until the deadline, never negative.
auto
remaining_budget(std::chrono::steady_clock::time_point deadline) -> std::chrono::milliseconds
{
  const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
    deadline - std::chrono::steady_clock::now());
  return std::max(left, std::chrono::milliseconds::zero());
}
} // namespace

auto
wait_until_ready(const couchbase::cluster& cluster, const options& opts) -> std::error_code
{
  if (opts.desired_state == cluster_state::offline) {
    return couchbase::errc::common::invalid_argument;
  }
  const auto deadline = std::chrono::steady_clock::now() + opts.timeout;
  for (;;) {
    const auto remaining = remaining_budget(deadline);
    if (remaining == std::chrono::milliseconds::zero()) {
      return couchbase::errc::common::unambiguous_timeout;
    }
    auto [err, report] = cluster.ping(make_ping_options(opts, remaining)).get();
    if (!err.ec() && ping_predicate_satisfied(report, opts.desired_state, opts.service_types)) {
      return {};
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return couchbase::errc::common::unambiguous_timeout;
    }
    std::this_thread::sleep_for(poll_interval);
  }
}

auto
wait_until_ready(const couchbase::cluster& cluster,
                 const std::string& bucket_name,
                 const options& opts) -> std::error_code
{
  if (opts.desired_state == cluster_state::offline) {
    return couchbase::errc::common::invalid_argument;
  }
  const auto deadline = std::chrono::steady_clock::now() + opts.timeout;
  // Opening the bucket is what makes its configuration (and therefore the
  // vbucket map we poll below) available on the core cluster.
  auto bucket = cluster.bucket(bucket_name);
  for (;;) {
    const auto remaining = remaining_budget(deadline);
    if (remaining == std::chrono::milliseconds::zero()) {
      return couchbase::errc::common::unambiguous_timeout;
    }
    // KV readiness (replica placement) first -- it is the condition durable
    // writes need and the one a fresh bucket fails; then confirm service health.
    if (bucket_kv_ready(cluster, bucket_name, deadline)) {
      auto [err, report] = bucket.ping(make_ping_options(opts, remaining)).get();
      if (!err.ec() && ping_predicate_satisfied(report, opts.desired_state, opts.service_types)) {
        return {};
      }
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return couchbase::errc::common::unambiguous_timeout;
    }
    std::this_thread::sleep_for(poll_interval);
  }
}

} // namespace fit_cxx::wait_until_ready
