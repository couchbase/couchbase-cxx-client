/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "core/diagnostics.hxx"
#include "core/impl/wait_until_ready.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/cluster_state.hxx>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace
{
using couchbase::cluster_state;
using couchbase::core::service_type;
namespace diag = couchbase::core::diag;
using vbucket_map = couchbase::core::topology::configuration::vbucket_map;

auto
endpoint(service_type type, diag::ping_state state) -> diag::endpoint_ping_info
{
  diag::endpoint_ping_info info{};
  info.type = type;
  info.state = state;
  return info;
}

auto
ping_report(std::map<service_type, std::vector<diag::endpoint_ping_info>> services)
  -> diag::ping_result
{
  diag::ping_result report{};
  report.services = std::move(services);
  return report;
}

auto
config_with_vbmap(std::optional<vbucket_map> vbmap, std::optional<std::uint32_t> num_replicas)
  -> couchbase::core::topology::configuration
{
  couchbase::core::topology::configuration config{};
  config.vbmap = std::move(vbmap);
  config.num_replicas = num_replicas;
  return config;
}
} // namespace

TEST_CASE("unit: wait_until_ready ping predicate (online)", "[unit]")
{
  using couchbase::core::impl::ping_predicate_satisfied;

  SECTION("all endpoints of the requested service ok -> ready")
  {
    const auto report = ping_report({
      { service_type::key_value,
        { endpoint(service_type::key_value, diag::ping_state::ok),
          endpoint(service_type::key_value, diag::ping_state::ok) } },
    });
    REQUIRE(ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }));
  }

  SECTION("a single non-ok endpoint fails online")
  {
    const auto report = ping_report({
      { service_type::key_value,
        { endpoint(service_type::key_value, diag::ping_state::ok),
          endpoint(service_type::key_value, diag::ping_state::error) } },
    });
    REQUIRE_FALSE(
      ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }));
  }

  SECTION("requested service absent from the report -> not ready")
  {
    const auto report = ping_report({
      { service_type::query, { endpoint(service_type::query, diag::ping_state::ok) } },
    });
    REQUIRE_FALSE(
      ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }));
  }

  SECTION("requested service present but with no endpoints -> not ready")
  {
    const auto report = ping_report({
      { service_type::key_value, {} },
    });
    REQUIRE_FALSE(
      ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }));
  }

  SECTION("one of several requested services missing -> not ready")
  {
    const auto report = ping_report({
      { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
    });
    REQUIRE_FALSE(ping_predicate_satisfied(
      report, cluster_state::online, { service_type::key_value, service_type::query }));
  }

  SECTION("empty request -> services derived from the report")
  {
    const auto report = ping_report({
      { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
      { service_type::query, { endpoint(service_type::query, diag::ping_state::ok) } },
    });
    REQUIRE(ping_predicate_satisfied(report, cluster_state::online, {}));
  }

  SECTION("empty request and empty report -> not ready")
  {
    REQUIRE_FALSE(ping_predicate_satisfied(ping_report({}), cluster_state::online, {}));
  }

  SECTION("empty request derives from report, so a single non-ok fails")
  {
    const auto report = ping_report({
      { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::timeout) } },
    });
    REQUIRE_FALSE(ping_predicate_satisfied(report, cluster_state::online, {}));
  }
}

TEST_CASE("unit: wait_until_ready ping predicate (degraded)", "[unit]")
{
  using couchbase::core::impl::ping_predicate_satisfied;

  SECTION("at least one ok endpoint -> ready even with failures present")
  {
    const auto report = ping_report({
      { service_type::key_value,
        { endpoint(service_type::key_value, diag::ping_state::error),
          endpoint(service_type::key_value, diag::ping_state::ok) } },
    });
    REQUIRE(ping_predicate_satisfied(report, cluster_state::degraded, { service_type::key_value }));
  }

  SECTION("no ok endpoint -> not ready")
  {
    const auto report = ping_report({
      { service_type::key_value,
        { endpoint(service_type::key_value, diag::ping_state::error),
          endpoint(service_type::key_value, diag::ping_state::timeout) } },
    });
    REQUIRE_FALSE(
      ping_predicate_satisfied(report, cluster_state::degraded, { service_type::key_value }));
  }

  SECTION("one of several requested services entirely missing -> not ready")
  {
    const auto report = ping_report({
      { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
    });
    REQUIRE_FALSE(ping_predicate_satisfied(
      report, cluster_state::degraded, { service_type::key_value, service_type::query }));
  }
}

TEST_CASE("unit: wait_until_ready vbucket map readiness", "[unit]")
{
  using couchbase::core::impl::vbucket_map_ready;

  SECTION("absent vbucket map -> not ready")
  {
    REQUIRE_FALSE(vbucket_map_ready(config_with_vbmap(std::nullopt, 1)));
  }

  SECTION("empty vbucket map -> not ready")
  {
    REQUIRE_FALSE(vbucket_map_ready(config_with_vbmap(vbucket_map{}, 1)));
  }

  SECTION("chain shorter than active+replicas -> not ready")
  {
    // num_replicas = 1 requires 2 copies, but the chain only lists the active.
    REQUIRE_FALSE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 } }, 1)));
  }

  SECTION("unassigned replica slot (-1) -> not ready")
  {
    REQUIRE_FALSE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0, -1 } }, 1)));
  }

  SECTION("fully placed active + replica -> ready")
  {
    REQUIRE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0, 1 }, { 1, 0 } }, 1)));
  }

  SECTION("zero replicas, active assigned -> ready")
  {
    REQUIRE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 }, { 1 } }, 0)));
  }

  SECTION("zero replicas, active unassigned -> not ready")
  {
    REQUIRE_FALSE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { -1 } }, 0)));
  }

  SECTION("missing num_replicas defaults to active-only readiness")
  {
    REQUIRE(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 }, { 1 } }, std::nullopt)));
  }
}
