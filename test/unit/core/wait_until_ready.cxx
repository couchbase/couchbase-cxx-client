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

#include "framework/test_registry.hxx"

#include "core/diagnostics.hxx"
#include "core/impl/wait_until_ready.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/cluster_state.hxx>

#include <cstdint>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::cluster_state;
using couchbase::core::service_type;
using couchbase::core::impl::ping_predicate_satisfied;
using couchbase::core::impl::vbucket_map_ready;
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

void
online_needs_every_endpoint_of_the_requested_service([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value,
      { endpoint(service_type::key_value, diag::ping_state::ok),
        endpoint(service_type::key_value, diag::ping_state::ok) } },
  });
  assert_true(ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }),
              "every endpoint answered");
}

void
online_is_refused_when_one_endpoint_fails([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value,
      { endpoint(service_type::key_value, diag::ping_state::ok),
        endpoint(service_type::key_value, diag::ping_state::error) } },
  });
  assert_false(ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }),
               "one failed endpoint is enough to refuse online");
}

void
online_is_refused_when_the_requested_service_is_absent([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::query, { endpoint(service_type::query, diag::ping_state::ok) } },
  });
  assert_false(ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }),
               "a service the report does not mention is not ready");
}

void
online_is_refused_when_the_requested_service_has_no_endpoints([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value, {} },
  });
  assert_false(ping_predicate_satisfied(report, cluster_state::online, { service_type::key_value }),
               "a service with nothing to ping is not ready");
}

void
online_is_refused_when_one_of_several_requested_services_is_missing([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
  });
  assert_false(ping_predicate_satisfied(
                 report, cluster_state::online, { service_type::key_value, service_type::query }),
               "every requested service has to be present");
}

void
an_empty_request_derives_the_services_from_the_report([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
    { service_type::query, { endpoint(service_type::query, diag::ping_state::ok) } },
  });
  assert_true(ping_predicate_satisfied(report, cluster_state::online, {}),
              "every service the report mentions answered");
}

void
an_empty_request_against_an_empty_report_is_refused([[maybe_unused]] context& ctx)
{
  assert_false(ping_predicate_satisfied(ping_report({}), cluster_state::online, {}),
               "a report naming no service is not ready");
}

void
an_empty_request_is_refused_when_a_derived_endpoint_fails([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::timeout) } },
  });
  assert_false(ping_predicate_satisfied(report, cluster_state::online, {}),
               "the derived service is held to the same bar");
}

void
degraded_needs_one_endpoint_of_the_requested_service([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value,
      { endpoint(service_type::key_value, diag::ping_state::error),
        endpoint(service_type::key_value, diag::ping_state::ok) } },
  });
  assert_true(
    ping_predicate_satisfied(report, cluster_state::degraded, { service_type::key_value }),
    "one endpoint that answered is enough for degraded");
}

void
degraded_is_refused_when_no_endpoint_answers([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value,
      { endpoint(service_type::key_value, diag::ping_state::error),
        endpoint(service_type::key_value, diag::ping_state::timeout) } },
  });
  assert_false(
    ping_predicate_satisfied(report, cluster_state::degraded, { service_type::key_value }),
    "a service nothing answered for is not degraded but absent");
}

void
degraded_is_refused_when_one_of_several_requested_services_is_missing([[maybe_unused]] context& ctx)
{
  const auto report = ping_report({
    { service_type::key_value, { endpoint(service_type::key_value, diag::ping_state::ok) } },
  });
  assert_false(ping_predicate_satisfied(
                 report, cluster_state::degraded, { service_type::key_value, service_type::query }),
               "every requested service has to be present");
}

void
an_absent_vbucket_map_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_vbmap(std::nullopt, 1)), "there is no map to judge");
}

void
an_empty_vbucket_map_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_vbmap(vbucket_map{}, 1)), "the map lists no vbucket");
}

void
a_chain_shorter_than_the_replica_count_is_not_ready([[maybe_unused]] context& ctx)
{
  // num_replicas = 1 requires 2 copies, but the chain only lists the active.
  assert_false(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 } }, 1)),
               "the replica has nowhere to live yet");
}

void
an_unassigned_replica_slot_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0, -1 } }, 1)),
               "-1 marks the replica slot unplaced");
}

void
a_fully_placed_active_and_replica_is_ready([[maybe_unused]] context& ctx)
{
  assert_true(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0, 1 }, { 1, 0 } }, 1)),
              "every copy of every vbucket has a node");
}

void
zero_replicas_with_an_assigned_active_is_ready([[maybe_unused]] context& ctx)
{
  assert_true(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 }, { 1 } }, 0)),
              "an active-only bucket needs only its actives placed");
}

void
zero_replicas_with_an_unassigned_active_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_vbmap(vbucket_map{ { -1 } }, 0)),
               "the active itself is unplaced");
}

void
a_missing_replica_count_needs_only_the_active([[maybe_unused]] context& ctx)
{
  assert_true(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0 }, { 1 } }, std::nullopt)),
              "a configuration that does not say falls back to the active");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(online_needs_every_endpoint_of_the_requested_service), {}, timeout::instant },
      { CASE(online_is_refused_when_one_endpoint_fails), {}, timeout::instant },
      { CASE(online_is_refused_when_the_requested_service_is_absent), {}, timeout::instant },
      { CASE(online_is_refused_when_the_requested_service_has_no_endpoints), {}, timeout::instant },
      { CASE(online_is_refused_when_one_of_several_requested_services_is_missing),
        {},
        timeout::instant },
      { CASE(an_empty_request_derives_the_services_from_the_report), {}, timeout::instant },
      { CASE(an_empty_request_against_an_empty_report_is_refused), {}, timeout::instant },
      { CASE(an_empty_request_is_refused_when_a_derived_endpoint_fails), {}, timeout::instant },
      { CASE(degraded_needs_one_endpoint_of_the_requested_service), {}, timeout::instant },
      { CASE(degraded_is_refused_when_no_endpoint_answers), {}, timeout::instant },
      { CASE(degraded_is_refused_when_one_of_several_requested_services_is_missing),
        {},
        timeout::instant },
      { CASE(an_absent_vbucket_map_is_not_ready), {}, timeout::instant },
      { CASE(an_empty_vbucket_map_is_not_ready), {}, timeout::instant },
      { CASE(a_chain_shorter_than_the_replica_count_is_not_ready), {}, timeout::instant },
      { CASE(an_unassigned_replica_slot_is_not_ready), {}, timeout::instant },
      { CASE(a_fully_placed_active_and_replica_is_ready), {}, timeout::instant },
      { CASE(zero_replicas_with_an_assigned_active_is_ready), {}, timeout::instant },
      { CASE(zero_replicas_with_an_unassigned_active_is_not_ready), {}, timeout::instant },
      { CASE(a_missing_replica_count_needs_only_the_active), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
