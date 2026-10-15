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
#include "utils/topology_fixtures.hxx"

#include "core/cluster.hxx"
#include "core/diagnostics.hxx"
#include "core/impl/wait_until_ready.hxx"
#include "core/origin.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/connection_string.hxx"

#include <couchbase/cluster_state.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/logger.hxx>

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::cluster_state;
using couchbase::core::service_type;
using couchbase::core::impl::ping_predicate_satisfied;
using couchbase::core::impl::placeable_replicas;
using couchbase::core::impl::unplaceable_replicas;
using couchbase::core::impl::vbucket_map_ready;
using couchbase::core::impl::warn_if_replicas_unplaceable;
namespace diag = couchbase::core::diag;
using ::test::utils::config_with_vbmap;
using ::test::utils::vbucket_map;

// Builds data_nodes nodes with a KV port, then other_nodes nodes with only a query port.
auto
config_with_data_nodes(std::optional<vbucket_map> vbmap,
                       std::optional<std::uint32_t> num_replicas,
                       std::size_t data_nodes,
                       std::size_t other_nodes = 0) -> couchbase::core::topology::configuration
{
  auto config = config_with_vbmap(std::move(vbmap), num_replicas, data_nodes + other_nodes);
  for (std::size_t index = 0; index < data_nodes; ++index) {
    config.nodes[index].services_plain.key_value = 11210;
  }
  for (std::size_t index = data_nodes; index < config.nodes.size(); ++index) {
    config.nodes[index].services_plain.query = 8093;
  }
  return config;
}

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
  assert_false(vbucket_map_ready(config_with_data_nodes(vbucket_map{ { 0 } }, 1, 2)),
               "the replica has nowhere to live yet");
}

void
an_unassigned_replica_slot_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_data_nodes(vbucket_map{ { 0, -1 } }, 1, 2)),
               "-1 marks the replica slot unplaced");
}

void
a_fully_placed_active_and_replica_is_ready([[maybe_unused]] context& ctx)
{
  assert_true(vbucket_map_ready(config_with_data_nodes(vbucket_map{ { 0, 1 }, { 1, 0 } }, 1, 2)),
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

void
a_single_data_node_waits_only_for_the_active([[maybe_unused]] context& ctx)
{
  // One node cannot hold a replica of its own active, so the server leaves the slot at -1.
  assert_true(vbucket_map_ready(config_with_data_nodes(vbucket_map{ { 0, -1 }, { 0, -1 } }, 1, 1)),
              "a replica with no node to live on is not awaited");
}

void
replicas_beyond_the_data_nodes_are_not_awaited([[maybe_unused]] context& ctx)
{
  assert_true(vbucket_map_ready(
                config_with_data_nodes(vbucket_map{ { 0, 1, -1, -1 }, { 1, 0, -1, -1 } }, 3, 2)),
              "two data nodes hold the active and one replica");
}

void
a_placeable_replica_still_unassigned_is_not_ready([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_data_nodes(vbucket_map{ { 0, -1, -1, -1 } }, 3, 2)),
               "the second data node can hold the first replica, so it is still awaited");
}

void
placeable_replicas_counts_only_data_nodes([[maybe_unused]] context& ctx)
{
  assert_eq(placeable_replicas(config_with_data_nodes(vbucket_map{ { 0, -1 } }, 1, 1, 2)),
            std::uint32_t{ 0 },
            "nodes without a KV port hold no vbucket copy");
  assert_eq(placeable_replicas(config_with_data_nodes(vbucket_map{ { 0, 1 } }, 1, 3)),
            std::uint32_t{ 1 },
            "num_replicas caps the count when there are enough data nodes");
  assert_eq(placeable_replicas(config_with_data_nodes(vbucket_map{ { 0, 1 } }, std::nullopt, 3)),
            std::uint32_t{ 0 },
            "a configuration without a replica count has none to place");
}

void
replicas_beyond_the_data_nodes_are_counted([[maybe_unused]] context& ctx)
{
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0, -1 } }, 1, 1)),
            std::uint32_t{ 1 },
            "one data node cannot hold a replica");
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0, 1, -1, -1 } }, 3, 2)),
            std::uint32_t{ 2 },
            "two data nodes hold one of three replicas");
}

void
placeable_replicas_are_not_counted([[maybe_unused]] context& ctx)
{
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0, 1 } }, 1, 2)),
            std::uint32_t{ 0 },
            "two data nodes hold one replica");
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0 } }, 0, 1)),
            std::uint32_t{ 0 },
            "a bucket without replicas places all of them");
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0, 1 } }, 1, 3)),
            std::uint32_t{ 0 },
            "data nodes beyond num_replicas + 1 do not wrap the count");
  assert_eq(unplaceable_replicas(config_with_data_nodes(vbucket_map{ { 0 } }, std::nullopt, 3)),
            std::uint32_t{ 0 },
            "a configuration without a replica count does not wrap the count");
  assert_eq(unplaceable_replicas(config_with_vbmap(vbucket_map{ { 0, -1 } }, 1)),
            std::uint32_t{ 0 },
            "a configuration listing no data node is not capped");
}

// The callback is process-wide and holds a reference into the case, so it is removed however the
// case leaves.
class captured_warnings
{
public:
  captured_warnings()
  {
    couchbase::logger::register_log_callback([this](std::string_view msg,
                                                    couchbase::logger::log_level level,
                                                    couchbase::logger::log_location) {
      if (level == couchbase::logger::log_level::warn) {
        messages.emplace_back(msg);
      }
    });
  }

  captured_warnings(const captured_warnings&) = delete;
  captured_warnings(captured_warnings&&) = delete;
  auto operator=(const captured_warnings&) -> captured_warnings& = delete;
  auto operator=(captured_warnings&&) -> captured_warnings& = delete;

  ~captured_warnings()
  {
    couchbase::logger::unregister_log_callback();
  }

  std::vector<std::string> messages{};
};

void
unplaceable_replicas_are_warned_about_once_per_wait([[maybe_unused]] context& ctx)
{
  const captured_warnings warnings;
  std::atomic_bool logged{ false };
  const auto config = config_with_data_nodes(vbucket_map{ { 0, -1 } }, 1, 1);

  warn_if_replicas_unplaceable(config, "travel-sample", logged);
  warn_if_replicas_unplaceable(config, "travel-sample", logged);

  assert_eq(warnings.messages.size(), std::size_t{ 1 }, "a repeated poll does not warn again");
  assert_contains(warnings.messages[0],
                  "bucket \"travel-sample\" is configured with 1 replica(s), but 1 data node(s) "
                  "can hold only 0 replica(s)",
                  "the warning names the bucket and the counts");
}

void
placeable_or_uncapped_replicas_are_not_warned_about([[maybe_unused]] context& ctx)
{
  const captured_warnings warnings;
  std::atomic_bool logged{ false };

  warn_if_replicas_unplaceable(config_with_data_nodes(vbucket_map{ { 0, 1 } }, 1, 2), "b", logged);
  warn_if_replicas_unplaceable(config_with_vbmap(vbucket_map{ { 0, -1 } }, 1), "b", logged);

  assert_eq(warnings.messages.size(), std::size_t{ 0 }, "no replica is unplaceable");
  assert_false(logged.load(), "a later unplaceable configuration still warns");
}

void
a_configuration_without_data_nodes_is_not_capped([[maybe_unused]] context& ctx)
{
  assert_false(vbucket_map_ready(config_with_vbmap(vbucket_map{ { 0, -1 } }, 1)),
               "with no node list to cap against, every replica is awaited");
}

void
a_tls_only_kv_port_marks_a_data_node([[maybe_unused]] context& ctx)
{
  auto config = config_with_vbmap(vbucket_map{ { 0, 1, -1, -1 } }, 3, 2);
  for (auto& node : config.nodes) {
    node.services_tls.key_value = 11207;
  }
  assert_eq(placeable_replicas(config),
            std::uint32_t{ 1 },
            "two TLS-only data nodes hold one of three replicas");
}

// A core cluster open against an address with no listener, so its bootstrap keeps failing. close()
// stops it the way the public cluster's close() does: close the core, then stop the io_context and
// join its thread. A continuation still queued after that never runs.
class closable_core
{
public:
  closable_core()
  {
    thread_ = std::thread([this]() {
      io_.run();
    });
    couchbase::core::cluster_credentials credentials;
    credentials.username = "Administrator";
    credentials.password = "password";
    core_.open(
      couchbase::core::origin{
        credentials, couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1:1") },
      [](std::error_code) {
      });
  }

  closable_core(const closable_core&) = delete;
  closable_core(closable_core&&) = delete;
  auto operator=(const closable_core&) -> closable_core& = delete;
  auto operator=(closable_core&&) -> closable_core& = delete;

  ~closable_core()
  {
    close();
  }

  [[nodiscard]] auto core() const -> const couchbase::core::cluster&
  {
    return core_;
  }

  [[nodiscard]] auto io() -> asio::io_context&
  {
    return io_;
  }

  void close()
  {
    if (closed_) {
      return;
    }
    closed_ = true;
    std::promise<void> stopped;
    core_.close([&stopped]() {
      stopped.set_value();
    });
    stopped.get_future().get();
    io_.stop();
    thread_.join();
  }

private:
  asio::io_context io_{};
  couchbase::core::cluster core_{ io_ };
  std::thread thread_{};
  bool closed_{ false };
};

auto
start_wait(const couchbase::core::cluster& core,
           std::optional<std::string> bucket_name,
           std::chrono::milliseconds timeout,
           cluster_state desired_state = cluster_state::online) -> std::future<std::error_code>
{
  auto done = std::make_shared<std::promise<std::error_code>>();
  auto future = done->get_future();
  couchbase::core::impl::wait_until_ready(
    core, std::move(bucket_name), timeout, desired_state, {}, [done](std::error_code ec) {
      done->set_value(ec);
    });
  return future;
}

void
assert_cluster_closed(std::future<std::error_code>& future, const std::string& what)
{
  assert_true(future.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
              what + " completes while the cluster object is alive");
  const auto ec = future.get();
  assert_true(ec == couchbase::errc::network::cluster_closed,
              what + " is cluster_closed: " + ec.message());
}

// Returns once every handler posted to `io` before the call has run. A classic wait has already
// armed its anchor timer inline, before wait_until_ready() returned; this confirms the io thread
// is running.
void
run_posted_handlers(asio::io_context& io)
{
  auto ran = std::make_shared<std::promise<void>>();
  asio::post(io, [ran]() {
    ran->set_value();
  });
  assert_true(ran->get_future().wait_for(std::chrono::seconds(5)) == std::future_status::ready,
              "the io thread runs posted handlers");
}

void
assert_pending(std::future<std::error_code>& future, const std::string& what)
{
  assert_true(future.wait_for(std::chrono::seconds(0)) == std::future_status::timeout,
              what + " is pending before close");
}

// Regression: close() stopped the io_context with a timer or posted continuation of the wait
// queued, so the wait did not complete.
void
a_cluster_wait_pending_at_close_completes_with_cluster_closed([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  auto wait = start_wait(cluster.core(), std::nullopt, std::chrono::seconds(30));
  run_posted_handlers(cluster.io());
  assert_pending(wait, "a cluster wait");
  cluster.close();
  assert_cluster_closed(wait, "a cluster wait pending at close");
}

// Regression: as above, for a wait that opens the bucket first.
void
a_bucket_wait_pending_at_close_completes_with_cluster_closed([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  auto wait = start_wait(cluster.core(), "default", std::chrono::seconds(30));
  run_posted_handlers(cluster.io());
  assert_pending(wait, "a bucket wait");
  cluster.close();
  assert_cluster_closed(wait, "a bucket wait pending at close");
}

// Regression: a wait completed by close() kept its handler, and the handler's captures, alive for
// as long as a continuation of the wait stayed queued on the stopped io_context.
void
a_wait_completed_on_close_releases_its_handler([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  auto sentinel = std::make_shared<int>(0);
  const std::weak_ptr<int> watched = sentinel;
  auto done = std::make_shared<std::promise<std::error_code>>();
  auto wait = done->get_future();
  couchbase::core::impl::wait_until_ready(
    cluster.core(),
    std::nullopt,
    std::chrono::seconds(30),
    cluster_state::online,
    {},
    [done, sentinel = std::move(sentinel)](std::error_code ec) {
      done->set_value(ec);
    });
  run_posted_handlers(cluster.io());
  cluster.close();

  assert_cluster_closed(wait, "a wait pending at close");
  assert_true(watched.expired(), "the handler's captures are released once it has run");
}

// Regression: the retry timer of a wait started after close() landed on the stopped io_context.
void
a_cluster_wait_after_close_completes_with_cluster_closed([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  cluster.close();
  auto wait = start_wait(cluster.core(), std::nullopt, std::chrono::seconds(30));
  assert_cluster_closed(wait, "a cluster wait after close");
}

// Guards the bucket wait after close(), which open_bucket() already answered with cluster_closed.
void
a_bucket_wait_after_close_completes_with_cluster_closed([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  cluster.close();
  auto wait = start_wait(cluster.core(), "default", std::chrono::seconds(30));
  assert_cluster_closed(wait, "a bucket wait after close");
}

// Regression: the offline rejection was posted to the stopped io_context.
void
an_offline_wait_after_close_completes_with_cluster_closed([[maybe_unused]] context& ctx)
{
  closable_core cluster;
  cluster.close();
  auto wait =
    start_wait(cluster.core(), std::nullopt, std::chrono::seconds(30), cluster_state::offline);
  assert_cluster_closed(wait, "an offline wait after close");
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
      { CASE(a_single_data_node_waits_only_for_the_active), {}, timeout::instant },
      { CASE(replicas_beyond_the_data_nodes_are_not_awaited), {}, timeout::instant },
      { CASE(a_placeable_replica_still_unassigned_is_not_ready), {}, timeout::instant },
      { CASE(placeable_replicas_counts_only_data_nodes), {}, timeout::instant },
      { CASE(replicas_beyond_the_data_nodes_are_counted), {}, timeout::instant },
      { CASE(placeable_replicas_are_not_counted), {}, timeout::instant },
      { CASE(unplaceable_replicas_are_warned_about_once_per_wait), {}, timeout::instant },
      { CASE(placeable_or_uncapped_replicas_are_not_warned_about), {}, timeout::instant },
      { CASE(a_configuration_without_data_nodes_is_not_capped), {}, timeout::instant },
      { CASE(a_tls_only_kv_port_marks_a_data_node), {}, timeout::instant },
      { CASE(a_cluster_wait_pending_at_close_completes_with_cluster_closed), {}, timeout::slow },
      { CASE(a_bucket_wait_pending_at_close_completes_with_cluster_closed), {}, timeout::slow },
      { CASE(a_wait_completed_on_close_releases_its_handler), {}, timeout::slow },
      { CASE(a_cluster_wait_after_close_completes_with_cluster_closed), {}, timeout::slow },
      { CASE(a_bucket_wait_after_close_completes_with_cluster_closed), {}, timeout::slow },
      { CASE(an_offline_wait_after_close_completes_with_cluster_closed), {}, timeout::slow },
    },
  };
}

} // namespace couchbase::test
