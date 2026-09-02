/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "core/range_scan_load_balancer.hxx"
#include "core/topology/configuration.hxx"

#include <cstdint>
#include <set>
#include <vector>

namespace couchbase::test
{
namespace
{
// Six vbuckets spread evenly over three nodes, indexed by vbucket id.
const std::vector<std::int16_t> vbucket_nodes{ 0, 0, 1, 1, 2, 2 };

auto
even_vbucket_map() -> couchbase::core::topology::configuration::vbucket_map
{
  return {
    { 0 }, { 0 }, { 1 }, { 1 }, { 2 }, { 2 },
  };
}

// Draw `count` vbuckets, requiring each to be selected exactly once.
auto
select_distinct(couchbase::core::range_scan_load_balancer& balancer, int count)
  -> std::set<std::uint16_t>
{
  std::set<std::uint16_t> selection{};
  for (auto i = 0; i < count; i++) {
    auto v = balancer.select_vbucket();
    assert_true(v.has_value(), "a vbucket is still available");

    auto [_, inserted] = selection.insert(v.value());
    assert_true(inserted, "the vbucket has not been handed out before");
  }
  return selection;
}

void
selecting_three_vbuckets_gives_one_from_each_node([[maybe_unused]] context& ctx)
{
  couchbase::core::range_scan_load_balancer balancer{ even_vbucket_map() };

  std::set<std::int16_t> nodes{};
  for (auto vid : select_distinct(balancer, 3)) {
    auto [_, inserted] = nodes.insert(vbucket_nodes[vid]);
    assert_true(inserted, "the node has not been drawn from before");
  }
}

void
a_vbucket_is_selected_from_the_least_busy_node([[maybe_unused]] context& ctx)
{
  couchbase::core::range_scan_load_balancer balancer{ even_vbucket_map() };

  std::set<std::int16_t> nodes{};
  for (auto vid : select_distinct(balancer, 3)) {
    auto [_, inserted] = nodes.insert(vbucket_nodes[vid]);
    assert_true(inserted, "the node has not been drawn from before");
  }

  balancer.notify_stream_ended(0);

  // Node 0 is now the only one without an in-progress stream.
  auto v = balancer.select_vbucket();
  assert_true(v.has_value(), "a vbucket is still available");
  assert_eq(vbucket_nodes[v.value()], std::int16_t{ 0 }, "the node with no stream in progress");
}

void
selecting_six_vbuckets_returns_all_of_them_exactly_once([[maybe_unused]] context& ctx)
{
  couchbase::core::range_scan_load_balancer balancer{ even_vbucket_map() };

  static_cast<void>(select_distinct(balancer, 6));
}

void
an_exhausted_balancer_selects_nothing([[maybe_unused]] context& ctx)
{
  couchbase::core::range_scan_load_balancer balancer{ even_vbucket_map() };

  static_cast<void>(select_distinct(balancer, 6));

  assert_false(balancer.select_vbucket().has_value(), "nothing is left to hand out");
}

void
a_vbucket_with_no_active_copy_is_left_out([[maybe_unused]] context& ctx)
{
  // A row that names no node at all is left out rather than read past its end,
  // and no node is invented to hold it. range_scan_orchestrator refuses a map
  // in this state, so the vbucket cannot go missing from a scan that runs.
  couchbase::core::range_scan_load_balancer balancer{ {
    { 0 },
    {},
    { 1 },
  } };

  assert_true(select_distinct(balancer, 2) == std::set<std::uint16_t>{ 0, 2 },
              "the two vbuckets with an active copy, and only those");
  assert_false(balancer.select_vbucket().has_value(), "nothing is left to hand out");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(selecting_three_vbuckets_gives_one_from_each_node), {}, timeout::instant },
      { CASE(a_vbucket_is_selected_from_the_least_busy_node), {}, timeout::instant },
      { CASE(selecting_six_vbuckets_returns_all_of_them_exactly_once), {}, timeout::instant },
      { CASE(an_exhausted_balancer_selects_nothing), {}, timeout::instant },
      { CASE(a_vbucket_with_no_active_copy_is_left_out), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
