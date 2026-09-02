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

#include "core/document_id.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/read_preference.hxx>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::read_preference;
using couchbase::core::topology::configuration;
using vbucket_map = configuration::vbucket_map;

auto
config_with_vbmap(std::optional<vbucket_map> vbmap,
                  std::optional<std::uint32_t> num_replicas,
                  std::size_t number_of_nodes = 0) -> configuration
{
  configuration config{};
  config.vbmap = std::move(vbmap);
  config.num_replicas = num_replicas;
  for (std::size_t index = 0; index < number_of_nodes; ++index) {
    configuration::node node{};
    node.index = index;
    config.nodes.emplace_back(node);
  }
  return config;
}

void
an_absent_vbucket_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(std::nullopt, 1);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "there is no map to look in");
}

void
a_vbucket_beyond_the_map_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(1, 0).has_value(), "the map holds one row");
}

void
a_chain_index_equal_to_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 2).has_value(), "the chain holds two entries");
}

void
a_chain_index_beyond_the_chain_length_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
  assert_false(config.server_by_vbucket(0, 42).has_value(), "the chain holds two entries");
}

void
an_empty_chain_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ {} }, 0);
  assert_false(config.server_by_vbucket(0, 0).has_value(), "the row lists no node at all");
}

void
an_unassigned_chain_slot_names_no_server([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 0, -1 } }, 1);
  assert_false(config.server_by_vbucket(0, 1).has_value(), "-1 marks the slot unassigned");
}

void
an_assigned_chain_slot_names_its_node_index([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{ { 3, 7 } }, 1);
  assert_true(config.server_by_vbucket(0, 0) == 3, "the active copy's node");
  assert_true(config.server_by_vbucket(0, 1) == 7, "the first replica's node");
}

void
replica_reads_stop_at_the_end_of_a_short_vbucket_row([[maybe_unused]] context& ctx)
{
  // A configuration may advertise more replicas than its vbucket-map rows
  // list: the two fields are parsed independently. Callers derive the replica
  // index from num_replicas, so the surplus indexes must not be read.
  const auto config =
    std::make_shared<configuration>(config_with_vbmap(vbucket_map{ { 0, 1 } }, 2, 3));
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };

  const auto nodes = couchbase::core::impl::effective_nodes(
    id, config, read_preference::no_preference, /* preferred_server_group */ "");

  assert_eq(nodes.size(), std::size_t{ 2 }, "one node per entry the row actually lists");
  assert_false(nodes[0].is_replica, "the first entry is the active copy");
  assert_eq(nodes[0].index, std::size_t{ 0 }, "the active copy's node");
  assert_true(nodes[1].is_replica, "the second entry is a replica");
  assert_eq(nodes[1].index, std::size_t{ 1 }, "the first replica's node");
}

void
a_vbucket_map_without_rows_maps_no_key([[maybe_unused]] context& ctx)
{
  // The vbucket count comes from the map itself, so a map present but without
  // rows must not be asked which vbucket a key belongs to.
  const auto config = config_with_vbmap(vbucket_map{}, 1);

  const auto [vbucket, server] = config.map_key("foo", 0);
  assert_eq(vbucket, 0, "no row can be chosen");
  assert_false(server.has_value(), "no row means no server");

  const std::vector<std::byte> binary_key{ std::byte{ 'f' }, std::byte{ 'o' }, std::byte{ 'o' } };
  const auto [binary_vbucket, binary_server] = config.map_key(binary_key, 0);
  assert_eq(binary_vbucket, 0, "no row can be chosen");
  assert_false(binary_server.has_value(), "no row means no server");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_absent_vbucket_map_names_no_server), {}, timeout::instant },
      { CASE(a_vbucket_beyond_the_map_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_equal_to_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(a_chain_index_beyond_the_chain_length_names_no_server), {}, timeout::instant },
      { CASE(an_empty_chain_names_no_server), {}, timeout::instant },
      { CASE(an_unassigned_chain_slot_names_no_server), {}, timeout::instant },
      { CASE(an_assigned_chain_slot_names_its_node_index), {}, timeout::instant },
      { CASE(replica_reads_stop_at_the_end_of_a_short_vbucket_row), {}, timeout::instant },
      { CASE(a_vbucket_map_without_rows_maps_no_key), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
