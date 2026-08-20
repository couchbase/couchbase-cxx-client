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
#include "utils/topology_fixtures.hxx"

#include "core/document_id.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/read_preference.hxx>

#include <memory>
#include <optional>
#include <vector>

namespace
{
using couchbase::read_preference;
using couchbase::core::topology::configuration;
using test::utils::config_with_vbmap;
using test::utils::vbucket_map;
} // namespace

TEST_CASE("unit: server_by_vbucket bounds", "[unit]")
{
  SECTION("absent vbucket map")
  {
    REQUIRE_FALSE(config_with_vbmap(std::nullopt, 1).server_by_vbucket(0, 0).has_value());
  }

  SECTION("vbucket beyond the map")
  {
    const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
    REQUIRE_FALSE(config.server_by_vbucket(1, 0).has_value());
  }

  SECTION("index equal to the chain length")
  {
    const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
    REQUIRE_FALSE(config.server_by_vbucket(0, 2).has_value());
  }

  SECTION("index beyond the chain length")
  {
    const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1);
    REQUIRE_FALSE(config.server_by_vbucket(0, 42).has_value());
  }

  SECTION("index zero against an empty chain")
  {
    const auto config = config_with_vbmap(vbucket_map{ {} }, 0);
    REQUIRE_FALSE(config.server_by_vbucket(0, 0).has_value());
  }

  SECTION("unassigned slot (-1) yields no server")
  {
    const auto config = config_with_vbmap(vbucket_map{ { 0, -1 } }, 1);
    REQUIRE_FALSE(config.server_by_vbucket(0, 1).has_value());
  }

  SECTION("assigned slot yields the node index")
  {
    const auto config = config_with_vbmap(vbucket_map{ { 3, 7 } }, 1);
    REQUIRE(config.server_by_vbucket(0, 0) == 3);
    REQUIRE(config.server_by_vbucket(0, 1) == 7);
  }
}

TEST_CASE("unit: replica reads against a chain shorter than num_replicas", "[unit]")
{
  // A configuration may advertise more replicas than its vbucket-map rows
  // list: the two fields are parsed independently. Callers derive the replica
  // index from num_replicas, so the surplus indexes must not be read.
  const auto config =
    std::make_shared<configuration>(config_with_vbmap(vbucket_map{ { 0, 1 } }, 2, 3));
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };

  const auto nodes = couchbase::core::impl::effective_nodes(
    id, config, read_preference::no_preference, /* preferred_server_group */ "");

  REQUIRE(nodes.size() == 2);
  REQUIRE_FALSE(nodes[0].is_replica);
  REQUIRE(nodes[0].index == 0);
  REQUIRE(nodes[1].is_replica);
  REQUIRE(nodes[1].index == 1);
}

TEST_CASE("unit: map_key against a vbucket map without rows", "[unit]")
{
  // The vbucket count comes from the map itself, so a map present but without
  // rows must not be asked which vbucket a key belongs to.
  const auto config = config_with_vbmap(vbucket_map{}, 1);

  const auto [vbucket, server] = config.map_key("foo", 0);
  REQUIRE(vbucket == 0);
  REQUIRE_FALSE(server.has_value());

  const std::vector<std::byte> binary_key{ std::byte{ 'f' }, std::byte{ 'o' }, std::byte{ 'o' } };
  const auto [binary_vbucket, binary_server] = config.map_key(binary_key, 0);
  REQUIRE(binary_vbucket == 0);
  REQUIRE_FALSE(binary_server.has_value());
}
