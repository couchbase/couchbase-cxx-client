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

#include "test_helper_integration.hxx"

#include "core/range_scan_load_balancer.hxx"
#include "core/topology/configuration.hxx"

TEST_CASE("unit: range scan load balancer", "[unit]")
{
  // Create a vbucket map with 6 vbuckets distributed evenly across 3 nodes
  std::vector<std::int16_t> vbucket_nodes{ 0, 0, 1, 1, 2, 2 };
  couchbase::core::range_scan_load_balancer balancer{ {
    { 0 },
    { 0 },
    { 1 },
    { 1 },
    { 2 },
    { 2 },
  } };

  SECTION("selecting three vbuckets gives one from each node")
  {
    std::set<std::uint16_t> selection{};
    for (auto i = 0; i < 3; i++) {
      auto v = balancer.select_vbucket();

      REQUIRE(v.has_value());

      auto [_, inserted] = selection.insert(v.value());
      REQUIRE(inserted);
    }

    std::set<std::int16_t> nodes{};
    for (auto vid : selection) {
      auto [_, inserted] = nodes.insert(vbucket_nodes[vid]);
      REQUIRE(inserted);
    }
  }

  SECTION("selecting a vbucket returns the one from the least busy node")
  {
    // Select three vbuckets, verify that they come from three different nodes and tell the balancer
    // that in one node the stream has ended
    std::set<std::int16_t> nodes{};
    for (auto i = 0; i < 3; i++) {
      auto v = balancer.select_vbucket();

      REQUIRE(v.has_value());

      auto [_, inserted] = nodes.insert(vbucket_nodes[v.value()]);
      REQUIRE(inserted);
    }

    balancer.notify_stream_ended(0);

    // Verify that the next selected vbucket will be for node 0 as the other two have an in-progress
    // stream
    auto v = balancer.select_vbucket();

    REQUIRE(v.has_value());
    REQUIRE(vbucket_nodes[v.value()] == 0);
  }

  SECTION("selecting six vbuckets returns all of them exactly once")
  {
    std::set<std::uint16_t> selection{};
    for (auto i = 0; i < 6; i++) {
      auto v = balancer.select_vbucket();

      REQUIRE(v.has_value());

      auto [_, inserted] = selection.insert(v.value());
      REQUIRE(inserted);
    }
  }

  SECTION("when there are no more vbuckets, select_vbucket() returns std::nullopt")
  {
    std::set<std::uint16_t> selection{};
    for (auto i = 0; i < 6; i++) {
      auto v = balancer.select_vbucket();

      REQUIRE(v.has_value());

      auto [_, inserted] = selection.insert(v.value());
      REQUIRE(inserted);
    }

    REQUIRE_FALSE(balancer.select_vbucket().has_value());
  }
}
