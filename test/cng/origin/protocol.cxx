/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Unit tests for origin::uses_protostellar() (CXXCBC-888): the protocol marker parsed from
// the connection string must reach the origin and survive copy/move. Pure construction, no
// server (env-agnostic).

#include "framework/test_runner.hxx"

#include "core/cluster_credentials.hxx"
#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <string>
#include <utility>

namespace couchbase::cng::test
{
namespace
{
using ::couchbase::core::cluster_credentials;
using ::couchbase::core::origin;
using ::couchbase::core::utils::parse_connection_string;

void
couchbase2_origin_is_protostellar()
{
  const auto cs = parse_connection_string("couchbase2://localhost");
  assert_false(cs.error.has_value(), "connection string parses");
  const origin o{ cluster_credentials{}, cs };
  assert_true(o.uses_protostellar(), "couchbase2 origin reports protostellar");
}

void
classic_origin_is_not_protostellar()
{
  const auto cs = parse_connection_string("couchbase://localhost");
  assert_false(cs.error.has_value(), "connection string parses");
  const origin o{ cluster_credentials{}, cs };
  assert_false(o.uses_protostellar(), "couchbase origin does not report protostellar");
}

void
protocol_survives_copy_and_move()
{
  const auto cs = parse_connection_string("couchbase2://localhost");
  assert_false(cs.error.has_value(), "connection string parses");
  origin o{ cluster_credentials{}, cs };

  const origin copied{ o };
  assert_true(copied.uses_protostellar(), "copy retains the protocol marker");

  const origin moved{ std::move(o) };
  assert_true(moved.uses_protostellar(), "move retains the protocol marker");

  // Copy-assignment must carry every observable field: a reassigned origin cannot report one
  // protocol while still holding another origin's connection string.
  origin assigned{ cluster_credentials{}, parse_connection_string("couchbase://elsewhere") };
  assigned = moved;
  assert_true(assigned.uses_protostellar(), "copy-assignment retains the protocol marker");
  assert_eq(assigned.connection_string(),
            std::string{ "couchbase2://localhost" },
            "copy-assignment copies the connection string");

  // Move-assignment is exercised too: it is the one special member the marker assertions above
  // would otherwise miss.
  origin move_target{ cluster_credentials{}, parse_connection_string("couchbase://elsewhere") };
  move_target = origin{ cluster_credentials{}, parse_connection_string("couchbase2://localhost") };
  assert_true(move_target.uses_protostellar(), "move-assignment retains the protocol marker");
  assert_eq(move_target.connection_string(),
            std::string{ "couchbase2://localhost" },
            "move-assignment carries the connection string");
}

// Rotation state (next_node_ / exhausted_) is deliberately NOT inherited by a copy, while a move
// does carry it. The asymmetry is load-bearing rather than incidental: next_address() marks an
// origin exhausted as it hands out the last node, so a single-node cluster origin is exhausted
// straight after bootstrap; open_bucket() copies it into every session, and
// mcbp_session::initiate_bootstrap() tests exhausted() before its first next_address(). Carrying
// the flag therefore costs an unconditional 500 ms backoff per session. Pinned here so the
// semantics cannot regress silently.
void
rotation_state_resets_on_copy_and_travels_on_move()
{
  auto cs = parse_connection_string("couchbase://a.example.com,b.example.com");
  assert_false(cs.error.has_value(), "connection string parses");
  // The constructor shuffles the node list unless told not to; fixing the order lets the cursor be
  // asserted positionally rather than only through exhausted().
  cs.options.preserve_bootstrap_nodes_order = true;
  const auto first = std::string{ "a.example.com" };
  const auto second = std::string{ "b.example.com" };

  origin o{ cluster_credentials{}, cs };
  assert_false(o.exhausted(), "a fresh origin is not exhausted");
  assert_eq(o.next_address().first, first, "the first node is handed out first");
  assert_false(o.exhausted(), "not exhausted after the first of two nodes");
  assert_eq(o.next_address().first, second, "the second node is handed out next");
  assert_true(o.exhausted(), "exhausted once the last node has been handed out");

  origin copied{ o };
  assert_false(copied.exhausted(), "a copy has attempted nothing, so it starts fresh");
  assert_eq(copied.next_address().first, first, "and its cursor restarts at the first node");

  origin copy_assigned{ cluster_credentials{}, cs };
  assert_eq(
    copy_assigned.next_address().first, first, "cursor advanced before being assigned over");
  copy_assigned = o;
  assert_false(copy_assigned.exhausted(), "copy-assignment starts fresh");
  assert_eq(copy_assigned.next_address().first, first, "and restarts at the first node");

  origin move_source{ cluster_credentials{}, cs };
  assert_eq(move_source.next_address().first, first, "source consumes the first node");
  assert_eq(move_source.next_address().first, second, "source consumes the last node");
  assert_true(move_source.exhausted(), "source is exhausted before being moved");
  const origin moved{ std::move(move_source) };
  assert_true(moved.exhausted(), "a move carries the exhausted flag");

  origin move_assign_source{ cluster_credentials{}, cs };
  assert_eq(move_assign_source.next_address().first, first, "source consumes the first node");
  assert_eq(move_assign_source.next_address().first, second, "source consumes the last node");
  origin move_assign_target{ cluster_credentials{}, cs };
  move_assign_target = std::move(move_assign_source);
  assert_true(move_assign_target.exhausted(), "move-assignment carries the exhausted flag");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "origin_protocol",
    {
      { "couchbase2_origin_is_protostellar", couchbase2_origin_is_protostellar },
      { "classic_origin_is_not_protostellar", classic_origin_is_not_protostellar },
      { "protocol_survives_copy_and_move", protocol_survives_copy_and_move },
      { "rotation_state_resets_on_copy_and_travels_on_move",
        rotation_state_resets_on_copy_and_travels_on_move },
    },
  };
}

} // namespace couchbase::cng::test
