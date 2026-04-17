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

#include "test_helper.hxx"

#include "core/impl/node_id.hxx"

#include <couchbase/node_id.hxx>

#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

TEST_CASE("unit: default node_id is falsy", "[unit]")
{
  couchbase::node_id nid;
  REQUIRE_FALSE(static_cast<bool>(nid));
  REQUIRE(nid.id().empty());
  REQUIRE(nid.node_uuid().empty());
  REQUIRE(nid.hostname().empty());
  REQUIRE(nid.port() == 0);
}

TEST_CASE("unit: node_id with node_uuid uses uuid as id", "[unit]")
{
  auto nid = couchbase::internal_node_id::build("abc-123", "172.18.0.2", 11210);
  REQUIRE(static_cast<bool>(nid));
  REQUIRE(nid.id() == "abc-123");
  REQUIRE(nid.node_uuid() == "abc-123");
  REQUIRE(nid.hostname() == "172.18.0.2");
  REQUIRE(nid.port() == 11210);
}

TEST_CASE("unit: node_id without node_uuid falls back to hash", "[unit]")
{
  auto nid = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  REQUIRE(static_cast<bool>(nid));
  // The fallback id should be non-empty and NOT the raw "host:port"
  REQUIRE_FALSE(nid.id().empty());
  REQUIRE(nid.id() != "172.18.0.2:11210");
  REQUIRE(nid.node_uuid().empty());
}

TEST_CASE("unit: node_id fallback hash is deterministic", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid2 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  REQUIRE(nid1.id() == nid2.id());
  REQUIRE(nid1 == nid2);
}

TEST_CASE("unit: node_id different host/port produce different fallback ids", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid2 = couchbase::internal_node_id::build("", "172.18.0.3", 11210);
  auto nid3 = couchbase::internal_node_id::build("", "172.18.0.2", 11207);
  REQUIRE(nid1 != nid2);
  REQUIRE(nid1 != nid3);
  REQUIRE(nid2 != nid3);
}

TEST_CASE("unit: node_id equality compares by id", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("same-uuid", "host-a", 11210);
  auto nid2 = couchbase::internal_node_id::build("same-uuid", "host-b", 11207);
  // Same uuid => same id() => equal, even though host/port differ
  REQUIRE(nid1 == nid2);
}

TEST_CASE("unit: node_id works in ordered containers", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  std::set<couchbase::node_id> s;
  s.insert(nid1);
  s.insert(nid2);
  REQUIRE(s.size() == 2);
  std::map<couchbase::node_id, int> m;
  m[nid1] = 1;
  m[nid2] = 2;
  REQUIRE(m.size() == 2);
}

TEST_CASE("unit: node_id works in unordered containers", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("uuid-1", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("uuid-2", "h", 1);
  std::unordered_set<couchbase::node_id> s;
  s.insert(nid1);
  s.insert(nid2);
  REQUIRE(s.size() == 2);
  std::unordered_map<couchbase::node_id, int> m;
  m[nid1] = 1;
  m[nid2] = 2;
  REQUIRE(m.size() == 2);
}
