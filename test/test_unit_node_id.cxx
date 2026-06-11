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

TEST_CASE("unit: node_id fallback hash is stable across runs (pinned)", "[unit]")
{
  // Pin the fallback CRC32 output for a known input. The contract is "stable
  // across runs and platforms" so a future change to the hash recipe (algorithm
  // swap, byte order, masking) must be a deliberate, breaking decision and
  // these assertions are how we notice.
  REQUIRE(couchbase::internal_node_id::build("", "172.18.0.2", 11210).id() == "00006091");
  REQUIRE(couchbase::internal_node_id::build("", "172.18.0.3", 11210).id() == "000046e6");
  REQUIRE(couchbase::internal_node_id::build("", "172.18.0.2", 11207).id() == "000067ee");
  REQUIRE(couchbase::internal_node_id::build("", "localhost", 11210).id() == "00001d1f");
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

TEST_CASE("unit: node_id falls back to empty when neither uuid nor host+port is usable", "[unit]")
{
  // The "falsy = unknown" contract: an unidentifiable node should produce a
  // falsy node_id, not a truthy-but-meaningless one. This is the guard that
  // keeps request- and response-side identity construction in agreement.
  REQUIRE_FALSE(static_cast<bool>(couchbase::internal_node_id::build("", "", 0)));
  REQUIRE_FALSE(static_cast<bool>(couchbase::internal_node_id::build("", "host", 0)));
  REQUIRE_FALSE(static_cast<bool>(couchbase::internal_node_id::build("", "", 11210)));

  // But a non-empty uuid is sufficient on its own.
  REQUIRE(static_cast<bool>(couchbase::internal_node_id::build("uuid-only", "", 0)));
}

TEST_CASE("unit: node_id with IPv6 literal hostname", "[unit]")
{
  auto nid = couchbase::internal_node_id::build("", "[::1]", 11210);
  REQUIRE(static_cast<bool>(nid));
  REQUIRE(nid.hostname() == "[::1]");
  REQUIRE(nid.id() == "000049ef");
}

TEST_CASE("unit: node_id symmetric inequality", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  REQUIRE(nid1 != nid2);
  REQUIRE(nid2 != nid1);
}

TEST_CASE("unit: node_id strict weak ordering", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  auto nid_eq = couchbase::internal_node_id::build("aaa", "h2", 9999);

  // Strict: a < b implies !(b < a)
  REQUIRE(nid1 < nid2);
  REQUIRE_FALSE(nid2 < nid1);

  // Irreflexive: !(a < a)
  REQUIRE_FALSE(nid1 < nid1);

  // Equality (by id_): neither side is less than the other
  REQUIRE_FALSE(nid1 < nid_eq);
  REQUIRE_FALSE(nid_eq < nid1);
}

TEST_CASE("unit: node_id full comparison set", "[unit]")
{
  auto nid_small = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid_large = couchbase::internal_node_id::build("bbb", "h", 1);
  auto nid_equal = couchbase::internal_node_id::build("aaa", "other", 9);

  REQUIRE(nid_small < nid_large);
  REQUIRE(nid_small <= nid_large);
  REQUIRE(nid_small <= nid_equal);
  REQUIRE(nid_large > nid_small);
  REQUIRE(nid_large >= nid_small);
  REQUIRE(nid_equal >= nid_small);

  REQUIRE_FALSE(nid_small > nid_equal);
  REQUIRE_FALSE(nid_small < nid_equal);
}

TEST_CASE("unit: std::hash<node_id> is consistent with equality", "[unit]")
{
  auto nid1 = couchbase::internal_node_id::build("same-uuid", "host-a", 11210);
  auto nid2 = couchbase::internal_node_id::build("same-uuid", "host-b", 11207);
  REQUIRE(nid1 == nid2);
  REQUIRE(std::hash<couchbase::node_id>{}(nid1) == std::hash<couchbase::node_id>{}(nid2));

  // Fallback variant
  auto nid3 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid4 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  REQUIRE(nid3 == nid4);
  REQUIRE(std::hash<couchbase::node_id>{}(nid3) == std::hash<couchbase::node_id>{}(nid4));
}

TEST_CASE("unit: node_id fallback hash collision sanity sweep", "[unit]")
{
  // Generate node_ids over a small grid of hostnames and ports and assert that
  // every distinct (host, port) maps to a distinct id. This is not a rigorous
  // collision proof — CRC32 is not a cryptographic hash — but it catches the
  // case where a future "optimization" accidentally truncates too aggressively
  // and starts colliding cheaply.
  std::unordered_set<std::string> ids;
  for (int host_octet = 1; host_octet <= 25; ++host_octet) {
    for (std::uint16_t port : { std::uint16_t{ 11207 }, std::uint16_t{ 11210 } }) {
      auto host = "10.0.0." + std::to_string(host_octet);
      auto nid = couchbase::internal_node_id::build("", host, port);
      auto inserted = ids.insert(nid.id()).second;
      INFO("host=" << host << " port=" << port << " id=" << nid.id());
      REQUIRE(inserted);
    }
  }
  REQUIRE(ids.size() == 50);
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
