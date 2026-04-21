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

#include "core/error_context/key_value.hxx"
#include "core/error_context/key_value_error_context.hxx"
#include "core/error_context/subdocument_error_context.hxx"
#include "core/impl/node_id.hxx"
#include "core/topology/configuration.hxx"

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

TEST_CASE("unit: configuration::node effective_node_id with uuid", "[unit]")
{
  couchbase::core::topology::configuration::node n;
  n.node_uuid = "test-uuid-xyz";
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  auto nid = n.effective_node_id(false);
  REQUIRE(static_cast<bool>(nid));
  REQUIRE(nid.id() == "test-uuid-xyz");
  REQUIRE(nid.hostname() == "172.18.0.2");
  REQUIRE(nid.port() == 11210);
}

TEST_CASE("unit: configuration::node effective_node_id without uuid uses KV port", "[unit]")
{
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;
  n.services_plain.management = 8091;
  n.services_tls.management = 18091;

  auto plain = n.effective_node_id(false);
  REQUIRE(static_cast<bool>(plain));
  REQUIRE(plain.port() == 11210);
  // The fallback id must be derived from the KV port that the client
  // actually connects over — not the management port.
  REQUIRE(plain == couchbase::internal_node_id::build("", "172.18.0.2", 11210));
  REQUIRE(plain != couchbase::internal_node_id::build("", "172.18.0.2", 8091));

  auto tls = n.effective_node_id(true);
  REQUIRE(static_cast<bool>(tls));
  REQUIRE(tls.port() == 11207);
  REQUIRE(tls == couchbase::internal_node_id::build("", "172.18.0.2", 11207));
  REQUIRE(tls != couchbase::internal_node_id::build("", "172.18.0.2", 18091));
}

TEST_CASE("unit: configuration::node effective_node_id differs between TLS and plain without uuid",
          "[unit]")
{
  // On pre-8.0 servers (no node_uuid) the plain and TLS variants must produce
  // different fallback ids — they hash different ports — so that a consumer
  // using the plain port on the request side cannot accidentally match an
  // id derived from the TLS port on the response side.
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  REQUIRE(n.effective_node_id(false) != n.effective_node_id(true));
}

TEST_CASE("unit: configuration::node effective_node_id without KV port", "[unit]")
{
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  // no KV port set

  auto nid = n.effective_node_id(false);
  REQUIRE(static_cast<bool>(nid));
  REQUIRE(nid.port() == 0);
}

TEST_CASE("unit: effective_node_id matches session canonicals (response side)", "[unit]")
{
  // mcbp_session populates canonical_hostname_ from node.hostname and
  // canonical_port_number_ from node.port_or(key_value, is_tls_, 0). The
  // error context feeds these into internal_node_id::build. This test
  // locks in that the request-side node_id (via effective_node_id) is
  // bit-for-bit identical to what the response side would produce for the
  // same node and TLS setting — even on servers that do not supply a
  // node_uuid (i.e. when the fallback hash is in play).
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;
  n.services_plain.management = 8091;
  n.services_tls.management = 18091;

  for (bool is_tls : { false, true }) {
    auto request_side = n.effective_node_id(is_tls);
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    REQUIRE(request_side == response_side);
    REQUIRE(request_side.id() == response_side.id());
    REQUIRE(request_side.hostname() == response_side.hostname());
    REQUIRE(request_side.port() == response_side.port());
  }
}

TEST_CASE("unit: effective_node_id matches session canonicals with node_uuid", "[unit]")
{
  // When the server provides a node_uuid (Server 8.0.1+), the id() is the
  // UUID itself — port and hostname differences no longer affect identity,
  // but the request/response sides should still agree on the exposed port.
  couchbase::core::topology::configuration::node n;
  n.node_uuid = "node-uuid-42";
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  for (bool is_tls : { false, true }) {
    auto request_side = n.effective_node_id(is_tls);
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    REQUIRE(request_side == response_side);
    REQUIRE(request_side.port() == response_side.port());
  }
}

TEST_CASE("unit: node_id ignores alternate addresses without node_uuid", "[unit]")
{
  // Mirrors a cluster where each node has a private (default) address used
  // inside the VPC and a public (external/alt) alias used from outside.
  // Regardless of which network the client selects on bootstrap, node_id
  // must be derived from the default-network hostname + KV port — matching
  // what mcbp_session stores in canonical_hostname_ / canonical_port_number_
  // on the response side — so that collection::node_id_for() (request side)
  // and result::node_id() / error::node_id() (response side) compare equal
  // regardless of transport. This is what makes external circuit breakers
  // keyed on node_id work under alt-network deployments.
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  couchbase::core::topology::configuration::alternate_address ext;
  ext.name = "external";
  ext.hostname = "172-18-0-2.my.cloud.com";
  ext.services_plain.key_value = 31100;
  ext.services_tls.key_value = 31207;
  n.alt["external"] = ext;

  for (bool is_tls : { false, true }) {
    auto request_side = n.effective_node_id(is_tls);
    // The accessors expose the default-network identity, not the alt alias.
    REQUIRE(request_side.hostname() == "172.18.0.2");
    REQUIRE(request_side.hostname() != ext.hostname);

    // The response-side formula (mcbp_session feeds canonical_hostname_ and
    // canonical_port_number_ into internal_node_id::build) must produce the
    // same id even when the client connected over the external network.
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    REQUIRE(request_side == response_side);

    // And an id built from the alt hostname/port must be different — the
    // public alias must never influence the node's identity.
    auto alt_port = is_tls ? ext.services_tls.key_value : ext.services_plain.key_value;
    auto alt_side =
      couchbase::internal_node_id::build(n.node_uuid, ext.hostname, alt_port.value_or(0));
    REQUIRE(request_side != alt_side);
  }
}

TEST_CASE("unit: node_id ignores alternate addresses with node_uuid", "[unit]")
{
  // With a server-provided node_uuid the identity is trivially transport-
  // independent (id() is just the uuid), but the hostname() and port()
  // accessors should still report the default-network values so that any
  // consumer surfacing them for logging sees a stable pair.
  couchbase::core::topology::configuration::node n;
  n.node_uuid = "stable-uuid-7";
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  couchbase::core::topology::configuration::alternate_address ext;
  ext.name = "external";
  ext.hostname = "172-18-0-2.my.cloud.com";
  ext.services_plain.key_value = 31100;
  ext.services_tls.key_value = 31207;
  n.alt["external"] = ext;

  auto plain = n.effective_node_id(false);
  REQUIRE(plain.id() == "stable-uuid-7");
  REQUIRE(plain.hostname() == "172.18.0.2");
  REQUIRE(plain.port() == 11210);

  auto tls = n.effective_node_id(true);
  REQUIRE(tls.id() == "stable-uuid-7");
  REQUIRE(tls.hostname() == "172.18.0.2");
  REQUIRE(tls.port() == 11207);
}

TEST_CASE("unit: vbucket map resolves to correct node_id", "[unit]")
{
  couchbase::core::topology::configuration config;
  config.nodes.resize(3);
  config.nodes[0].node_uuid = "node-0";
  config.nodes[0].hostname = "h0";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[1].node_uuid = "node-1";
  config.nodes[1].hostname = "h1";
  config.nodes[1].services_plain.key_value = 11210;
  config.nodes[2].node_uuid = "node-2";
  config.nodes[2].hostname = "h2";
  config.nodes[2].services_plain.key_value = 11210;

  // Create a simple vbucket map: 4 vbuckets, distributed across 3 nodes
  config.vbmap = couchbase::core::topology::configuration::vbucket_map{
    { 0, 1 },
    { 1, 2 },
    { 2, 0 },
    { 0, 2 },
  };

  auto [vb0, idx0] = config.map_key("some-key", 0);
  REQUIRE(idx0.has_value());
  auto nid = config.nodes[idx0.value()].effective_node_id(false);
  REQUIRE(static_cast<bool>(nid));
  // The node_id should be one of our configured nodes
  REQUIRE((nid.id() == "node-0" || nid.id() == "node-1" || nid.id() == "node-2"));
}

TEST_CASE("unit: make_subdocument_error_context propagates node_id", "[unit]")
{
  // make_subdocument_error_context builds a subdocument_error_context from a
  // key_value_error_context. Before CXXCBC-821 the node_id carried on the
  // source ctx was silently dropped, so subdoc results/errors never surfaced
  // it. This locks in that it is forwarded end-to-end.
  auto expected = couchbase::internal_node_id::build("node-xyz", "172.18.0.2", 11210);

  couchbase::core::key_value_error_context kv_ctx{ "op-1",
                                                   {},
                                                   std::string{ "172.18.0.2:11210" },
                                                   std::string{ "127.0.0.1:55555" },
                                                   0,
                                                   {},
                                                   expected,
                                                   "doc-1",
                                                   "bucket",
                                                   "_default",
                                                   "_default",
                                                   42,
                                                   {},
                                                   couchbase::cas{ 1 },
                                                   {},
                                                   {} };
  REQUIRE(kv_ctx.last_dispatched_to_node_id() == expected);

  auto subdoc_ctx = couchbase::core::make_subdocument_error_context(kv_ctx, {}, {}, {}, false);
  REQUIRE(subdoc_ctx.last_dispatched_to_node_id() == expected);
  REQUIRE(subdoc_ctx.last_dispatched_to_node_id().id() == "node-xyz");
}

TEST_CASE("unit: subdocument_error_context constructor accepts node_id", "[unit]")
{
  auto expected = couchbase::internal_node_id::build("uuid-abc", "h", 11210);
  couchbase::core::subdocument_error_context ctx{
    "op-2",
    {},
    {},
    {},
    0,
    {},
    expected,
    "doc-2",
    "bucket",
    "_default",
    "_default",
    7,
    {},
    couchbase::cas{ 2 },
    {},
    {},
    std::optional<std::string>{ "some.path" },
    std::optional<std::uint64_t>{ 0 },
    false,
  };
  REQUIRE(ctx.last_dispatched_to_node_id() == expected);
  REQUIRE(ctx.first_error_path() == std::optional<std::string>{ "some.path" });
  REQUIRE(ctx.first_error_index() == std::optional<std::size_t>{ 0 });
}
