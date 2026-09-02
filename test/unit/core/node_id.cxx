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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/error_context/key_value.hxx"
#include "core/error_context/key_value_error_context.hxx"
#include "core/error_context/subdocument_error_context.hxx"
#include "core/impl/node_id.hxx"
#include "core/impl/resolve_node_id.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/node_id.hxx>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace couchbase::test
{
// A failed comparison of two node_ids is unreadable without the identities, and every case below
// compares them.
template<>
struct operand_printer<couchbase::node_id> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const couchbase::node_id& value) -> std::string
  {
    return value.id().empty() ? std::string{ "(unknown)" } : detail::quoted(value.id());
  }
};

namespace
{
constexpr auto
as_transport(bool is_tls) noexcept -> couchbase::core::topology::transport
{
  return is_tls ? couchbase::core::topology::transport::tls
                : couchbase::core::topology::transport::plain;
}

void
a_default_node_id_is_unknown([[maybe_unused]] context& ctx)
{
  couchbase::node_id nid;
  assert_false(static_cast<bool>(nid), "a default node_id identifies nothing");
  assert_true(nid.id().empty(), "the identity");
  assert_true(nid.node_uuid().empty(), "the node uuid");
  assert_true(nid.hostname().empty(), "the hostname");
  assert_eq(nid.port(), std::uint16_t{ 0 }, "the port");
}

void
a_node_uuid_becomes_the_identity([[maybe_unused]] context& ctx)
{
  auto nid = couchbase::internal_node_id::build("abc-123", "172.18.0.2", 11210);
  assert_true(static_cast<bool>(nid), "the node is identified");
  assert_eq(nid.id(), "abc-123", "the identity is the uuid itself");
  assert_eq(nid.node_uuid(), "abc-123", "the uuid is kept");
  assert_eq(nid.hostname(), "172.18.0.2", "the hostname is kept");
  assert_eq(nid.port(), std::uint16_t{ 11210 }, "the port is kept");
}

void
without_a_node_uuid_the_identity_is_a_hash([[maybe_unused]] context& ctx)
{
  auto nid = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  assert_true(static_cast<bool>(nid), "host and port alone identify the node");
  assert_false(nid.id().empty(), "the fallback identity is not empty");
  assert_ne(nid.id(), "172.18.0.2:11210", "the fallback is hashed, not the raw host and port");
  assert_true(nid.node_uuid().empty(), "no uuid is invented");
}

void
the_fallback_hash_is_the_same_for_the_same_host_and_port([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid2 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  assert_eq(nid1.id(), nid2.id(), "the identity");
  assert_eq(nid1, nid2, "the node ids compare equal");
}

void
the_fallback_hash_is_pinned_to_known_values([[maybe_unused]] context& ctx)
{
  // Pin the fallback CRC32 output for a known input. The contract is "stable
  // across runs and platforms" so a future change to the hash recipe (algorithm
  // swap, byte order, masking) must be a deliberate, breaking decision and
  // these assertions are how we notice.
  assert_eq(couchbase::internal_node_id::build("", "172.18.0.2", 11210).id(),
            "00006091",
            "the pinned identity for 172.18.0.2:11210");
  assert_eq(couchbase::internal_node_id::build("", "172.18.0.3", 11210).id(),
            "000046e6",
            "the pinned identity for 172.18.0.3:11210");
  assert_eq(couchbase::internal_node_id::build("", "172.18.0.2", 11207).id(),
            "000067ee",
            "the pinned identity for 172.18.0.2:11207");
  assert_eq(couchbase::internal_node_id::build("", "localhost", 11210).id(),
            "00001d1f",
            "the pinned identity for localhost:11210");
}

void
different_hosts_and_ports_hash_differently([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid2 = couchbase::internal_node_id::build("", "172.18.0.3", 11210);
  auto nid3 = couchbase::internal_node_id::build("", "172.18.0.2", 11207);
  assert_ne(nid1, nid2, "a different host is a different node");
  assert_ne(nid1, nid3, "a different port is a different node");
  assert_ne(nid2, nid3, "both differ from each other too");
}

void
equality_compares_the_identity_and_nothing_else([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("same-uuid", "host-a", 11210);
  auto nid2 = couchbase::internal_node_id::build("same-uuid", "host-b", 11207);
  assert_eq(nid1, nid2, "the same uuid is the same node whatever the host and port");
}

void
an_unidentifiable_node_is_unknown([[maybe_unused]] context& ctx)
{
  // The "falsy = unknown" contract: an unidentifiable node should produce a
  // falsy node_id, not a truthy-but-meaningless one. This is the guard that
  // keeps request- and response-side identity construction in agreement.
  assert_false(static_cast<bool>(couchbase::internal_node_id::build("", "", 0)),
               "neither a uuid nor a host and port");
  assert_false(static_cast<bool>(couchbase::internal_node_id::build("", "host", 0)),
               "a host without a port");
  assert_false(static_cast<bool>(couchbase::internal_node_id::build("", "", 11210)),
               "a port without a host");

  assert_true(static_cast<bool>(couchbase::internal_node_id::build("uuid-only", "", 0)),
              "a uuid alone is enough");
}

void
an_ipv6_literal_hostname_hashes_like_any_other([[maybe_unused]] context& ctx)
{
  auto nid = couchbase::internal_node_id::build("", "[::1]", 11210);
  assert_true(static_cast<bool>(nid), "the node is identified");
  assert_eq(nid.hostname(), "[::1]", "the brackets are part of the hostname");
  assert_eq(nid.id(), "000049ef", "the pinned identity for [::1]:11210");
}

void
inequality_is_symmetric([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  assert_ne(nid1, nid2, "the two nodes differ");
  assert_ne(nid2, nid1, "and differ the other way round");
}

void
ordering_is_strict_and_irreflexive([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  auto nid_eq = couchbase::internal_node_id::build("aaa", "h2", 9999);

  assert_true(nid1 < nid2, "the smaller identity orders first");
  assert_false(nid2 < nid1, "and the larger one does not");

  assert_false(nid1 < nid1, "nothing precedes itself");

  assert_false(nid1 < nid_eq, "equal identities do not order");
  assert_false(nid_eq < nid1, "in either direction");
}

void
every_relational_operator_agrees_with_the_ordering([[maybe_unused]] context& ctx)
{
  auto nid_small = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid_large = couchbase::internal_node_id::build("bbb", "h", 1);
  auto nid_equal = couchbase::internal_node_id::build("aaa", "other", 9);

  assert_true(nid_small < nid_large, "less than");
  assert_true(nid_small <= nid_large, "less than or equal, when less");
  assert_true(nid_small <= nid_equal, "less than or equal, when equal");
  assert_true(nid_large > nid_small, "greater than");
  assert_true(nid_large >= nid_small, "greater than or equal, when greater");
  assert_true(nid_equal >= nid_small, "greater than or equal, when equal");

  assert_false(nid_small > nid_equal, "equal identities are not greater");
  assert_false(nid_small < nid_equal, "nor less");
}

void
the_hash_agrees_with_equality([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("same-uuid", "host-a", 11210);
  auto nid2 = couchbase::internal_node_id::build("same-uuid", "host-b", 11207);
  assert_eq(nid1, nid2, "the two are the same node");
  assert_eq(std::hash<couchbase::node_id>{}(nid1),
            std::hash<couchbase::node_id>{}(nid2),
            "equal nodes hash equally");

  auto nid3 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  auto nid4 = couchbase::internal_node_id::build("", "172.18.0.2", 11210);
  assert_eq(nid3, nid4, "the two are the same node through the fallback hash");
  assert_eq(std::hash<couchbase::node_id>{}(nid3),
            std::hash<couchbase::node_id>{}(nid4),
            "equal nodes hash equally");
}

void
the_fallback_hash_does_not_collide_over_a_small_grid([[maybe_unused]] context& ctx)
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
      assert_true(ids.insert(nid.id()).second, "the identity has not been seen before");
    }
  }
  assert_eq(ids.size(), std::size_t{ 50 }, "every host and port pair produced its own identity");
}

void
a_node_id_is_usable_as_an_ordered_container_key([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("aaa", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("bbb", "h", 1);
  std::set<couchbase::node_id> s;
  s.insert(nid1);
  s.insert(nid2);
  assert_eq(s.size(), std::size_t{ 2 }, "the set keeps both nodes apart");
  std::map<couchbase::node_id, int> m;
  m[nid1] = 1;
  m[nid2] = 2;
  assert_eq(m.size(), std::size_t{ 2 }, "the map keeps both nodes apart");
}

void
a_node_id_is_usable_as_an_unordered_container_key([[maybe_unused]] context& ctx)
{
  auto nid1 = couchbase::internal_node_id::build("uuid-1", "h", 1);
  auto nid2 = couchbase::internal_node_id::build("uuid-2", "h", 1);
  std::unordered_set<couchbase::node_id> s;
  s.insert(nid1);
  s.insert(nid2);
  assert_eq(s.size(), std::size_t{ 2 }, "the set keeps both nodes apart");
  std::unordered_map<couchbase::node_id, int> m;
  m[nid1] = 1;
  m[nid2] = 2;
  assert_eq(m.size(), std::size_t{ 2 }, "the map keeps both nodes apart");
}

void
a_topology_node_with_a_uuid_reports_it_as_its_identity([[maybe_unused]] context& ctx)
{
  couchbase::core::topology::configuration::node n;
  n.node_uuid = "test-uuid-xyz";
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  auto nid = n.effective_node_id(couchbase::core::topology::transport::plain);
  assert_true(static_cast<bool>(nid), "the node is identified");
  assert_eq(nid.id(), "test-uuid-xyz", "the identity is the uuid");
  assert_eq(nid.hostname(), "172.18.0.2", "the hostname is the default network's");
  assert_eq(nid.port(), std::uint16_t{ 11210 }, "the port is the plain key/value port");
}

void
a_topology_node_without_a_uuid_hashes_its_key_value_port([[maybe_unused]] context& ctx)
{
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;
  n.services_plain.management = 8091;
  n.services_tls.management = 18091;

  auto plain = n.effective_node_id(couchbase::core::topology::transport::plain);
  assert_true(static_cast<bool>(plain), "the node is identified");
  assert_eq(plain.port(), std::uint16_t{ 11210 }, "the plain key/value port");
  // The fallback id must be derived from the KV port that the client
  // actually connects over — not the management port.
  assert_eq(plain,
            couchbase::internal_node_id::build("", "172.18.0.2", 11210),
            "the identity hashes the key/value port");
  assert_ne(plain,
            couchbase::internal_node_id::build("", "172.18.0.2", 8091),
            "and not the management port");

  auto tls = n.effective_node_id(couchbase::core::topology::transport::tls);
  assert_true(static_cast<bool>(tls), "the node is identified over TLS too");
  assert_eq(tls.port(), std::uint16_t{ 11207 }, "the TLS key/value port");
  assert_eq(tls,
            couchbase::internal_node_id::build("", "172.18.0.2", 11207),
            "the identity hashes the key/value port");
  assert_ne(tls,
            couchbase::internal_node_id::build("", "172.18.0.2", 18091),
            "and not the management port");
}

void
without_a_uuid_the_two_transports_give_different_identities([[maybe_unused]] context& ctx)
{
  // On pre-8.0 servers (no node_uuid) the plain and TLS variants must produce
  // different fallback ids — they hash different ports — so that a consumer
  // using the plain port on the request side cannot accidentally match an
  // id derived from the TLS port on the response side.
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";
  n.services_plain.key_value = 11210;
  n.services_tls.key_value = 11207;

  assert_ne(n.effective_node_id(couchbase::core::topology::transport::plain),
            n.effective_node_id(couchbase::core::topology::transport::tls),
            "the two transports hash different ports");
}

void
a_topology_node_without_a_key_value_port_is_unknown([[maybe_unused]] context& ctx)
{
  // A topology node missing a KV port for the active transport cannot be
  // uniquely identified — internal_node_id::build returns a default (falsy)
  // node_id so request-side identity agrees with the response-side guard,
  // and downstream consumers (e.g. circuit-breaker registries keyed on
  // node_id) never accumulate ghost entries for un-bound nodes.
  couchbase::core::topology::configuration::node n;
  n.hostname = "172.18.0.2";

  auto nid = n.effective_node_id(couchbase::core::topology::transport::plain);
  assert_false(static_cast<bool>(nid), "a node with no key/value port identifies nothing");
  assert_true(nid.id().empty(), "the identity");
}

void
the_request_side_identity_matches_the_response_side_without_a_uuid([[maybe_unused]] context& ctx)
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
    auto request_side = n.effective_node_id(as_transport(is_tls));
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    assert_eq(request_side, response_side, "the two sides agree on the node");
    assert_eq(request_side.id(), response_side.id(), "the identity");
    assert_eq(request_side.hostname(), response_side.hostname(), "the hostname");
    assert_eq(request_side.port(), response_side.port(), "the port");
  }
}

void
the_request_side_identity_matches_the_response_side_with_a_uuid([[maybe_unused]] context& ctx)
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
    auto request_side = n.effective_node_id(as_transport(is_tls));
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    assert_eq(request_side, response_side, "the two sides agree on the node");
    assert_eq(request_side.port(), response_side.port(), "the port");
  }
}

void
an_alternate_address_does_not_change_the_hashed_identity([[maybe_unused]] context& ctx)
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
    auto request_side = n.effective_node_id(as_transport(is_tls));
    // The accessors expose the default-network identity, not the alt alias.
    assert_eq(request_side.hostname(), "172.18.0.2", "the default network's hostname");
    assert_ne(request_side.hostname(), ext.hostname, "not the external alias");

    // The response-side formula (mcbp_session feeds canonical_hostname_ and
    // canonical_port_number_ into internal_node_id::build) must produce the
    // same id even when the client connected over the external network.
    auto response_side = couchbase::internal_node_id::build(
      n.node_uuid, n.hostname, n.port_or(couchbase::core::service_type::key_value, is_tls, 0));
    assert_eq(request_side, response_side, "the two sides agree on the node");

    // And an id built from the alt hostname/port must be different — the
    // public alias must never influence the node's identity.
    auto alt_port = is_tls ? ext.services_tls.key_value : ext.services_plain.key_value;
    auto alt_side =
      couchbase::internal_node_id::build(n.node_uuid, ext.hostname, alt_port.value_or(0));
    assert_ne(request_side, alt_side, "the external alias is a different identity");
  }
}

void
an_alternate_address_does_not_change_the_reported_host_and_port([[maybe_unused]] context& ctx)
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

  auto plain = n.effective_node_id(couchbase::core::topology::transport::plain);
  assert_eq(plain.id(), "stable-uuid-7", "the identity is the uuid");
  assert_eq(plain.hostname(), "172.18.0.2", "the default network's hostname");
  assert_eq(plain.port(), std::uint16_t{ 11210 }, "the default network's plain port");

  auto tls = n.effective_node_id(couchbase::core::topology::transport::tls);
  assert_eq(tls.id(), "stable-uuid-7", "the identity is the uuid");
  assert_eq(tls.hostname(), "172.18.0.2", "the default network's hostname");
  assert_eq(tls.port(), std::uint16_t{ 11207 }, "the default network's TLS port");
}

void
an_empty_topology_enumerates_no_node_ids([[maybe_unused]] context& ctx)
{
  // The pure enumerator maps a configuration with no nodes to an empty
  // list — it applies no error policy. The error policy lives one layer
  // up: resolve_node_ids_from_config() turns both a null configuration
  // and an empty result into configuration_not_available, so the public
  // node_ids() API never hands back a successful empty vector.
  couchbase::core::topology::configuration config;
  assert_true(config.effective_node_ids(couchbase::core::topology::transport::plain).empty(),
              "no node is enumerated over plain");
  assert_true(config.effective_node_ids(couchbase::core::topology::transport::tls).empty(),
              "no node is enumerated over TLS");
}

void
enumeration_preserves_the_topology_order([[maybe_unused]] context& ctx)
{
  couchbase::core::topology::configuration config;
  config.nodes.resize(3);
  config.nodes[0].node_uuid = "uuid-a";
  config.nodes[0].hostname = "h0";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[1].node_uuid = "uuid-b";
  config.nodes[1].hostname = "h1";
  config.nodes[1].services_plain.key_value = 11210;
  config.nodes[2].node_uuid = "uuid-c";
  config.nodes[2].hostname = "h2";
  config.nodes[2].services_plain.key_value = 11210;

  auto ids = config.effective_node_ids(couchbase::core::topology::transport::plain);
  assert_eq(ids.size(), std::size_t{ 3 }, "every node is enumerated");
  assert_eq(ids[0].id(), "uuid-a", "the first node stays first");
  assert_eq(ids[1].id(), "uuid-b", "the second stays second");
  assert_eq(ids[2].id(), "uuid-c", "the third stays third");
}

void
enumeration_drops_nodes_without_a_key_value_port_on_that_transport([[maybe_unused]] context& ctx)
{
  // Mirrors a heterogeneous deployment where the query and search nodes
  // do not host the KV service for this bucket. A circuit breaker keyed
  // on node_id only cares about KV-serving nodes, so dropping them keeps
  // the registry's keys directly comparable to the SDK's KV result/error
  // node_ids on every code path.
  couchbase::core::topology::configuration config;
  config.nodes.resize(4);
  config.nodes[0].node_uuid = "kv-1";
  config.nodes[0].hostname = "h0";
  config.nodes[0].services_plain.key_value = 11210;
  // node 1 is a query-only node — no KV port at all
  config.nodes[1].node_uuid = "query-only";
  config.nodes[1].hostname = "h1";
  config.nodes[1].services_plain.query = 8093;
  // node 2 has KV but only on TLS — must drop on plain transport
  config.nodes[2].node_uuid = "tls-only-kv";
  config.nodes[2].hostname = "h2";
  config.nodes[2].services_tls.key_value = 11207;
  config.nodes[3].node_uuid = "kv-2";
  config.nodes[3].hostname = "h3";
  config.nodes[3].services_plain.key_value = 11210;
  config.nodes[3].services_tls.key_value = 11207;

  auto plain = config.effective_node_ids(couchbase::core::topology::transport::plain);
  assert_eq(plain.size(), std::size_t{ 2 }, "only the plain key/value nodes are enumerated");
  assert_eq(plain[0].id(), "kv-1", "the first plain key/value node");
  assert_eq(plain[1].id(), "kv-2", "the second plain key/value node");

  auto tls = config.effective_node_ids(couchbase::core::topology::transport::tls);
  assert_eq(tls.size(), std::size_t{ 2 }, "only the TLS key/value nodes are enumerated");
  assert_eq(tls[0].id(), "tls-only-kv", "the first TLS key/value node");
  assert_eq(tls[1].id(), "kv-2", "the second TLS key/value node");
}

void
enumeration_agrees_with_the_per_node_identity([[maybe_unused]] context& ctx)
{
  // For every node that survives the filter, the result must be byte-for-byte
  // identical to what node::effective_node_id(is_tls) produces on its own —
  // node_ids() is documented as the same identity the SDK reports on KV
  // results/errors, and that identity comes through node::effective_node_id.
  couchbase::core::topology::configuration config;
  config.nodes.resize(2);
  config.nodes[0].hostname = "172.18.0.2";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[0].services_tls.key_value = 11207;
  config.nodes[1].node_uuid = "node-uuid-7";
  config.nodes[1].hostname = "172.18.0.3";
  config.nodes[1].services_plain.key_value = 11210;
  config.nodes[1].services_tls.key_value = 11207;

  for (bool is_tls : { false, true }) {
    auto ids = config.effective_node_ids(as_transport(is_tls));
    assert_eq(ids.size(), std::size_t{ 2 }, "both nodes are enumerated");
    assert_eq(ids[0],
              config.nodes[0].effective_node_id(as_transport(is_tls)),
              "the hashed node's enumerated identity is its own");
    assert_eq(ids[1],
              config.nodes[1].effective_node_id(as_transport(is_tls)),
              "the uuid node's enumerated identity is its own");
  }
}

void
enumeration_differs_between_transports_without_uuids([[maybe_unused]] context& ctx)
{
  // Without UUIDs the fallback hash incorporates the actual KV port the
  // client connects to, so the same node yields a different node_id over
  // TLS vs plain. node_ids() must reflect that — otherwise a registry
  // populated from TLS-side results could fail to match keys derived from
  // plain-side topology data.
  couchbase::core::topology::configuration config;
  config.nodes.resize(1);
  config.nodes[0].hostname = "172.18.0.2";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[0].services_tls.key_value = 11207;

  auto plain = config.effective_node_ids(couchbase::core::topology::transport::plain);
  auto tls = config.effective_node_ids(couchbase::core::topology::transport::tls);
  assert_eq(plain.size(), std::size_t{ 1 }, "the node is enumerated over plain");
  assert_eq(tls.size(), std::size_t{ 1 }, "and over TLS");
  assert_ne(plain[0], tls[0], "the two transports hash different ports");
}

void
enumeration_agrees_between_transports_with_uuids([[maybe_unused]] context& ctx)
{
  // The reverse of the previous test: with UUIDs the identity is transport-
  // independent, so the same node has the same node_id on both transports.
  // This is the property that makes a circuit breaker registry portable
  // across a TLS upgrade on a Server 8.0.1+ cluster.
  couchbase::core::topology::configuration config;
  config.nodes.resize(2);
  config.nodes[0].node_uuid = "uuid-a";
  config.nodes[0].hostname = "172.18.0.2";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[0].services_tls.key_value = 11207;
  config.nodes[1].node_uuid = "uuid-b";
  config.nodes[1].hostname = "172.18.0.3";
  config.nodes[1].services_plain.key_value = 11210;
  config.nodes[1].services_tls.key_value = 11207;

  auto plain = config.effective_node_ids(couchbase::core::topology::transport::plain);
  auto tls = config.effective_node_ids(couchbase::core::topology::transport::tls);
  assert_eq(plain.size(), std::size_t{ 2 }, "both nodes are enumerated over plain");
  assert_eq(tls.size(), std::size_t{ 2 }, "and over TLS");
  for (std::size_t i = 0; i < plain.size(); ++i) {
    assert_eq(plain[i], tls[i], "a uuid identity does not depend on the transport");
  }
}

void
enumeration_ignores_alternate_addresses([[maybe_unused]] context& ctx)
{
  // A consumer using the default-network identity (which the SDK exposes
  // via node_ids()) must not see entries from the external/alt alias —
  // otherwise the same physical node could appear twice in the breaker
  // registry under two different keys.
  couchbase::core::topology::configuration config;
  config.nodes.resize(1);
  config.nodes[0].hostname = "172.18.0.2";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[0].services_tls.key_value = 11207;

  couchbase::core::topology::configuration::alternate_address ext;
  ext.name = "external";
  ext.hostname = "172-18-0-2.my.cloud.com";
  ext.services_plain.key_value = 31100;
  ext.services_tls.key_value = 31207;
  config.nodes[0].alt["external"] = ext;

  for (bool is_tls : { false, true }) {
    auto ids = config.effective_node_ids(as_transport(is_tls));
    assert_eq(ids.size(), std::size_t{ 1 }, "the alias does not add a second entry");
    assert_eq(ids[0].hostname(), "172.18.0.2", "the default network's hostname");
    assert_ne(ids[0].hostname(), ext.hostname, "not the external alias");
  }
}

void
enumerated_node_ids_diff_against_a_tracked_registry([[maybe_unused]] context& ctx)
{
  // The point of node_ids() is to be diffable against application-side
  // unordered_map<node_id, ...> registries. Lock in that the result type
  // works as both a key (via std::hash specialization) and as a member of
  // an unordered_set so callers can express "which keys do I track that
  // the cluster no longer has?" with a single set_difference-equivalent.
  couchbase::core::topology::configuration config;
  config.nodes.resize(3);
  config.nodes[0].node_uuid = "uuid-a";
  config.nodes[0].hostname = "h0";
  config.nodes[0].services_plain.key_value = 11210;
  config.nodes[1].node_uuid = "uuid-b";
  config.nodes[1].hostname = "h1";
  config.nodes[1].services_plain.key_value = 11210;
  config.nodes[2].node_uuid = "uuid-c";
  config.nodes[2].hostname = "h2";
  config.nodes[2].services_plain.key_value = 11210;

  auto ids = config.effective_node_ids(couchbase::core::topology::transport::plain);
  std::unordered_set<couchbase::node_id> live{ ids.begin(), ids.end() };
  assert_eq(live.size(), std::size_t{ 3 }, "every enumerated node is its own key");

  // Tracker registry that has one stale key (the cluster has dropped uuid-c
  // and added a node with no entry yet for "stale-x").
  std::unordered_map<couchbase::node_id, int> tracked;
  tracked[couchbase::internal_node_id::build("uuid-a", "h0", 11210)] = 1;
  tracked[couchbase::internal_node_id::build("uuid-b", "h1", 11210)] = 1;
  tracked[couchbase::internal_node_id::build("stale-x", "h-gone", 11210)] = 1;

  std::vector<couchbase::node_id> retired;
  for (const auto& [k, _] : tracked) {
    if (live.find(k) == live.end()) {
      retired.push_back(k);
    }
  }
  assert_eq(retired.size(), std::size_t{ 1 }, "only the key the cluster dropped is retired");
  assert_eq(retired[0].id(), "stale-x", "and it is the one that was never enumerated");
}

void
a_key_maps_through_the_vbucket_map_to_a_configured_node([[maybe_unused]] context& ctx)
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

  auto idx0 = config.map_key("some-key", 0).second;
  assert_true(idx0.has_value(), "the key maps to a server index");
  auto nid =
    config.nodes[idx0.value()].effective_node_id(couchbase::core::topology::transport::plain);
  assert_true(static_cast<bool>(nid), "the node at that index is identified");
  assert_true(nid.id() == "node-0" || nid.id() == "node-1" || nid.id() == "node-2",
              "the identity belongs to one of the configured nodes");
}

void
a_subdocument_error_context_carries_the_node_id_forward([[maybe_unused]] context& ctx)
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
  assert_eq(kv_ctx.last_dispatched_to_node_id(),
            expected,
            "the key/value context records the node it was dispatched to");

  auto subdoc_ctx = couchbase::core::make_subdocument_error_context(kv_ctx, {}, {}, {}, false);
  assert_eq(
    subdoc_ctx.last_dispatched_to_node_id(), expected, "the subdocument context carries it across");
  assert_eq(subdoc_ctx.last_dispatched_to_node_id().id(), "node-xyz", "and it is the same uuid");
}

void
a_subdocument_error_context_records_the_node_id_it_is_built_with([[maybe_unused]] context& ctx)
{
  auto expected = couchbase::internal_node_id::build("uuid-abc", "h", 11210);
  couchbase::core::subdocument_error_context error_ctx{
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
  assert_eq(
    error_ctx.last_dispatched_to_node_id(), expected, "the node the context was built with");
  assert_true(error_ctx.first_error_path() == std::optional<std::string>{ "some.path" },
              "the first failing path");
  assert_true(error_ctx.first_error_index() == std::optional<std::size_t>{ 0 },
              "the index of the first failing spec");
}

void
a_subdocument_error_context_invents_no_node_id([[maybe_unused]] context& ctx)
{
  // Symmetric case to the propagation test: when the source key_value
  // context carries a default-constructed (falsy) node_id, the subdoc
  // wrapper must not invent identity. Guards against future "if node_id is
  // empty, default to something" regressions.
  couchbase::core::key_value_error_context kv_ctx{
    "op-empty",  {},       {},         {},         0, {}, couchbase::node_id{}, // explicitly empty
    "doc-empty", "bucket", "_default", "_default", 0, {}, couchbase::cas{ 0 },  {}, {}
  };
  assert_false(static_cast<bool>(kv_ctx.last_dispatched_to_node_id()),
               "the source context names no node");

  auto subdoc_ctx = couchbase::core::make_subdocument_error_context(kv_ctx, {}, {}, {}, false);
  assert_false(static_cast<bool>(subdoc_ctx.last_dispatched_to_node_id()),
               "and neither does the subdocument context");
}

void
resolving_a_node_id_rejects_an_empty_key([[maybe_unused]] context& ctx)
{
  // Locks in the invalid_argument guard from collection_impl::node_id_for —
  // an empty key would otherwise map to vbucket 0 unconditionally and emit a
  // deterministic-but-meaningless node_id.
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(1);
  config->nodes[0].node_uuid = "any-node";
  config->nodes[0].hostname = "h";
  config->nodes[0].services_plain.key_value = 11210;
  config->vbmap = couchbase::core::topology::configuration::vbucket_map{ { 0 } };

  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    config, "", couchbase::core::topology::transport::plain);
  assert_eq(ec, couchbase::errc::common::invalid_argument, "an empty key is refused by name");
  assert_false(static_cast<bool>(nid), "and no identity is invented");
}

void
resolving_a_node_id_without_a_configuration_is_refused([[maybe_unused]] context& ctx)
{
  // The (origin, ec) preconditions are enforced by node_id_for itself; this
  // covers the null-config path that with_bucket_configuration may invoke
  // after a successful origin lookup but before a config snapshot is known.
  std::shared_ptr<couchbase::core::topology::configuration> null_config;
  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    null_config, "doc", couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "the missing configuration is named");
  assert_false(static_cast<bool>(nid), "and no identity is invented");
}

void
resolving_a_node_id_without_a_vbucket_map_is_refused([[maybe_unused]] context& ctx)
{
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(1);
  config->nodes[0].hostname = "h";

  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    config, "doc", couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "a configuration with no map cannot place a key");
  assert_false(static_cast<bool>(nid), "and no identity is invented");
}

void
resolving_a_node_id_with_an_empty_vbucket_map_is_refused([[maybe_unused]] context& ctx)
{
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(1);
  config->nodes[0].hostname = "h";
  config->vbmap = couchbase::core::topology::configuration::vbucket_map{};

  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    config, "doc", couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "a map with no row cannot place a key");
  assert_false(static_cast<bool>(nid), "and no identity is invented");
}

void
resolving_a_node_id_through_an_out_of_range_server_index_is_refused([[maybe_unused]] context& ctx)
{
  // The vbmap entry points to a server_index that does not exist in
  // configuration::nodes — surfaces as configuration_not_available, not
  // request_canceled.
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(1);
  config->nodes[0].hostname = "h";
  config->nodes[0].services_plain.key_value = 11210;
  // vbmap references node index 99 which is outside nodes.size() == 1
  config->vbmap = couchbase::core::topology::configuration::vbucket_map{ { 99 } };

  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    config, "doc", couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "a row naming a node the configuration does not have is a stale configuration");
  assert_false(static_cast<bool>(nid), "and no identity is invented");
}

void
resolving_a_node_id_answers_with_a_configured_node([[maybe_unused]] context& ctx)
{
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(2);
  config->nodes[0].node_uuid = "node-0";
  config->nodes[0].hostname = "h0";
  config->nodes[0].services_plain.key_value = 11210;
  config->nodes[1].node_uuid = "node-1";
  config->nodes[1].hostname = "h1";
  config->nodes[1].services_plain.key_value = 11210;
  config->vbmap = couchbase::core::topology::configuration::vbucket_map{ { 0 }, { 1 } };

  auto [ec, nid] = couchbase::core::impl::resolve_node_id_from_config(
    config, "any-key", couchbase::core::topology::transport::plain);
  assert_success(ec, "a placeable key resolves");
  assert_true(static_cast<bool>(nid), "the node is identified");
  assert_true(nid.id() == "node-0" || nid.id() == "node-1",
              "the identity belongs to one of the configured nodes");
}

void
resolving_all_node_ids_without_a_configuration_is_refused([[maybe_unused]] context& ctx)
{
  // collection_impl::node_ids depends on this pure helper to map a topology
  // snapshot to (error_code, vector). The null-config branch is otherwise
  // only reachable when with_bucket_configuration() returns success but
  // with no config — vanishingly rare from a healthy cluster, so unit
  // coverage is the only practical way to exercise it.
  std::shared_ptr<couchbase::core::topology::configuration> null_config;
  auto [ec, nids] = couchbase::core::impl::resolve_node_ids_from_config(
    null_config, couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "the missing configuration is named");
  assert_true(nids.empty(), "and no node is enumerated");
}

void
resolving_all_node_ids_with_no_key_value_node_is_refused([[maybe_unused]] context& ctx)
{
  // Mid-rebalance transient: a topology with no KV-serving nodes for the
  // requested transport must surface as configuration_not_available, not
  // as a silent empty-success. Without this guarantee, a
  // sweep-against-registry loop would conclude the cluster has zero
  // KV-serving nodes and discard every tracker entry.
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(1);
  config->nodes[0].hostname = "h0";
  // no services_plain.key_value / services_tls.key_value set, so the node
  // is filtered out of effective_node_ids regardless of transport.

  auto [ec, nids] = couchbase::core::impl::resolve_node_ids_from_config(
    config, couchbase::core::topology::transport::plain);
  assert_eq(ec,
            couchbase::errc::network::configuration_not_available,
            "an empty enumeration is refused rather than answered as success");
  assert_true(nids.empty(), "and no node is enumerated");
}

void
resolving_all_node_ids_answers_with_every_key_value_node([[maybe_unused]] context& ctx)
{
  auto config = std::make_shared<couchbase::core::topology::configuration>();
  config->nodes.resize(2);
  config->nodes[0].node_uuid = "uuid-a";
  config->nodes[0].hostname = "h0";
  config->nodes[0].services_plain.key_value = 11210;
  config->nodes[1].node_uuid = "uuid-b";
  config->nodes[1].hostname = "h1";
  config->nodes[1].services_plain.key_value = 11210;

  auto [ec, nids] = couchbase::core::impl::resolve_node_ids_from_config(
    config, couchbase::core::topology::transport::plain);
  assert_success(ec, "a topology with key/value nodes resolves");
  assert_eq(nids.size(), std::size_t{ 2 }, "both nodes are enumerated");
  assert_eq(nids[0].id(), "uuid-a", "the first node");
  assert_eq(nids[1].id(), "uuid-b", "the second node");
}

void
enumeration_selects_per_transport_in_a_mixed_topology([[maybe_unused]] context& ctx)
{
  // A topology where each node exposes a different subset of KV ports:
  // - node 0: plain only
  // - node 1: TLS only
  // - node 2: both
  // node_ids(plain) must return {0, 2}; node_ids(tls) must return {1, 2}.
  // This locks in the filter's interaction with effective_node_ids per
  // transport — a regression in the filter logic would silently expose
  // identities for KV-less nodes.
  couchbase::core::topology::configuration config;
  config.nodes.resize(3);
  config.nodes[0].node_uuid = "plain-only";
  config.nodes[0].hostname = "h0";
  config.nodes[0].services_plain.key_value = 11210;
  // no TLS

  config.nodes[1].node_uuid = "tls-only";
  config.nodes[1].hostname = "h1";
  config.nodes[1].services_tls.key_value = 11207;
  // no plain

  config.nodes[2].node_uuid = "both";
  config.nodes[2].hostname = "h2";
  config.nodes[2].services_plain.key_value = 11210;
  config.nodes[2].services_tls.key_value = 11207;

  auto plain_ids = config.effective_node_ids(couchbase::core::topology::transport::plain);
  assert_eq(plain_ids.size(), std::size_t{ 2 }, "only the plain key/value nodes are enumerated");
  assert_eq(plain_ids[0].id(), "plain-only", "the plain-only node");
  assert_eq(plain_ids[1].id(), "both", "and the node serving both");

  auto tls_ids = config.effective_node_ids(couchbase::core::topology::transport::tls);
  assert_eq(tls_ids.size(), std::size_t{ 2 }, "only the TLS key/value nodes are enumerated");
  assert_eq(tls_ids[0].id(), "tls-only", "the TLS-only node");
  assert_eq(tls_ids[1].id(), "both", "and the node serving both");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_default_node_id_is_unknown), {}, timeout::instant },
      { CASE(a_node_uuid_becomes_the_identity), {}, timeout::instant },
      { CASE(without_a_node_uuid_the_identity_is_a_hash), {}, timeout::instant },
      { CASE(the_fallback_hash_is_the_same_for_the_same_host_and_port), {}, timeout::instant },
      { CASE(the_fallback_hash_is_pinned_to_known_values), {}, timeout::instant },
      { CASE(different_hosts_and_ports_hash_differently), {}, timeout::instant },
      { CASE(equality_compares_the_identity_and_nothing_else), {}, timeout::instant },
      { CASE(an_unidentifiable_node_is_unknown), {}, timeout::instant },
      { CASE(an_ipv6_literal_hostname_hashes_like_any_other), {}, timeout::instant },
      { CASE(inequality_is_symmetric), {}, timeout::instant },
      { CASE(ordering_is_strict_and_irreflexive), {}, timeout::instant },
      { CASE(every_relational_operator_agrees_with_the_ordering), {}, timeout::instant },
      { CASE(the_hash_agrees_with_equality), {}, timeout::instant },
      { CASE(the_fallback_hash_does_not_collide_over_a_small_grid), {}, timeout::instant },
      { CASE(a_node_id_is_usable_as_an_ordered_container_key), {}, timeout::instant },
      { CASE(a_node_id_is_usable_as_an_unordered_container_key), {}, timeout::instant },
      { CASE(a_topology_node_with_a_uuid_reports_it_as_its_identity), {}, timeout::instant },
      { CASE(a_topology_node_without_a_uuid_hashes_its_key_value_port), {}, timeout::instant },
      { CASE(without_a_uuid_the_two_transports_give_different_identities), {}, timeout::instant },
      { CASE(a_topology_node_without_a_key_value_port_is_unknown), {}, timeout::instant },
      { CASE(the_request_side_identity_matches_the_response_side_without_a_uuid),
        {},
        timeout::instant },
      { CASE(the_request_side_identity_matches_the_response_side_with_a_uuid),
        {},
        timeout::instant },
      { CASE(an_alternate_address_does_not_change_the_hashed_identity), {}, timeout::instant },
      { CASE(an_alternate_address_does_not_change_the_reported_host_and_port),
        {},
        timeout::instant },
      { CASE(an_empty_topology_enumerates_no_node_ids), {}, timeout::instant },
      { CASE(enumeration_preserves_the_topology_order), {}, timeout::instant },
      { CASE(enumeration_drops_nodes_without_a_key_value_port_on_that_transport),
        {},
        timeout::instant },
      { CASE(enumeration_agrees_with_the_per_node_identity), {}, timeout::instant },
      { CASE(enumeration_differs_between_transports_without_uuids), {}, timeout::instant },
      { CASE(enumeration_agrees_between_transports_with_uuids), {}, timeout::instant },
      { CASE(enumeration_ignores_alternate_addresses), {}, timeout::instant },
      { CASE(enumerated_node_ids_diff_against_a_tracked_registry), {}, timeout::instant },
      { CASE(a_key_maps_through_the_vbucket_map_to_a_configured_node), {}, timeout::instant },
      { CASE(a_subdocument_error_context_carries_the_node_id_forward), {}, timeout::instant },
      { CASE(a_subdocument_error_context_records_the_node_id_it_is_built_with),
        {},
        timeout::instant },
      { CASE(a_subdocument_error_context_invents_no_node_id), {}, timeout::instant },
      { CASE(resolving_a_node_id_rejects_an_empty_key), {}, timeout::instant },
      { CASE(resolving_a_node_id_without_a_configuration_is_refused), {}, timeout::instant },
      { CASE(resolving_a_node_id_without_a_vbucket_map_is_refused), {}, timeout::instant },
      { CASE(resolving_a_node_id_with_an_empty_vbucket_map_is_refused), {}, timeout::instant },
      { CASE(resolving_a_node_id_through_an_out_of_range_server_index_is_refused),
        {},
        timeout::instant },
      { CASE(resolving_a_node_id_answers_with_a_configured_node), {}, timeout::instant },
      { CASE(resolving_all_node_ids_without_a_configuration_is_refused), {}, timeout::instant },
      { CASE(resolving_all_node_ids_with_no_key_value_node_is_refused), {}, timeout::instant },
      { CASE(resolving_all_node_ids_answers_with_every_key_value_node), {}, timeout::instant },
      { CASE(enumeration_selects_per_transport_in_a_mixed_topology), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
