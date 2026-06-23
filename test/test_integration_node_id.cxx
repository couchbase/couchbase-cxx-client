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

#include "test_helper_integration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>
#include <couchbase/node_id.hxx>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <unordered_set>

static const tao::json::value basic_doc = {
  { "a", 1.0 },
  { "b", 2.0 },
};

namespace
{
// Server 8.0.0+ supplies a stable nodeUUID; pre-8.0 servers fall through to
// the SDK's CRC32 fallback hash of hostname + KV port. We use this in dual-mode
// assertions so the same test catches a silent regression on either side
// rather than passing identically against both server families.
auto
server_supplies_node_uuid(const test::utils::server_version& v) -> bool
{
  return v.major >= 8;
}
} // namespace

TEST_CASE("integration: node_id_for returns valid for known key", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_test");

  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  {
    auto [err, nid] = collection.node_id_for(id).get();
    REQUIRE_FALSE(err.ec());
    REQUIRE(static_cast<bool>(nid));
    REQUIRE_FALSE(nid.id().empty());
    REQUIRE_FALSE(nid.hostname().empty());
  }
}

TEST_CASE("integration: node_id_for is deterministic", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_deterministic");

  auto [err1, nid1] = collection.node_id_for(id).get();
  REQUIRE_FALSE(err1.ec());

  auto [err2, nid2] = collection.node_id_for(id).get();
  REQUIRE_FALSE(err2.ec());

  REQUIRE(nid1 == nid2);
}

TEST_CASE("integration: result carries node_id on success", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_result");

  {
    auto [err, resp] = collection.upsert(id, basic_doc, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(static_cast<bool>(resp.node_id()));
    REQUIRE_FALSE(resp.node_id().id().empty());
  }

  {
    auto [err, resp] = collection.get(id, {}).get();
    REQUIRE_SUCCESS(err.ec());
    REQUIRE(static_cast<bool>(resp.node_id()));
    REQUIRE_FALSE(resp.node_id().id().empty());
  }
}

TEST_CASE("integration: result.node_id matches node_id_for", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_match");

  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto [get_err, get_resp] = collection.get(id, {}).get();
  REQUIRE_SUCCESS(get_err.ec());

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  REQUIRE(get_resp.node_id() == nid);
}

TEST_CASE("integration: error carries node_id on failure", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_does_not_exist");

  auto err = collection.get(id, {}).get().first;
  REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);
  REQUIRE(static_cast<bool>(err.node_id()));
  REQUIRE_FALSE(err.node_id().id().empty());
}

TEST_CASE("integration: error.node_id matches node_id_for", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_error_match");

  auto err = collection.get(id, {}).get().first;
  REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  REQUIRE(err.node_id() == nid);
}

TEST_CASE("integration: node_id_for fails cleanly on missing bucket", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket("bucket-that-does-not-exist").default_collection();

  auto id = test::utils::uniq_id("node_id_no_bucket");
  auto [err, nid] =
    collection.node_id_for(id, couchbase::node_id_for_options{}.timeout(std::chrono::seconds{ 5 }))
      .get();
  // The "missing bucket" case is surfaced via the ec parameter of
  // with_bucket_configuration, not via the request timeout. This is the
  // contract the commit message commits to: bucket_not_found means
  // "bucket does not exist", while configuration_not_available is reserved
  // for a present-but-unusable map and unambiguous_timeout for the genuine
  // "no config ever arrived" path.
  REQUIRE(err.ec() == couchbase::errc::common::bucket_not_found);
  REQUIRE_FALSE(static_cast<bool>(nid));
}

TEST_CASE("integration: node_id_for rejects empty key", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto [err, nid] = collection.node_id_for("").get();
  REQUIRE(err.ec() == couchbase::errc::common::invalid_argument);
  REQUIRE_FALSE(static_cast<bool>(nid));
}

TEST_CASE("integration: node_id surfaces nodeUUID iff the server provides it", "[integration]")
{
  // This is the central behavioural distinction CXXCBC-820/821 introduces:
  // - On Server 8.0.0+ the cluster ships a stable nodeUUID that the SDK
  //   uses verbatim as node_id::id().
  // - On older servers (pre-8.0) the SDK derives id() from a CRC32 hash
  //   of hostname + KV port, and node_id::node_uuid() is empty.
  //
  // Without this dual-mode assertion the integration suite silently runs
  // only one branch on any given cluster and we lose coverage of the other.
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_uuid_mode");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto [err, resp] = collection.get(id, {}).get();
  REQUIRE_SUCCESS(err.ec());
  const auto& nid = resp.node_id();
  REQUIRE(static_cast<bool>(nid));

  if (server_supplies_node_uuid(integration.cluster_version())) {
    REQUIRE_FALSE(nid.node_uuid().empty());
    REQUIRE(nid.id() == nid.node_uuid());
  } else {
    REQUIRE(nid.node_uuid().empty());
    // Fallback id is the 8-hex-char CRC32 form.
    REQUIRE(nid.id().size() == 8);
  }
}

TEST_CASE("integration: node_ids returns the bucket's KV-serving nodes", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto [err, nids] = collection.node_ids().get();
  REQUIRE_FALSE(err.ec());
  REQUIRE_FALSE(nids.empty());
  // Tighter bound than "<= number_of_nodes": every node in the test
  // cluster's service-aware count is by definition a KV-serving node, so
  // we expect exact equality. If a future cluster topology adds non-KV
  // nodes (analytics-only, search-only, …) this test will need an update
  // — that is the explicit signal we want.
  REQUIRE(nids.size() == integration.number_of_nodes_with_service("kv"));

  // Every entry must be a fully-formed node_id: truthy, with a non-empty
  // id() and hostname(). Mirrors the per-node assertions used elsewhere
  // in this file for result.node_id() and error.node_id().
  for (const auto& nid : nids) {
    REQUIRE(static_cast<bool>(nid));
    REQUIRE_FALSE(nid.id().empty());
    REQUIRE_FALSE(nid.hostname().empty());
  }
}

TEST_CASE("integration: node_ids contains the result of node_id_for", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_ids_contains_for");

  auto [for_err, target] = collection.node_id_for(id).get();
  REQUIRE_FALSE(for_err.ec());

  auto [ids_err, nids] = collection.node_ids().get();
  REQUIRE_FALSE(ids_err.ec());

  std::unordered_set<couchbase::node_id> live{ nids.begin(), nids.end() };
  REQUIRE(live.find(target) != live.end());
}

TEST_CASE("integration: node_ids contains the result.node_id of a successful KV op",
          "[integration]")
{
  // The end-to-end consistency check: the node_id surfaced on a real KV
  // result must be one of the entries node_ids() reports as live. This is
  // what makes a registry keyed on result.node_id() directly diffable
  // against node_ids() in a sweep.
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_ids_contains_result");

  auto [up_err, up_resp] = collection.upsert(id, basic_doc, {}).get();
  REQUIRE_SUCCESS(up_err.ec());
  REQUIRE(static_cast<bool>(up_resp.node_id()));

  auto [ids_err, nids] = collection.node_ids().get();
  REQUIRE_FALSE(ids_err.ec());

  std::unordered_set<couchbase::node_id> live{ nids.begin(), nids.end() };
  REQUIRE(live.find(up_resp.node_id()) != live.end());
}

TEST_CASE("integration: node_ids is deterministic across calls", "[integration]")
{
  // This test assumes the cluster topology is stable across the two calls.
  // A rebalance between the calls would change the order or membership of
  // the returned set and flake the assertion; CI is expected to provide a
  // stable topology while this test runs. (We deliberately do not weaken
  // the assertion to "same multiset" because the public contract
  // documents topology order as deterministic on a stable cluster, and
  // weakening here would hide an order regression.)
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto [err1, nids1] = collection.node_ids().get();
  REQUIRE_FALSE(err1.ec());

  auto [err2, nids2] = collection.node_ids().get();
  REQUIRE_FALSE(err2.ec());

  // Topology order is deterministic on a stable cluster, so the vectors
  // must compare element-wise — not merely have the same set of entries.
  REQUIRE(nids1 == nids2);
}

TEST_CASE("integration: node_ids fails cleanly on missing bucket", "[integration]")
{
  // Mirrors node_id_for_fails_cleanly_on_missing_bucket: the missing-bucket
  // case is surfaced via the ec parameter of with_bucket_configuration,
  // not the request timeout. Pinning the exact error code prevents a future
  // change from silently dropping into a different (e.g. timeout) branch.
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket("bucket-that-does-not-exist").default_collection();

  auto [err, nids] =
    collection.node_ids(couchbase::node_ids_options{}.timeout(std::chrono::seconds{ 5 })).get();
  REQUIRE(err.ec() == couchbase::errc::common::bucket_not_found);
  REQUIRE(nids.empty());
}

TEST_CASE("integration: lookup_in result carries node_id on success", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("subdoc_node_id_lookup");

  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto specs = couchbase::lookup_in_specs{
    couchbase::lookup_in_specs::get("a"),
  };
  auto [err, resp] = collection.lookup_in(id, specs, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(static_cast<bool>(resp.node_id()));
  REQUIRE_FALSE(resp.node_id().id().empty());

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  REQUIRE(resp.node_id() == nid);
}

TEST_CASE("integration: mutate_in result carries node_id on success", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("subdoc_node_id_mutate");

  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto specs = couchbase::mutate_in_specs{
    couchbase::mutate_in_specs::upsert("c", 3.0),
  };
  auto [err, resp] = collection.mutate_in(id, specs, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(static_cast<bool>(resp.node_id()));
  REQUIRE_FALSE(resp.node_id().id().empty());

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  REQUIRE(resp.node_id() == nid);
}

TEST_CASE("integration: lookup_in error carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("subdoc_node_id_lookup_missing");

  auto specs = couchbase::lookup_in_specs{
    couchbase::lookup_in_specs::get("a"),
  };
  auto err = collection.lookup_in(id, specs, {}).get().first;
  REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);
  REQUIRE(static_cast<bool>(err.node_id()));
  REQUIRE_FALSE(err.node_id().id().empty());
}

TEST_CASE("integration: binary append result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();
  auto binary = collection.binary();

  auto id = test::utils::uniq_id("binary_node_id_append");

  {
    auto err = collection
                 .upsert<couchbase::codec::raw_binary_transcoder>(
                   id, std::vector<std::byte>{ std::byte{ 'a' } }, {})
                 .get()
                 .first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto [err, resp] = binary.append(id, std::vector<std::byte>{ std::byte{ 'b' } }, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(static_cast<bool>(resp.node_id()));
  REQUIRE_FALSE(resp.node_id().id().empty());

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  REQUIRE(resp.node_id() == nid);
}

TEST_CASE("integration: binary increment result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();
  auto binary = collection.binary();

  auto id = test::utils::uniq_id("binary_node_id_increment");

  auto [err, resp] = binary.increment(id, couchbase::increment_options{}.initial(1).delta(1)).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(static_cast<bool>(resp.node_id()));
  REQUIRE_FALSE(resp.node_id().id().empty());

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  REQUIRE(resp.node_id() == nid);
}

TEST_CASE("integration: get_all_replicas entries carry node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (integration.number_of_replicas() == 0) {
    SKIP("bucket has no replicas configured");
  }
  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP("cluster has fewer KV nodes than configured replicas");
  }

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("all_replicas_node_id");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto [err, result] = collection.get_all_replicas(id).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(result.size() == integration.number_of_replicas() + 1);

  // Every entry should carry a node_id that round-trips to a valid hostname.
  for (const auto& entry : result) {
    REQUIRE(static_cast<bool>(entry.node_id()));
    REQUIRE_FALSE(entry.node_id().id().empty());
    REQUIRE_FALSE(entry.node_id().hostname().empty());
  }

  // And the active replica's node_id must agree with the client-side vBucket
  // resolution via node_id_for() — this is the same guarantee we lock in for
  // single-target ops.
  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  auto active_it = std::find_if(result.begin(), result.end(), [](const auto& e) {
    return !e.is_replica();
  });
  REQUIRE(active_it != result.end());
  REQUIRE(active_it->node_id() == nid);
}

TEST_CASE("integration: lookup_in_all_replicas entries carry node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.has_bucket_capability("subdoc.ReplicaRead")) {
    SKIP("bucket does not support replica_read");
  }
  if (integration.number_of_replicas() == 0) {
    SKIP("bucket has no replicas configured");
  }
  if (integration.number_of_nodes() <= integration.number_of_replicas()) {
    SKIP("cluster has fewer KV nodes than configured replicas");
  }

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("all_replicas_lookup_node_id");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }

  auto specs = couchbase::lookup_in_specs{
    couchbase::lookup_in_specs::get("a"),
  };
  auto [err, result] = collection.lookup_in_all_replicas(id, specs).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(result.size() == integration.number_of_replicas() + 1);

  for (const auto& entry : result) {
    REQUIRE(static_cast<bool>(entry.node_id()));
    REQUIRE_FALSE(entry.node_id().id().empty());
    REQUIRE_FALSE(entry.node_id().hostname().empty());
  }

  auto [nid_err, nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());
  auto active_it = std::find_if(result.begin(), result.end(), [](const auto& e) {
    return !e.is_replica();
  });
  REQUIRE(active_it != result.end());
  REQUIRE(active_it->node_id() == nid);
}

TEST_CASE("integration: remove result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_remove");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = collection.remove(id, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: touch result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_touch");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = collection.touch(id, std::chrono::seconds{ 60 }, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: exists result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_exists");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = collection.exists(id, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.exists());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: get_and_touch result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_get_and_touch");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = collection.get_and_touch(id, std::chrono::seconds{ 60 }, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: get_and_lock and unlock results carry node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_lock");
  {
    auto err = collection.upsert(id, basic_doc, {}).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [lock_err, locked] = collection.get_and_lock(id, std::chrono::seconds{ 5 }, {}).get();
  REQUIRE_SUCCESS(lock_err.ec());
  REQUIRE(locked.node_id() == expected_nid);

  auto unlock_err = collection.unlock(id, locked.cas(), {}).get();
  REQUIRE_SUCCESS(unlock_err.ec());
}

TEST_CASE("integration: binary decrement result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();
  auto binary = collection.binary();

  auto id = test::utils::uniq_id("node_id_decrement");
  // Seed with an initial value via increment(initial=10).
  {
    auto err =
      binary.increment(id, couchbase::increment_options{}.initial(10).delta(1)).get().first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = binary.decrement(id, couchbase::decrement_options{}.delta(1)).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: binary prepend result carries node_id", "[integration]")
{
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();
  auto binary = collection.binary();

  auto id = test::utils::uniq_id("node_id_prepend");
  {
    auto err = collection
                 .upsert<couchbase::codec::raw_binary_transcoder>(
                   id, std::vector<std::byte>{ std::byte{ 'a' } }, {})
                 .get()
                 .first;
    REQUIRE_SUCCESS(err.ec());
  }
  auto [nid_err, expected_nid] = collection.node_id_for(id).get();
  REQUIRE_FALSE(nid_err.ec());

  auto [err, resp] = binary.prepend(id, std::vector<std::byte>{ std::byte{ 'X' } }, {}).get();
  REQUIRE_SUCCESS(err.ec());
  REQUIRE(resp.node_id() == expected_nid);
}

TEST_CASE("integration: remove failure on missing doc carries node_id", "[integration]")
{
  // The error path through make_key_value_error_context: removing a
  // non-existent document returns document_not_found and the error must
  // still carry the node identity of the responding session.
  test::utils::integration_test_guard integration;
  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  auto id = test::utils::uniq_id("node_id_remove_missing");

  auto err = collection.remove(id, {}).get().first;
  REQUIRE(err.ec() == couchbase::errc::key_value::document_not_found);
  REQUIRE(static_cast<bool>(err.node_id()));
  REQUIRE_FALSE(err.node_id().id().empty());
}
