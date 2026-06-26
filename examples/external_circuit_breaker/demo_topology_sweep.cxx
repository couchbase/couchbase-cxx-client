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

#include "demo_topology_sweep.hxx"

#include "ui.hxx"

#include <couchbase/node_id.hxx>

#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace example::demo
{

namespace
{

// Number of consecutive sweeps a node must be missing before its tracker is
// retired. See sweep_against() for why a single miss is not enough.
constexpr int retire_after_consecutive_misses = 2;

// The canonical sweep, factored out so both the steady-state pass and the
// "shrink simulation" pass below use the same code path.  This is the
// loop a production process would run on a timer (e.g. once per minute):
// fetch the current set of KV-serving nodes, diff against what the
// breaker is tracking, and forget nodes that have been gone long enough.
//
// node_ids() returns a point-in-time topology snapshot; during a rebalance
// a node can transiently drop out of one snapshot and reappear in the next
// (see the @note on couchbase::collection::node_ids()). Retiring on a
// single miss would discard a node's accumulated failure history just
// because a sweep happened to land mid-rebalance. So we debounce: @p
// absent_streak (owned by the caller, persisted across sweeps) counts
// consecutive misses per node, and a tracker is forgotten only once that
// count reaches retire_after_consecutive_misses. Any reappearance resets
// the streak.
auto
sweep_against(example::cb::circuit_breaker& breaker,
              const std::unordered_set<couchbase::node_id>& live,
              std::unordered_map<couchbase::node_id, int>& absent_streak)
  -> std::vector<couchbase::node_id>
{
  // Snapshot the registry's keys before mutating: forget() under the hood
  // mutates nodes_, which is the same map all_metrics() iterated to build
  // its return value.  We decide on copied-out keys and only then call
  // forget() to keep that mutation safely outside the iteration.
  std::vector<couchbase::node_id> retired;
  for (const auto& [tracked, _] : breaker.all_metrics()) {
    if (live.find(tracked) == live.end()) {
      if (++absent_streak[tracked] >= retire_after_consecutive_misses) {
        retired.push_back(tracked);
      }
    } else {
      // Present again — a transient absence must not count toward retirement.
      absent_streak.erase(tracked);
    }
  }
  for (const auto& nid : retired) {
    breaker.forget(nid);
    absent_streak.erase(nid);
  }
  return retired;
}

} // namespace

auto
demo_topology_sweep(couchbase::collection& collection, example::cb::circuit_breaker& breaker)
  -> void
{
  using namespace std::chrono_literals;
  example::ui::print_demo_header(
    "Demo 7",
    "Topology sweep — retire breakers for nodes that left the cluster",
    "A long-running process accumulates per-node breaker state forever unless something prunes "
    "entries for nodes the cluster no longer has.  Diff breaker.all_metrics() against "
    "collection.node_ids() and forget the difference.");

  // Step 1: ground truth from the SDK. node_ids() is the canonical source
  // for "which cluster nodes serve KV traffic right now" — exactly the
  // identities the SDK reports back on every result and error, so the
  // returned set is directly comparable to anything the breaker is keying
  // on.  No network round-trip — this comes off the client-side topology.
  example::ui::print_step("Calling collection::node_ids() to learn the current cluster shape...");
  // .get() is convenient for a synchronous demo but blocks the calling
  // thread for up to the configured timeout. In production code prefer
  // the handler overload of node_ids() (or wait_for() on the future) so a
  // misbehaving cluster does not stall the sweep loop.
  auto [ids_err, live] = collection.node_ids().get();
  if (ids_err) {
    example::ui::print_warning("node_ids() failed: " + std::string{ ids_err.ec().message() } +
                               " — skipping demo.");
    return;
  }
  // Defensive: the SDK surfaces an empty-KV-set topology as
  // errc::network::configuration_not_available rather than a successful
  // empty vector, so this branch is normally unreachable when ids_err is
  // falsy. The check stays as a belt-and-braces guard for future SDK
  // versions whose contract may relax.
  if (live.empty()) {
    example::ui::print_warning("node_ids() returned an empty set — skipping demo.");
    return;
  }
  example::ui::print_note("Cluster currently exposes " + std::to_string(live.size()) +
                          " KV-serving node(s).");

  // Step 2: populate the registry by exercising every live node. We use
  // node_id_for() per key + record_success() to materialize one breaker
  // per node without depending on the keys actually mapping uniformly —
  // a real workload will populate the registry via execute() on its hot
  // path; both flows produce identical registry state.
  example::ui::print_step(
    "Populating the registry with one entry per live node via node_id_for() + record_success()...");
  std::unordered_set<couchbase::node_id> populated;
  for (int i = 0; i < 64 && populated.size() < live.size(); ++i) {
    auto key = "cb-demo-sweep-" + std::to_string(i);
    auto [err, target] = collection.node_id_for(key).get();
    if (err || !target) {
      continue;
    }
    if (populated.insert(target).second) {
      breaker.record_success(target, 0ms);
    }
  }
  example::ui::print_note("Registry now tracks " + std::to_string(breaker.all_metrics().size()) +
                          " entry/entries (one per live node we routed to).");

  // The per-node consecutive-absence counter, persisted across every sweep
  // below exactly as a production sweep loop would persist it between
  // timer ticks. This is what makes the debounce work.
  std::unordered_map<couchbase::node_id, int> absent_streak;

  // Step 3: steady-state sweep. With the registry populated from the live
  // cluster, the diff is empty — exactly what every sweep iteration looks
  // like when the topology has not changed since the last pass.  This is
  // the no-op baseline the production loop must handle cheaply.
  example::ui::print_step("Steady-state sweep against the current node_ids() set...");
  std::unordered_set<couchbase::node_id> live_set{ live.begin(), live.end() };
  auto retired_steady = sweep_against(breaker, live_set, absent_streak);
  example::ui::print_note("Retired " + std::to_string(retired_steady.size()) +
                          " entry/entries (expected: 0 in steady state).");

  // Step 4: simulate a topology shrink.  We cannot decommission a real
  // cluster node from a demo against a live deployment, so instead we
  // construct a smaller "live" set that omits one of the entries the
  // registry currently holds.  This is exactly the shape collection.
  // node_ids() would return on the first sweep after a node has been
  // removed from the cluster, so the sweep code path it exercises is
  // the same one production would execute against the real shrunk set.
  if (populated.size() < 2) {
    example::ui::print_warning(
      "Single-node cluster — cannot simulate a topology shrink.  The mechanism is the same "
      "regardless of cluster size: a node absent from collection.node_ids() for two consecutive "
      "sweeps is retired from breaker.all_metrics().");
    return;
  }
  auto shrunk = live_set;
  auto victim = *shrunk.begin();
  shrunk.erase(shrunk.begin());
  example::ui::print_step("Simulating a topology shrink: pretending node " +
                          example::ui::short_node(victim.id()) +
                          " has been decommissioned (omitted from the next node_ids() snapshot).");

  // First sweep against the smaller set. node_ids() is only a point-in-time
  // snapshot, so a single absence might just be a node briefly dropping out
  // mid-rebalance. The debounce therefore holds off: the node's streak ticks
  // to 1 but nothing is retired yet.
  auto retired_first = sweep_against(breaker, shrunk, absent_streak);
  example::ui::print_note(
    "Sweep 1 after the shrink: retired " + std::to_string(retired_first.size()) +
    " entry/entries — a single missed snapshot could be a transient rebalance blip, so the "
    "debounce waits for confirmation.");

  // Second consecutive sweep with the node still gone. Now the absence is
  // confirmed (streak reaches two) and the tracker is retired.
  auto retired_second = sweep_against(breaker, shrunk, absent_streak);
  example::ui::print_note("Sweep 2 with the node still absent: retired " +
                          std::to_string(retired_second.size()) +
                          " confirmed-stale entry/entries.");
  for (const auto& nid : retired_second) {
    example::ui::print_note(std::string{ "  forget(" } + example::ui::short_node(nid.id()) + ")");
  }

  example::ui::print_step("Post-sweep registry:");
  example::ui::print_metrics_table(breaker);

  // Final message: spell out the production shape so the reader knows
  // exactly how to wire this into their own scheduler.  Cadence is a
  // tradeoff between memory reclaim latency and the (tiny) cost of one
  // node_ids() call plus the all_metrics()/forget() loop.
  example::ui::print_success(
    "In production, replace the simulated 'shrunk' set with the actual collection.node_ids() "
    "result and run sweep_against() on a timer (once per minute is plenty), persisting the "
    "absent-streak map across ticks. The two-sweep debounce means a node is reclaimed only "
    "after it has been absent from two consecutive snapshots, so a transient drop-out during a "
    "rebalance never discards a live node's failure history — matching the guidance on "
    "collection.node_ids().");

  // Re-populate the entry we dropped so we leave the registry in the
  // same shape we found it — purely a courtesy to anything that runs
  // after this demo.
  breaker.record_success(victim, 0ms);
}

} // namespace example::demo
