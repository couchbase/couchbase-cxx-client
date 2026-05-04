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

#include "demo_per_node_isolation.hxx"

#include "ui.hxx"

#include <cstdint>
#include <string>

namespace example::demo
{

auto
demo_per_node_isolation(couchbase::collection& collection, example::cb::circuit_breaker& breaker)
  -> void
{
  using namespace std::chrono_literals;
  example::ui::print_demo_header("Demo 6",
                                 "One sick node doesn't poison the rest",
                                 "Per-node isolation: only keys routed to the bad node are "
                                 "short-circuited — traffic to healthy nodes keeps "
                                 "flowing.");

  auto [nid_err, victim] = collection.node_id_for("cb-demo-0").get();
  if (nid_err || !victim) {
    example::ui::print_warning("could not resolve a victim node, skipping");
    return;
  }
  breaker.reset(victim);
  example::ui::print_step("Poisoning one node with 25 synthetic failures...");
  for (int i = 0; i < 25; ++i) {
    breaker.record_failure(victim, 50ms);
  }

  std::uint32_t short_circuited = 0;
  std::uint32_t passed = 0;
  std::uint32_t unresolved = 0;
  constexpr int total_keys = 30;
  for (int i = 0; i < total_keys; ++i) {
    auto key = "cb-demo-iso-" + std::to_string(i);
    auto [e, target] = collection.node_id_for(key).get();
    if (e) {
      ++unresolved;
      continue;
    }
    if (!breaker.allow(target)) {
      ++short_circuited;
    } else {
      ++passed;
    }
  }
  auto summary = "Routed " + std::to_string(passed + short_circuited) + " of " +
                 std::to_string(total_keys) +
                 " keys across the cluster: " + std::to_string(passed) + " allowed to proceed, " +
                 std::to_string(short_circuited) +
                 " short-circuited because they happen to map to the poisoned node.";
  if (unresolved != 0) {
    summary += " " + std::to_string(unresolved) +
               " key(s) could not be resolved to a node and were skipped.";
  }
  example::ui::print_success(summary);
  example::ui::print_metrics_table(breaker);

  breaker.reset(victim);
}

} // namespace example::demo
