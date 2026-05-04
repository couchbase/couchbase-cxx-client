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

#include "demo_state_transitions.hxx"

#include "ui.hxx"

#include <string>
#include <thread>

namespace example::demo
{

auto
demo_state_transitions(example::cb::circuit_breaker& breaker, const couchbase::node_id& victim)
  -> void
{
  using namespace std::chrono_literals;
  example::ui::print_demo_header("Demo 2",
                                 "A node turns bad, recovers, and rejoins",
                                 "Closed → Open → Half-Open → Closed, walked step by step.");
  if (!victim) {
    example::ui::print_warning("no victim node available, skipping");
    return;
  }

  breaker.reset(victim);
  example::ui::print_step("Victim node: " + std::string{ victim.id() });
  example::ui::print_step("Injecting 25 synthetic failures to simulate an unhealthy node...");
  for (int i = 0; i < 25; ++i) {
    breaker.record_failure(victim, 50ms);
  }
  example::ui::print_note("The circuit has tripped: further calls to this node are now "
                          "short-circuited, protecting client threads from piling up on a bad "
                          "endpoint.");

  bool allowed_during_open = breaker.allow(victim);
  if (allowed_during_open) {
    example::ui::print_warning("unexpected: breaker allowed a call while OPEN");
  } else {
    example::ui::print_success("Attempted call during OPEN was rejected instantly (no round-trip "
                               "to the node).");
  }

  // Sleep just past the configured cool-off so the next allow() promotes
  // the breaker to HALF_OPEN.  Reading the duration from the breaker's
  // config keeps the demo correct if the timing in main.cxx is tweaked.
  auto cool_off = breaker.config().wait_duration_in_open_state() + 100ms;
  example::ui::print_step("Sleeping " + std::to_string(cool_off.count()) +
                          "ms to let the cool-off window elapse...");
  std::this_thread::sleep_for(cool_off);

  example::ui::print_step("The first call after cool-off promotes the circuit to HALF_OPEN "
                          "— a small number of probe requests are now allowed through.");
  for (int i = 1; i <= 3; ++i) {
    if (!breaker.allow(victim)) {
      example::ui::print_warning("probe " + std::to_string(i) + " was not admitted");
      continue;
    }
    // Print before record_success() so the "Probe #N succeeded" note shows
    // up *before* any transition log fired by recording — otherwise the
    // closing transition triggered by the third probe is rendered between
    // probes 2 and 3, which reads as if the close happened first.
    example::ui::print_note("Probe #" + std::to_string(i) + " succeeded.");
    breaker.record_success(victim, 30ms);
  }
  example::ui::print_success(
    "Enough probes succeeded — the breaker closed and normal traffic resumes.");
  example::ui::print_metrics_table(breaker);
}

} // namespace example::demo
