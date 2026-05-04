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

#include "demo_failed_probe.hxx"

#include "ui.hxx"

#include <string>
#include <thread>

namespace example::demo
{

auto
demo_failed_probe(example::cb::circuit_breaker& breaker, const couchbase::node_id& victim) -> void
{
  using namespace std::chrono_literals;
  example::ui::print_demo_header("Demo 3",
                                 "A failing probe re-opens the circuit",
                                 "If the half-open probe shows the node is still unhealthy, the "
                                 "breaker immediately re-opens — no thundering herd.");
  if (!victim) {
    example::ui::print_warning("no victim node available, skipping");
    return;
  }
  breaker.reset(victim);
  example::ui::print_step("Tripping the breaker with 25 synthetic failures...");
  for (int i = 0; i < 25; ++i) {
    breaker.record_failure(victim, 50ms);
  }
  auto cool_off = breaker.config().wait_duration_in_open_state() + 100ms;
  example::ui::print_step("Waiting for the cool-off window (" + std::to_string(cool_off.count()) +
                          "ms)...");
  std::this_thread::sleep_for(cool_off);
  example::ui::print_step("Cool-off elapsed; sending a probe that simulates a still-bad node...");
  if (!breaker.allow(victim)) {
    example::ui::print_warning(
      "probe was not admitted; the breaker did not transition to HALF_OPEN");
    return;
  }
  breaker.record_failure(victim, 50ms);
  example::ui::print_success("One failed probe is enough: the circuit jumped straight back to OPEN "
                             "without needing to see all 25 failures again.");
}

} // namespace example::demo
