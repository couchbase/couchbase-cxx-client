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

#include "demo_slow_calls.hxx"

#include "ui.hxx"

#include <string>

namespace example::demo
{

auto
demo_slow_calls(example::cb::circuit_breaker& breaker, const couchbase::node_id& victim) -> void
{
  using namespace std::chrono_literals;
  example::ui::print_demo_header(
    "Demo 5",
    "Slow is the new broken",
    "A node that answers every request but takes too long is just as bad as one that fails "
    "outright — the breaker trips on slow-call rate too.");
  if (!victim) {
    example::ui::print_warning("no victim node available, skipping");
    return;
  }
  breaker.reset(victim);
  example::ui::print_step(
    "Recording 25 successful calls, but each one is above the slow-call threshold (2.0s)...");
  for (int i = 0; i < 25; ++i) {
    breaker.record_success(victim, 3000ms);
  }
  auto m = breaker.metrics_of(victim).value();
  example::ui::print_success(
    "slow-call rate " + std::to_string(m.slow_call_rate_percent) +
    "% crossed the configured threshold and tripped the breaker, even though zero calls failed.");
}

} // namespace example::demo
