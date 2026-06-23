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

#include "demo_admin.hxx"

#include "ui.hxx"

#include <string>

namespace example::demo
{

auto
demo_admin(example::cb::circuit_breaker& breaker, const couchbase::node_id& victim) -> void
{
  example::ui::print_demo_header("Demo 4",
                                 "Operator overrides",
                                 "An on-call engineer can force the breaker into a specific state "
                                 "for maintenance or incident response.");
  if (!victim) {
    example::ui::print_warning("no victim node available, skipping");
    return;
  }
  breaker.reset(victim);

  example::ui::print_step(
    "force_open() — every call to this node is rejected until disable() or reset().");
  breaker.force_open(victim);
  example::ui::print_note(std::string{ "allow() = " } +
                          (breaker.allow(victim) ? "ALLOWED" : "REJECTED"));

  example::ui::print_step("disable() — the breaker steps aside and every call is "
                          "passed through, even if the node is unhealthy.");
  breaker.disable(victim);
  example::ui::print_note(std::string{ "allow() = " } +
                          (breaker.allow(victim) ? "ALLOWED" : "REJECTED"));

  example::ui::print_step("reset()  — back to a clean slate.");
  breaker.reset(victim);
}

} // namespace example::demo
