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

#include "demo_healthy_traffic.hxx"

#include "demo_common.hxx"
#include "ui.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/mutation_result.hxx>

#include <cstdint>
#include <future>
#include <string>
#include <utility>

namespace example::demo
{

auto
demo_healthy_traffic(couchbase::collection& collection, example::cb::circuit_breaker& breaker)
  -> void
{
  example::ui::print_demo_header("Demo 1",
                                 "Healthy traffic",
                                 "While every node is responsive, the breaker stays out of the way "
                                 "— every node remains CLOSED.");
  const tao::json::value doc{ { "type", "cb-demo" }, { "value", 42 } };
  constexpr int total_ops = 30;
  example::ui::print_step("Sending " + std::to_string(total_ops) +
                          " upsert operations through the breaker...");
  std::uint32_t ok = 0;
  std::uint32_t failed = 0;
  std::uint32_t unresolved = 0;
  for (int i = 0; i < total_ops; ++i) {
    auto key = "cb-demo-" + std::to_string(i);
    auto [nid_err, target] = collection.node_id_for(key).get();
    if (nid_err) {
      example::ui::print_error("node_id_for(\"" + key + "\") failed: " + nid_err.ec().message());
      ++unresolved;
      continue;
    }
    auto [err, resp] = breaker.execute(
      target,
      [&]() -> std::future<std::pair<couchbase::error, couchbase::mutation_result>> {
        return collection.upsert(key, doc, {});
      },
      is_breaker_failure);
    if (err) {
      ++failed;
    } else {
      ++ok;
    }
  }
  if (failed == 0 && unresolved == 0) {
    example::ui::print_success("All " + std::to_string(total_ops) +
                               " operations completed successfully — nothing to trip the breaker.");
  } else {
    auto msg = std::to_string(ok) + " succeeded, " + std::to_string(failed) + " failed";
    if (unresolved != 0) {
      msg += ", " + std::to_string(unresolved) + " skipped (node_id_for failed)";
    }
    msg += " — but still below the trip threshold.";
    example::ui::print_warning(msg);
  }
  example::ui::print_metrics_table(breaker);
}

} // namespace example::demo
