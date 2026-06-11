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

#pragma once

#include "core/topology/configuration.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/node_id.hxx>

#include <memory>
#include <string>
#include <system_error>

namespace couchbase::core::impl
{
/**
 * Pure helper that resolves a document key to the owning node's identity
 * against a topology snapshot. Extracted out of @c collection_impl::node_id_for
 * so the error-mapping branches (no config, no vbmap, out-of-range
 * server_index, success) can be unit-tested without an io_context or a live
 * cluster.
 *
 * @param config        bucket configuration snapshot (may be null)
 * @param document_key  document key to map; empty input is reported as
 *                      @c errc::common::invalid_argument
 * @param t             which port pair (plain/TLS) to use when deriving the
 *                      node identity
 * @return pair of (error_code, node_id). On success the @c error_code is
 *         default and the @c node_id is truthy; on failure the @c node_id
 *         is default-constructed (falsy).
 */
[[nodiscard]] inline auto
resolve_node_id_from_config(const std::shared_ptr<topology::configuration>& config,
                            const std::string& document_key,
                            topology::transport t) -> std::pair<std::error_code, couchbase::node_id>
{
  if (document_key.empty()) {
    return { errc::common::invalid_argument, {} };
  }
  if (!config || !config->vbmap.has_value() || config->vbmap->empty()) {
    return { errc::network::configuration_not_available, {} };
  }
  // Only the server_index half of the mapping is needed here; the vbucket id
  // is intentionally discarded (avoids an unused structured-binding name).
  const auto server_index = config->map_key(document_key, 0).second;
  if (!server_index.has_value() || server_index.value() >= config->nodes.size()) {
    // An unresolvable server_index means the vBucket map is present but does
    // not map this key to a known node (out-of-range or missing entry).
    // Surface this as configuration_not_available rather than
    // request_canceled so callers can distinguish an unusable map from an
    // actual caller-initiated cancellation.
    return { errc::network::configuration_not_available, {} };
  }
  auto resolved = config->nodes[server_index.value()].effective_node_id(t);
  if (!resolved) {
    // The node maps to the key but carries no identity for the selected
    // transport (e.g. no KV port for that port pair). Returning success here
    // would violate the contract that a non-error result is always truthy,
    // so report it as an unusable configuration instead.
    return { errc::network::configuration_not_available, {} };
  }
  // Type the first element explicitly so the pair's perfect-forwarding
  // constructor is selected and `resolved` is genuinely moved (a leading `{}`
  // would bind the const-reference constructor instead, defeating the move).
  return { std::error_code{}, std::move(resolved) };
}
} // namespace couchbase::core::impl
