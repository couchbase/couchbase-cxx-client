
/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "replica_utils.hxx"

#include "core/impl/error.hxx"
#include "core/logger/logger.hxx"

#include <couchbase/error_codes.hxx>

#include <cstddef>
#include <optional>
#include <utility>

namespace couchbase::core::impl
{
auto
make_get_replica_error(const key_value_error_context& ctx) -> couchbase::error
{
  if (ctx.ec() != errc::key_value::document_not_found) {
    return make_error(ctx);
  }
  auto cause = make_error(ctx);
  auto error_context = cause.ctx();
  return { errc::key_value::document_not_found_on_replica,
           {},
           std::move(error_context),
           std::move(cause),
           ctx.last_dispatched_to_node_id() };
}

auto
resolve_replica_index(const topology::configuration& config,
                      std::uint16_t vbucket,
                      std::size_t requested_replica,
                      bool wrap) -> replica_route_decision
{
  replica_route_decision decision{};
  decision.partition = vbucket;

  if (!config.vbmap.has_value() || vbucket >= config.vbmap->size()) {
    return decision;
  }

  if (!config.num_replicas.has_value()) {
    // The configured count and the map are parsed independently, so a map can
    // arrive without a count. Bounds cannot be decided without it, and taking
    // the row as the count would route to a copy the bucket may not advertise,
    // so this is treated the same as a map that has not arrived.
    return decision;
  }

  // num_replicas bounds the index; the vbucket-map row bounds what is readable.
  // A rebalance reshapes the rows after the new count is published, so a row
  // can be shorter than the count. Such an index is valid and not yet readable,
  // so it is not out of bounds.
  const auto number_of_replicas = std::size_t{ *config.num_replicas };
  if (number_of_replicas == 0) {
    // This also keeps the wrap modulo below from dividing by zero.
    decision.ec = errc::key_value::replica_index_out_of_bounds;
    return decision;
  }

  // The row carries the active at position 0, so replica N is at N + 1 and the
  // chain is one shorter than the row.
  const auto& chain = config.vbmap->at(vbucket);
  const auto chain_length = chain.empty() ? 0 : chain.size() - 1;

  const auto readable = [&](std::size_t replica) -> std::optional<std::size_t> {
    if (replica >= chain_length) {
      return {};
    }
    // A -1 entry and a node index the topology does not list mean the same
    // thing here: nothing to send to.
    if (const auto server = config.server_by_vbucket(vbucket, replica + 1);
        server.has_value() && server.value() < config.nodes.size()) {
      return server;
    }
    return {};
  };

  if (!wrap) {
    if (requested_replica >= number_of_replicas) {
      decision.ec = errc::key_value::replica_index_out_of_bounds;
      return decision;
    }
    if (const auto server = readable(requested_replica); server.has_value()) {
      decision.replica_position = requested_replica + 1;
      decision.server_index = server;
      return decision;
    }
    decision.ec = errc::key_value::replica_index_currently_unavailable;
    return decision;
  }

  // wrap walks num_replicas, not the row, so an index past a short row still
  // reaches the replicas the row does list.
  const auto start = requested_replica % number_of_replicas;
  for (std::size_t offset = 0; offset < number_of_replicas; ++offset) {
    const auto replica = (start + offset) % number_of_replicas;
    if (const auto server = readable(replica); server.has_value()) {
      decision.replica_position = replica + 1;
      decision.server_index = server;
      return decision;
    }
  }

  decision.ec = errc::key_value::replica_index_currently_unavailable;
  return decision;
}

auto
effective_nodes(const document_id& id,
                const std::shared_ptr<topology::configuration>& config,
                const read_preference& preference,
                const std::string& preferred_server_group) -> std::vector<readable_node>
{
  if (preference == read_preference::selected_server_group && preferred_server_group.empty()) {
    CB_LOG_WARNING("Preferred server group is required for zone-aware replica reads");
    return {};
  }

  std::vector<readable_node> available_nodes{};
  std::vector<readable_node> local_nodes{};

  for (std::size_t idx = 0U; idx <= config->num_replicas.value_or(0U); ++idx) {
    auto [vbid, server] = config->map_key(id.key(), idx);
    if (server.has_value() && server.value() < config->nodes.size()) {
      const bool is_replica = idx != 0;
      available_nodes.emplace_back(readable_node{ is_replica, idx });
      if (preferred_server_group == config->nodes[server.value()].server_group) {
        local_nodes.emplace_back(readable_node{ is_replica, idx });
      }
    }
  }

  switch (preference) {
    case read_preference::no_preference:
      return available_nodes;

    case read_preference::selected_server_group:
      return local_nodes;

    case read_preference::selected_server_group_or_all_available:
      if (local_nodes.empty()) {
        return available_nodes;
      }
      return local_nodes;
  }
  return available_nodes;
}
} // namespace couchbase::core::impl
