
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

#pragma once

#include "core/document_id.hxx"
#include "core/error_context/key_value_error_context.hxx"
#include "core/topology/configuration.hxx"

#include "couchbase/error.hxx"
#include "couchbase/read_preference.hxx"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::core::impl
{
struct readable_node {
  bool is_replica;
  std::size_t index;
};

/**
 * Where a replica read should be sent, or why it cannot be sent at all.
 *
 * The three states are distinct: @c ec set is terminal and completes the
 * operation, @c ec clear with no @c server_index means the current topology
 * cannot route the request and the caller should take its retry path, and
 * @c server_index set is the node to dispatch to.
 */
struct replica_route_decision {
  std::error_code ec{};
  std::uint16_t partition{};
  std::size_t replica_position{};
  std::optional<std::size_t> server_index{};
};

/**
 * Resolves a replica index, as the user requested it, against a vbucket map
 * row.
 *
 * The number of replicas of a vbucket is the smaller of what the row lists and
 * what the bucket is configured for: neither alone is authoritative, because
 * the server publishes them independently.
 *
 * @param config current topology
 * @param vbucket the vbucket the document belongs to
 * @param requested_replica zero-based replica index, where 0 is the first
 * replica and the active copy cannot be selected
 * @param wrap resolve an index that is out of bounds or unassigned to the next
 * available replica instead of failing
 */
auto
resolve_replica_index(const topology::configuration& config,
                      std::uint16_t vbucket,
                      std::size_t requested_replica,
                      bool wrap) -> replica_route_decision;

/**
 * Builds the error a replica read completes with.
 *
 * A document missing from the replica is reported as
 * @ref errc::key_value::document_not_found_on_replica, with
 * @ref errc::key_value::document_not_found as its cause so that a caller can
 * opt into the general condition by reading the cause. Every other code is
 * reported as it is.
 */
auto
make_get_replica_error(const key_value_error_context& ctx) -> couchbase::error;

/**
 * Returns list of server indexes to send operations. The index values are
 * in range [0, number_of_replicas).
 *
 * In other words, the result is the subset of the vbucket array, which is
 * filtered by optional read affinity and preferred server group.
 */
auto
effective_nodes(const document_id& id,
                const std::shared_ptr<topology::configuration>& config,
                const read_preference& preference,
                const std::string& preferred_server_group) -> std::vector<readable_node>;
} // namespace couchbase::core::impl
