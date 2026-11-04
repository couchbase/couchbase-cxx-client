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

#include "core/cluster.hxx"
#include "core/diagnostics.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"
#include "core/utils/movable_function.hxx"

#include <couchbase/cluster_state.hxx>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>

namespace couchbase::core::impl
{
/**
 * Returns whether @p report satisfies @p state for the @p requested services.
 *
 * online   : every requested service has at least one endpoint and ALL of them are ok.
 * degraded : every requested service has at least one endpoint that is ok.
 *
 * When @p requested is empty, whichever services the report contains are considered; an empty
 * report (nothing pinged back yet) is treated as not-ready. Exposed for unit testing.
 */
auto
ping_predicate_satisfied(const diag::ping_result& report,
                         couchbase::cluster_state state,
                         const std::set<service_type>& requested) -> bool;

/**
 * Returns the number of replica copies of each vbucket the data nodes can hold: num_replicas,
 * capped at one per data node besides the one holding the active. The server leaves the remaining
 * replica slots at -1 for as long as no node is added. A configuration listing no data node is not
 * capped. Exposed for unit testing.
 */
auto
placeable_replicas(const topology::configuration& config) -> std::uint32_t;

/**
 * Returns how many of the configured replicas the data nodes cannot hold. Exposed for unit testing.
 */
auto
unplaceable_replicas(const topology::configuration& config) -> std::uint32_t;

/**
 * Logs a warning naming @p bucket_name when unplaceable_replicas() is non-zero and @p logged is
 * still false, then sets @p logged. Exposed for unit testing.
 */
void
warn_if_replicas_unplaceable(const topology::configuration& config,
                             std::string_view bucket_name,
                             std::atomic_bool& logged);

/**
 * Returns true once every vbucket has its active and placeable_replicas() replica copies assigned
 * to a node. A freshly created bucket reports an empty/partial vbucket map (replica slots set to
 * -1) until the server finishes placing replicas; durable (MAJORITY) writes are ambiguous until
 * then. Exposed for unit testing.
 */
auto
vbucket_map_ready(const topology::configuration& config) -> bool;

/**
 * Polls the cluster (or, when @p bucket_name is set, a bucket) until it reaches @p desired_state or
 * @p timeout elapses, then invokes @p handler exactly once with the resulting error code (empty on
 * success, @c errc::common::unambiguous_timeout on timeout, @c errc::common::invalid_argument when
 * @p desired_state is @c cluster_state::offline, @c errc::network::cluster_closed when the
 * io_context is torn down while the wait is in flight).
 *
 * Readiness is assessed with @c cluster::ping(). When @p desired_state is @c cluster_state::online
 * the bucket variant additionally waits until every vbucket has its active and
 * placeable_replicas() replica copies assigned -- the readiness that durable writes require and
 * that a freshly created bucket does not satisfy immediately. Replicas beyond what the data nodes
 * can hold are not awaited, and warn_if_replicas_unplaceable() logs them once per wait. The @c
 * degraded target is ping-only, since it deliberately tolerates partial availability.
 *
 * The operation is fully asynchronous: it owns itself for its lifetime and drives the poll cadence
 * with an asio timer on the cluster's io_context; it does not block a thread.
 *
 * On a couchbase2:// (Protostellar) cluster there is no MCBP bootstrap to ping, so readiness is
 * instead the gRPC channel's connectivity state: the call polls until the channel reaches READY or
 * @p timeout elapses (mirrors Java/Go; no health RPC is sent). @p bucket_name and @p services have
 * no per-bucket/per-service connection to probe and are ignored; @c cluster_state::offline is still
 * rejected with @c errc::common::invalid_argument.
 */
void
wait_until_ready(core::cluster core,
                 std::optional<std::string> bucket_name,
                 std::chrono::milliseconds timeout,
                 couchbase::cluster_state desired_state,
                 std::set<service_type> services,
                 utils::movable_function<void(std::error_code)> handler);
} // namespace couchbase::core::impl
