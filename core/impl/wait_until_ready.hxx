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

#include <chrono>
#include <optional>
#include <set>
#include <string>
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
 * Returns true once every vbucket has its active and all replica copies assigned to a node. A
 * freshly created bucket reports an empty/partial vbucket map (replica slots set to -1) until the
 * server finishes placing replicas; durable (MAJORITY) writes are ambiguous until then. Exposed for
 * unit testing.
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
 * the bucket variant additionally waits until the bucket's vbucket map is fully placed (every
 * vbucket has its active and all replica copies assigned) -- the readiness that durable writes
 * require and that a freshly created bucket does not satisfy immediately. The @c degraded target is
 * ping-only, since it deliberately tolerates partial availability.
 *
 * The operation is fully asynchronous: it owns itself for its lifetime and drives the poll cadence
 * with an asio timer on the cluster's io_context; it does not block a thread.
 */
void
wait_until_ready(core::cluster core,
                 std::optional<std::string> bucket_name,
                 std::chrono::milliseconds timeout,
                 couchbase::cluster_state desired_state,
                 std::set<service_type> services,
                 utils::movable_function<void(std::error_code)> handler);
} // namespace couchbase::core::impl
