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

#pragma once

#include "circuit_breaker.hxx"

#include <couchbase/collection.hxx>

namespace example::demo
{

/**
 * Topology sweep: a long-running process accumulates per-node breaker
 * entries forever unless someone retires the entries for nodes that have
 * left the cluster.  This demo shows the canonical sweep — diff the
 * registry's keys against couchbase::collection::node_ids() and forget
 * the difference — using a synthesized stale entry to stand in for a
 * node that has been removed from the topology since the breaker first
 * saw it.
 */
auto
demo_topology_sweep(couchbase::collection& collection, example::cb::circuit_breaker& breaker)
  -> void;

} // namespace example::demo
