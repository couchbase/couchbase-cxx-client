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

#include <couchbase/cluster.hxx>
#include <couchbase/service_type.hxx>

#include <chrono>
#include <set>
#include <string>
#include <system_error>

// wait_until_ready
// ----------------
//
// The C++ SDK does not (yet) expose a `waitUntilReady()` like the other SDKs
// (Java/Go/.NET).  The FIT driver relies on it for test stability: after
// creating a bucket it calls `bucket.waitUntilReady()` to block until the
// bucket is actually serviceable.  When the performer does not advertise the
// capability the driver substitutes a fixed short sleep, which is not enough on
// a slow/loaded cluster for a freshly created bucket to finish placing its
// replica vbuckets -- so the first durable (e.g. MAJORITY) write can come back
// `durability_ambiguous`.
//
// This is a deliberately self-contained, free-standing implementation built
// only on primitives that are (or could be) part of the public SDK surface:
//   * `couchbase::cluster::ping()` / `bucket.ping()` for service health, and
//   * the bucket's topology configuration (vbucket map) for KV readiness.
//
// It is intentionally NOT coupled to any FIT/protobuf type so that it can serve
// as the basis for a future real `cluster.wait_until_ready()` /
// `bucket.wait_until_ready()` SDK feature: lift this file into the SDK, keep the
// algorithm, and swap the core-cluster access for the SDK's internal handle.
namespace fit_cxx::wait_until_ready
{

// Desired terminal state, mirroring the SDK `ClusterState` other SDKs expose.
enum class cluster_state {
  // Every requested endpoint is reachable.
  online,
  // At least one endpoint per requested service is reachable.
  degraded,
  // Not a meaningful target to *wait* for; rejected with invalid_argument.
  offline,
};

struct options {
  std::chrono::milliseconds timeout{ std::chrono::seconds(10) };
  cluster_state desired_state{ cluster_state::online };
  // Empty => consider whichever services the topology reports.
  std::set<couchbase::service_type> service_types{};
};

// Cluster-level readiness.  Polls `cluster.ping()` until the desired-state
// predicate over the requested services holds, or the timeout elapses.
auto
wait_until_ready(const couchbase::cluster& cluster, const options& opts = {}) -> std::error_code;

// Bucket-level readiness.  In addition to the service ping predicate, this first
// waits until the bucket's vbucket map is fully placed -- every vbucket has its
// active and all `num_replicas` replica copies assigned to a node.  That is the
// readiness durable writes require and that a freshly created bucket does not
// satisfy immediately; the cluster-level variant cannot observe it.
auto
wait_until_ready(const couchbase::cluster& cluster,
                 const std::string& bucket_name,
                 const options& opts = {}) -> std::error_code;

} // namespace fit_cxx::wait_until_ready
