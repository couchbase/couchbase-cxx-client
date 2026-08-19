/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// ping()/diagnostics() over couchbase2 (CXXCBC-907). couchbase2 is a thin gRPC client with no
// per-service sessions to probe, and RFC 77 defines no ping/diagnostics semantics for it, so both
// return feature_not_available at the public layer (mirroring Java/Go/.NET) rather than an empty,
// misleading success report. The error is returned before any I/O, so this is env-agnostic: a lazy
// connect to an unroutable endpoint is enough (no gateway required).

#include "framework/test_runner.hxx"

#include <couchbase/cluster.hxx>

#include <string>
#include <utility>

namespace couchbase::test
{
namespace
{
void
ping_and_diagnostics_are_feature_not_available_over_couchbase2()
{
  couchbase::cluster_options options{ "Administrator", "password" };
  // couchbase2:// is TLS; skip verification so the lazy connect does not depend on a real cert.
  options.security().tls_verify(couchbase::tls_verify_mode::none);

  // Port 1 has no listener, but the couchbase2 open is lazy (the channel dials on first use), so
  // connect succeeds without a gateway and ping/diagnostics short-circuit before any I/O.
  auto [connect_err, cluster] =
    couchbase::cluster::connect("couchbase2://127.0.0.1:1", options).get();
  assert_false(connect_err.ec().operator bool(), "connect(couchbase2://) succeeds (lazy)");

  auto [ping_err, ping] = cluster.ping().get();
  static_cast<void>(ping); // only the error code matters: the op is rejected before a report exists
  assert_true(ping_err.ec() == couchbase::errc::common::feature_not_available,
              "cluster.ping() over couchbase2 is feature_not_available");

  auto [diag_err, diag] = cluster.diagnostics().get();
  static_cast<void>(diag);
  assert_true(diag_err.ec() == couchbase::errc::common::feature_not_available,
              "cluster.diagnostics() over couchbase2 is feature_not_available");

  auto [bucket_ping_err, bucket_ping] = cluster.bucket("default").ping().get();
  static_cast<void>(bucket_ping);
  assert_true(bucket_ping_err.ec() == couchbase::errc::common::feature_not_available,
              "bucket.ping() over couchbase2 is feature_not_available");

  cluster.close().get();
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_ping_diagnostics",
    {
      { "ping_and_diagnostics_are_feature_not_available_over_couchbase2",
        ping_and_diagnostics_are_feature_not_available_over_couchbase2,
        timeout::integration,
        test_env::agnostic },
    },
  };
}

} // namespace couchbase::test
