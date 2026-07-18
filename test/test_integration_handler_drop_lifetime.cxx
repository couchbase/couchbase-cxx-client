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

#include "test_helper_integration.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/get_options.hxx>
#include <couchbase/get_result.hxx>
#include <couchbase/node_id.hxx>
#include <couchbase/scan_options.hxx>
#include <couchbase/scan_result.hxx>
#include <couchbase/scan_type.hxx>

#include <chrono>
#include <future>
#include <utility>
#include <vector>

using namespace std::literals::chrono_literals;

// Regression test for the handler-drop lifetime fixes in the async public-API operations.
//
// Several self-owning operations wait for a bucket configuration by parking a completion
// continuation in the core's bucket-bootstrap queue. close() does not drain that queue, so tearing
// the cluster down while such an operation is in flight used to drop the continuation without ever
// running it: a std::future caller observed broken_promise and a callback caller hung forever.
//
// A collection scan is the representative path that can be driven into that state deterministically
// through the public API: scanning a bucket that never opens leaves scan()'s with_bucket_config
// wait parked in the bootstrap window, with no dependency on any server-side service being present.
// The sibling paths fixed in the same change (observe-based legacy durability, bucket-level ping,
// watch_query_indexes) share the with_bucket_config_or_timeout scaffold but each needs a successful
// precursor operation or a specific service, so they cannot be forced in-flight-at-teardown without
// a race; this case exercises the shared mechanism through the one deterministic entry point.
//
// Unlike a pure leak (which is only visible to the sanitizer), the fix here restores a
// caller-visible contract, so the test asserts it directly: the future must complete with
// errc::network::cluster_closed rather than be dropped.
TEST_CASE("integration: a scan future completes when the cluster is closed mid-bootstrap",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  std::future<std::pair<couchbase::error, couchbase::scan_result>> pending;
  {
    auto cluster = integration.public_cluster();

    // A scan of a bucket that never becomes reachable parks its bucket-config wait in the
    // bootstrap window: the bucket never opens, so the wait never resolves on its own. The future
    // is intentionally not awaited -- the cluster is torn down while the wait is still parked.
    pending = cluster.bucket("this_bucket_does_not_exist")
                .default_collection()
                .scan(couchbase::range_scan{}, {});

    // The regression requires the scan to still be in flight when the cluster is torn down. If it
    // has already completed (e.g. against a mock, or an unusually fast environment), the in-flight
    // condition cannot be set up, so skip rather than fail nondeterministically.
    if (pending.wait_for(0s) != std::future_status::timeout) {
      SUCCEED("scan completed too quickly to exercise an in-flight bucket bootstrap");
      return;
    }

    // `cluster` is destroyed here, stopping the io_context while the scan's bucket-config wait is
    // still parked. The fail-closed backstop must complete the parked handler.
  }

  // With the fix the fail-closed backstop completes the future shortly after teardown. Bound the
  // wait so a regression (the parked handler stranded, so the future never becomes ready) fails
  // this test in seconds instead of hanging the whole integration run. Once ready, get() surfaces
  // the outcome -- and a dropped-rather-than-stranded promise still fails fast via broken_promise.
  if (pending.wait_for(std::chrono::seconds{ 10 }) != std::future_status::ready) {
    FAIL("scan future was not completed after the cluster was closed -- the handler was stranded");
  }
  auto result = pending.get();
  REQUIRE(result.first.ec() == couchbase::errc::network::cluster_closed);
}

// node_ids() reaches the bucket configuration through the same with_bucket_config_or_timeout
// scaffold as scan(), so it exercises the same parked-in-bootstrap-window teardown path through a
// second public entry point. A bucket that never opens keeps the wait parked; tearing the cluster
// down must complete the future with errc::network::cluster_closed rather than strand it.
TEST_CASE("integration: a node_ids future completes when the cluster is closed mid-bootstrap",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  std::future<std::pair<couchbase::error, std::vector<couchbase::node_id>>> pending;
  {
    auto cluster = integration.public_cluster();

    pending = cluster.bucket("this_bucket_does_not_exist").default_collection().node_ids();

    if (pending.wait_for(0s) != std::future_status::timeout) {
      SUCCEED("node_ids completed too quickly to exercise an in-flight bucket bootstrap");
      return;
    }
    // `cluster` is destroyed here while the bucket-config wait is still parked.
  }

  if (pending.wait_for(std::chrono::seconds{ 10 }) != std::future_status::ready) {
    FAIL(
      "node_ids future was not completed after the cluster was closed -- the handler was stranded");
  }
  auto result = pending.get();
  REQUIRE(result.first.ec() == couchbase::errc::network::cluster_closed);
}

// A key/value get reaches the bucket through direct_dispatch/open_bucket rather than the
// with_bucket_config_or_timeout scaffold, so it exercises the bucket-bootstrap teardown from a
// different call path (the one guarded by bucket_impl::bootstrap()'s closed_ check). The
// caller-visible contract under test is the lifetime guarantee: teardown must complete the future
// -- a dropped continuation would surface to a std::future caller as broken_promise (or hang a
// callback caller) -- regardless of the exact error code the KV path reports.
TEST_CASE("integration: a get future completes when the cluster is closed mid-bootstrap",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  std::future<std::pair<couchbase::error, couchbase::get_result>> pending;
  {
    auto cluster = integration.public_cluster();

    pending = cluster.bucket("this_bucket_does_not_exist")
                .default_collection()
                .get(test::utils::uniq_id("cxxcbc-853-get"));

    if (pending.wait_for(0s) != std::future_status::timeout) {
      SUCCEED("get completed too quickly to exercise an in-flight bucket bootstrap");
      return;
    }
    // `cluster` is destroyed here while the KV operation is still parked in the bootstrap window.
  }

  if (pending.wait_for(std::chrono::seconds{ 10 }) != std::future_status::ready) {
    FAIL("get future was not completed after the cluster was closed -- the handler was stranded");
  }
  auto result = pending.get();
  // The cluster was torn down mid-bootstrap, so the operation must fail -- the point is that it
  // completes at all rather than being dropped.
  REQUIRE(result.first.ec());
}
