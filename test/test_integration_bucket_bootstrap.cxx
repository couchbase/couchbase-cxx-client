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

#include <chrono>
#include <future>

using namespace std::literals::chrono_literals;

// Regression test for CXXCBC-852.
//
// Opening a bucket starts bootstrapping its first key/value session, and that session is only moved
// into the bucket's session list once it finishes connecting. While it is still bootstrapping it
// lives solely inside its own bootstrap continuation, which captures a copy of the session and the
// bucket. If the cluster is closed before that bootstrap completes, close() -- which stops only the
// already-established sessions -- never stops the in-flight one, so its self-capture strands the
// session, the bucket, and the whole cluster graph.
//
// There is no caller-visible symptom (the pending operation fails either way); the leak is only
// observable to AddressSanitizer/LeakSanitizer, which reports it as an all-indirect (cyclic) leak.
// This test therefore reproduces the teardown-while-bootstrapping scenario and is verified by the
// sanitizer CI job: it leaks without the fix and is clean with it.
TEST_CASE("integration: closing a cluster while a bucket is still bootstrapping does not leak",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  {
    auto cluster = integration.public_cluster();

    // An operation on a bucket that never becomes reachable starts a KV session bootstrapping and
    // leaves it in flight: the bucket never opens, so the operation stays pending and the session
    // is never moved into the established-sessions list. The future is intentionally not awaited --
    // we tear the cluster down while the bootstrap is still going.
    auto pending = cluster.bucket("this_bucket_does_not_exist")
                     .default_collection()
                     .exists(test::utils::uniq_id("cxxcbc-852"));

    // The regression requires the bucket bootstrap to still be in flight when the cluster is torn
    // down. If the operation has already completed (e.g. against a mock, or an unusually fast
    // environment), the in-flight condition cannot be set up, so skip rather than fail
    // nondeterministically.
    if (pending.wait_for(0s) != std::future_status::timeout) {
      SUCCEED("operation completed too quickly to exercise an in-flight bucket bootstrap");
      return;
    }

    // `cluster` is destroyed here, tearing down the io_context while the bucket bootstrap is still
    // in flight. close() must stop the bootstrapping session so its continuation runs and the
    // self-capture is released.
  }

  // The regression is a memory leak with no caller-visible symptom, so the real check is performed
  // by the AddressSanitizer/LeakSanitizer CI job: without the fix the in-flight session (and,
  // reachable only through it, the bucket and the whole cluster graph) is stranded and reported as
  // an all-indirect (cyclic) leak; with the fix the run is clean. Record a passing assertion so the
  // case is not reported as assertion-free.
  SUCCEED("cluster torn down while a bucket was bootstrapping without stranding the session");
}
