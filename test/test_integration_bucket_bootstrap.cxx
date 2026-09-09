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
#include <couchbase/get_options.hxx>
#include <couchbase/get_result.hxx>

#include "core/mcbp/queue_request.hxx"
#include "core/mcbp/queue_response.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/utils/binary.hxx"
#include <asio/executor_work_guard.hpp>

#include <chrono>
#include <couchbase/error_codes.hxx>
#include <couchbase/fail_fast_retry_strategy.hxx>
#include <future>
#include <thread>
#include <utility>

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

// Companion to the case above, on the bootstrap-SUCCESS path. The case above tears the cluster down
// while a bucket that never opens is bootstrapping; here the bucket is a real, reachable one whose
// bootstrap would have succeeded. Tearing the cluster down while that bootstrap is still in flight
// exercises close() stopping an in-flight-but-otherwise-healthy bootstrap session -- the operation
// must still complete (the promise must not be dropped) and the session/bucket/cluster graph must
// not be stranded (verified leak-free by the AddressSanitizer/LeakSanitizer and Valgrind CI jobs).
TEST_CASE("integration: closing a cluster while a valid bucket is still bootstrapping does not "
          "strand the operation",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  std::future<std::pair<couchbase::error, couchbase::get_result>> pending;
  {
    auto cluster = integration.public_cluster();

    // Issue an operation on a valid bucket so its first KV session starts bootstrapping, then tear
    // the cluster down before that bootstrap finishes. If the bootstrap has already completed (fast
    // environment / cached config), the in-flight condition cannot be set up, so skip rather than
    // fail nondeterministically.
    pending = cluster.bucket(integration.ctx.bucket)
                .default_collection()
                .get(test::utils::uniq_id("cxxcbc-852-valid"));

    if (pending.wait_for(0s) != std::future_status::timeout) {
      SUCCEED("operation completed too quickly to exercise an in-flight bucket bootstrap");
      return;
    }
    // `cluster` is destroyed here while the valid bucket's bootstrap is still in flight.
  }

  // The caller-visible guarantee: teardown completes the operation rather than dropping it (which a
  // std::future caller would observe as broken_promise). The exact error code is not asserted --
  // the point is that the future becomes ready at all.
  if (pending.wait_for(std::chrono::seconds{ 10 }) != std::future_status::ready) {
    FAIL("get future was not completed after the cluster was closed -- the operation was stranded");
  }
  SUCCEED("cluster torn down mid-bootstrap of a valid bucket without stranding the operation");
}

namespace
{
// Runs an io_context on its own thread and joins it on every exit path, including an
// exception thrown by open_cluster() or by a failing REQUIRE. Unwinding past a joinable
// std::thread calls std::terminate, which would abort the run instead of reporting.
//
// The work guard is declared before the thread so it is constructed first: an io_context
// with nothing to do returns from run() at once, and the cluster queues its work later.
class io_runner
{
public:
  explicit io_runner(asio::io_context& io)
    : io_{ io }
    , work_{ asio::make_work_guard(io) }
    , thread_{ [&io]() {
      io.run();
    } }
  {
  }

  io_runner(const io_runner&) = delete;
  io_runner(io_runner&&) = delete;
  auto operator=(const io_runner&) -> io_runner& = delete;
  auto operator=(io_runner&&) -> io_runner& = delete;

  ~io_runner()
  {
    work_.reset();
    io_.stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

private:
  asio::io_context& io_;
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  std::thread thread_;
};
} // namespace

// Regression test for CXXCBC-1023, at the cluster level.
//
// cluster_impl::direct_dispatch() dispatches a second time from the open_bucket()
// continuation. Nothing reads the error that second dispatch returns, because
// open_bucket()'s caller has already returned. The continuation therefore has to
// complete the request itself; without that, a request the second dispatch refuses is
// never completed and fails at its deadline.
//
// Reaching that continuation takes two things.
//
// The bucket must not be open when the dispatch starts, or the bucket is found directly
// and open_bucket() is never called. So the test connects a cluster of its own: the
// guard's cluster already has it open.
//
// The second dispatch must then refuse. Asking for a replica index the bucket does not
// have makes that certain: routing names no server for it, no session is started, and a
// fail-fast strategy declines the retry.
TEST_CASE("integration: a request refused after its bucket opens is completed", "[integration]")
{
  test::utils::integration_test_guard integration;
  if (!integration.cluster_version().supports_collections()) {
    SKIP("the server does not support collections");
  }

  asio::io_context io{ 1 };
  const io_runner runner{ io };
  const couchbase::core::cluster cluster{ io };
  test::utils::open_cluster(cluster, integration.origin);

  // The callback runs on the I/O thread; the promise carries its error code back here.
  std::promise<std::error_code> callback_result{};
  auto callback_result_future = callback_result.get_future();
  auto request = std::make_shared<couchbase::core::mcbp::queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [&callback_result](std::shared_ptr<couchbase::core::mcbp::queue_response> /* response */,
                       std::shared_ptr<couchbase::core::mcbp::queue_request> /* request */,
                       std::error_code error) mutable {
      callback_result.set_value(error);
    });
  const auto key = test::utils::uniq_id("cxxcbc-1023");
  request->key_ = couchbase::core::utils::to_binary(key);
  // Beyond any replica the bucket can have, so routing names no server for it.
  request->replica_index_ = 8;
  request->retry_strategy_ = std::make_shared<couchbase::fail_fast_retry_strategy>();

  // The bucket is not open on this cluster, so the dispatch goes through open_bucket().
  const auto rc = cluster.direct_dispatch(integration.ctx.bucket, request);

  const auto callback_ran =
    callback_result_future.wait_for(std::chrono::seconds{ 30 }) == std::future_status::ready;
  const auto callback_ec = callback_ran ? callback_result_future.get() : std::error_code{};

  test::utils::close_cluster(cluster);

  // The callback ran, so the request was completed rather than left to its deadline.
  REQUIRE(callback_ran);

  // The specific code matters. Any error would also be satisfied by open_bucket() failing,
  // which never reaches the continuation under test; service_not_available is what the
  // second dispatch returns when it declines the retry.
  REQUIRE(callback_ec == couchbase::errc::common::service_not_available);

  // direct_dispatch() itself returns success: the refusal happens later, in the continuation.
  REQUIRE_FALSE(rc);
}
