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

/*
 * Regression tests for CXXCBC-846.
 *
 * A KV/config continuation must never be parked in a bucket_impl deferred queue that close()
 * has already drained. A stranded continuation strongly captures the owning cluster/bucket, so
 * it forms a shared_ptr reference cycle (cluster_impl -> bucket -> deferred_commands_ ->
 * continuation -> cluster_impl) that survives process exit; LeakSanitizer then reports the whole
 * cluster graph as leaked. The leak was observed in "integration: subdoc any replica reads",
 * whose lookup_in_any_replica op parks its continuation via with_configuration().
 *
 * bucket_impl needs an io_context but no live connection to construct. These tests never call
 * io_context::run() and exercise only defer_command()/with_configuration()/close(), none of
 * which perform network I/O.
 */

#include "core/bucket.hxx"
#include "core/origin.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/tls_context_provider.hxx"
#include "core/topology/configuration.hxx"

#include <catch2/catch_test_macros.hpp>
#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <atomic>
#include <memory>
#include <system_error>
#include <thread>
#include <vector>

namespace
{
// Minimal, network-free bucket: the observability wrappers are only stored (never dereferenced)
// by the deferred-queue / close paths, so nullptr is sufficient.
auto
make_detached_bucket(asio::io_context& ctx, couchbase::core::tls_context_provider& tls)
  -> std::shared_ptr<couchbase::core::bucket>
{
  return std::make_shared<couchbase::core::bucket>(
    "test-client-id",
    ctx,
    tls,
    nullptr, // tracer_wrapper
    nullptr, // meter_wrapper
    nullptr, // orphan_reporter
    nullptr, // app_telemetry_meter
    "test-bucket",
    couchbase::core::origin{},
    std::vector<couchbase::core::protocol::hello_feature>{},
    nullptr); // bootstrap_state_listener
}
} // namespace

TEST_CASE("defer_command on a closed bucket completes the command instead of stranding it",
          "[unit]")
{
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};
  auto bucket = make_detached_bucket(ctx, tls);

  // Marks the bucket closed and drains its (empty) deferred queue.
  bucket->close();

  std::atomic_bool invoked{ false };
  std::error_code observed{};
  bucket->defer_command([&invoked, &observed](std::error_code ec) {
    observed = ec;
    invoked.store(true);
  });

  // Before the fix defer_command emplaced unconditionally, so a command deferred after the drain
  // sat in the queue forever and its handler never ran. The fix rejects enqueues into a closed
  // bucket and completes the command with request_canceled.
  REQUIRE(invoked.load());
  REQUIRE(observed == couchbase::errc::common::request_canceled);
}

TEST_CASE("a closed bucket does not retain a self-referential deferred command", "[unit]")
{
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};

  std::weak_ptr<couchbase::core::bucket> weak;
  {
    auto bucket = make_detached_bucket(ctx, tls);
    weak = bucket;
    bucket->close();

    // Model the production cycle: the continuation captures a strong reference back to the owner
    // (here the bucket itself). A stranded command therefore keeps the bucket alive even with no
    // external reference.
    bucket->defer_command([self = bucket](std::error_code /* ec */) {
      (void)self;
    });
  }

  // The local strong reference is gone. If the command was stranded, the cycle keeps the bucket
  // alive (weak not expired); if it was completed and destroyed, the bucket is freed.
  REQUIRE(weak.expired());
}

TEST_CASE("with_configuration does not strand a continuation while racing close", "[unit]")
{
  // The reported leak parks its continuation via with_configuration() when the bucket is not yet
  // configured. with_configuration() reads closed_ before taking deferred_commands_mutex_, so it
  // can enqueue after close() has already drained the queue. Drive that TOCTOU window directly:
  // launch a batch of with_configuration() calls on a fresh, unconfigured bucket, then close()
  // concurrently. Every continuation must run exactly once regardless of interleaving.
  //
  // A start gate releases every parker and the close() below at (as close as possible to) the
  // same instant, concentrating contention into the check-vs-enqueue window. That hits the race
  // far more reliably than free-running threads, so the counts stay small to keep the unit suite
  // fast and CI-friendly.
  constexpr int rounds = 50;
  constexpr int parkers_per_round = 16;

  for (int round = 0; round < rounds; ++round) {
    asio::io_context ctx;
    couchbase::core::tls_context_provider tls{};
    auto bucket = make_detached_bucket(ctx, tls);

    std::vector<std::shared_ptr<std::atomic_bool>> flags;
    std::vector<std::thread> parkers;
    flags.reserve(parkers_per_round);
    parkers.reserve(parkers_per_round);

    std::atomic_bool go{ false };

    for (int i = 0; i < parkers_per_round; ++i) {
      auto invoked = std::make_shared<std::atomic_bool>(false);
      flags.push_back(invoked);
      parkers.emplace_back([bucket, invoked, &go]() {
        while (!go.load(std::memory_order_acquire)) {
          // Yield instead of a bare busy-spin: the gate still releases all parkers at
          // essentially the same instant, but we don't starve the scheduler on CI runners.
          std::this_thread::yield();
        }
        bucket->with_configuration(
          [invoked](std::error_code /* ec */,
                    std::shared_ptr<couchbase::core::topology::configuration> /* cfg */) {
            invoked->store(true);
          });
      });
    }

    go.store(true, std::memory_order_release);
    bucket->close();
    for (auto& t : parkers) {
      t.join();
    }

    // Whether a continuation was answered by the early closed_ check, drained by close(), or
    // rejected by the fix, it must have been invoked. A continuation enqueued after the drain is
    // never invoked before the fix.
    for (const auto& invoked : flags) {
      REQUIRE(invoked->load());
    }
  }
}

TEST_CASE(
  "with_configuration on an already-closed bucket completes with configuration_not_available "
  "and breaks reference cycles",
  "[unit]")
{
  asio::io_context ctx;
  couchbase::core::tls_context_provider tls{};

  std::weak_ptr<couchbase::core::bucket> weak;
  std::atomic_bool invoked{ false };
  std::error_code observed{};

  {
    auto bucket = make_detached_bucket(ctx, tls);
    weak = bucket;
    bucket->close();

    // Call with_configuration on the closed bucket. The callback captures a strong reference to the
    // bucket to model real-world reference cycles (e.g., cluster_impl capturing bucket, etc.).
    bucket->with_configuration(
      [&invoked, &observed, self = bucket](
        std::error_code ec, std::shared_ptr<couchbase::core::topology::configuration> /* cfg */) {
        observed = ec;
        invoked.store(true);
        (void)self;
      });
  }

  // Verify that the handler was immediately invoked on the closed bucket
  REQUIRE(invoked.load());
  REQUIRE(observed == couchbase::errc::network::configuration_not_available);

  // Since the callback completed immediately, its captured state is freed, meaning the reference
  // cycle is broken. Verify that the bucket's resource is fully cleaned up.
  REQUIRE(weak.expired());
}
