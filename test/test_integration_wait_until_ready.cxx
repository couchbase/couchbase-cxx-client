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
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/management/bucket_settings.hxx>
#include <couchbase/upsert_options.hxx>

#include <atomic>
#include <chrono>
#include <future>

using namespace std::literals::chrono_literals;

TEST_CASE("integration: cluster wait_until_ready", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto cluster = integration.public_cluster();

  SECTION("reaches online state")
  {
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::key_value });
    auto err = cluster.wait_until_ready(10s, options).get();
    REQUIRE_SUCCESS(err.ec());
  }

  SECTION("reaches degraded state")
  {
    couchbase::wait_until_ready_options options{};
    options.desired_state(couchbase::cluster_state::degraded);
    options.service_types({ couchbase::service_type::key_value });
    auto err = cluster.wait_until_ready(10s, options).get();
    REQUIRE_SUCCESS(err.ec());
  }

  SECTION("offline is not a valid target")
  {
    couchbase::wait_until_ready_options options{};
    options.desired_state(couchbase::cluster_state::offline);
    auto err = cluster.wait_until_ready(10s, options).get();
    REQUIRE(err.ec() == couchbase::errc::common::invalid_argument);
  }

  SECTION("options timeout is rejected in favour of the positional argument")
  {
    couchbase::wait_until_ready_options options{};
    options.timeout(1s);
    auto err = cluster.wait_until_ready(10s, options).get();
    REQUIRE(err.ec() == couchbase::errc::common::invalid_argument);
  }

  SECTION("callback overload completes exactly once")
  {
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::key_value });

    std::atomic<int> calls{ 0 };
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    auto future = barrier->get_future();
    cluster.wait_until_ready(10s, options, [&calls, barrier](auto err) {
      ++calls;
      barrier->set_value(err);
    });
    auto err = future.get();
    REQUIRE_SUCCESS(err.ec());
    // complete() is one-shot, so by the time the future resolves the handler has run exactly once
    // and nothing further is scheduled -- no need to sleep to catch a spurious second invocation.
    REQUIRE(calls.load() == 1);
  }

  SECTION("unavailable service times out")
  {
    if (integration.has_service(couchbase::core::service_type::eventing)) {
      SUCCEED("eventing is deployed on this cluster; cannot assert the unavailable-service path");
      return;
    }
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::eventing });
    auto err = cluster.wait_until_ready(2s, options).get();
    REQUIRE(err.ec() == couchbase::errc::common::unambiguous_timeout);
  }
}

TEST_CASE("integration: bucket wait_until_ready", "[integration]")
{
  test::utils::integration_test_guard integration;

  auto cluster = integration.public_cluster();

  SECTION("existing bucket reaches online state")
  {
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::key_value });
    auto err = cluster.bucket(integration.ctx.bucket).wait_until_ready(10s, options).get();
    REQUIRE_SUCCESS(err.ec());
  }

  SECTION("existing bucket reaches degraded state")
  {
    couchbase::wait_until_ready_options options{};
    options.desired_state(couchbase::cluster_state::degraded);
    options.service_types({ couchbase::service_type::key_value });
    auto err = cluster.bucket(integration.ctx.bucket).wait_until_ready(10s, options).get();
    REQUIRE_SUCCESS(err.ec());
  }

  SECTION("options timeout is rejected in favour of the positional argument")
  {
    couchbase::wait_until_ready_options options{};
    options.timeout(1s);
    auto err = cluster.bucket(integration.ctx.bucket).wait_until_ready(10s, options).get();
    REQUIRE(err.ec() == couchbase::errc::common::invalid_argument);
  }

  SECTION("reaches online state for a non-KV service")
  {
    // A bucket-scoped ping must also report the cluster-level (HTTP) services, not only key/value.
    // Before that fix, requesting a non-KV service here saw no report for it and timed out.
    if (!integration.has_service(couchbase::core::service_type::query)) {
      SUCCEED("query service is not deployed on this cluster");
      return;
    }
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::query });
    auto err = cluster.bucket(integration.ctx.bucket).wait_until_ready(10s, options).get();
    REQUIRE_SUCCESS(err.ec());
  }

  SECTION("callback overload completes exactly once")
  {
    std::atomic<int> calls{ 0 };
    auto barrier = std::make_shared<std::promise<couchbase::error>>();
    auto future = barrier->get_future();
    cluster.bucket(integration.ctx.bucket).wait_until_ready(10s, {}, [&calls, barrier](auto err) {
      ++calls;
      barrier->set_value(err);
    });
    auto err = future.get();
    REQUIRE_SUCCESS(err.ec());
    // complete() is one-shot, so by the time the future resolves the handler has run exactly once
    // and nothing further is scheduled -- no need to sleep to catch a spurious second invocation.
    REQUIRE(calls.load() == 1);
  }

  SECTION("missing bucket times out")
  {
    auto err = cluster.bucket("this_bucket_does_not_exist").wait_until_ready(2s).get();
    REQUIRE(err.ec() == couchbase::errc::common::unambiguous_timeout);
  }
}

TEST_CASE("integration: freshly created bucket is durable-write ready after wait_until_ready",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  if (integration.number_of_nodes() < 2) {
    SUCCEED("majority durability with a replica needs at least two data nodes");
    return;
  }

  auto cluster = integration.public_cluster();
  const auto bucket_name = test::utils::uniq_id("wait_until_ready");

  couchbase::management::cluster::bucket_settings settings{};
  settings.name = bucket_name;
  settings.bucket_type = couchbase::management::cluster::bucket_type::couchbase;
  settings.ram_quota_mb = 100;
  settings.num_replicas = 1;

  auto create_err = cluster.buckets().create_bucket(settings).get();
  REQUIRE_SUCCESS(create_err.ec());

  // wait_until_ready(online) must not return until every vbucket has its active and replica placed,
  // so a majority-durable write issued immediately afterwards is unambiguous.
  couchbase::wait_until_ready_options options{};
  options.service_types({ couchbase::service_type::key_value });
  auto ready_err = cluster.bucket(bucket_name).wait_until_ready(60s, options).get();
  REQUIRE_SUCCESS(ready_err.ec());

  couchbase::upsert_options upsert{};
  upsert.durability(couchbase::durability_level::majority);
  auto [upsert_err, result] =
    cluster.bucket(bucket_name)
      .default_collection()
      .upsert(test::utils::uniq_id("key"), tao::json::value{ { "answer", 42 } }, upsert)
      .get();
  REQUIRE_SUCCESS(upsert_err.ec());

  auto drop_err = cluster.buckets().drop_bucket(bucket_name).get();
  REQUIRE_SUCCESS(drop_err.ec());
}

TEST_CASE("integration: wait_until_ready completes when the cluster is torn down mid-wait",
          "[integration]")
{
  test::utils::integration_test_guard integration;

  // Keep the wait pending by targeting a service the cluster does not run, so it never reaches the
  // desired state. This is a cluster-level wait on purpose: it exercises the teardown path without
  // opening a bucket, so it does not depend on the separate core fix for an in-flight bucket
  // bootstrap being drained on close (which is what a bucket-level teardown here would require).
  if (integration.has_service(couchbase::core::service_type::eventing)) {
    SUCCEED("eventing is deployed on this cluster; cannot keep a cluster-level wait pending");
    return;
  }

  std::future<couchbase::error> future;
  {
    auto cluster = integration.public_cluster();
    couchbase::wait_until_ready_options options{};
    options.service_types({ couchbase::service_type::eventing });
    future = cluster.wait_until_ready(60s, options);
    // The wait is in flight immediately -- eventing never responds, so the future is still pending.
    REQUIRE(future.wait_for(std::chrono::seconds{ 0 }) == std::future_status::timeout);
    // `cluster` is destroyed here, tearing down the io_context while the wait is in flight. The
    // operation is anchored on the io_context (not owned by the core's deferred queues), so its
    // fail-closed completion still runs instead of leaving the promise unset (which a std::future
    // caller would observe as broken_promise) or hanging forever.
  }

  REQUIRE(future.wait_for(30s) == std::future_status::ready);
  couchbase::error err;
  REQUIRE_NOTHROW(err = future.get());
  // Fail closed with the accurate error, not merely "some error": tearing down the io_context while
  // the wait is in flight must surface cluster_closed, never a broken_promise or a timeout.
  REQUIRE(err.ec() == couchbase::errc::network::cluster_closed);
}
