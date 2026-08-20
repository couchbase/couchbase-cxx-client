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

#include "test_helper.hxx"
#include "utils/topology_fixtures.hxx"

#include "core/app_telemetry_meter.hxx"
#include "core/bucket.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/impl/get_replica.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/operations/document_get.hxx"
#include "core/origin.hxx"
#include "core/orphan_reporter.hxx"
#include "core/tls_context_provider.hxx"
#include "core/tracing/tracer_wrapper.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/fail_fast_retry_strategy.hxx>

#include <asio/io_context.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace
{
using test::utils::config_with_vbmap;
using test::utils::vbucket_map;

constexpr std::uint16_t key_value_port{ 11210 };

[[nodiscard]] auto
lazy_options() -> couchbase::core::cluster_options
{
  couchbase::core::cluster_options options{};
  options.enable_lazy_connections = true;
  return options;
}

/**
 * A bucket that opens no sessions of its own. enable_lazy_connections is what
 * buys that: without it update_config() eagerly opens a session per node in the
 * configuration, and since the nodes here carry a loopback key-value address,
 * tearing the fixture down runs those bootstraps for real -- ten seconds of
 * connect retries against a port nothing is listening on, or an indefinite wait
 * where something is. Lazy is also the production precondition for
 * connect_session(), which only runs when no session exists for the index.
 */
class bucket_fixture
{
public:
  bucket_fixture()
  {
    couchbase::core::cluster_credentials auth{};
    auth.username = "Administrator";
    auth.password = "password";
    bucket_ = std::make_shared<couchbase::core::bucket>(
      "client-id",
      ctx_,
      tls_,
      couchbase::core::tracing::tracer_wrapper::create(nullptr, nullptr),
      couchbase::core::metrics::meter_wrapper::create(nullptr, nullptr),
      std::make_shared<couchbase::core::orphan_reporter>(
        ctx_, couchbase::core::orphan_reporter_options{}),
      std::make_shared<couchbase::core::app_telemetry_meter>(),
      "default",
      couchbase::core::origin{ auth, "127.0.0.1", key_value_port, lazy_options() },
      std::vector<couchbase::core::protocol::hello_feature>{},
      nullptr);
  }

  ~bucket_fixture()
  {
    bucket_->close();
    ctx_.run();
  }

  bucket_fixture(const bucket_fixture&) = delete;
  bucket_fixture(bucket_fixture&&) = delete;
  auto operator=(const bucket_fixture&) -> bucket_fixture& = delete;
  auto operator=(bucket_fixture&&) -> bucket_fixture& = delete;

  /**
   * @param rev must increase between calls, since update_config() keeps the
   * newer revision and would otherwise ignore a shrink.
   * @param with_key_value_port when false the nodes carry no address at all,
   * which is the topology a non-key-value node presents to this path.
   */
  void install_config(std::size_t number_of_nodes, std::uint64_t rev, bool with_key_value_port)
  {
    auto config = config_with_vbmap(vbucket_map{ { 0 } }, /*num_replicas=*/0, number_of_nodes);
    config.rev = rev;
    if (with_key_value_port) {
      for (auto& node : config.nodes) {
        node.hostname = "127.0.0.1";
        node.services_plain.key_value = key_value_port;
      }
    }
    bucket_->update_config(std::move(config));
  }

  [[nodiscard]] auto get() const -> const std::shared_ptr<couchbase::core::bucket>&
  {
    return bucket_;
  }

  void run()
  {
    ctx_.run();
  }

private:
  asio::io_context ctx_{};
  couchbase::core::tls_context_provider tls_{};
  std::shared_ptr<couchbase::core::bucket> bucket_{};
};
} // namespace

TEST_CASE("unit: connect_session reports whether anything will drain the deferred queue", "[unit]")
{
  // map_and_send() parks a command on the deferred queue after asking for a
  // session, and only a bootstrap completion drains that queue. A true return
  // therefore promises that a parked command will be woken -- by a pending
  // bootstrap, or by the cancellation a closed bucket applies.
  bucket_fixture fixture{};
  const auto& bucket = fixture.get();

  SECTION("a routable node is connected")
  {
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/true);
    REQUIRE(bucket->connect_session(0));
  }

  SECTION("an index one past the node list names no node")
  {
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/true);
    REQUIRE_FALSE(bucket->connect_session(3));
  }

  SECTION("an index far beyond the node list names no node")
  {
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/true);
    REQUIRE_FALSE(bucket->connect_session(99));
  }

  SECTION("an index valid under an earlier configuration is stale after a shrink")
  {
    // The index is resolved under one lock and consumed under another, so this
    // is the interleaving the bounds check exists for.
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/true);
    fixture.install_config(2, /*rev=*/2, /*with_key_value_port=*/true);
    REQUIRE_FALSE(bucket->connect_session(2));
  }

  SECTION("a node advertising no key-value port starts nothing")
  {
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/false);
    REQUIRE_FALSE(bucket->connect_session(0));
  }

  SECTION("a bucket with no configuration starts nothing")
  {
    REQUIRE_FALSE(bucket->connect_session(0));
  }

  SECTION("a closed bucket reports success, because parking cancels instead")
  {
    // defer_command() completes a command with request_canceled once the bucket
    // is closed, so the caller must park rather than retry.
    fixture.install_config(3, /*rev=*/1, /*with_key_value_port=*/true);
    bucket->close();
    REQUIRE(bucket->connect_session(0));
  }
}

TEST_CASE("unit: a command is not parked behind a session that cannot start", "[unit]")
{
  bucket_fixture fixture{};
  const auto& bucket = fixture.get();
  // Node 0 owns the vbucket and is present in the topology, but advertises no
  // key-value port, so no session opens for it and no bootstrap completion will
  // drain the deferred queue. Parking the command here would hold it until its
  // deadline; the dispatch path has to retry instead, and a strategy that
  // declines to retry turns that into an immediate completion.
  fixture.install_config(1, /*rev=*/1, /*with_key_value_port=*/false);

  // retry_context holds a const member, so the strategy is placed by aggregate
  // initialization rather than assigned after the fact.
  const couchbase::core::operations::get_request request{
    couchbase::core::document_id{ "default", "_default", "_default", "key" },
    /*partition=*/0,
    /*opaque=*/0,
    /*timeout=*/{},
    { std::make_shared<couchbase::fail_fast_retry_strategy>() },
  };

  std::error_code ec{};
  auto completed{ false };
  bucket->execute(request, [&ec, &completed](const auto& response) {
    completed = true;
    ec = response.ctx.ec();
  });
  fixture.run();

  REQUIRE(completed);
  REQUIRE(ec == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: an expired deadline on a replica read is unambiguous", "[unit]")
{
  bucket_fixture fixture{};
  const auto& bucket = fixture.get();
  // The routed node advertises no key-value port, so the operation retries for
  // as long as its deadline allows and never dispatches. A read cannot have
  // mutated anything, so the deadline must classify as unambiguous.
  fixture.install_config(1, /*rev=*/1, /*with_key_value_port=*/false);

  const couchbase::core::impl::get_replica_request request{
    couchbase::core::document_id{ "default", "_default", "_default", "key" },
    /*timeout=*/std::chrono::milliseconds{ 200 },
  };

  std::error_code ec{};
  auto completed{ false };
  bucket->execute(request, [&ec, &completed](const auto& response) {
    completed = true;
    ec = response.ctx.ec();
  });
  fixture.run();

  REQUIRE(completed);
  REQUIRE(ec == couchbase::errc::common::unambiguous_timeout);
}
