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

#include "test_helper.hxx"
#include "utils/topology_fixtures.hxx"

#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/impl/get_replica.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/impl/with_cancellation.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/operations_fwd.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/get_replica_strategy.hxx>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

namespace
{
using couchbase::replica_index;
using couchbase::core::impl::get_replica_request;
using couchbase::core::impl::replica_route_decision;
using couchbase::core::impl::resolve_replica_index;
using test::utils::config_with_vbmap;
using test::utils::vbucket_map;

auto
request_for(std::optional<couchbase::get_replica_strategy> strategy) -> get_replica_request
{
  get_replica_request request{ couchbase::core::document_id{
    "default", "_default", "_default", "foo" } };
  if (strategy.has_value()) {
    request.strategy = strategy->build();
  }
  return request;
}

/**
 * Resolves against a single-row vbucket map, where the vbucket is always 0
 * regardless of the document key, so every expectation below is a literal.
 */
auto
resolve(vbucket_map map, std::uint32_t num_replicas, replica_index requested, bool wrap)
  -> replica_route_decision
{
  const auto config = config_with_vbmap(std::move(map), num_replicas, /* number_of_nodes = */ 4);
  return resolve_replica_index(config, 0, static_cast<std::size_t>(requested), wrap);
}

void
requires_route(const replica_route_decision& decision, std::size_t position, std::size_t server)
{
  REQUIRE_FALSE(decision.ec);
  REQUIRE(decision.replica_position == position);
  REQUIRE(decision.server_index == server);
}

void
requires_error(const replica_route_decision& decision, std::error_code ec)
{
  REQUIRE(decision.ec == ec);
  REQUIRE_FALSE(decision.server_index.has_value());
}

void
requires_retry(const replica_route_decision& decision)
{
  REQUIRE_FALSE(decision.ec);
  REQUIRE_FALSE(decision.server_index.has_value());
}
} // namespace

TEST_CASE("unit: get_replica_strategy::from_index", "[unit]")
{
  using couchbase::get_replica_strategy;
  using couchbase::get_replica_strategy_from_index_options;
  using couchbase::replica_index;

  SECTION("defaults")
  {
    const auto strategy = get_replica_strategy::from_index(replica_index::first).build();
    REQUIRE(strategy.replica_index == 0);
    REQUIRE_FALSE(strategy.wrap);
    REQUIRE(strategy.revalidate_on_retry);
  }

  SECTION("index is preserved")
  {
    REQUIRE(get_replica_strategy::from_index(replica_index::second).build().replica_index == 1);
    REQUIRE(get_replica_strategy::from_index(replica_index::third).build().replica_index == 2);
  }

  SECTION("options survive build()")
  {
    const auto strategy =
      get_replica_strategy::from_index(
        replica_index::first,
        get_replica_strategy_from_index_options{}.wrap(true).revalidate_on_retry(false))
        .build();
    REQUIRE(strategy.wrap);
    REQUIRE_FALSE(strategy.revalidate_on_retry);
  }
}

TEST_CASE("unit: resolve_replica_index", "[unit]")
{
  const auto out_of_bounds = couchbase::errc::key_value::replica_index_out_of_bounds;
  const auto unavailable = couchbase::errc::key_value::replica_index_currently_unavailable;

  SECTION("first replica of a one-replica chain")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 1,
                           replica_index::first,
                           /* wrap = */ false),
                   /* position = */ 1,
                   /* server = */ 1);
  }

  SECTION("second replica resolves to the second position")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                           /* num_replicas = */ 2,
                           replica_index::second,
                           /* wrap = */ false),
                   /* position = */ 2,
                   /* server = */ 2);
  }

  SECTION("third replica resolves to the third position")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2, 3 } },
                           /* num_replicas = */ 3,
                           replica_index::third,
                           /* wrap = */ false),
                   /* position = */ 3,
                   /* server = */ 3);
  }

  SECTION("index at the replica count is out of bounds")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 1,
                           replica_index::second,
                           /* wrap = */ false),
                   out_of_bounds);
  }

  SECTION("index beyond the replica count is out of bounds")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 1,
                           replica_index::third,
                           /* wrap = */ false),
                   out_of_bounds);
  }

  SECTION("wrap resolves an index at the replica count")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 1,
                           replica_index::second,
                           /* wrap = */ true),
                   /* position = */ 1,
                   /* server = */ 1);
  }

  SECTION("wrap resolves an index well beyond the replica count")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 1,
                           replica_index::third,
                           /* wrap = */ true),
                   /* position = */ 1,
                   /* server = */ 1);
  }

  SECTION("wrap of the third replica on a two-replica chain selects the first")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                           /* num_replicas = */ 2,
                           replica_index::third,
                           /* wrap = */ true),
                   /* position = */ 1,
                   /* server = */ 1);
  }

  SECTION("a bucket without replicas is out of bounds")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0 } },
                           /* num_replicas = */ 0,
                           replica_index::first,
                           /* wrap = */ false),
                   out_of_bounds);
  }

  SECTION("a bucket without replicas is out of bounds even with wrap")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0 } },
                           /* num_replicas = */ 0,
                           replica_index::first,
                           /* wrap = */ true),
                   out_of_bounds);
  }

  SECTION("an unassigned replica is unavailable")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, -1 } },
                           /* num_replicas = */ 1,
                           replica_index::first,
                           /* wrap = */ false),
                   unavailable);
  }

  SECTION("without wrap an unassigned replica is not skipped")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, 2 } },
                           /* num_replicas = */ 2,
                           replica_index::first,
                           /* wrap = */ false),
                   unavailable);
  }

  SECTION("wrap advances past an unassigned replica")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, -1, 2 } },
                           /* num_replicas = */ 2,
                           replica_index::first,
                           /* wrap = */ true),
                   /* position = */ 2,
                   /* server = */ 2);
  }

  SECTION("wrap laps around to the replica before the requested one")
  {
    requires_route(resolve(/* map = */ vbucket_map{ { 0, 2, -1 } },
                           /* num_replicas = */ 2,
                           replica_index::second,
                           /* wrap = */ true),
                   /* position = */ 1,
                   /* server = */ 2);
  }

  SECTION("a chain without any assigned replica is unavailable under wrap")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, -1 } },
                           /* num_replicas = */ 2,
                           replica_index::first,
                           /* wrap = */ true),
                   unavailable);
  }

  SECTION("a chain without any assigned replica is unavailable entering mid-chain")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, -1 } },
                           /* num_replicas = */ 2,
                           replica_index::second,
                           /* wrap = */ true),
                   unavailable);
  }

  SECTION("the replica count caps a chain longer than num_replicas")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                           /* num_replicas = */ 1,
                           replica_index::second,
                           /* wrap = */ false),
                   out_of_bounds);
  }

  SECTION("the chain length caps a num_replicas larger than the chain")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                           /* num_replicas = */ 2,
                           replica_index::second,
                           /* wrap = */ false),
                   out_of_bounds);
  }

  SECTION("a replica on a node the topology does not list is unavailable")
  {
    requires_error(resolve(/* map = */ vbucket_map{ { 0, 9 } },
                           /* num_replicas = */ 1,
                           replica_index::first,
                           /* wrap = */ false),
                   unavailable);
  }

  SECTION("an absent vbucket map is not routable yet")
  {
    const auto config = config_with_vbmap(std::nullopt, 1, 4);
    requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
  }

  SECTION("a map without a configured replica count is not routable yet")
  {
    // numReplicas and the vbucket map are parsed independently, so a map can
    // arrive before the count. Bounds cannot be decided without it, and the row
    // alone must not stand in for it.
    const auto config = config_with_vbmap(vbucket_map{ { 0, 1, 2 } }, std::nullopt, 4);
    requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
    requires_retry(resolve_replica_index(config, 0, 2, /* wrap = */ true));
  }

  SECTION("a vbucket the map does not cover is not routable yet")
  {
    // The row is reached with at(), so dropping the bounds check throws rather
    // than reading out of range.
    const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1, 4);
    requires_retry(resolve_replica_index(config, 1, 0, /*wrap=*/false));
    requires_retry(resolve_replica_index(config, 99, 0, /*wrap=*/true));
  }

  SECTION("an empty vbucket map is not routable yet")
  {
    const auto config = config_with_vbmap(vbucket_map{}, 1, 4);
    requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
  }
}

TEST_CASE("unit: get_replica_request::resolve_route across topology snapshots", "[unit]")
{
  using couchbase::get_replica_strategy;
  using couchbase::get_replica_strategy_from_index_options;

  // Resolving twice against two snapshots is a rebalance that shrank or grew
  // the replica chain between two retry attempts.
  const auto wider = config_with_vbmap(vbucket_map{ { 0, 1, 2 } }, 2, 4);
  const auto narrower = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1, 4);

  SECTION("revalidation reports an index the new chain does not have")
  {
    auto request = request_for(get_replica_strategy::from_index(replica_index::second));
    requires_route(request.resolve_route(wider), 2, 2);
    requires_error(request.resolve_route(narrower),
                   couchbase::errc::key_value::replica_index_out_of_bounds);
  }

  SECTION("revalidation with wrap moves to a replica the new chain has")
  {
    auto request = request_for(get_replica_strategy::from_index(
      replica_index::second, get_replica_strategy_from_index_options{}.wrap(true)));
    requires_route(request.resolve_route(wider), 2, 2);
    requires_route(request.resolve_route(narrower), 1, 1);
  }

  SECTION("revalidation lets a grown chain satisfy a previously invalid index")
  {
    auto request = request_for(get_replica_strategy::from_index(replica_index::second));
    requires_error(request.resolve_route(narrower),
                   couchbase::errc::key_value::replica_index_out_of_bounds);
    requires_route(request.resolve_route(wider), 2, 2);
  }

  SECTION("without revalidation the resolved replica is pinned and waited for")
  {
    auto request = request_for(get_replica_strategy::from_index(
      replica_index::second, get_replica_strategy_from_index_options{}.revalidate_on_retry(false)));
    requires_route(request.resolve_route(wider), 2, 2);
    REQUIRE(request.id.node_index() == 2);

    requires_retry(request.resolve_route(narrower));
    REQUIRE(request.id.node_index() == 2);
  }
}

TEST_CASE("unit: get_replica_request::resolve_route without a strategy", "[unit]")
{
  SECTION("routes to the pinned node index")
  {
    auto request = request_for({});
    request.id.node_index(2);
    const auto decision =
      request.resolve_route(config_with_vbmap(vbucket_map{ { 3, 4, 5 } }, 2, 6));
    REQUIRE_FALSE(decision.ec);
    REQUIRE(decision.partition == 0);
    REQUIRE(decision.server_index == 5);
  }

  SECTION("an unassigned pinned position is not routable yet")
  {
    auto request = request_for({});
    request.id.node_index(1);
    requires_retry(request.resolve_route(config_with_vbmap(vbucket_map{ { 3, -1 } }, 1, 6)));
  }
}

TEST_CASE("unit: only get_replica_request resolves its own route", "[unit]")
{
  namespace operations = couchbase::core::operations;
  STATIC_REQUIRE(operations::resolves_own_route_v<couchbase::core::impl::get_replica_request>);
  // The cancellable wrapper inherits the request, so losing the trait here
  // would route a strategy through the vbucket map and read the active copy.
  STATIC_REQUIRE(operations::resolves_own_route_v<
                 couchbase::core::operations::get_replica_request_with_cancellation>);
  STATIC_REQUIRE_FALSE(operations::resolves_own_route_v<operations::get_request>);
  STATIC_REQUIRE_FALSE(operations::resolves_own_route_v<operations::upsert_request>);
  STATIC_REQUIRE_FALSE(
    operations::resolves_own_route_v<couchbase::core::operations::get_request_with_cancellation>);
}

TEST_CASE("unit: make_get_replica_error", "[unit]")
{
  using couchbase::core::make_key_value_error_context;
  using couchbase::core::impl::make_get_replica_error;

  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };
  const std::vector<std::error_code> codes{
    couchbase::errc::key_value::document_not_found,
    couchbase::errc::key_value::replica_index_out_of_bounds,
    couchbase::errc::key_value::replica_index_currently_unavailable,
    couchbase::errc::common::unambiguous_timeout,
    couchbase::errc::common::ambiguous_timeout,
    couchbase::errc::common::request_canceled,
    couchbase::errc::common::feature_not_available,
  };

  SECTION("a document missing from the replica keeps document_not_found as its cause")
  {
    const auto error = make_get_replica_error(
      make_key_value_error_context(couchbase::errc::key_value::document_not_found, id));
    REQUIRE(error.ec() == couchbase::errc::key_value::document_not_found_on_replica);
    REQUIRE(error.cause().has_value());
    REQUIRE(error.cause()->ec() == couchbase::errc::key_value::document_not_found);
  }

  SECTION("every other code passes through without a cause")
  {
    for (const auto& ec : codes) {
      if (ec == couchbase::errc::key_value::document_not_found) {
        continue;
      }
      const auto error = make_get_replica_error(make_key_value_error_context(ec, id));
      REQUIRE(error.ec() == ec);
      REQUIRE_FALSE(error.cause().has_value());
    }
  }

  SECTION("a context without an error yields no error")
  {
    REQUIRE_FALSE(make_get_replica_error(make_key_value_error_context({}, id)));
  }
}
