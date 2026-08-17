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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"
#include "utils/topology_fixtures.hxx"

#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/impl/replica_utils.hxx"
#include "core/impl/with_cancellation.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_replica.hxx"
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

namespace couchbase::test
{
namespace
{
using couchbase::get_replica_strategy;
using couchbase::get_replica_strategy_from_index_options;
using couchbase::replica_index;
using couchbase::core::impl::replica_route_decision;
using couchbase::core::impl::resolve_replica_index;
using couchbase::core::operations::get_replica_request;

using ::test::utils::config_with_vbmap;
using ::test::utils::vbucket_map;

constexpr auto out_of_bounds = errc::key_value::replica_index_out_of_bounds;
constexpr auto unavailable = errc::key_value::replica_index_currently_unavailable;

auto
request_for(std::optional<get_replica_strategy> strategy) -> get_replica_request
{
  get_replica_request request{ couchbase::core::document_id{
    "default", "_default", "_default", "foo" } };
  if (strategy.has_value()) {
    // Same translation collection.cxx performs, so the cases exercise the core
    // type the request actually carries.
    const auto built = strategy->build();
    request.selection = couchbase::core::impl::replica_selection{ built.replica_index, built.wrap };
  }
  return request;
}

/**
 * Resolves against a single-row vbucket map, where the vbucket is always 0
 * regardless of the document key, so every expectation below is a literal.
 */
auto
resolve(vbucket_map map, std::uint32_t num_replicas, replica_index requested, bool wrap)
  -> std::optional<replica_route_decision>
{
  const auto config = config_with_vbmap(std::move(map), num_replicas, /* number_of_nodes = */ 4);
  return resolve_replica_index(config, 0, static_cast<std::size_t>(requested), wrap);
}

// resolve_route() answers with an optional; every helper below requires a decision, so a
// request that declined to resolve fails the case rather than reading a default.
auto
decided(const std::optional<replica_route_decision>& decision,
        source_location loc = source_location::current()) -> replica_route_decision
{
  assert_true(decision.has_value(), "the request resolved its own route", loc);
  return decision.value_or(replica_route_decision{});
}

// The location is forwarded, so a failure names the case rather than this helper.
void
requires_route(const std::optional<replica_route_decision>& maybe,
               std::size_t position,
               std::size_t server,
               source_location loc = source_location::current())
{
  const auto decision = decided(maybe, loc);
  assert_success(decision.ec, "the index resolved to a replica", loc);
  assert_eq(decision.replica_position, position, "the position in the replica chain", loc);
  assert_true(decision.server_index.has_value(), "a server was selected", loc);
  assert_eq(decision.server_index.value(), server, "the selected server index", loc);
}

void
requires_error(const std::optional<replica_route_decision>& maybe,
               std::error_code ec,
               source_location loc = source_location::current())
{
  const auto decision = decided(maybe, loc);
  assert_error(decision.ec, ec, "the index was refused with this code", loc);
  assert_false(decision.server_index.has_value(), "no server was selected", loc);
}

// Neither an error nor a route: the topology cannot answer yet, so the operation retries.
void
requires_retry(const std::optional<replica_route_decision>& maybe,
               source_location loc = source_location::current())
{
  const auto decision = decided(maybe, loc);
  assert_success(decision.ec, "a not-yet-usable topology is not an index error", loc);
  assert_false(decision.server_index.has_value(), "no server was selected", loc);
}

void
from_index_defaults_to_the_first_replica_without_wrap([[maybe_unused]] context& ctx)
{
  const auto strategy = get_replica_strategy::from_index(replica_index::first).build();
  assert_eq(strategy.replica_index, std::size_t{ 0 }, "the requested index");
  assert_false(strategy.wrap, "wrap is off by default");
}

void
from_index_preserves_the_requested_index([[maybe_unused]] context& ctx)
{
  assert_eq(get_replica_strategy::from_index(replica_index::second).build().replica_index,
            std::size_t{ 1 },
            "the second replica");
  assert_eq(get_replica_strategy::from_index(replica_index::third).build().replica_index,
            std::size_t{ 2 },
            "the third replica");
}

void
from_index_options_survive_build([[maybe_unused]] context& ctx)
{
  const auto strategy =
    get_replica_strategy::from_index(replica_index::first,
                                     get_replica_strategy_from_index_options{}.wrap(true))
      .build();
  assert_true(strategy.wrap, "wrap");
}

void
the_first_replica_of_a_one_replica_chain_resolves_to_the_first_position(
  [[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 1,
                         replica_index::first,
                         /* wrap = */ false),
                 /* position = */ 1,
                 /* server = */ 1);
}

void
the_second_replica_resolves_to_the_second_position([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::second,
                         /* wrap = */ false),
                 /* position = */ 2,
                 /* server = */ 2);
}

void
the_third_replica_resolves_to_the_third_position([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2, 3 } },
                         /* num_replicas = */ 3,
                         replica_index::third,
                         /* wrap = */ false),
                 /* position = */ 3,
                 /* server = */ 3);
}

void
an_index_at_the_replica_count_is_out_of_bounds([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 1,
                         replica_index::second,
                         /* wrap = */ false),
                 out_of_bounds);
}

void
an_index_beyond_the_replica_count_is_out_of_bounds([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 1,
                         replica_index::third,
                         /* wrap = */ false),
                 out_of_bounds);
}

void
wrap_resolves_an_index_at_the_replica_count([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 1,
                         replica_index::second,
                         /* wrap = */ true),
                 /* position = */ 1,
                 /* server = */ 1);
}

void
wrap_resolves_an_index_well_beyond_the_replica_count([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 1,
                         replica_index::third,
                         /* wrap = */ true),
                 /* position = */ 1,
                 /* server = */ 1);
}

void
wrap_of_the_third_replica_on_a_two_replica_chain_selects_the_first([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::third,
                         /* wrap = */ true),
                 /* position = */ 1,
                 /* server = */ 1);
}

void
a_bucket_without_replicas_is_out_of_bounds([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0 } },
                         /* num_replicas = */ 0,
                         replica_index::first,
                         /* wrap = */ false),
                 out_of_bounds);
}

void
a_bucket_without_replicas_is_out_of_bounds_even_with_wrap([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0 } },
                         /* num_replicas = */ 0,
                         replica_index::first,
                         /* wrap = */ true),
                 out_of_bounds);
}

void
an_unassigned_replica_is_unavailable([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, -1 } },
                         /* num_replicas = */ 1,
                         replica_index::first,
                         /* wrap = */ false),
                 unavailable);
}

void
without_wrap_an_unassigned_replica_is_not_skipped([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::first,
                         /* wrap = */ false),
                 unavailable);
}

void
wrap_advances_past_an_unassigned_replica([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, -1, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::first,
                         /* wrap = */ true),
                 /* position = */ 2,
                 /* server = */ 2);
}

void
wrap_laps_around_to_the_replica_before_the_requested_one([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 2, -1 } },
                         /* num_replicas = */ 2,
                         replica_index::second,
                         /* wrap = */ true),
                 /* position = */ 1,
                 /* server = */ 2);
}

void
a_chain_without_any_assigned_replica_is_unavailable_under_wrap([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, -1 } },
                         /* num_replicas = */ 2,
                         replica_index::first,
                         /* wrap = */ true),
                 unavailable);
}

void
a_chain_without_any_assigned_replica_is_unavailable_entering_mid_chain(
  [[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, -1, -1 } },
                         /* num_replicas = */ 2,
                         replica_index::second,
                         /* wrap = */ true),
                 unavailable);
}

void
wrap_keeps_a_reachable_replica_an_earlier_gap_precedes([[maybe_unused]] context& ctx)
{
  // The walk starts at the requested position, so an unassigned replica below it
  // is never reached and cannot move the request. Resolving by compacting the row
  // and indexing into what is left would answer from the first replica instead.
  requires_route(resolve(/* map = */ vbucket_map{ { 0, -1, 1, 2 } },
                         /* num_replicas = */ 3,
                         replica_index::third,
                         /* wrap = */ true),
                 /* position = */ 3,
                 /* server = */ 2);
}

void
without_wrap_an_earlier_gap_does_not_move_the_request([[maybe_unused]] context& ctx)
{
  requires_route(resolve(/* map = */ vbucket_map{ { 0, -1, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::second,
                         /* wrap = */ false),
                 /* position = */ 2,
                 /* server = */ 2);
}

void
wrap_advances_past_a_replica_on_a_node_the_topology_does_not_list([[maybe_unused]] context& ctx)
{
  // A server index the node list does not cover is as unusable as a -1, so wrap
  // steps over it for the same reason.
  requires_route(resolve(/* map = */ vbucket_map{ { 0, 9, 2 } },
                         /* num_replicas = */ 2,
                         replica_index::first,
                         /* wrap = */ true),
                 /* position = */ 2,
                 /* server = */ 2);
}

void
an_empty_chain_is_unavailable([[maybe_unused]] context& ctx)
{
  // A row that covers the vbucket but names nobody, active included. The bucket
  // still advertises a replica, so the index is in bounds and the row is what
  // has not arrived.
  requires_error(resolve(/* map = */ vbucket_map{ {} },
                         /* num_replicas = */ 1,
                         replica_index::first,
                         /* wrap = */ false),
                 unavailable);
}

void
a_row_longer_than_the_replica_count_does_not_extend_the_index([[maybe_unused]] context& ctx)
{
  // A replica count being reduced: the lower num_replicas is published at once
  // and the wider rows stay until a rebalance. Bounding on the row would read a
  // copy the bucket no longer keeps.
  requires_error(resolve(/* map = */ vbucket_map{ { 0, 1, 2 } },
                         /* num_replicas = */ 1,
                         replica_index::second,
                         /* wrap = */ false),
                 out_of_bounds);
}

void
the_rfc_worked_examples_resolve_as_the_table_says([[maybe_unused]] context& ctx)
{
  // RFC-0053 GetReplicaStrategy.fromIndex, "Worked examples", one row per entry.
  // The chains there exclude the active, so each row below prepends one: a chain
  // of [1, 2] is written { 0, 1, 2 }. Node indices are kept under the fixture's
  // four nodes, so 7, 3 and 9 become 3, 2 and 3.
  //
  // | chain     | num_replicas | requested | wrap  | result           |
  requires_route(resolve(vbucket_map{ { 0, 1, 2 } }, 2, replica_index::second, false), 2, 2);
  requires_error(resolve(vbucket_map{ { 0, 1, -1 } }, 2, replica_index::second, false),
                 unavailable);
  requires_error(resolve(vbucket_map{ { 0, 1, 2 } }, 2, replica_index::third, false),
                 out_of_bounds);
  requires_error(resolve(vbucket_map{ { 0, 3, 2, 3 } }, 1, replica_index::second, false),
                 out_of_bounds);
  requires_error(resolve(vbucket_map{ { 0, 3 } }, 3, replica_index::second, false), unavailable);

  requires_route(resolve(vbucket_map{ { 0, 3, 2 } }, 2, replica_index::third, true), 1, 3);
  requires_route(resolve(vbucket_map{ { 0, 1, -1, 2 } }, 3, replica_index::third, true), 3, 2);
  requires_route(resolve(vbucket_map{ { 0, -1, 1, 2 } }, 3, replica_index::first, true), 2, 1);
  requires_error(resolve(vbucket_map{ { 0, -1, -1, -1 } }, 3, replica_index::first, true),
                 unavailable);
  requires_route(resolve(vbucket_map{ { 0, 3, 2, 3 } }, 1, replica_index::second, true), 1, 3);
  requires_route(resolve(vbucket_map{ { 0, 3 } }, 3, replica_index::second, true), 1, 3);
}

void
a_row_shorter_than_the_replica_count_is_unavailable_not_out_of_bounds([[maybe_unused]] context& ctx)
{
  // The bucket is configured for two replicas, so the second index exists; the
  // row has not caught up yet, which a rebalance resolves. Reporting this as
  // out_of_bounds would tell a caller to stop asking for an index that is valid.
  requires_error(resolve(/* map = */ vbucket_map{ { 0, 1 } },
                         /* num_replicas = */ 2,
                         replica_index::second,
                         /* wrap = */ false),
                 unavailable);
}

void
a_replica_on_a_node_the_topology_does_not_list_is_unavailable([[maybe_unused]] context& ctx)
{
  requires_error(resolve(/* map = */ vbucket_map{ { 0, 9 } },
                         /* num_replicas = */ 1,
                         replica_index::first,
                         /* wrap = */ false),
                 unavailable);
}

void
an_absent_vbucket_map_is_not_routable_yet([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(std::nullopt, 1, 4);
  requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
}

void
a_map_without_a_configured_replica_count_is_not_routable_yet([[maybe_unused]] context& ctx)
{
  // numReplicas and the vbucket map are parsed independently, so a map can
  // arrive before the count. Bounds cannot be decided without it, and the row
  // alone must not stand in for it.
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1, 2 } }, std::nullopt, 4);
  requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
  requires_retry(resolve_replica_index(config, 0, 2, /* wrap = */ true));
}

void
a_vbucket_the_map_does_not_cover_is_not_routable_yet([[maybe_unused]] context& ctx)
{
  // The row is reached with at(), so dropping the bounds check throws rather
  // than reading out of range.
  const auto config = config_with_vbmap(vbucket_map{ { 0, 1 } }, 1, 4);
  requires_retry(resolve_replica_index(config, 1, 0, /* wrap = */ false));
  requires_retry(resolve_replica_index(config, 99, 0, /* wrap = */ true));
}

void
an_empty_vbucket_map_is_not_routable_yet([[maybe_unused]] context& ctx)
{
  const auto config = config_with_vbmap(vbucket_map{}, 1, 4);
  requires_retry(resolve_replica_index(config, 0, 0, /* wrap = */ false));
}

// Two snapshots of one vbucket: a rebalance that grew or shrank the replica chain between two
// retry attempts of the same request.
auto
wider_chain() -> couchbase::core::topology::configuration
{
  return config_with_vbmap(vbucket_map{ { 0, 1, 2 } }, 2, 4);
}

auto
narrower_chain() -> couchbase::core::topology::configuration
{
  return config_with_vbmap(vbucket_map{ { 0, 1 } }, 1, 4);
}

void
revalidation_reports_an_index_the_new_chain_does_not_have([[maybe_unused]] context& ctx)
{
  auto request = request_for(get_replica_strategy::from_index(replica_index::second));
  requires_route(request.resolve_route(wider_chain()), 2, 2);
  requires_error(request.resolve_route(narrower_chain()), out_of_bounds);
}

void
revalidation_with_wrap_moves_to_a_replica_the_new_chain_has([[maybe_unused]] context& ctx)
{
  auto request = request_for(get_replica_strategy::from_index(
    replica_index::second, get_replica_strategy_from_index_options{}.wrap(true)));
  requires_route(request.resolve_route(wider_chain()), 2, 2);
  requires_route(request.resolve_route(narrower_chain()), 1, 1);
}

void
revalidation_lets_a_grown_chain_satisfy_a_previously_invalid_index([[maybe_unused]] context& ctx)
{
  auto request = request_for(get_replica_strategy::from_index(replica_index::second));
  requires_error(request.resolve_route(narrower_chain()), out_of_bounds);
  requires_route(request.resolve_route(wider_chain()), 2, 2);
}

void
an_unset_strategy_declines_to_resolve_its_own_route([[maybe_unused]] context& ctx)
{
  // The replica fan-out callers leave the strategy unset. Declining here sends them
  // down the dispatch path's vbucket-map routing, which honours id.node_index().
  auto request = request_for({});
  request.id.node_index(2);
  assert_false(
    request.resolve_route(config_with_vbmap(vbucket_map{ { 3, 4, 5 } }, 2, 6)).has_value(),
    "an unset strategy produced no decision");
}

void
only_get_replica_request_resolves_its_own_route([[maybe_unused]] context& ctx)
{
  namespace operations = couchbase::core::operations;
  static_assert(operations::resolves_own_route_v<couchbase::core::operations::get_replica_request>);
  // The cancellable wrapper inherits the request, so losing the trait here
  // would route a strategy through the vbucket map and read the active copy.
  static_assert(
    operations::resolves_own_route_v<operations::get_replica_request_with_cancellation>);
  static_assert(!operations::resolves_own_route_v<operations::get_request>);
  static_assert(!operations::resolves_own_route_v<operations::upsert_request>);
  static_assert(!operations::resolves_own_route_v<operations::get_request_with_cancellation>);
}

void
a_document_missing_from_the_replica_keeps_document_not_found_as_its_cause(
  [[maybe_unused]] context& ctx)
{
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };
  const auto error = couchbase::core::impl::make_get_replica_error(
    couchbase::core::make_key_value_error_context(errc::key_value::document_not_found, id));
  assert_error(error, errc::key_value::document_not_found_on_replica, "the reported code");
  assert_true(error.cause().has_value(), "the general condition is carried as a cause");
  assert_error(error.cause()->ec(), errc::key_value::document_not_found, "the cause");
}

void
every_other_code_passes_through_without_a_cause([[maybe_unused]] context& ctx)
{
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };
  const std::vector<std::error_code> codes{
    errc::key_value::replica_index_out_of_bounds,
    errc::key_value::replica_index_currently_unavailable,
    errc::common::unambiguous_timeout,
    errc::common::ambiguous_timeout,
    errc::common::request_canceled,
    errc::common::feature_not_available,
  };
  for (const auto& ec : codes) {
    const auto error = couchbase::core::impl::make_get_replica_error(
      couchbase::core::make_key_value_error_context(ec, id));
    assert_error(error, ec, "the code is reported unchanged");
    assert_false(error.cause().has_value(), "and carries no cause");
  }
}

void
a_context_without_an_error_yields_no_error([[maybe_unused]] context& ctx)
{
  const couchbase::core::document_id id{ "default", "_default", "_default", "foo" };
  assert_success(couchbase::core::impl::make_get_replica_error(
                   couchbase::core::make_key_value_error_context({}, id)),
                 "a successful context is not turned into an error");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(from_index_defaults_to_the_first_replica_without_wrap), {}, timeout::instant },
      { CASE(from_index_preserves_the_requested_index), {}, timeout::instant },
      { CASE(from_index_options_survive_build), {}, timeout::instant },
      { CASE(the_first_replica_of_a_one_replica_chain_resolves_to_the_first_position),
        {},
        timeout::instant },
      { CASE(the_second_replica_resolves_to_the_second_position), {}, timeout::instant },
      { CASE(the_third_replica_resolves_to_the_third_position), {}, timeout::instant },
      { CASE(an_index_at_the_replica_count_is_out_of_bounds), {}, timeout::instant },
      { CASE(an_index_beyond_the_replica_count_is_out_of_bounds), {}, timeout::instant },
      { CASE(wrap_resolves_an_index_at_the_replica_count), {}, timeout::instant },
      { CASE(wrap_resolves_an_index_well_beyond_the_replica_count), {}, timeout::instant },
      { CASE(wrap_of_the_third_replica_on_a_two_replica_chain_selects_the_first),
        {},
        timeout::instant },
      { CASE(a_bucket_without_replicas_is_out_of_bounds), {}, timeout::instant },
      { CASE(a_bucket_without_replicas_is_out_of_bounds_even_with_wrap), {}, timeout::instant },
      { CASE(an_unassigned_replica_is_unavailable), {}, timeout::instant },
      { CASE(without_wrap_an_unassigned_replica_is_not_skipped), {}, timeout::instant },
      { CASE(wrap_advances_past_an_unassigned_replica), {}, timeout::instant },
      { CASE(wrap_laps_around_to_the_replica_before_the_requested_one), {}, timeout::instant },
      { CASE(a_chain_without_any_assigned_replica_is_unavailable_under_wrap),
        {},
        timeout::instant },
      { CASE(a_chain_without_any_assigned_replica_is_unavailable_entering_mid_chain),
        {},
        timeout::instant },
      { CASE(wrap_keeps_a_reachable_replica_an_earlier_gap_precedes), {}, timeout::instant },
      { CASE(without_wrap_an_earlier_gap_does_not_move_the_request), {}, timeout::instant },
      { CASE(wrap_advances_past_a_replica_on_a_node_the_topology_does_not_list),
        {},
        timeout::instant },
      { CASE(an_empty_chain_is_unavailable), {}, timeout::instant },
      { CASE(a_row_longer_than_the_replica_count_does_not_extend_the_index), {}, timeout::instant },
      { CASE(the_rfc_worked_examples_resolve_as_the_table_says), {}, timeout::instant },
      { CASE(a_row_shorter_than_the_replica_count_is_unavailable_not_out_of_bounds),
        {},
        timeout::instant },
      { CASE(a_replica_on_a_node_the_topology_does_not_list_is_unavailable), {}, timeout::instant },
      { CASE(an_absent_vbucket_map_is_not_routable_yet), {}, timeout::instant },
      { CASE(a_map_without_a_configured_replica_count_is_not_routable_yet), {}, timeout::instant },
      { CASE(a_vbucket_the_map_does_not_cover_is_not_routable_yet), {}, timeout::instant },
      { CASE(an_empty_vbucket_map_is_not_routable_yet), {}, timeout::instant },
      { CASE(revalidation_reports_an_index_the_new_chain_does_not_have), {}, timeout::instant },
      { CASE(revalidation_with_wrap_moves_to_a_replica_the_new_chain_has), {}, timeout::instant },
      { CASE(revalidation_lets_a_grown_chain_satisfy_a_previously_invalid_index),
        {},
        timeout::instant },
      { CASE(an_unset_strategy_declines_to_resolve_its_own_route), {}, timeout::instant },
      { CASE(only_get_replica_request_resolves_its_own_route), {}, timeout::instant },
      { CASE(a_document_missing_from_the_replica_keeps_document_not_found_as_its_cause),
        {},
        timeout::instant },
      { CASE(every_other_code_passes_through_without_a_cause), {}, timeout::instant },
      { CASE(a_context_without_an_error_yields_no_error), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
