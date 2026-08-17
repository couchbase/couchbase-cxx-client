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

#include "get_replica.hxx"

#include "core/error_context/key_value_error_context.hxx"
#include "core/io/mcbp_context.hxx"

#include <system_error>
#include <utility>

namespace couchbase::core::impl
{
auto
get_replica_request::resolve_route(const topology::configuration& config) -> replica_route_decision
{
  // Named vbucket rather than partition: the request carries a partition member,
  // and MSVC rejects a local that hides it.
  if (!strategy.has_value()) {
    auto [vbucket, server] = config.map_key(id.key(), id.node_index());
    return { {}, vbucket, id.node_index(), server };
  }

  const auto vbucket = config.map_key(id.key(), 0).first;
  auto decision = resolve_replica_index(config, vbucket, strategy->replica_index, strategy->wrap);
  if (!strategy->revalidate_on_retry && decision.server_index.has_value()) {
    // Pin the resolved position and clear the strategy, so later attempts take
    // the branch above: they route to whichever node holds that position and
    // wait for it, rather than re-applying the rules — wrap included — to a
    // chain that may have changed.
    id.node_index(decision.replica_position);
    strategy.reset();
  }
  return decision;
}

auto
get_replica_request::encode_to(get_replica_request::encoded_request_type& encoded,
                               core::mcbp_context&& /* context */) const -> std::error_code
{
  encoded.opaque(opaque);
  encoded.partition(partition);
  encoded.body().id(id);
  return {};
}

auto
get_replica_request::make_response(key_value_error_context&& ctx,
                                   const encoded_response_type& encoded) const
  -> get_replica_response
{
  get_replica_response response{ std::move(ctx) };
  if (!response.ctx.ec()) {
    response.value = encoded.body().value();
    response.cas = encoded.cas();
    response.flags = encoded.body().flags();
  }
  return response;
}
} // namespace couchbase::core::impl
