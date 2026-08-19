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

#include <catch2/catch_test_macros.hpp>

#include "core/cluster.hxx"
#include "core/collections_component.hxx"
#include "core/core_sdk_shim.hxx"
#include "core/dispatcher.hxx"
#include "core/mcbp/queue_request.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <memory>

namespace
{
using couchbase::core::mcbp::queue_request;

// A request for a non-default collection: those are the ones routed through the
// collection-id cache rather than dispatched directly.
auto
make_request() -> std::shared_ptr<queue_request>
{
  auto request =
    std::make_shared<queue_request>(couchbase::core::protocol::magic::client_request,
                                    couchbase::core::protocol::client_opcode::get,
                                    [](auto /* response */, auto /* request */, auto /* error */) {
                                    });
  request->scope_name_ = "a-scope";
  request->collection_name_ = "a-collection";
  return request;
}

// A cluster that rejects every dispatch, with no I/O and no server: close() marks
// the cluster stopped before it posts anything, and direct_dispatch() on a stopped
// cluster answers cluster_closed.
auto
make_closed_cluster(asio::io_context& io) -> couchbase::core::cluster
{
  auto cluster = couchbase::core::cluster(io);
  cluster.close([]() {
  });
  return cluster;
}

auto
make_component(asio::io_context& io, const couchbase::core::cluster& cluster)
  -> couchbase::core::collections_component
{
  return { io,
           couchbase::core::dispatcher{ "a-bucket", couchbase::core::core_sdk_shim{ cluster } },
           couchbase::core::collections_component_options{ 1024, nullptr } };
}
} // namespace

TEST_CASE("unit: a collection id refresh that cannot be dispatched surfaces the error", "[unit]")
{
  asio::io_context io;
  auto cluster = make_closed_cluster(io);
  auto component = make_component(io, cluster);

  auto op = component.dispatch(make_request());
  REQUIRE_FALSE(op.has_value());
  REQUIRE(op.error() == couchbase::errc::network::cluster_closed);
}

TEST_CASE("unit: a request is released when its collection id refresh cannot be dispatched",
          "[unit]")
{
  // The regression this catches is a leak rather than a wrong answer. dispatch()
  // pushes the request into the cache entry's queue before it asks the server for
  // the collection id, so a resolution that never starts leaves the entry holding
  // the last reference to the request, and through the request's callback the
  // agent and the whole cluster graph behind it.
  asio::io_context io;
  auto cluster = make_closed_cluster(io);
  auto component = make_component(io, cluster);

  std::weak_ptr<queue_request> observer;
  {
    auto request = make_request();
    observer = request;
    REQUIRE_FALSE(component.dispatch(request).has_value());
  }
  REQUIRE(observer.expired());
}
