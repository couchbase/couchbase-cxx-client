/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#pragma once

// Views are deprecated in the core API but still routable over couchbase2, so the component names
// document_view_request in its interface. Suppress the core-deprecation attribute for this header's
// includes and declarations (portable convention shared with cluster.cxx). push/pop the macro so an
// including translation unit that already defined it keeps its state restored.
#pragma push_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

// The Protostellar KV component: the couchbase2 counterpart to the MCBP data-plane. It owns the
// gRPC stubs over a channel and executes core KV operations by encoding the request (kv_converter),
// dispatching it through the gRPC<->asio bridge, and decoding the reply plus a mapped error
// context. cluster_impl routes KV ops here when the cluster was opened with couchbase2://.

#include "core/cluster_credentials.hxx"
#include "core/operations/document_analytics.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_exists.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_and_lock.hxx"
#include "core/operations/document_get_and_touch.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_query.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_search.hxx"
#include "core/operations/document_touch.hxx"
#include "core/operations/document_unlock.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/operations/document_view.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/operations/management/bucket_drop.hxx"
#include "core/operations/management/bucket_flush.hxx"
#include "core/operations/management/bucket_get.hxx"
#include "core/operations/management/bucket_get_all.hxx"
#include "core/operations/management/bucket_update.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_drop.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/query_index_build_deferred.hxx"
#include "core/operations/management/query_index_create.hxx"
#include "core/operations/management/query_index_drop.hxx"
#include "core/operations/management/query_index_get_all.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"
#include "core/operations/management/scope_get_all.hxx"
#include "core/protostellar/dispatcher.hxx"
#include "core/timeout_defaults.hxx"
#include "core/utils/movable_function.hxx"

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>

namespace couchbase::core::protostellar
{

// Per-service defaults, used when a request does not carry its own timeout. Each field is defaulted
// from timeout_defaults rather than left value-initialised: a zero here no longer means "no limit"
// but "budget already spent", so a default-constructed aggregate would reject every request that
// relies on the default instead of running it.
struct component_timeouts {
  std::chrono::milliseconds key_value{ timeout_defaults::key_value_timeout };
  std::chrono::milliseconds query{ timeout_defaults::query_timeout };
  std::chrono::milliseconds analytics{ timeout_defaults::analytics_timeout };
  std::chrono::milliseconds search{ timeout_defaults::search_timeout };
  std::chrono::milliseconds view{ timeout_defaults::view_timeout };
  std::chrono::milliseconds management{ timeout_defaults::management_timeout };
};

// Construction parameters for `component`, grouped so that adding one is a source-compatible
// change. Every field a caller can reasonably omit carries a default, so a new trailing field
// leaves existing initialisers compiling instead of silently shifting a positional argument onto
// the wrong parameter -- which is the failure this shape exists to prevent, the constructor having
// gained and lost trailing parameters repeatedly as services were added.
struct component_config {
  std::shared_ptr<grpc::Channel> channel{};
  cluster_credentials credentials{};
  component_timeouts timeouts{};
};

// The core KV request types the component can execute over couchbase2. cluster_impl routes only
// these to the component; any other KV request is answered with feature_not_available.
template<typename Request>
inline constexpr bool component_supports_v = false;
template<>
inline constexpr bool component_supports_v<operations::get_request> = true;
template<>
inline constexpr bool component_supports_v<operations::get_projected_request> = true;
template<>
inline constexpr bool component_supports_v<operations::upsert_request> = true;
template<>
inline constexpr bool component_supports_v<operations::insert_request> = true;
template<>
inline constexpr bool component_supports_v<operations::replace_request> = true;
template<>
inline constexpr bool component_supports_v<operations::remove_request> = true;
template<>
inline constexpr bool component_supports_v<operations::touch_request> = true;
template<>
inline constexpr bool component_supports_v<operations::exists_request> = true;
template<>
inline constexpr bool component_supports_v<operations::get_and_lock_request> = true;
template<>
inline constexpr bool component_supports_v<operations::unlock_request> = true;
template<>
inline constexpr bool component_supports_v<operations::get_and_touch_request> = true;
template<>
inline constexpr bool component_supports_v<operations::increment_request> = true;
template<>
inline constexpr bool component_supports_v<operations::decrement_request> = true;
template<>
inline constexpr bool component_supports_v<operations::append_request> = true;
template<>
inline constexpr bool component_supports_v<operations::prepend_request> = true;

// Default per-service operation timeouts applied when a request does not carry its own. Grouped in
// a struct so adding a service does not grow the component constructor's positional argument list.

class component
{
public:
  component(asio::io_context& io, component_config config);
  component(const component&) = delete;
  component(component&&) = delete;
  auto operator=(const component&) -> component& = delete;
  auto operator=(component&&) -> component& = delete;
  // Out of line: `stubs` is incomplete here, so its deleter cannot be instantiated in this header.
  ~component();

  auto execute(operations::get_request request,
               utils::movable_function<void(operations::get_response)>&& handler) -> pending_call;
  auto execute(operations::get_projected_request request,
               utils::movable_function<void(operations::get_projected_response)>&& handler)
    -> pending_call;
  auto execute(operations::upsert_request request,
               utils::movable_function<void(operations::upsert_response)>&& handler)
    -> pending_call;
  auto execute(operations::insert_request request,
               utils::movable_function<void(operations::insert_response)>&& handler)
    -> pending_call;
  auto execute(operations::replace_request request,
               utils::movable_function<void(operations::replace_response)>&& handler)
    -> pending_call;
  auto execute(operations::remove_request request,
               utils::movable_function<void(operations::remove_response)>&& handler)
    -> pending_call;
  auto execute(operations::touch_request request,
               utils::movable_function<void(operations::touch_response)>&& handler) -> pending_call;
  auto execute(operations::exists_request request,
               utils::movable_function<void(operations::exists_response)>&& handler)
    -> pending_call;
  auto execute(operations::get_and_lock_request request,
               utils::movable_function<void(operations::get_and_lock_response)>&& handler)
    -> pending_call;
  auto execute(operations::unlock_request request,
               utils::movable_function<void(operations::unlock_response)>&& handler)
    -> pending_call;
  auto execute(operations::get_and_touch_request request,
               utils::movable_function<void(operations::get_and_touch_response)>&& handler)
    -> pending_call;
  auto execute(operations::increment_request request,
               utils::movable_function<void(operations::increment_response)>&& handler)
    -> pending_call;
  auto execute(operations::decrement_request request,
               utils::movable_function<void(operations::decrement_response)>&& handler)
    -> pending_call;
  auto execute(operations::append_request request,
               utils::movable_function<void(operations::append_response)>&& handler)
    -> pending_call;
  auto execute(operations::prepend_request request,
               utils::movable_function<void(operations::prepend_response)>&& handler)
    -> pending_call;

  // N1QL query over the couchbase2 server-streaming transport. Rows are buffered into the response;
  // the terminal message carries the metadata.
  auto execute(operations::query_request request,
               utils::movable_function<void(operations::query_response)>&& handler) -> pending_call;

  // Analytics over the couchbase2 server-streaming transport; same shape as query.
  auto execute(operations::analytics_request request,
               utils::movable_function<void(operations::analytics_response)>&& handler)
    -> pending_call;

  // FTS search over the couchbase2 server-streaming transport. Hits are buffered into the response.
  auto execute(operations::search_request request,
               utils::movable_function<void(operations::search_response)>&& handler)
    -> pending_call;

  // Map/reduce views over the couchbase2 server-streaming transport.
  auto execute(operations::document_view_request request,
               utils::movable_function<void(operations::document_view_response)>&& handler)
    -> pending_call;

  // Bucket management (unary admin RPCs over admin.bucket.v1).
  auto execute(
    operations::management::bucket_get_all_request request,
    utils::movable_function<void(operations::management::bucket_get_all_response)>&& handler)
    -> pending_call;
  auto execute(operations::management::bucket_get_request request,
               utils::movable_function<void(operations::management::bucket_get_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::bucket_create_request request,
    utils::movable_function<void(operations::management::bucket_create_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::bucket_update_request request,
    utils::movable_function<void(operations::management::bucket_update_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::bucket_drop_request request,
    utils::movable_function<void(operations::management::bucket_drop_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::bucket_flush_request request,
    utils::movable_function<void(operations::management::bucket_flush_response)>&& handler)
    -> pending_call;

  // Scope/collection management (unary admin RPCs over admin.collection.v1).
  auto execute(
    operations::management::scope_get_all_request request,
    utils::movable_function<void(operations::management::scope_get_all_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::scope_create_request request,
    utils::movable_function<void(operations::management::scope_create_response)>&& handler)
    -> pending_call;
  auto execute(operations::management::scope_drop_request request,
               utils::movable_function<void(operations::management::scope_drop_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::collection_create_request request,
    utils::movable_function<void(operations::management::collection_create_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::collection_update_request request,
    utils::movable_function<void(operations::management::collection_update_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::collection_drop_request request,
    utils::movable_function<void(operations::management::collection_drop_response)>&& handler)
    -> pending_call;

  // Query index management (unary admin RPCs over admin.query.v1).
  auto execute(
    operations::management::query_index_get_all_request request,
    utils::movable_function<void(operations::management::query_index_get_all_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::query_index_create_request request,
    utils::movable_function<void(operations::management::query_index_create_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::query_index_drop_request request,
    utils::movable_function<void(operations::management::query_index_drop_response)>&& handler)
    -> pending_call;
  auto execute(
    operations::management::query_index_build_deferred_request request,
    utils::movable_function<void(operations::management::query_index_build_deferred_response)>&&
      handler) -> pending_call;

private:
  // The generated gRPC stubs are held behind an opaque pointer so the generated protobuf and gRPC
  // surface stays out of every translation unit that includes this header -- none of it appears in
  // the interface, and the stub count grows with every service the transport learns to speak.
  struct stubs;

  asio::io_context& io_;
  std::unique_ptr<stubs> stubs_;
  std::string authorization_;
  component_timeouts timeouts_;
  // Declared last, so it is destroyed first: ~dispatcher() cancels and drains the calls issued
  // through the stubs above, which must therefore still be alive while it runs.
  dispatcher dispatcher_;
};

} // namespace couchbase::core::protostellar

#pragma pop_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
