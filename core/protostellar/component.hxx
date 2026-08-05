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

// The Protostellar KV component: the couchbase2 counterpart to the MCBP data-plane. It owns the
// gRPC stubs over a channel and executes core KV operations by encoding the request (kv_converter),
// dispatching it through the gRPC<->asio bridge, and decoding the reply plus a mapped error
// context. cluster_impl routes KV ops here when the cluster was opened with couchbase2://.

#include "core/cluster_credentials.hxx"
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
#include "core/operations/document_touch.hxx"
#include "core/operations/document_unlock.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/protostellar/dispatcher.hxx"
#include "core/timeout_defaults.hxx"
#include "core/utils/movable_function.hxx"

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>

namespace couchbase::core::protostellar
{

// Construction parameters for `component`, grouped so that adding one is a source-compatible
// change. Every field a caller can reasonably omit carries a default, so a new trailing field
// leaves existing initialisers compiling instead of silently shifting a positional argument onto
// the wrong parameter -- which is the failure this shape exists to prevent, the constructor having
// gained and lost trailing parameters repeatedly as services were added.
struct component_config {
  std::shared_ptr<grpc::Channel> channel{};
  cluster_credentials credentials{};
  std::chrono::milliseconds default_kv_timeout{ timeout_defaults::key_value_timeout };
  std::chrono::milliseconds default_query_timeout{ timeout_defaults::query_timeout };
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

private:
  // The generated gRPC stubs are held behind an opaque pointer so the generated protobuf and gRPC
  // surface stays out of every translation unit that includes this header -- none of it appears in
  // the interface, and the stub count grows with every service the transport learns to speak.
  struct stubs;

  asio::io_context& io_;
  std::unique_ptr<stubs> stubs_;
  std::string authorization_;
  std::chrono::milliseconds default_kv_timeout_;
  std::chrono::milliseconds default_query_timeout_;
  // Declared last, so it is destroyed first: ~dispatcher() cancels and drains the calls issued
  // through the stubs above, which must therefore still be alive while it runs.
  dispatcher dispatcher_;
};

} // namespace couchbase::core::protostellar
