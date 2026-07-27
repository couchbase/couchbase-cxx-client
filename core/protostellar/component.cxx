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

#include "core/protostellar/component.hxx"

#include "core/error_context/analytics.hxx"
#include "core/error_context/key_value.hxx"
#include "core/error_context/query.hxx"
#include "core/operations/analytics_response_parsing.hxx"
#include "core/operations/query_response_parsing.hxx"
#include "core/protostellar/analytics_converter.hxx"
#include "core/protostellar/credentials.hxx"
#include "core/protostellar/error_utils.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/protostellar/query_converter.hxx"

#include <couchbase/error_codes.hxx>

#include "core/protostellar/query_proto.hxx"
#include <couchbase/analytics/v1/analytics.grpc.pb.h>

#include <couchbase/kv/v1/kv.grpc.pb.h>

#include <asio/post.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core::protostellar
{
namespace v1 = ::couchbase::kv::v1;
namespace query_v1 = ::couchbase::query::v1;
namespace analytics_v1 = ::couchbase::analytics::v1;

// Defined here rather than in the header so the generated gRPC types stay out of every consumer of
// component.hxx.
struct component::stubs {
  std::unique_ptr<v1::KvService::Stub> kv;
  std::unique_ptr<query_v1::QueryService::Stub> query;
  std::unique_ptr<analytics_v1::AnalyticsService::Stub> analytics;
};

namespace
{
// A request whose time budget is already spent must not be dispatched. dispatcher::unary only sets
// a deadline when the timeout is positive, so handing it a non-positive one produces a call with no
// deadline at all -- an RPC that runs until the channel breaks, and that cluster::close() then
// blocks on. Turning "no time left" into "no limit" is the wrong direction for the mistake to go.
//
// Nothing is sent, so the operation provably did not reach the server. That makes it an
// unambiguous timeout for every operation, mutating ones included: the ambiguity that
// ambiguous_timeout warns about is whether the server applied the write, and here it never saw it.
template<typename Request, typename Handler>
auto
fail_expired(asio::io_context& io, const Request& request, Handler& handler) -> pending_call
{
  auto response = request.make_response(
    make_key_value_error_context(errc::common::unambiguous_timeout, request.id),
    typename Request::encoded_response_type{});
  // Delivered through the io_context, not inline on the caller's thread: "completions arrive on the
  // SDK's execution context" is the contract every other path here honours, and a synchronous
  // callback out of execute() would also re-enter the caller before it has its pending_call back.
  asio::post(io, [handler = std::move(handler), response = std::move(response)]() mutable {
    handler(std::move(response));
  });
  return {};
}

// The same rule for the services whose responses carry their own error context rather than a
// key_value one: they resolve a timeout exactly as the KV overloads do, so a non-positive one
// would reach the dispatcher and produce a call with no deadline there too. The response the
// overload would have built is stamped and delivered without anything being sent.
template<typename Response, typename Handler>
auto
fail_expired_ctx(asio::io_context& io, Handler& handler, Response response) -> pending_call
{
  response.ctx.ec = errc::common::unambiguous_timeout;
  asio::post(io, [handler = std::move(handler), response = std::move(response)]() mutable {
    handler(std::move(response));
  });
  return {};
}
} // namespace

component::component(asio::io_context& io, component_config config)
  : io_{ io }
  , stubs_{ std::make_unique<stubs>(
      stubs{ v1::KvService::NewStub(config.channel),
             query_v1::QueryService::NewStub(config.channel),
             analytics_v1::AnalyticsService::NewStub(config.channel) }) }
  , authorization_{ authorization_header(config.credentials) }
  , default_kv_timeout_{ config.default_kv_timeout }
  , default_query_timeout_{ config.default_query_timeout }
  , default_analytics_timeout_{ config.default_analytics_timeout }
  // Initialised last (see the declaration order in the header), so the channel can be moved in.
  , dispatcher_{ io, std::move(config.channel) }
{
}

component::~component() = default;

auto
component::execute(operations::get_request request,
                   utils::movable_function<void(operations::get_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::GetRequest>(kv::encode(request));
  const auto id = request.id;
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::GetResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::GetResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Get(&ctx, proto.get(), &resp, std::move(cb));
    },
    // `proto` is captured here too so the request outlives the in-flight call.
    [handler = std::move(handler), id, proto](grpc::Status status, v1::GetResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      // A get reads and nothing more, so a deadline here is unambiguous: the document cannot have
      // changed. kv::decode owns the compressed-content rule (it reports feature_not_available
      // rather than handing back an empty value with a success status).
      auto ctx = make_error_context(status, id, operation_kind::read_only);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::get_projected_request request,
                   utils::movable_function<void(operations::get_projected_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::GetResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::GetResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Get(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto, request](grpc::Status status,
                                                       v1::GetResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::read_only);
      if (status.ok() && resp.content_case() == v1::GetResponse::kContentCompressed) {
        ctx = make_key_value_error_context(errc::common::feature_not_available, id);
      }
      handler(kv::decode(resp, std::move(ctx), request));
    });
}

auto
component::execute(operations::upsert_request request,
                   utils::movable_function<void(operations::upsert_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::UpsertRequest>(kv::encode(request));
  const auto id = request.id;
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::UpsertResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::UpsertResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Upsert(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::UpsertResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::insert_request request,
                   utils::movable_function<void(operations::insert_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::InsertRequest>(kv::encode(request));
  const auto id = request.id;
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::InsertResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::InsertResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Insert(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::InsertResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::replace_request request,
                   utils::movable_function<void(operations::replace_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::ReplaceRequest>(kv::encode(request));
  const auto id = request.id;
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::ReplaceResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::ReplaceResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Replace(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::ReplaceResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::remove_request request,
                   utils::movable_function<void(operations::remove_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::RemoveRequest>(kv::encode(request));
  const auto id = request.id;
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::RemoveResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::RemoveResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Remove(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::RemoveResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::touch_request request,
                   utils::movable_function<void(operations::touch_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::TouchRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::TouchResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::TouchResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Touch(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status, v1::TouchResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::exists_request request,
                   utils::movable_function<void(operations::exists_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::ExistsRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::ExistsResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::ExistsResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Exists(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::ExistsResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::read_only);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::get_and_lock_request request,
                   utils::movable_function<void(operations::get_and_lock_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetAndLockRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::GetAndLockResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        v1::GetAndLockResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->GetAndLock(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::GetAndLockResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      if (status.ok() && resp.content_case() == v1::GetAndLockResponse::kContentCompressed) {
        ctx = make_key_value_error_context(errc::common::feature_not_available, id);
      }
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::unlock_request request,
                   utils::movable_function<void(operations::unlock_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::UnlockRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::UnlockResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::UnlockResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Unlock(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::UnlockResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::get_and_touch_request request,
                   utils::movable_function<void(operations::get_and_touch_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetAndTouchRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::GetAndTouchResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        v1::GetAndTouchResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->GetAndTouch(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::GetAndTouchResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      if (status.ok() && resp.content_case() == v1::GetAndTouchResponse::kContentCompressed) {
        ctx = make_key_value_error_context(errc::common::feature_not_available, id);
      }
      handler(kv::decode(resp, std::move(ctx)));
    });
}

namespace
{
// initial_value is uint64 while the proto's `initial` field is int64, so anything above INT64_MAX
// would reach the gateway as a negative seed for the counter. Refused here rather than in the
// converter: kv::encode() returns the request message by value and has no error channel, so the
// narrowing has to be caught before anything is encoded or sent. The classic path carries the same
// representation mismatch, so this is not a couchbase2 regression -- this boundary is simply the
// first place that can reject it instead of silently wrapping.
template<typename Request>
[[nodiscard]] auto
counter_initial_value_out_of_range(const Request& request) -> bool
{
  return request.initial_value.has_value() &&
         request.initial_value.value() >
           static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
}

// Same delivery contract as fail_expired: nothing is sent, and the completion still arrives on the
// io_context rather than inline on the caller's thread.
template<typename Request, typename Handler>
auto
fail_invalid_argument(asio::io_context& io, const Request& request, Handler& handler)
  -> pending_call
{
  auto response =
    request.make_response(make_key_value_error_context(errc::common::invalid_argument, request.id),
                          typename Request::encoded_response_type{});
  asio::post(io, [handler = std::move(handler), response = std::move(response)]() mutable {
    handler(std::move(response));
  });
  return {};
}
} // namespace

auto
component::execute(operations::increment_request request,
                   utils::movable_function<void(operations::increment_response)>&& handler)
  -> pending_call
{
  if (counter_initial_value_out_of_range(request)) {
    return fail_invalid_argument(io_, request, handler);
  }
  auto proto = std::make_shared<v1::IncrementRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::IncrementResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::IncrementResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Increment(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::IncrementResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::decrement_request request,
                   utils::movable_function<void(operations::decrement_response)>&& handler)
  -> pending_call
{
  if (counter_initial_value_out_of_range(request)) {
    return fail_invalid_argument(io_, request, handler);
  }
  auto proto = std::make_shared<v1::DecrementRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::DecrementResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::DecrementResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Decrement(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::DecrementResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::append_request request,
                   utils::movable_function<void(operations::append_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::AppendRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::AppendResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::AppendResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Append(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::AppendResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::prepend_request request,
                   utils::movable_function<void(operations::prepend_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::PrependRequest>(kv::encode(request));
  const auto id = request.id;
  const auto timeout = request.timeout.value_or(default_kv_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  const auto auth = authorization_;
  auto* stub = stubs_->kv.get();
  return dispatcher_.unary<v1::PrependResponse>(
    timeout,
    [stub, proto, auth](
      grpc::ClientContext& ctx, v1::PrependResponse& resp, std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Prepend(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), id, proto](grpc::Status status,
                                              v1::PrependResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx = make_error_context(status, id, operation_kind::mutating);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(operations::query_request request,
                   utils::movable_function<void(operations::query_response)>&& handler)
  -> pending_call
{
  auto statement = request.statement;
  auto client_context_id = request.client_context_id.value_or(std::string{});
  // Stamped even on the paths that send nothing: the error context identifies which query failed,
  // and a caller correlating by statement or client_context_id has no other handle on it.
  const auto stamped = [&statement, &client_context_id]() {
    operations::query_response response;
    response.ctx.statement = statement;
    response.ctx.client_context_id = client_context_id;
    return response;
  };

  const auto timeout = request.timeout.value_or(default_query_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped());
  }

  // Features with no couchbase2 equivalent (raw passthrough, use_replica, streaming row_callback,
  // node targeting, out-of-range tuning values): fail cleanly rather than silently dropping the
  // caller's intent.
  if (!query::can_encode(request)) {
    auto response = stamped();
    response.ctx.ec = errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired above gives: completions arrive
    // on the SDK's execution context, and calling back inline would re-enter the caller before it
    // has its pending_call.
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }

  auto proto = std::make_shared<query_v1::QueryRequest>(query::encode(request));
  const auto auth = authorization_;
  // N1QL is the one service where read-only-ness is a property of the statement rather than of
  // the operation, and the request already carries the flag, so a timed-out read-only query is
  // reported as unambiguous exactly like a KV read.
  const auto kind = request.readonly ? operation_kind::read_only : operation_kind::mutating;
  auto* stub = stubs_->query.get();

  // Accumulated across streamed messages; shared between the row and completion callbacks.
  auto rows = std::make_shared<std::vector<std::string>>();
  auto meta = std::make_shared<operations::query_response::query_meta_data>();
  // Whether a MetaData message arrived at all, which is not the same as what it said. The gateway
  // leaves MetaData nil when its own result.MetaData() fails and still ends the stream OK, and an
  // absent status must not be classified as a failed one.
  auto meta_received = std::make_shared<bool>(false);

  return dispatcher_.server_stream<query_v1::QueryResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        grpc::ClientReadReactor<query_v1::QueryResponse>* reactor) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->Query(&ctx, proto.get(), reactor);
    },
    [rows, meta, meta_received](query_v1::QueryResponse message) {
      for (const auto& row : message.rows()) {
        rows->push_back(row);
      }
      if (message.has_meta_data()) {
        *meta_received = true;
        query::decode_meta_data(message.meta_data(), *meta);
      }
    },
    // `proto` is captured so the request outlives the in-flight stream.
    [handler = std::move(handler),
     rows,
     meta,
     meta_received,
     proto,
     statement,
     client_context_id,
     kind](grpc::Status status) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::query_response response;
      response.ctx.ec = map_status(status, kind);
      response.ctx.statement = std::move(statement);
      response.ctx.client_context_id =
        meta->client_context_id.empty() ? std::move(client_context_id) : meta->client_context_id;
      if (!status.ok()) {
        response.ctx.first_error_message = error_message(status);
      }
      response.meta = std::move(*meta);
      response.rows = std::move(*rows);
      // A terminal status other than "success" arriving with an OK trailer would otherwise be
      // reported as a successful query whose own metadata says it failed. map_query_error is the
      // classifier the classic path uses, so the two transports agree on the mapping; the proto
      // carries no errors list, and for that case it yields internal_server_failure -- the same
      // fallback document_query.cxx applies when the payload has no error code to classify.
      //
      // A transport-level error already in ctx.ec is more specific, so it wins.
      if (!response.ctx.ec && *meta_received) {
        response.ctx.ec = operations::map_query_error(response.meta);
      }
      handler(std::move(response));
    });
}

auto
component::execute(operations::analytics_request request,
                   utils::movable_function<void(operations::analytics_response)>&& handler)
  -> pending_call
{
  auto statement = request.statement;
  auto client_context_id = request.client_context_id.value_or(std::string{});
  // Stamped even on the paths that send nothing: the error context identifies which statement
  // failed, and a caller correlating by statement or client_context_id has no other handle on it.
  const auto stamped = [&statement, &client_context_id]() {
    operations::analytics_response response;
    response.ctx.statement = statement;
    response.ctx.client_context_id = client_context_id;
    return response;
  };

  const auto timeout = request.timeout.value_or(default_analytics_timeout_);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped());
  }

  // Scoped analytics, raw passthrough, and the streaming row_callback have no couchbase2 mapping:
  // fail cleanly rather than dropping the caller's intent.
  if (!analytics::can_encode(request)) {
    auto response = stamped();
    response.ctx.ec = errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired above gives: completions arrive
    // on the SDK's execution context, and calling back inline would re-enter the caller before it
    // has its pending_call.
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }

  auto proto = std::make_shared<analytics_v1::AnalyticsQueryRequest>(analytics::encode(request));
  const auto auth = authorization_;
  // Analytics carries the same readonly flag as N1QL, so the same RFC 77 rule applies.
  const auto kind = request.readonly ? operation_kind::read_only : operation_kind::mutating;
  auto* stub = stubs_->analytics.get();

  auto rows = std::make_shared<std::vector<std::string>>();
  auto meta = std::make_shared<operations::analytics_response::analytics_meta_data>();
  // Whether a MetaData message arrived, which the status alone cannot tell us: analytics_status's
  // first enumerator is `running`, so a default-constructed meta is indistinguishable from one the
  // gateway reported as running. Classifying without this would turn a stream that ended OK with no
  // metadata into an error.
  auto meta_received = std::make_shared<bool>(false);

  return dispatcher_.server_stream<analytics_v1::AnalyticsQueryResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        grpc::ClientReadReactor<analytics_v1::AnalyticsQueryResponse>* reactor) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->AnalyticsQuery(&ctx, proto.get(), reactor);
    },
    [rows, meta, meta_received](analytics_v1::AnalyticsQueryResponse message) {
      for (const auto& row : message.rows()) {
        rows->push_back(row);
      }
      if (message.has_meta_data()) {
        *meta_received = true;
        analytics::decode_meta_data(message.meta_data(), *meta);
      }
    },
    // `proto` is captured so the request outlives the in-flight stream.
    [handler = std::move(handler),
     rows,
     meta,
     meta_received,
     proto,
     statement,
     client_context_id,
     kind](grpc::Status status) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::analytics_response response;
      response.ctx.ec = map_status(status, kind);
      response.ctx.statement = std::move(statement);
      response.ctx.client_context_id =
        meta->client_context_id.empty() ? std::move(client_context_id) : meta->client_context_id;
      if (!status.ok()) {
        response.ctx.first_error_message = error_message(status);
      }
      response.meta = std::move(*meta);
      response.rows = std::move(*rows);
      // A terminal status other than success arriving with an OK trailer would otherwise be
      // reported as a successful query whose own metadata says it failed. map_analytics_error is
      // the classifier the classic path uses, so both transports agree; the proto carries no errors
      // list, and for that case it yields internal_server_failure -- the fallback its own comment
      // documents for exactly this caller.
      //
      // A transport-level error already in ctx.ec is more specific, so it wins.
      if (!response.ctx.ec && *meta_received) {
        response.ctx.ec = operations::map_analytics_error(response.meta);
      }
      handler(std::move(response));
    });
}

} // namespace couchbase::core::protostellar
