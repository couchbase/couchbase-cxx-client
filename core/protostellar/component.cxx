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
#include "core/error_context/search.hxx"
#include "core/error_context/view.hxx"
#include "core/operations/analytics_response_parsing.hxx"
#include "core/operations/query_response_parsing.hxx"
#include "core/protostellar/analytics_converter.hxx"
#include "core/protostellar/bucket_admin_converter.hxx"
#include "core/protostellar/collection_admin_converter.hxx"
#include "core/protostellar/credentials.hxx"
#include "core/protostellar/error_utils.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/protostellar/query_converter.hxx"
#include "core/protostellar/query_index_admin_converter.hxx"
#include "core/protostellar/search_converter.hxx"
#include "core/protostellar/search_index_admin_converter.hxx"
#include "core/protostellar/view_converter.hxx"

#include <couchbase/error_codes.hxx>

#include "core/protostellar/query_proto.hxx"
#include <couchbase/analytics/v1/analytics.grpc.pb.h>

#include <couchbase/admin/bucket/v1/bucket.grpc.pb.h>
#include <couchbase/admin/collection/v1/collection.grpc.pb.h>
#include <couchbase/admin/query/v1/query.grpc.pb.h>
#include <couchbase/admin/search/v1/search.grpc.pb.h>
#include <couchbase/kv/v1/kv.grpc.pb.h>
#include <couchbase/search/v1/search.grpc.pb.h>
#include <couchbase/view/v1/view.grpc.pb.h>

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
namespace search_v1 = ::couchbase::search::v1;
namespace view_v1 = ::couchbase::view::v1;
namespace bucket_admin_v1 = ::couchbase::admin::bucket::v1;
namespace collection_admin_v1 = ::couchbase::admin::collection::v1;
namespace query_admin_v1 = ::couchbase::admin::query::v1;
namespace search_admin_v1 = ::couchbase::admin::search::v1;

// Defined here rather than in the header so the generated gRPC types stay out of every consumer of
// component.hxx.
struct component::stubs {
  std::unique_ptr<v1::KvService::Stub> kv;
  std::unique_ptr<query_v1::QueryService::Stub> query;
  std::unique_ptr<analytics_v1::AnalyticsService::Stub> analytics;
  std::unique_ptr<search_v1::SearchService::Stub> search;
  std::unique_ptr<view_v1::ViewService::Stub> view;
  std::unique_ptr<bucket_admin_v1::BucketAdminService::Stub> bucket_admin;
  std::unique_ptr<collection_admin_v1::CollectionAdminService::Stub> collection_admin;
  std::unique_ptr<query_admin_v1::QueryAdminService::Stub> query_admin;
  std::unique_ptr<search_admin_v1::SearchAdminService::Stub> search_admin;
};

namespace
{
// A request whose time budget is already spent must not be dispatched. dispatcher::unary only sets
// a deadline when the timeout is positive, so handing it a non-positive one produces a call with no
// deadline at all -- an RPC that runs until the channel breaks, and that cluster::close() then
// blocks on. Turning "no time left" into "no limit" is the wrong direction for the mistake to go.
//
// This attempt sends nothing, but that does not make the operation unsent: the retry loop reaches
// here to end an operation whose earlier attempts were dispatched. The ambiguity ambiguous_timeout
// warns about is whether the server applied a write, so it is decided by whether anything was ever
// sent, not by whether this attempt was -- the same rule the MCBP path applies in
// mcbp_command::cancel(). An idempotent operation is safe to replay either way, and a first attempt
// (no retries recorded) provably reached nobody.
template<typename Request, typename Handler>
auto
fail_expired(asio::io_context& io, const Request& request, Handler& handler) -> pending_call
{
  const auto ec =
    (request.retries.idempotent() || request.retries.retry_attempts() == 0)
      ? errc::common::unambiguous_timeout // safe to retry, or never sent to the server
      : errc::common::ambiguous_timeout;  // non-idempotent and dispatched at least once
  auto response = request.make_response(
    make_key_value_error_context(
      ec, request.id, request.retries.retry_attempts(), request.retries.retry_reasons()),
    typename Request::encoded_response_type{});
  // Delivered through the io_context, not inline on the caller's thread: "completions arrive on the
  // SDK's execution context" is the contract every other path here honours, and a synchronous
  // callback out of execute() would also re-enter the caller before it has its pending_call back.
  asio::post(io, [handler = std::move(handler), response = std::move(response)]() mutable {
    handler(std::move(response));
  });
  return {};
}

// Deliver a prepared response carrying ec without sending anything. Posted rather than invoked:
// completions arrive on the io context, never inline out of execute(), which would re-enter the
// caller before it holds its pending_call.
template<typename Response, typename Handler>
auto
fail_ctx(asio::io_context& io, Handler& handler, Response response, std::error_code ec)
  -> pending_call
{
  response.ctx.ec = ec;
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
  return fail_ctx(io, handler, std::move(response), errc::common::unambiguous_timeout);
}

// A management response carrying the caller's client_context_id, which is what correlates it with
// the request that produced it when several are in flight. The http error context has the field and
// the classic path fills it, so leaving it empty here would lose the identifier on the couchbase2
// path alone -- most visibly on the paths that send nothing, where there is no server response to
// correlate by either.
//
// Only what the caller supplied is echoed. The classic path substitutes a generated uuid for an
// absent one because it travels with the HTTP request and comes back in the server's own logs;
// couchbase2 sends no such field, so an id the caller never chose would identify nothing.
template<typename Request>
[[nodiscard]] auto
stamped_management(const Request& request) -> typename Request::response_type
{
  typename Request::response_type response;
  response.ctx.client_context_id = request.client_context_id.value_or(std::string{});
  return response;
}

// FTS answers each of the search index management endpoints with {"status":"ok"} on success, and
// the classic path copies that string into the response, where core-API consumers read it.
// admin.search.v1 replies with empty messages, so a successful RPC is the same signal and carries
// the same meaning. A failed one leaves the field empty rather than inventing a status the server
// never reported; the error code is what describes it.
template<typename Response>
void
set_ok_status(Response& response)
{
  if (!response.ctx.ec) {
    response.status = "ok";
  }
}
} // namespace

component::component(asio::io_context& io, component_config config)
  : io_{ io }
  , stubs_{ std::make_unique<stubs>(
      stubs{ v1::KvService::NewStub(config.channel),
             query_v1::QueryService::NewStub(config.channel),
             analytics_v1::AnalyticsService::NewStub(config.channel),
             search_v1::SearchService::NewStub(config.channel),
             view_v1::ViewService::NewStub(config.channel),
             bucket_admin_v1::BucketAdminService::NewStub(config.channel),
             collection_admin_v1::CollectionAdminService::NewStub(config.channel),
             query_admin_v1::QueryAdminService::NewStub(config.channel),
             search_admin_v1::SearchAdminService::NewStub(config.channel) }) }
  , authorization_{ authorization_header(config.credentials) }
  , timeouts_{ config.timeouts }
  // Initialised last (see the declaration order in the header), so the channel can be moved in.
  , dispatcher_{ io, std::move(config.channel) }
{
}

component::~component() = default;

auto
component::execute(const operations::get_request& request,
                   utils::movable_function<void(operations::get_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::GetRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::GetResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      // A get reads and nothing more, so a deadline here is unambiguous: the document cannot have
      // changed. kv::decode owns the compressed-content rule (it reports feature_not_available
      // rather than handing back an empty value with a success status).
      auto ctx =
        make_error_context(status, id, operation_kind::read_only, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::get_projected_request& request,
                   utils::movable_function<void(operations::get_projected_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, request, retry_attempts, retry_reasons](
      grpc::Status status, v1::GetResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::read_only, retry_attempts, retry_reasons);
      if (status.ok() && resp.content_case() == v1::GetResponse::kContentCompressed) {
        ctx = make_key_value_error_context(
          errc::common::feature_not_available, id, retry_attempts, retry_reasons);
      }
      handler(kv::decode(resp, std::move(ctx), request));
    });
}

auto
component::execute(const operations::upsert_request& request,
                   utils::movable_function<void(operations::upsert_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::UpsertRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::UpsertResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::insert_request& request,
                   utils::movable_function<void(operations::insert_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::InsertRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::InsertResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::replace_request& request,
                   utils::movable_function<void(operations::replace_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::ReplaceRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::ReplaceResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::remove_request& request,
                   utils::movable_function<void(operations::remove_response)>&& handler)
  -> pending_call
{
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired(io_, request, handler);
  }
  auto proto = std::make_shared<v1::RemoveRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::RemoveResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::touch_request& request,
                   utils::movable_function<void(operations::touch_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::TouchRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::TouchResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::exists_request& request,
                   utils::movable_function<void(operations::exists_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::ExistsRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::ExistsResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::read_only, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::get_and_lock_request& request,
                   utils::movable_function<void(operations::get_and_lock_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetAndLockRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::GetAndLockResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      if (status.ok() && resp.content_case() == v1::GetAndLockResponse::kContentCompressed) {
        ctx = make_key_value_error_context(
          errc::common::feature_not_available, id, retry_attempts, retry_reasons);
      }
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::unlock_request& request,
                   utils::movable_function<void(operations::unlock_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::UnlockRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::UnlockResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::get_and_touch_request& request,
                   utils::movable_function<void(operations::get_and_touch_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::GetAndTouchRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::GetAndTouchResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      if (status.ok() && resp.content_case() == v1::GetAndTouchResponse::kContentCompressed) {
        ctx = make_key_value_error_context(
          errc::common::feature_not_available, id, retry_attempts, retry_reasons);
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
component::execute(const operations::increment_request& request,
                   utils::movable_function<void(operations::increment_response)>&& handler)
  -> pending_call
{
  if (counter_initial_value_out_of_range(request)) {
    return fail_invalid_argument(io_, request, handler);
  }
  auto proto = std::make_shared<v1::IncrementRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::IncrementResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::decrement_request& request,
                   utils::movable_function<void(operations::decrement_response)>&& handler)
  -> pending_call
{
  if (counter_initial_value_out_of_range(request)) {
    return fail_invalid_argument(io_, request, handler);
  }
  auto proto = std::make_shared<v1::DecrementRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::DecrementResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::append_request& request,
                   utils::movable_function<void(operations::append_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::AppendRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::AppendResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::prepend_request& request,
                   utils::movable_function<void(operations::prepend_response)>&& handler)
  -> pending_call
{
  auto proto = std::make_shared<v1::PrependRequest>(kv::encode(request));
  const auto id = request.id;
  const auto retry_attempts = request.retries.retry_attempts();
  const auto retry_reasons = request.retries.retry_reasons();
  const auto timeout = request.timeout.value_or(timeouts_.key_value);
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
    [handler = std::move(handler), id, proto, retry_attempts, retry_reasons](
      grpc::Status status, v1::PrependResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      auto ctx =
        make_error_context(status, id, operation_kind::mutating, retry_attempts, retry_reasons);
      handler(kv::decode(resp, std::move(ctx)));
    });
}

auto
component::execute(const operations::query_request& request,
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

  const auto timeout = request.timeout.value_or(timeouts_.query);
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
component::execute(const operations::analytics_request& request,
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

  const auto timeout = request.timeout.value_or(timeouts_.analytics);
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

auto
component::execute(const operations::search_request& request,
                   utils::movable_function<void(operations::search_response)>&& handler)
  -> pending_call
{
  auto index_name = request.index_name;
  auto query_str = request.query.str();
  auto client_context_id = request.client_context_id.value_or(std::string{});
  // Stamped even on the paths that send nothing, as the query overload does: the error context
  // identifies which search failed, and a caller correlating by index or client_context_id has no
  // other handle on it.
  const auto stamped = [&index_name, &query_str, &client_context_id]() {
    operations::search_response response;
    response.ctx.index_name = index_name;
    response.ctx.query = query_str;
    response.ctx.client_context_id = client_context_id;
    return response;
  };

  const auto timeout = request.timeout.value_or(timeouts_.search);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped());
  }

  // Unmappable query shape or gated feature (facets, sort specs, vector search, mutation tokens,
  // raw): fail cleanly rather than dropping the caller's intent. A query that is not JSON at all is
  // the caller's own error and is reported as invalid_argument, so a malformed query is not
  // mistaken for a gap in couchbase2 search support.
  auto encoded = search::encode(request);
  if (!encoded.has_value()) {
    auto response = stamped();
    response.ctx.ec = search::query_is_malformed(request) ? errc::common::invalid_argument
                                                          : errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired above gives: completions arrive
    // on the io context, never inline out of execute().
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }

  auto proto = std::make_shared<search_v1::SearchQueryRequest>(std::move(*encoded));
  const auto auth = authorization_;
  auto* stub = stubs_->search.get();
  auto response = std::make_shared<operations::search_response>();

  return dispatcher_.server_stream<search_v1::SearchQueryResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        grpc::ClientReadReactor<search_v1::SearchQueryResponse>* reactor) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->SearchQuery(&ctx, proto.get(), reactor);
    },
    [response](search_v1::SearchQueryResponse message) {
      search::decode(message, *response);
    },
    [handler = std::move(handler), response, proto, index_name, query_str, client_context_id](
      grpc::Status status) mutable {
      (void)proto; // kept only to keep the request alive for the call
      response->ctx.ec = map_status(status, operation_kind::read_only);
      response->ctx.first_error_message = error_message(status);
      response->ctx.index_name = std::move(index_name);
      response->ctx.query = std::move(query_str);
      response->ctx.client_context_id = std::move(client_context_id);
      handler(std::move(*response));
    });
}

auto
component::execute(const operations::document_view_request& request,
                   utils::movable_function<void(operations::document_view_response)>&& handler)
  -> pending_call
{
  auto design_document = request.document_name;
  auto view_name = request.view_name;
  auto client_context_id = request.client_context_id.value_or(std::string{});
  // Stamped on every path for the reason the query and search overloads give: a response naming no
  // view leaves the caller nothing to correlate the failure with.
  const auto stamped = [&design_document, &view_name, &client_context_id]() {
    operations::document_view_response response;
    response.ctx.design_document_name = design_document;
    response.ctx.view_name = view_name;
    response.ctx.client_context_id = client_context_id;
    return response;
  };

  const auto timeout = request.timeout.value_or(timeouts_.view);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped());
  }

  // raw / query_string / full_set have no couchbase2 mapping: fail cleanly.
  if (!view::can_encode(request)) {
    auto response = stamped();
    response.ctx.ec = errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired above gives: completions arrive
    // on the io context, never inline out of execute().
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }

  auto proto = std::make_shared<view_v1::ViewQueryRequest>(view::encode(request));
  const auto auth = authorization_;
  auto* stub = stubs_->view.get();
  auto response = std::make_shared<operations::document_view_response>();

  return dispatcher_.server_stream<view_v1::ViewQueryResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        grpc::ClientReadReactor<view_v1::ViewQueryResponse>* reactor) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ViewQuery(&ctx, proto.get(), reactor);
    },
    [response](view_v1::ViewQueryResponse message) {
      view::decode_rows(message, *response);
    },
    [handler = std::move(handler), response, proto, design_document, view_name, client_context_id](
      grpc::Status status) mutable {
      (void)proto; // kept only to keep the request alive for the call
      response->ctx.ec = map_status(status, operation_kind::read_only);
      response->ctx.first_error_message = error_message(status);
      response->ctx.design_document_name = std::move(design_document);
      response->ctx.view_name = std::move(view_name);
      response->ctx.client_context_id = std::move(client_context_id);
      handler(std::move(*response));
    });
}

auto
component::execute(
  const operations::management::bucket_get_all_request& request,
  utils::movable_function<void(operations::management::bucket_get_all_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<bucket_admin_v1::ListBucketsRequest>();
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::ListBucketsResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::ListBucketsResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ListBuckets(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, bucket_admin_v1::ListBucketsResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_get_all_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      for (const auto& bucket : resp.buckets()) {
        response.buckets.push_back(bucket_admin::decode_bucket(bucket));
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::bucket_get_request& request,
  utils::movable_function<void(operations::management::bucket_get_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  // admin.bucket.v1 has no GetBucket RPC; filter the bucket list by name.
  auto proto = std::make_shared<bucket_admin_v1::ListBucketsRequest>();
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  const auto name = request.name;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::ListBucketsResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::ListBucketsResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ListBuckets(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id, name](
      grpc::Status status, bucket_admin_v1::ListBucketsResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_get_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      if (!response.ctx.ec) {
        bool found = false;
        for (const auto& bucket : resp.buckets()) {
          if (bucket.bucket_name() == name) {
            response.bucket = bucket_admin::decode_bucket(bucket);
            found = true;
            break;
          }
        }
        if (!found) {
          response.ctx.ec = errc::common::bucket_not_found;
        }
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::bucket_create_request& request,
  utils::movable_function<void(operations::management::bucket_create_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<bucket_admin_v1::CreateBucketRequest>();
  bucket_admin::apply_settings(request.bucket, *proto);
  if (!bucket_admin::apply_create_only_settings(request.bucket, *proto)) {
    operations::management::bucket_create_response response;
    response.ctx.client_context_id = client_context_id;
    response.ctx.ec = errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired gives: completions arrive on the
    // io context, never inline out of execute().
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::CreateBucketResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::CreateBucketResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->CreateBucket(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, bucket_admin_v1::CreateBucketResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_create_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      if (response.ctx.ec) {
        response.error_message = error_message(status);
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::bucket_update_request& request,
  utils::movable_function<void(operations::management::bucket_update_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<bucket_admin_v1::UpdateBucketRequest>();
  bucket_admin::apply_settings(request.bucket, *proto);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::UpdateBucketResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::UpdateBucketResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->UpdateBucket(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, bucket_admin_v1::UpdateBucketResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_update_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      if (response.ctx.ec) {
        response.error_message = error_message(status);
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::bucket_drop_request& request,
  utils::movable_function<void(operations::management::bucket_drop_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<bucket_admin_v1::DeleteBucketRequest>();
  proto->set_bucket_name(request.name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::DeleteBucketResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::DeleteBucketResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DeleteBucket(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, bucket_admin_v1::DeleteBucketResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_drop_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::bucket_flush_request& request,
  utils::movable_function<void(operations::management::bucket_flush_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<bucket_admin_v1::FlushBucketRequest>();
  proto->set_bucket_name(request.name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->bucket_admin.get();
  return dispatcher_.unary<bucket_admin_v1::FlushBucketResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        bucket_admin_v1::FlushBucketResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->FlushBucket(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, bucket_admin_v1::FlushBucketResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::bucket_flush_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::scope_get_all_request& request,
  utils::movable_function<void(operations::management::scope_get_all_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<collection_admin_v1::ListCollectionsRequest>();
  proto->set_bucket_name(request.bucket_name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::ListCollectionsResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::ListCollectionsResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ListCollections(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::ListCollectionsResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::scope_get_all_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      if (!response.ctx.ec) {
        response.manifest = collection_admin::decode_manifest(resp);
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::scope_create_request& request,
  utils::movable_function<void(operations::management::scope_create_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<collection_admin_v1::CreateScopeRequest>();
  proto->set_bucket_name(request.bucket_name);
  proto->set_scope_name(request.scope_name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::CreateScopeResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::CreateScopeResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->CreateScope(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::CreateScopeResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::scope_create_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.uid = resp.manifest_uid();
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::scope_drop_request& request,
  utils::movable_function<void(operations::management::scope_drop_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<collection_admin_v1::DeleteScopeRequest>();
  proto->set_bucket_name(request.bucket_name);
  proto->set_scope_name(request.scope_name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::DeleteScopeResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::DeleteScopeResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DeleteScope(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::DeleteScopeResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::scope_drop_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.uid = resp.manifest_uid();
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::collection_create_request& request,
  utils::movable_function<void(operations::management::collection_create_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  // -1 is the lowest value the sign carries a meaning for, and the classic path refuses anything
  // below it before encoding (collection_create.cxx:39-44). encode_max_expiry maps every negative
  // onto the no-expiry wire form, so without this the same request would be invalid over
  // couchbase:// and create a never-expiring collection over couchbase2://.
  if (request.max_expiry.value_or(0) < -1) {
    auto response = stamped_management(request);
    response.ctx.ec = errc::common::invalid_argument;
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }

  auto proto = std::make_shared<collection_admin_v1::CreateCollectionRequest>();
  proto->set_bucket_name(request.bucket_name);
  proto->set_scope_name(request.scope_name);
  proto->set_collection_name(request.collection_name);
  // A created collection inherits the bucket default from an unset max_expiry_secs, which is what
  // a core max_expiry of 0 asks for, so encode_max_expiry declining to produce a value is already
  // the right wire form here.
  if (request.max_expiry.has_value()) {
    if (const auto secs = collection_admin::encode_max_expiry(*request.max_expiry);
        secs.has_value()) {
      proto->set_max_expiry_secs(secs.value());
    }
  }
  if (request.history.has_value()) {
    proto->set_history_retention_enabled(*request.history);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::CreateCollectionResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::CreateCollectionResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->CreateCollection(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::CreateCollectionResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::collection_create_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.uid = resp.manifest_uid();
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::collection_update_request& request,
  utils::movable_function<void(operations::management::collection_update_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  // The sentinel translation create does, minus the one value update cannot express, and with the
  // same lower bound. Below -1 the classic path refuses the request (collection_update.cxx:39-45).
  // An explicit 0 is the reset to the bucket's own expiry that the classic path sends as maxTTL=0,
  // and there is no wire form left for it: an explicit 0 already means "no expiry", and an unset
  // field means "leave the collection as it is" rather than "inherit the bucket default". Refuse
  // it, because reporting success for a setting the gateway was never asked to apply would leave
  // the caller's documents expiring on the old policy.
  if (request.max_expiry.has_value()) {
    std::error_code refusal{};
    if (*request.max_expiry < -1) {
      refusal = errc::common::invalid_argument;
    } else if (*request.max_expiry == 0) {
      refusal = errc::common::feature_not_available;
    }
    if (refusal) {
      auto response = stamped_management(request);
      response.ctx.ec = refusal;
      // Posted rather than invoked inline, so the completion arrives on the SDK's execution context
      // after the caller holds its pending_call.
      asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
        handler(std::move(response));
      });
      return {};
    }
  }

  auto proto = std::make_shared<collection_admin_v1::UpdateCollectionRequest>();
  proto->set_bucket_name(request.bucket_name);
  proto->set_scope_name(request.scope_name);
  proto->set_collection_name(request.collection_name);
  // Zero is refused above, so what reaches here is a no-expiry sentinel or a positive TTL, and
  // both of those encode to a value.
  if (request.max_expiry.has_value()) {
    if (const auto secs = collection_admin::encode_max_expiry(*request.max_expiry);
        secs.has_value()) {
      proto->set_max_expiry_secs(secs.value());
    }
  }
  if (request.history.has_value()) {
    proto->set_history_retention_enabled(*request.history);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::UpdateCollectionResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::UpdateCollectionResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->UpdateCollection(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::UpdateCollectionResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::collection_update_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.uid = resp.manifest_uid();
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::collection_drop_request& request,
  utils::movable_function<void(operations::management::collection_drop_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<collection_admin_v1::DeleteCollectionRequest>();
  proto->set_bucket_name(request.bucket_name);
  proto->set_scope_name(request.scope_name);
  proto->set_collection_name(request.collection_name);
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->collection_admin.get();
  return dispatcher_.unary<collection_admin_v1::DeleteCollectionResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        collection_admin_v1::DeleteCollectionResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DeleteCollection(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, collection_admin_v1::DeleteCollectionResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::collection_drop_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.uid = resp.manifest_uid();
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::query_index_get_all_request& request,
  utils::movable_function<void(operations::management::query_index_get_all_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<query_admin_v1::GetAllIndexesRequest>(
    query_index_admin::encode_get_all(request));
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->query_admin.get();
  return dispatcher_.unary<query_admin_v1::GetAllIndexesResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        query_admin_v1::GetAllIndexesResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->GetAllIndexes(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, query_admin_v1::GetAllIndexesResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::query_index_get_all_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      for (const auto& index : resp.indexes()) {
        response.indexes.push_back(query_index_admin::decode_index(index));
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::query_index_create_request& request,
  utils::movable_function<void(operations::management::query_index_create_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (!query_index_admin::can_encode(request)) {
    auto response = stamped_management(request);
    response.ctx.ec = errc::common::feature_not_available;
    // Posted rather than invoked here, for the reason fail_expired above gives: completions arrive
    // on the io context, never inline out of execute().
    asio::post(io_, [handler = std::move(handler), response = std::move(response)]() mutable {
      handler(std::move(response));
    });
    return {};
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->query_admin.get();

  if (request.is_primary) {
    auto proto = std::make_shared<query_admin_v1::CreatePrimaryIndexRequest>(
      query_index_admin::encode_create_primary(request));
    return dispatcher_.unary<query_admin_v1::CreatePrimaryIndexResponse>(
      timeout,
      [stub, proto, auth](grpc::ClientContext& ctx,
                          query_admin_v1::CreatePrimaryIndexResponse& resp,
                          std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->CreatePrimaryIndex(&ctx, proto.get(), &resp, std::move(cb));
      },
      [handler = std::move(handler), proto, client_context_id](
        grpc::Status status, query_admin_v1::CreatePrimaryIndexResponse /* resp */) mutable {
        (void)proto; // kept only to keep the request alive for the call
        operations::management::query_index_create_response response;
        response.ctx.client_context_id = client_context_id;
        response.ctx.ec = map_status(status, operation_kind::mutating);
        handler(std::move(response));
      });
  }

  auto proto =
    std::make_shared<query_admin_v1::CreateIndexRequest>(query_index_admin::encode_create(request));
  return dispatcher_.unary<query_admin_v1::CreateIndexResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        query_admin_v1::CreateIndexResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->CreateIndex(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, query_admin_v1::CreateIndexResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::query_index_create_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::query_index_drop_request& request,
  utils::movable_function<void(operations::management::query_index_drop_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->query_admin.get();

  if (request.is_primary) {
    auto proto = std::make_shared<query_admin_v1::DropPrimaryIndexRequest>(
      query_index_admin::encode_drop_primary(request));
    return dispatcher_.unary<query_admin_v1::DropPrimaryIndexResponse>(
      timeout,
      [stub, proto, auth](grpc::ClientContext& ctx,
                          query_admin_v1::DropPrimaryIndexResponse& resp,
                          std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->DropPrimaryIndex(&ctx, proto.get(), &resp, std::move(cb));
      },
      [handler = std::move(handler), proto, client_context_id](
        grpc::Status status, query_admin_v1::DropPrimaryIndexResponse /* resp */) mutable {
        (void)proto; // kept only to keep the request alive for the call
        operations::management::query_index_drop_response response;
        response.ctx.client_context_id = client_context_id;
        response.ctx.ec = map_status(status, operation_kind::mutating);
        handler(std::move(response));
      });
  }

  auto proto =
    std::make_shared<query_admin_v1::DropIndexRequest>(query_index_admin::encode_drop(request));
  return dispatcher_.unary<query_admin_v1::DropIndexResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        query_admin_v1::DropIndexResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DropIndex(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, query_admin_v1::DropIndexResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::query_index_drop_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::query_index_build_deferred_request& request,
  utils::movable_function<void(operations::management::query_index_build_deferred_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<query_admin_v1::BuildDeferredIndexesRequest>(
    query_index_admin::encode_build_deferred(request));
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->query_admin.get();
  return dispatcher_.unary<query_admin_v1::BuildDeferredIndexesResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        query_admin_v1::BuildDeferredIndexesResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->BuildDeferredIndexes(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, query_admin_v1::BuildDeferredIndexesResponse /* resp */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::query_index_build_deferred_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_upsert_request& request,
  utils::movable_function<void(operations::management::search_index_upsert_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index.name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  const auto name = request.index.name;
  auto* stub = stubs_->search_admin.get();

  // A uuid identifies a definition that already exists, so an upsert carrying one is an update and
  // an upsert without one is a create. That is the same rule the classic path applies -- it sends
  // the uuid as the body's "uuid" member, which FTS reads as prevIndexUUID -- and the two RPCs
  // divide along the same line: UpdateIndex refuses a request with no uuid, and CreateIndex refuses
  // a name that already exists.
  //
  // The proto is built before the handler is moved into the completion, so a definition the
  // converter refuses is still reported through the handler the caller passed.
  std::shared_ptr<search_admin_v1::UpdateIndexRequest> update_proto;
  std::shared_ptr<search_admin_v1::CreateIndexRequest> create_proto;
  if (!request.index.uuid.empty()) {
    update_proto = std::make_shared<search_admin_v1::UpdateIndexRequest>();
    if (!search_index_admin::apply_index(request.index, *update_proto->mutable_index())) {
      return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
    }
    if (request.bucket_name.has_value()) {
      update_proto->set_bucket_name(*request.bucket_name);
    }
    if (request.scope_name.has_value()) {
      update_proto->set_scope_name(*request.scope_name);
    }
  } else {
    create_proto = std::make_shared<search_admin_v1::CreateIndexRequest>();
    if (!search_index_admin::apply_index(request.index, *create_proto)) {
      return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
    }
    if (request.bucket_name.has_value()) {
      create_proto->set_bucket_name(*request.bucket_name);
    }
    if (request.scope_name.has_value()) {
      create_proto->set_scope_name(*request.scope_name);
    }
  }

  auto invoke =
    [handler = std::move(handler), client_context_id, name](grpc::Status status) mutable {
      operations::management::search_index_upsert_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      response.name = name;
      set_ok_status(response);
      handler(std::move(response));
    };

  if (update_proto) {
    return dispatcher_.unary<search_admin_v1::UpdateIndexResponse>(
      timeout,
      [stub, proto = update_proto, auth](grpc::ClientContext& ctx,
                                         search_admin_v1::UpdateIndexResponse& resp,
                                         std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->UpdateIndex(&ctx, proto.get(), &resp, std::move(cb));
      },
      [invoke = std::move(invoke), proto = update_proto](
        grpc::Status status, search_admin_v1::UpdateIndexResponse /* r */) mutable {
        invoke(std::move(status));
      });
  }
  return dispatcher_.unary<search_admin_v1::CreateIndexResponse>(
    timeout,
    [stub, proto = create_proto, auth](grpc::ClientContext& ctx,
                                       search_admin_v1::CreateIndexResponse& resp,
                                       std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->CreateIndex(&ctx, proto.get(), &resp, std::move(cb));
    },
    [invoke = std::move(invoke), proto = create_proto](
      grpc::Status status, search_admin_v1::CreateIndexResponse /* r */) mutable {
      invoke(std::move(status));
    });
}

auto
component::execute(
  const operations::management::search_index_get_request& request,
  utils::movable_function<void(operations::management::search_index_get_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  auto proto = std::make_shared<search_admin_v1::GetIndexRequest>();
  proto->set_name(request.index_name);
  if (request.bucket_name.has_value()) {
    proto->set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto->set_scope_name(*request.scope_name);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->search_admin.get();
  return dispatcher_.unary<search_admin_v1::GetIndexResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::GetIndexResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->GetIndex(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, search_admin_v1::GetIndexResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::search_index_get_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      if (!response.ctx.ec) {
        // A success carrying no index describes no index. Reporting it as one -- the response
        // would hold a default-constructed definition with an empty name -- is worse than saying
        // the index is not there.
        if (!resp.has_index()) {
          response.ctx.ec = errc::common::index_not_found;
        } else if (auto index = search_index_admin::decode_index(resp.index()); index.has_value()) {
          response.index = std::move(*index);
        } else {
          response.ctx.ec = errc::common::parsing_failure;
        }
      }
      set_ok_status(response);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_get_all_request& request,
  utils::movable_function<void(operations::management::search_index_get_all_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<search_admin_v1::ListIndexesRequest>();
  if (request.bucket_name.has_value()) {
    proto->set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto->set_scope_name(*request.scope_name);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->search_admin.get();
  return dispatcher_.unary<search_admin_v1::ListIndexesResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::ListIndexesResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ListIndexes(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, search_admin_v1::ListIndexesResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::search_index_get_all_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      for (const auto& index : resp.indexes()) {
        auto decoded = search_index_admin::decode_index(index);
        if (!decoded.has_value()) {
          // One definition that cannot be represented fails the listing. Skipping it would return
          // a list shorter than the server's, which reads as "that index does not exist".
          response.ctx.ec = errc::common::parsing_failure;
          response.indexes.clear();
          break;
        }
        response.indexes.push_back(std::move(*decoded));
      }
      set_ok_status(response);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_drop_request& request,
  utils::movable_function<void(operations::management::search_index_drop_response)>&& handler)
  -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  auto proto = std::make_shared<search_admin_v1::DeleteIndexRequest>();
  proto->set_name(request.index_name);
  if (request.bucket_name.has_value()) {
    proto->set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto->set_scope_name(*request.scope_name);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->search_admin.get();
  return dispatcher_.unary<search_admin_v1::DeleteIndexResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::DeleteIndexResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DeleteIndex(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, search_admin_v1::DeleteIndexResponse /* r */) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::search_index_drop_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::mutating);
      set_ok_status(response);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_analyze_document_request& request,
  utils::movable_function<void(operations::management::search_index_analyze_document_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  auto proto = std::make_shared<search_admin_v1::AnalyzeDocumentRequest>();
  proto->set_name(request.index_name);
  proto->set_doc(request.encoded_document);
  if (request.bucket_name.has_value()) {
    proto->set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto->set_scope_name(*request.scope_name);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->search_admin.get();
  return dispatcher_.unary<search_admin_v1::AnalyzeDocumentResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::AnalyzeDocumentResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->AnalyzeDocument(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, search_admin_v1::AnalyzeDocumentResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::search_index_analyze_document_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      response.analysis = resp.analyzed();
      // The only response in this service that relays FTS's own status string; the rest infer it
      // from the RPC. Prefer what the server said, and fall back only when it said nothing.
      if (!response.ctx.ec && !resp.status().empty()) {
        response.status = resp.status();
      } else {
        set_ok_status(response);
      }
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_get_documents_count_request& request,
  utils::movable_function<void(operations::management::search_index_get_documents_count_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  auto proto = std::make_shared<search_admin_v1::GetIndexedDocumentsCountRequest>();
  proto->set_name(request.index_name);
  if (request.bucket_name.has_value()) {
    proto->set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto->set_scope_name(*request.scope_name);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  auto* stub = stubs_->search_admin.get();
  return dispatcher_.unary<search_admin_v1::GetIndexedDocumentsCountResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::GetIndexedDocumentsCountResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->GetIndexedDocumentsCount(&ctx, proto.get(), &resp, std::move(cb));
    },
    [handler = std::move(handler), proto, client_context_id](
      grpc::Status status, search_admin_v1::GetIndexedDocumentsCountResponse resp) mutable {
      (void)proto; // kept only to keep the request alive for the call
      operations::management::search_index_get_documents_count_response response;
      response.ctx.client_context_id = client_context_id;
      response.ctx.ec = map_status(status, operation_kind::read_only);
      response.count = resp.count();
      set_ok_status(response);
      handler(std::move(response));
    });
}

auto
component::execute(
  const operations::management::search_index_control_ingest_request& request,
  utils::movable_function<void(operations::management::search_index_control_ingest_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  const auto name = request.index_name;
  const auto bucket = request.bucket_name;
  const auto scope = request.scope_name;
  auto* stub = stubs_->search_admin.get();
  auto invoke = [handler = std::move(handler), client_context_id](grpc::Status status) mutable {
    operations::management::search_index_control_ingest_response response;
    response.ctx.client_context_id = client_context_id;
    response.ctx.ec = map_status(status, operation_kind::mutating);
    set_ok_status(response);
    handler(std::move(response));
  };
  if (request.pause) {
    auto proto = std::make_shared<search_admin_v1::PauseIndexIngestRequest>();
    proto->set_name(name);
    if (bucket.has_value()) {
      proto->set_bucket_name(*bucket);
    }
    if (scope.has_value()) {
      proto->set_scope_name(*scope);
    }
    return dispatcher_.unary<search_admin_v1::PauseIndexIngestResponse>(
      timeout,
      [stub, proto, auth](grpc::ClientContext& ctx,
                          search_admin_v1::PauseIndexIngestResponse& resp,
                          std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->PauseIndexIngest(&ctx, proto.get(), &resp, std::move(cb));
      },
      [invoke = std::move(invoke),
       proto](grpc::Status status, search_admin_v1::PauseIndexIngestResponse /* r */) mutable {
        invoke(std::move(status));
      });
  }
  auto proto = std::make_shared<search_admin_v1::ResumeIndexIngestRequest>();
  proto->set_name(name);
  if (bucket.has_value()) {
    proto->set_bucket_name(*bucket);
  }
  if (scope.has_value()) {
    proto->set_scope_name(*scope);
  }
  return dispatcher_.unary<search_admin_v1::ResumeIndexIngestResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::ResumeIndexIngestResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->ResumeIndexIngest(&ctx, proto.get(), &resp, std::move(cb));
    },
    [invoke = std::move(invoke),
     proto](grpc::Status status, search_admin_v1::ResumeIndexIngestResponse /* r */) mutable {
      invoke(std::move(status));
    });
}

auto
component::execute(
  const operations::management::search_index_control_query_request& request,
  utils::movable_function<void(operations::management::search_index_control_query_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  const auto name = request.index_name;
  const auto bucket = request.bucket_name;
  const auto scope = request.scope_name;
  auto* stub = stubs_->search_admin.get();
  auto invoke = [handler = std::move(handler), client_context_id](grpc::Status status) mutable {
    operations::management::search_index_control_query_response response;
    response.ctx.client_context_id = client_context_id;
    response.ctx.ec = map_status(status, operation_kind::mutating);
    set_ok_status(response);
    handler(std::move(response));
  };
  if (request.allow) {
    auto proto = std::make_shared<search_admin_v1::AllowIndexQueryingRequest>();
    proto->set_name(name);
    if (bucket.has_value()) {
      proto->set_bucket_name(*bucket);
    }
    if (scope.has_value()) {
      proto->set_scope_name(*scope);
    }
    return dispatcher_.unary<search_admin_v1::AllowIndexQueryingResponse>(
      timeout,
      [stub, proto, auth](grpc::ClientContext& ctx,
                          search_admin_v1::AllowIndexQueryingResponse& resp,
                          std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->AllowIndexQuerying(&ctx, proto.get(), &resp, std::move(cb));
      },
      [invoke = std::move(invoke),
       proto](grpc::Status status, search_admin_v1::AllowIndexQueryingResponse /* r */) mutable {
        invoke(std::move(status));
      });
  }
  auto proto = std::make_shared<search_admin_v1::DisallowIndexQueryingRequest>();
  proto->set_name(name);
  if (bucket.has_value()) {
    proto->set_bucket_name(*bucket);
  }
  if (scope.has_value()) {
    proto->set_scope_name(*scope);
  }
  return dispatcher_.unary<search_admin_v1::DisallowIndexQueryingResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::DisallowIndexQueryingResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->DisallowIndexQuerying(&ctx, proto.get(), &resp, std::move(cb));
    },
    [invoke = std::move(invoke),
     proto](grpc::Status status, search_admin_v1::DisallowIndexQueryingResponse /* r */) mutable {
      invoke(std::move(status));
    });
}

auto
component::execute(
  const operations::management::search_index_control_plan_freeze_request& request,
  utils::movable_function<void(operations::management::search_index_control_plan_freeze_response)>&&
    handler) -> pending_call
{
  const auto client_context_id = request.client_context_id.value_or(std::string{});
  if (request.index_name.empty()) {
    return fail_ctx(io_, handler, stamped_management(request), errc::common::invalid_argument);
  }
  const auto timeout = request.timeout.value_or(timeouts_.management);
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail_expired_ctx(io_, handler, stamped_management(request));
  }
  const auto auth = authorization_;
  const auto name = request.index_name;
  const auto bucket = request.bucket_name;
  const auto scope = request.scope_name;
  auto* stub = stubs_->search_admin.get();
  auto invoke = [handler = std::move(handler), client_context_id](grpc::Status status) mutable {
    operations::management::search_index_control_plan_freeze_response response;
    response.ctx.client_context_id = client_context_id;
    response.ctx.ec = map_status(status, operation_kind::mutating);
    set_ok_status(response);
    handler(std::move(response));
  };
  if (request.freeze) {
    auto proto = std::make_shared<search_admin_v1::FreezeIndexPlanRequest>();
    proto->set_name(name);
    if (bucket.has_value()) {
      proto->set_bucket_name(*bucket);
    }
    if (scope.has_value()) {
      proto->set_scope_name(*scope);
    }
    return dispatcher_.unary<search_admin_v1::FreezeIndexPlanResponse>(
      timeout,
      [stub, proto, auth](grpc::ClientContext& ctx,
                          search_admin_v1::FreezeIndexPlanResponse& resp,
                          std::function<void(grpc::Status)> cb) {
        if (!auth.empty()) {
          ctx.AddMetadata("authorization", auth);
        }
        stub->async()->FreezeIndexPlan(&ctx, proto.get(), &resp, std::move(cb));
      },
      [invoke = std::move(invoke),
       proto](grpc::Status status, search_admin_v1::FreezeIndexPlanResponse /* r */) mutable {
        invoke(std::move(status));
      });
  }
  auto proto = std::make_shared<search_admin_v1::UnfreezeIndexPlanRequest>();
  proto->set_name(name);
  if (bucket.has_value()) {
    proto->set_bucket_name(*bucket);
  }
  if (scope.has_value()) {
    proto->set_scope_name(*scope);
  }
  return dispatcher_.unary<search_admin_v1::UnfreezeIndexPlanResponse>(
    timeout,
    [stub, proto, auth](grpc::ClientContext& ctx,
                        search_admin_v1::UnfreezeIndexPlanResponse& resp,
                        std::function<void(grpc::Status)> cb) {
      if (!auth.empty()) {
        ctx.AddMetadata("authorization", auth);
      }
      stub->async()->UnfreezeIndexPlan(&ctx, proto.get(), &resp, std::move(cb));
    },
    [invoke = std::move(invoke),
     proto](grpc::Status status, search_admin_v1::UnfreezeIndexPlanResponse /* r */) mutable {
      invoke(std::move(status));
    });
}

} // namespace couchbase::core::protostellar
