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

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace couchbase::core::protostellar
{

// Registry of in-flight gRPC calls so the transport can cancel and drain them at shutdown. Without
// it, teardown could destroy the io_context (and channel) while a gRPC callback is still pending on
// gRPC's threadpool -- the callback would then post onto a freed io_context (use-after-free). Each
// completion posts its handler and *then* removes its context; cancel_and_drain() TryCancels
// whatever is still outstanding and blocks until every callback has done both. That order is the
// contract, not an incidental detail: remove() is what releases the drain, so deregistering first
// would let the drain return while a gRPC thread was still posting onto the io_context -- exactly
// the use-after-free above. Held via shared_ptr so a late callback can safely touch it even while
// the owning dispatcher is being destroyed.
class call_tracker
{
public:
  // The tracker OWNS a shared_ptr to each context so a snapshot can keep it alive while cancelling
  // even if a concurrent completion removes and releases the caller's own reference.
  void add(std::shared_ptr<grpc::ClientContext> context)
  {
    const std::scoped_lock lock{ mutex_ };
    outstanding_.emplace(context.get(), std::move(context));
  }

  void remove(grpc::ClientContext* context)
  {
    const std::scoped_lock lock{ mutex_ };
    outstanding_.erase(context);
    if (outstanding_.empty()) {
      idle_.notify_all();
    }
  }

  // Cancel every in-flight call and block until all of their gRPC callbacks have run. Runs on the
  // io_context thread at shutdown while gRPC's own threads deliver the cancellations, so there is
  // no self-deadlock (the completions post back onto the io_context, which stays queued until the
  // drain returns).
  void cancel_and_drain()
  {
    // Snapshot owning shared_ptrs under the lock, then release it before calling TryCancel so no
    // external gRPC call runs while mutex_ is held (a completion delivered on a gRPC thread takes
    // that same lock in remove()). The snapshot keeps every context alive across the unlocked
    // cancel, so a concurrent completion that removes and releases its own reference cannot leave a
    // dangling pointer under TryCancel.
    std::vector<std::shared_ptr<grpc::ClientContext>> pending;
    {
      const std::scoped_lock lock{ mutex_ };
      pending.reserve(outstanding_.size());
      for (const auto& [ptr, context] : outstanding_) {
        pending.push_back(context);
      }
    }
    for (const auto& context : pending) {
      context->TryCancel();
    }
    std::unique_lock lock{ mutex_ };
    idle_.wait(lock, [this] {
      return outstanding_.empty();
    });
  }

private:
  std::mutex mutex_{};
  std::condition_variable idle_{};
  std::unordered_map<grpc::ClientContext*, std::shared_ptr<grpc::ClientContext>> outstanding_{};
};

// Handle to an in-flight unary call. cancel() maps to gRPC's client-side TryCancel; the call still
// completes afterwards (with a CANCELLED status) and its handler is posted as usual, so callers
// never leak a pending operation. As with any completion, the handler runs when the io_context runs
// it -- cancelling does not make it run inline.
class pending_call
{
public:
  pending_call() = default;
  explicit pending_call(std::shared_ptr<grpc::ClientContext> context)
    : context_{ std::move(context) }
  {
  }

  void cancel() const
  {
    if (context_) {
      context_->TryCancel();
    }
  }

private:
  std::shared_ptr<grpc::ClientContext> context_{};
};

// Default channel arguments: TCP keepalive so an idle channel detects a dead gateway.
[[nodiscard]] auto
default_channel_arguments() -> grpc::ChannelArguments;

// Build an insecure (plaintext) channel for the non-TLS couchbase2 path. TLS and per-call
// credentials live on the secure-channel path (see credentials.hxx/.cxx).
[[nodiscard]] auto
make_insecure_channel(const std::string& endpoint) -> std::shared_ptr<grpc::Channel>;

// Bridges gRPC's callback API onto an asio::io_context. A unary RPC is launched on gRPC's own
// threadpool; its completion is posted back onto the io_context, so callers observe results on
// the SDK's execution context — the same threading contract as the MCBP path.
class dispatcher
{
public:
  dispatcher(asio::io_context& io, std::shared_ptr<grpc::Channel> channel);
  dispatcher(const dispatcher&) = delete;
  dispatcher(dispatcher&&) = delete;
  auto operator=(const dispatcher&) -> dispatcher& = delete;
  auto operator=(dispatcher&&) -> dispatcher& = delete;

  // Cancels and drains every in-flight call before the io_context/channel go away, so no gRPC
  // callback can fire against freed state after the owning component is destroyed.
  ~dispatcher()
  {
    tracker_->cancel_and_drain();
  }

  [[nodiscard]] auto channel() const -> const std::shared_ptr<grpc::Channel>&;

  // Issue a unary RPC.
  //   invoker(grpc::ClientContext&, Response&, callback) launches the async call, e.g.
  //     stub->async()->Get(&ctx, &request, &response, std::move(callback));
  //   handler(grpc::Status, Response&&) runs once, on the io_context, when the call completes.
  // The request must outlive the call (the caller owns it). Returns a pending_call for
  // cancellation.
  template<typename Response, typename Invoker, typename Handler>
  auto unary(std::chrono::milliseconds timeout, Invoker&& invoker, Handler&& handler)
    -> pending_call
  {
    struct call_state {
      std::shared_ptr<grpc::ClientContext> context{ std::make_shared<grpc::ClientContext>() };
      Response response{};
    };
    auto state = std::make_shared<call_state>();
    if (timeout.count() > 0) {
      state->context->set_deadline(std::chrono::system_clock::now() + timeout);
    }

    // Register the call so shutdown can cancel and drain it. The context is kept alive both by
    // `state` (captured by the callback below) and by the tracker's own reference, so the raw
    // pointer the tracker keys on is valid for the TryCancel in cancel_and_drain().
    auto* context = state->context.get();
    tracker_->add(state->context);

    // Hold the handler in a shared_ptr so the gRPC-thread callback can hand it to the posted
    // task. Capture the io_context by pointer (never a reference to a local) so nothing dangles
    // once unary() returns; the dispatcher (and its io_context) must outlive the call. Capture the
    // tracker (shared) so deregistration is safe even as the dispatcher is being destroyed.
    auto handler_holder = std::make_shared<std::decay_t<Handler>>(std::forward<Handler>(handler));
    auto* io = &io_;
    auto tracker = tracker_;

    // If the invoker throws before it launches the async call, the completion callback below never
    // runs, so deregister here to keep cancel_and_drain() from waiting on a call that will never
    // complete.
    try {
      std::forward<Invoker>(invoker)(
        *state->context,
        state->response,
        [state, handler_holder, io, tracker, context](grpc::Status status) {
          // Post before deregistering. remove() is what releases cancel_and_drain(), so
          // deregistering first would let ~dispatcher() return -- and its owner tear the
          // io_context down -- while this thread is still inside asio::post(*io, ...), which is
          // the use-after-free the tracker exists to prevent. Ordered this way, the drain cannot
          // return until the post has been issued. The captures keep state and the tracker alive,
          // so nothing here depends on the dispatcher still existing.
          asio::post(*io, [state, handler_holder, status = std::move(status)]() mutable {
            (*handler_holder)(std::move(status), std::move(state->response));
          });
          tracker->remove(context);
        });
    } catch (...) {
      tracker_->remove(context);
      throw;
    }

    return pending_call{ state->context };
  }

private:
  asio::io_context& io_;
  std::shared_ptr<grpc::Channel> channel_;
  std::shared_ptr<call_tracker> tracker_{ std::make_shared<call_tracker>() };
};

} // namespace couchbase::core::protostellar
