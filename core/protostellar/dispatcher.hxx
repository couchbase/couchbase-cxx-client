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

#include "core/utils/movable_function.hxx"

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_callback.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace couchbase::core::protostellar
{

// RFC 77 (Bootstrapping -> Maximum Message Size): couchbase2 clients raise the receive limit to
// 25 MiB, so nothing the transport hands to a converter can legitimately exceed it -- including a
// value a compressed payload claims to inflate to.
constexpr std::size_t max_receive_message_size{ std::size_t{ 25 } * 1024 * 1024 };

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
    auto* key = context.get();
    const std::scoped_lock lock{ mutex_ };
    outstanding_.emplace(key, entry{ std::move(context), {}, {} });
  }

  // As above, for a call that cancellation alone does not drive to completion. `on_cancel` runs
  // straight after TryCancel; `owner` keeps the call's own object alive for as long as it stays
  // registered, so that hook cannot run against freed storage.
  void add(std::shared_ptr<grpc::ClientContext> context,
           std::function<void()> on_cancel,
           std::shared_ptr<void> owner)
  {
    auto* key = context.get();
    const std::scoped_lock lock{ mutex_ };
    outstanding_.emplace(key, entry{ std::move(context), std::move(on_cancel), std::move(owner) });
  }

  void remove(grpc::ClientContext* context)
  {
    const std::scoped_lock lock{ mutex_ };
    outstanding_.erase(context);
    if (outstanding_.empty()) {
      idle_.notify_all();
    }
  }

  // Cancel every in-flight call and block until all of their gRPC callbacks have run. Runs on
  // whichever thread destroys the dispatcher: the io_context thread, another thread while the io
  // thread keeps running, or a thread that never runs the io_context at all. Nothing here may
  // therefore depend on io_context work making progress. gRPC's own threads deliver the
  // cancellations and the completions post back onto the io_context, where they stay queued until
  // the drain returns, so blocking here cannot deadlock against them.
  void cancel_and_drain()
  {
    // Snapshot the entries under the lock, then release it before calling TryCancel or any cancel
    // hook so no external gRPC call runs while mutex_ is held (a completion delivered on a gRPC
    // thread takes that same lock in remove(), and releasing a read hold can deliver OnDone
    // inline). The snapshot keeps every context and owner alive across the unlocked cancel, so a
    // concurrent completion that removes and releases its own reference cannot leave a dangling
    // pointer here.
    std::vector<entry> pending;
    {
      const std::scoped_lock lock{ mutex_ };
      pending.reserve(outstanding_.size());
      for (const auto& [ptr, item] : outstanding_) {
        pending.push_back(item);
      }
    }
    for (const auto& item : pending) {
      item.context->TryCancel();
      if (item.on_cancel) {
        item.on_cancel();
      }
    }
    std::unique_lock lock{ mutex_ };
    idle_.wait(lock, [this] {
      return outstanding_.empty();
    });
  }

private:
  struct entry {
    std::shared_ptr<grpc::ClientContext> context;
    // Run straight after TryCancel for calls that cancellation alone does not finish. A streaming
    // call registers one that releases its read hold: gRPC defers OnDone while a hold is active,
    // and the hold is otherwise released only from an io_context continuation -- which cannot run
    // when the drain is itself blocking the io thread. Empty for unary calls, which take no holds.
    std::function<void()> on_cancel;
    // Keeps a self-owning call object (the streaming reactor) alive for as long as it is
    // registered, so on_cancel cannot run against freed storage.
    std::shared_ptr<void> owner;
  };

  std::mutex mutex_{};
  std::condition_variable idle_{};
  std::unordered_map<grpc::ClientContext*, entry> outstanding_{};
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

// Every call gets a deadline, including one whose budget is already spent.
//
// gRPC has no "expire immediately" input: leaving the deadline unset means the call runs until the
// channel breaks, so guarding the set_deadline with `timeout > 0` turns "no time left" into "no
// limit" -- the wrong direction for the mistake to go, and one that leaves cluster::close() waiting
// on a call with no reason to finish. Clamping to the smallest positive deadline expires the call
// instead, which is what an exhausted budget should do.
//
// This is the second line of defence, not the only one: every caller in component.cxx rejects a
// non-positive timeout up front (see fail_expired there), because failing before anything is sent
// carries information this layer cannot reconstruct -- that the operation provably never reached
// the server, hence unambiguous_timeout even for a mutation. The clamp exists so a caller that
// forgets, or a retry loop passing on what is left of a budget, cannot produce an unbounded call.
[[nodiscard]] inline auto
call_deadline(std::chrono::milliseconds timeout) -> std::chrono::system_clock::time_point
{
  return std::chrono::system_clock::now() + std::max(timeout, std::chrono::milliseconds{ 1 });
}

// Default channel arguments: TCP keepalive so an idle channel detects a dead gateway.
[[nodiscard]] auto
default_channel_arguments() -> grpc::ChannelArguments;

// Build an insecure (plaintext) channel for the non-TLS couchbase2 path. TLS and per-call
// credentials live on the secure-channel path (see credentials.hxx/.cxx).
[[nodiscard]] auto
make_insecure_channel(const std::string& endpoint) -> std::shared_ptr<grpc::Channel>;

// Reads a gRPC server stream via the callback API and hands each message, plus the terminal
// status, to the io_context. Assumes a single-threaded io_context (the SDK contract), so the FIFO
// post order guarantees every row is delivered before the completion runs.
//
// Held by shared_ptr, with every posted continuation carrying a strong reference. That covers both
// endings: the last reference is dropped either by the io_context running the completion or by the
// io_context being destroyed with the completion still queued. It also lets the read hold below be
// released from another thread without racing the reactor's own destruction.
template<typename Response>
class streaming_reactor final
  : public grpc::ClientReadReactor<Response>
  , public std::enable_shared_from_this<streaming_reactor<Response>>
{
public:
  streaming_reactor(asio::io_context& io,
                    std::shared_ptr<call_tracker> tracker,
                    std::shared_ptr<grpc::ClientContext> context,
                    utils::movable_function<void(Response)> on_row,
                    utils::movable_function<void(grpc::Status)> on_done)
    : io_{ io }
    , tracker_{ std::move(tracker) }
    , context_{ std::move(context) }
    , on_row_{ std::move(on_row) }
    , on_done_{ std::move(on_done) }
  {
  }

  void begin()
  {
    // Because backpressure re-issues StartRead from the io_context (outside the reaction, see
    // OnReadDone), a hold keeps the call alive between a completed read and the next StartRead --
    // otherwise gRPC could finalize the call (fire OnDone, free the reader) while a queued read
    // task still holds a pointer to it, and the stray StartRead would hit a destroyed call. This is
    // gRPC's documented requirement for issuing an operation from outside a reaction, not a local
    // precaution: see the AddHold contract in grpcpp/impl/codegen/client_callback.h.
    //
    // The hold is released in OnReadDone once a read comes back not-ok, or by release_hold() when
    // the call is cancelled while no read is outstanding. That second caller is not the read loop's
    // owner and cannot know whether a read task is still queued, so the hold alone does not close
    // the window -- release_hold() and the queued StartRead are ordered against each other instead.
    //
    // This runs under the same lock, because server_stream() registers the cancel hook before the
    // invoker binds the reactor to a call: a drain in that window would otherwise reach RemoveHold
    // with no reader bound and no hold taken. Under the lock the two orders are both well defined
    // -- either the hold is taken and the drain releases it, or the drain gets there first and no
    // hold is taken at all. The call is still started in that second case, so it runs to OnDone and
    // releases the drain rather than stranding it.
    const std::scoped_lock lock{ mutex_ };
    if (!hold_released_) {
      this->AddHold();
      hold_taken_ = true;
    }
    this->StartRead(&current_);
    this->StartCall();
  }

  // Releases begin()'s hold, at most once however many times this is called -- RemoveHold has to
  // balance AddHold exactly, and two callers race for it: OnReadDone when reads stop, and
  // cancel_and_drain() through the hook registered in server_stream().
  //
  // The drain needs that hook because a hold defers OnDone. Between a completed read and the next
  // StartRead no read is outstanding for TryCancel to fail, so nothing would drive the call to
  // completion, and the drain -- which may be running on the io_context thread itself -- would wait
  // for an OnDone that cannot fire until a continuation that cannot run has run.
  //
  // The lock orders this release against the queued StartRead in OnReadDone; it is not there to
  // make the flag thread-safe. Removing the last hold destroys the reader synchronously, inside
  // RemoveHold, so the two must not overlap: whichever side acquires the lock first, the other sees
  // a decision already made rather than a reader mid-destruction. Releasing while a read is
  // outstanding is harmless -- the outstanding read keeps gRPC's own count above zero, so it
  // finalizes only once that read completes.
  void release_hold()
  {
    const std::scoped_lock lock{ mutex_ };
    if (!hold_released_) {
      hold_released_ = true;
      // Nothing to balance if begin() has not run yet: the reactor is reachable from the tracker
      // before the invoker binds it to a call, and RemoveHold on an unbound reactor dereferences a
      // reader that does not exist. Setting the flag is enough -- begin() reads it and skips the
      // hold.
      if (hold_taken_) {
        this->RemoveHold();
      }
    }
  }

  void OnReadDone(bool ok) override
  {
    if (!ok) {
      release_hold(); // no more reads; let the call finalize and OnDone fire
      return;
    }
    auto message = std::make_shared<Response>(std::move(current_));
    // Backpressure: issue the next read from *inside* the io_context continuation, only after the
    // consumer has processed this row. On a single-threaded io_context that caps the amount gRPC
    // reads ahead of a slow consumer at one message, instead of draining the whole stream into the
    // asio post queue. current_ is moved-from here but no read is outstanding until StartRead runs.
    // The continuation holds a strong reference so it cannot outlive its own reactor.
    //
    // The consumer runs before the lock is taken, deliberately: it is caller code, and holding the
    // reactor's lock across it would let a slow or blocking consumer stall a concurrent drain.
    asio::post(io_, [self = this->shared_from_this(), message]() {
      self->on_row_(std::move(*message));
      const std::scoped_lock lock{ self->mutex_ };
      if (self->hold_released_) {
        // The hold is gone, so gRPC may already have destroyed the reader -- it does so inside
        // RemoveHold -- and a StartRead would dispatch through it. Nothing is lost by stopping
        // here: the hold was released either because reads had finished or because the call was
        // cancelled, and either way it is already on its way to OnDone.
        return;
      }
      self->StartRead(&self->current_);
    });
  }

  void OnDone(const grpc::Status& status) override
  {
    // Post before deregistering, for the same reason as unary(): remove() releases
    // cancel_and_drain(), so deregistering first would let ~dispatcher() return while this thread
    // is still inside asio::post(). The handle and the context pointer are cached in locals first
    // because the posted task owns this reactor and may destroy it on the io thread as soon as it
    // is enqueued -- reading tracker_ or context_ after the post could touch freed storage. The
    // tracker owns a shared_ptr to the context, so the cached pointer stays valid as a key until
    // remove() erases it, and remove() never dereferences it.
    auto tracker = tracker_;
    auto* context = context_.get();
    // Copy the status into the closure -- it is a reference parameter, so the posted task must own
    // its own copy rather than capture a reference that dangles once OnDone returns.
    //
    // The strong reference is what destroys the reactor: dropped when this task runs, or when the
    // task is destroyed unrun because the io_context went away first. A shared_ptr rather than the
    // unique_ptr this held before backpressure, because the cancel hook needs a weak reference to
    // the same object.
    asio::post(io_, [self = this->shared_from_this(), status_copy = status]() mutable {
      // Moved rather than copied: the task owns this status and does not touch it again, and
      // grpc::Status carries two std::strings.
      self->on_done_(std::move(status_copy));
    });
    tracker->remove(context);
  }

private:
  // Guards the hold flags and, with them, the ordering between releasing the hold and issuing a
  // read -- both the next read in OnReadDone and the first one in begin(). Uncontended in steady
  // state: only the io_context thread takes it, and the cancel hook once at teardown.
  std::mutex mutex_{};
  bool hold_taken_{ false };
  bool hold_released_{ false };
  asio::io_context& io_;
  std::shared_ptr<call_tracker> tracker_;
  Response current_{};
  std::shared_ptr<grpc::ClientContext> context_;
  utils::movable_function<void(Response)> on_row_;
  utils::movable_function<void(grpc::Status)> on_done_;
};

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

  // The io_context this dispatcher bridges gRPC completions onto. Exposed so transport-level
  // helpers (e.g. the wait_until_ready channel-state poll) can schedule work on the SDK thread.
  [[nodiscard]] auto io_context() const -> asio::io_context&
  {
    return io_;
  }

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
    state->context->set_deadline(call_deadline(timeout));

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

  // Issue a server-streaming RPC.
  //   invoker(grpc::ClientContext&, grpc::ClientReadReactor<Response>*) binds the reactor to the
  //     call, e.g. stub->async()->Query(&ctx, &request, reactor).
  //   on_row(Response) runs on the io_context for each streamed message; on_done(grpc::Status)
  //     runs once when the stream ends. The request must outlive the call (capture it in on_done).
  template<typename Response, typename Invoker>
  auto server_stream(std::chrono::milliseconds timeout,
                     Invoker&& invoker,
                     utils::movable_function<void(Response)> on_row,
                     utils::movable_function<void(grpc::Status)> on_done) -> pending_call
  {
    auto context = std::make_shared<grpc::ClientContext>();
    context->set_deadline(call_deadline(timeout));
    // gRPC only borrows the reactor for the duration of the call; ownership stays with this
    // shared_ptr and the continuations the reactor posts.
    auto reactor = std::make_shared<streaming_reactor<Response>>(
      io_, tracker_, context, std::move(on_row), std::move(on_done));
    // Register before launch so shutdown can cancel it; the reactor deregisters in OnDone. The hook
    // releases the reactor's read hold, without which a cancelled-but-parked stream never reaches
    // OnDone and the drain blocks forever; the reactor doubles as the owner so the hook always has
    // a live object to call.
    tracker_->add(
      context,
      [weak = std::weak_ptr<streaming_reactor<Response>>{ reactor }]() {
        if (const auto locked = weak.lock()) {
          locked->release_hold();
        }
      },
      reactor);
    // If the invoker fails to bind the reactor to a call, OnDone never fires, so deregister here
    // rather than block cancel_and_drain() forever at shutdown. Dropping the tracker's reference
    // leaves this scope holding the last one, so the reactor is destroyed as the exception unwinds.
    bool bound{ false };
    try {
      std::forward<Invoker>(invoker)(*context, reactor.get());
      bound = true;
      reactor->begin();
    } catch (...) {
      if (!bound) {
        tracker_->remove(context.get());
      }
      throw;
    }
    return pending_call{ context };
  }

private:
  asio::io_context& io_;
  std::shared_ptr<grpc::Channel> channel_;
  std::shared_ptr<call_tracker> tracker_{ std::make_shared<call_tracker>() };
};

} // namespace couchbase::core::protostellar
