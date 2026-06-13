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

#pragma once

#include "core/cluster.hxx"
#include "core/topology/configuration.hxx"

#include <couchbase/error.hxx>
#include <couchbase/error_codes.hxx>

#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core::impl
{
/**
 * Async scaffold shared between @c collection_impl::node_id_for and
 * @c collection_impl::node_ids — and any future client-side API that needs
 * to wait for a bucket configuration with a hard timeout.
 *
 * The scaffold owns the atomic-done guard, the timer, and the asio::post
 * back to the io_context. Callers supply a resolver that maps a topology
 * snapshot to a (error_code, payload) pair; the user handler is invoked
 * with (error, payload) exactly once — including on io_context shutdown,
 * where a fail-closed destructor backstop completes the handler with
 * @c errc::network::cluster_closed rather than leaking a dropped completion
 * (which would surface as a broken_promise to a std::future caller or a
 * permanent hang to a callback caller).
 *
 * Thread-safety: the SDK's core io_context is single-threaded for KV work
 * in the current release. The timer and atomic-done dance are safe under
 * that assumption. If the io_context is ever multi-threaded, both
 * @c timer.cancel() and the handler dispatch must be wrapped in a strand
 * because @c asio::steady_timer is not thread-safe.
 *
 * @tparam Payload      The success-path payload type (e.g. @c node_id or
 *                      @c std::vector<node_id>). Must be default-
 *                      constructible (used for the timeout / error branches)
 *                      and move-constructible.
 * @tparam Handler      Callable accepting @c (couchbase::error, Payload).
 * @tparam Resolver     Callable accepting
 *                      @c const std::shared_ptr<topology::configuration>&
 *                      and returning @c std::pair<std::error_code, Payload>.
 *                      Invoked at most once, on the config-callback path
 *                      when @c ec is falsy.
 */
template<typename Payload, typename Handler, typename Resolver>
void
with_bucket_config_or_timeout(const couchbase::core::cluster& core,
                              const std::string& bucket_name,
                              std::chrono::milliseconds timeout,
                              Handler handler,
                              Resolver resolver)
{
  struct state {
    std::atomic<bool> done{ false };
    std::atomic<bool> delivered{ false };
    asio::steady_timer timer;
    Handler handler;

    state(asio::io_context& ioc, Handler&& h)
      : timer{ ioc }
      , handler{ std::move(h) }
    {
    }

    // Invoke the user handler at most once. The `done` flag arbitrates the
    // timer-vs-config race; `delivered` guards the handler call itself so the
    // destructor can fail closed (below) without ever risking a double call.
    void deliver(couchbase::error err, Payload payload)
    {
      if (!delivered.exchange(true, std::memory_order_acq_rel)) {
        handler(std::move(err), std::move(payload));
      }
    }

    // Fail-closed backstop. If every async continuation is torn down without
    // the handler having run — io_context stopped on shutdown drops both the
    // pending timer wait and any deferred asio::post — complete here so a
    // std::future caller never observes broken_promise and a callback caller
    // never hangs. By the time the destructor runs the last shared_ptr
    // reference is gone, so no other thread can be in deliver() concurrently.
    // The handler is user code; a destructor must not let an exception escape.
    ~state()
    {
      try {
        deliver(couchbase::error{ errc::network::cluster_closed }, Payload{});
      } catch (...) { // NOLINT(bugprone-empty-catch)
      }
    }
  };

  auto shared = std::make_shared<state>(core.io_context(), std::move(handler));
  // Capture the io_context by pointer value (not by reference to a local
  // reference variable): core and its io_context outlive this operation, so
  // the pointer stays valid even though the bucket-config callback may run
  // after this function has returned.
  auto* io_context = &core.io_context();

  shared->timer.expires_after(timeout);
  shared->timer.async_wait([shared](std::error_code timer_ec) -> void {
    if (timer_ec == asio::error::operation_aborted) {
      return;
    }
    // acq_rel matches the other writer (config callback): the only thing
    // we synchronise on is whether we won the race to fire the handler.
    if (shared->done.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    shared->deliver(couchbase::error{ errc::common::unambiguous_timeout }, Payload{});
  });

  core.with_bucket_configuration(
    bucket_name,
    // Capture a weak_ptr, not a strong one. with_bucket_configuration() queues
    // this callback in the bucket's deferred-commands list until a config
    // arrives; if the timeout fires first and a config never arrives, a strong
    // capture would pin the user handler, promise and timer in that queue
    // indefinitely. The pending timer owns the state while we wait, so a config
    // that arrives before the timeout still locks successfully; once the
    // timeout has fired the state is released and this callback is a no-op.
    [weak = std::weak_ptr<state>(shared), io_context, resolver = std::move(resolver)](
      std::error_code ec,
      const std::shared_ptr<core::topology::configuration>& config) mutable -> void {
      auto shared = weak.lock();
      if (!shared || shared->done.exchange(true, std::memory_order_acq_rel)) {
        return;
      }

      std::error_code result_ec = ec;
      Payload payload{};
      if (!ec) {
        std::tie(result_ec, payload) = resolver(config);
      }

      // Hop back onto the io_context before touching the timer or invoking
      // the user handler. with_bucket_configuration() invokes its callback
      // synchronously on the caller's thread when the bucket configuration
      // is already cached (see bucket.cxx). Two things must therefore not
      // happen inline here:
      //   1. timer.cancel() — asio::steady_timer is not thread-safe and is
      //      bound to core.io_context(); cancelling it from an arbitrary
      //      application thread races with the io_context processing the
      //      pending async_wait. It must run on the timer's executor.
      //   2. the user handler — it reasonably expects an async hop and may
      //      itself re-enter the SDK.
      // The done flag above is atomic, so the race is already decided; the
      // deferred cancel() is a harmless no-op if the timer has since fired
      // (its handler short-circuits on operation_aborted / done).
      asio::post(*io_context, [shared, result_ec, payload = std::move(payload)]() mutable {
        shared->timer.cancel();
        shared->deliver(couchbase::error{ result_ec }, std::move(payload));
      });
    });
}
} // namespace couchbase::core::impl
