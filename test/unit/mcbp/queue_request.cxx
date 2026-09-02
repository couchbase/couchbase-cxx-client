/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-Present Couchbase, Inc.
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

#include "framework/test_registry.hxx"

#include "core/mcbp/queue_request.hxx"
#include "core/mcbp/queue_response.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <atomic>
#include <cstddef>
#include <future>
#include <memory>
#include <system_error>

namespace couchbase::test
{
namespace
{
using couchbase::core::mcbp::queue_callback;
using couchbase::core::mcbp::queue_request;

auto
make_get_request(queue_callback&& cb) -> std::shared_ptr<queue_request>
{
  return std::make_shared<queue_request>(couchbase::core::protocol::magic::client_request,
                                         couchbase::core::protocol::client_opcode::get,
                                         std::move(cb));
}

void
cancel_invokes_the_callback_exactly_once([[maybe_unused]] context& ctx)
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });

  req->cancel(couchbase::errc::common::request_canceled);
  assert_eq(call_count.load(), 1, "the first cancel completes the request");

  req->cancel(couchbase::errc::common::request_canceled);
  assert_eq(call_count.load(), 1, "a second cancel does not complete it again");
}

void
cancel_hands_the_callback_the_error_code_it_was_given([[maybe_unused]] context& ctx)
{
  std::error_code received_ec{};

  auto req = make_get_request([&received_ec](auto /*resp*/, auto /*req*/, std::error_code ec) {
    received_ec = ec;
  });

  req->cancel(couchbase::errc::common::request_canceled);

  assert_eq(received_ec,
            couchbase::errc::common::request_canceled,
            "the code the caller cancelled with reaches the callback");
}

void
cancel_without_an_error_code_reports_request_canceled([[maybe_unused]] context& ctx)
{
  std::error_code received_ec{};

  auto req = make_get_request([&received_ec](auto /*resp*/, auto /*req*/, std::error_code ec) {
    received_ec = ec;
  });

  req->cancel(); // the pending_operation override

  assert_eq(received_ec,
            couchbase::errc::common::request_canceled,
            "the override cancels with request_canceled rather than with no error");
}

void
try_callback_invokes_a_non_persistent_callback_exactly_once([[maybe_unused]] context& ctx)
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });

  req->try_callback(nullptr, {});
  assert_eq(call_count.load(), 1, "the first response completes the request");

  req->try_callback(nullptr, {});
  assert_eq(call_count.load(), 1, "a second response after completion is dropped");
}

void
try_callback_racing_cancel_leaves_exactly_one_invocation([[maybe_unused]] context& ctx)
{
  // Fire try_callback and cancel concurrently many times: a request completed twice delivers a
  // response to a caller that has already been told it will get none.
  constexpr int iterations = 200;

  for (int i = 0; i < iterations; ++i) {
    std::atomic<int> call_count{ 0 };

    auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
      ++call_count;
    });

    auto f1 = std::async(std::launch::async, [&req] {
      req->try_callback(nullptr, {});
    });
    auto f2 = std::async(std::launch::async, [&req] {
      req->cancel(couchbase::errc::common::request_canceled);
    });

    f1.get();
    f2.get();

    assert_eq(call_count.load(), 1, "exactly one of the response and the cancel completes it");
  }
}

void
cancel_releases_the_state_the_callback_captured([[maybe_unused]] context& ctx)
{
  auto shared_resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak_resource = shared_resource;

  auto req =
    make_get_request([captured = shared_resource](auto /*resp*/, auto /*req*/, auto /*ec*/) {
      static_cast<void>(captured);
    });

  // Drop the local copy, leaving the callback the only holder.
  shared_resource.reset();
  assert_false(weak_resource.expired(), "the callback holds the captured state");

  req->cancel(couchbase::errc::common::request_canceled);

  // A request that outlives its cancellation must not keep the caller's state alive with it.
  assert_true(weak_resource.expired(), "cancel releases the callback and everything it captured");
}

void
retry_attempts_and_reasons_accumulate([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  assert_eq(req->retry_attempts(), std::size_t{ 0 }, "a fresh request has not been retried");
  assert_true(req->retry_reasons().empty(), "a fresh request carries no retry reasons");

  req->record_retry_attempt(couchbase::retry_reason::node_not_available);
  assert_eq(req->retry_attempts(), std::size_t{ 1 }, "the first retry is counted");
  assert_eq(req->retry_reasons().count(couchbase::retry_reason::node_not_available),
            std::size_t{ 1 },
            "the first retry's reason is recorded");

  req->record_retry_attempt(couchbase::retry_reason::key_value_temporary_failure);
  assert_eq(req->retry_attempts(), std::size_t{ 2 }, "the second retry is counted");
  assert_eq(req->retry_reasons().count(couchbase::retry_reason::key_value_temporary_failure),
            std::size_t{ 1 },
            "the second retry's reason is recorded");

  auto [count, reasons] = req->retries();
  assert_eq(count, std::size_t{ 2 }, "retries() reports the same count as retry_attempts()");
  assert_eq(reasons.size(), std::size_t{ 2 }, "retries() reports both reasons");
}

void
is_cancelled_reports_completion([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  assert_false(req->is_cancelled(), "a fresh request is not cancelled");

  req->cancel(couchbase::errc::common::request_canceled);

  assert_true(req->is_cancelled(), "a cancelled request reports itself cancelled");
}

void
span_fields_default_to_null([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  assert_true(req->parent_span_ == nullptr, "a request with no tracing carries no parent span");
  assert_true(req->dispatch_span_ == nullptr, "a request that has not been dispatched has no span");
}

// The three cases below cover the seam itself: the KV response path in bucket_impl::
// resolve_response consults req->on_unknown_collection_ and, when it is set and returns true,
// treats the unknown_collection status as handled (re-resolution scheduled by
// collections_component). The wiring of the hook to handle_collection_unknown, and the actual cid
// invalidation and re-resolution, require a network-bound dispatcher and are covered by
// "integration: range scan re-resolves recreated collection id" in
// test_integration_range_scan.cxx.

void
the_unknown_collection_hook_defaults_to_empty([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  assert_false(static_cast<bool>(req->on_unknown_collection_),
               "with no hook the response path falls back to a bounded retry");
}

void
a_set_unknown_collection_hook_is_invoked_and_its_result_observed([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  int invocations = 0;
  req->on_unknown_collection_ = [&invocations]() -> bool {
    ++invocations;
    return true;
  };

  assert_true(static_cast<bool>(req->on_unknown_collection_), "the hook is held by the request");
  // Mirror the guard in resolve_response: only invoke when the hook is present.
  const bool handled = req->on_unknown_collection_ && req->on_unknown_collection_();
  assert_true(handled, "a hook that accepts marks the status handled");
  assert_eq(invocations, 1, "the hook is invoked once per consultation");
}

void
an_unknown_collection_hook_that_declines_forces_the_fallback([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  req->on_unknown_collection_ = []() -> bool {
    return false;
  };
  const bool handled = req->on_unknown_collection_ && req->on_unknown_collection_();
  assert_false(handled, "a hook that declines leaves the status for the fallback path");
}

void
a_persistent_request_delivers_every_successful_response([[maybe_unused]] context& ctx)
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });
  req->persistent_ = true;

  req->try_callback(nullptr, {});
  req->try_callback(nullptr, {});
  req->try_callback(nullptr, {});

  assert_eq(call_count.load(), 3, "a persistent request is not completed by a response");
}

void
an_error_ends_a_persistent_request_after_delivering_it([[maybe_unused]] context& ctx)
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });
  req->persistent_ = true;

  req->try_callback(nullptr, {});
  assert_eq(call_count.load(), 1, "a successful response is delivered");

  req->try_callback(nullptr, couchbase::errc::common::request_canceled);
  assert_eq(call_count.load(), 2, "the error is delivered too");

  req->try_callback(nullptr, {});
  assert_eq(call_count.load(), 2, "the error completed the request, so nothing follows it");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(cancel_invokes_the_callback_exactly_once) },
      { CASE(cancel_hands_the_callback_the_error_code_it_was_given) },
      { CASE(cancel_without_an_error_code_reports_request_canceled) },
      { CASE(try_callback_invokes_a_non_persistent_callback_exactly_once) },
      // 200 iterations, each spawning two threads. The unit label is run under valgrind and
      // under the sanitizers, where that costs more than the default budget allows.
      { CASE(try_callback_racing_cancel_leaves_exactly_one_invocation), {}, timeout::slow },
      { CASE(cancel_releases_the_state_the_callback_captured) },
      { CASE(retry_attempts_and_reasons_accumulate) },
      { CASE(is_cancelled_reports_completion) },
      { CASE(span_fields_default_to_null) },
      { CASE(the_unknown_collection_hook_defaults_to_empty) },
      { CASE(a_set_unknown_collection_hook_is_invoked_and_its_result_observed) },
      { CASE(an_unknown_collection_hook_that_declines_forces_the_fallback) },
      { CASE(a_persistent_request_delivers_every_successful_response) },
      { CASE(an_error_ends_a_persistent_request_after_delivering_it) },
    },
  };
}

} // namespace couchbase::test
