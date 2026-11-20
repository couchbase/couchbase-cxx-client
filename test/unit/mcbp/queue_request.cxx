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
#include <couchbase/tracing/request_span.hxx>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <string>
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

    // One gate for both tasks, released once both exist: launched in sequence, the first can
    // complete before the second is created, and that iteration applies no concurrent pressure.
    std::promise<void> start_gate;
    auto start_future = start_gate.get_future().share();

    auto f1 = std::async(std::launch::async, [&req, start_future] {
      start_future.wait();
      req->try_callback(nullptr, {});
    });
    auto f2 = std::async(std::launch::async, [&req, start_future] {
      start_future.wait();
      req->cancel(couchbase::errc::common::request_canceled);
    });

    start_gate.set_value();
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
a_set_unknown_collection_hook_is_held_and_forwards_its_verdict([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  int invocations = 0;
  req->on_unknown_collection_ = [&invocations]() -> bool {
    ++invocations;
    return true;
  };

  assert_true(static_cast<bool>(req->on_unknown_collection_), "the hook is held by the request");
  // The consultation below is this file's own, not the production path's: nothing here reaches
  // bucket_impl::resolve_response, so these two cases hold the seam -- the field is carried on the
  // request and returns the verdict it was given -- and not that anything consults it. That the
  // KV response path does is held end to end by the range-scan integration test named above.
  const bool handled = req->on_unknown_collection_ && req->on_unknown_collection_();
  assert_true(handled, "a hook that accepts marks the status handled");
  assert_eq(invocations, 1, "the hook is invoked once per consultation");
}

void
an_unknown_collection_hook_that_declines_reports_false([[maybe_unused]] context& ctx)
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
class recording_span : public couchbase::tracing::request_span
{
public:
  recording_span()
    : request_span("dispatch_to_server")
  {
  }

  void add_tag(const std::string& name, std::uint64_t value) override
  {
    int_tags[name] = value;
  }

  void add_tag(const std::string& name, const std::string& value) override
  {
    string_tags[name] = value;
  }

  void end() override
  {
    ++end_count;
  }

  std::map<std::string, std::uint64_t> int_tags{};
  std::map<std::string, std::string> string_tags{};
  int end_count{ 0 };
};

void
a_dispatch_span_ends_on_the_response([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });
  auto span = std::make_shared<recording_span>();
  req->set_dispatch_span(span, "peer", 11210, "server", 11210);

  req->record_dispatch_response(std::chrono::microseconds{ 7 }, true);
  assert_eq(span->end_count, 1, "a response ends the span of a non-persistent request");
  assert_eq(span->int_tags["couchbase.server_duration"],
            std::uint64_t{ 7 },
            "the span records the server duration");

  req->try_callback(nullptr, {});
  assert_eq(span->end_count, 1, "completing the request does not end the span again");
}

void
a_persistent_dispatch_span_sums_every_frame_and_ends_at_the_final_one([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });
  req->persistent_ = true;
  auto span = std::make_shared<recording_span>();
  req->set_dispatch_span(span, "peer", 11210, "server", 11210);

  req->record_dispatch_response(std::chrono::microseconds{ 3 }, false);
  req->record_dispatch_response(std::chrono::microseconds{ 4 }, false);
  assert_eq(span->end_count, 0, "a persistent request keeps its span open between frames");

  req->record_dispatch_response(std::chrono::microseconds{ 2 }, true);
  assert_eq(span->end_count, 1, "the final frame ends the span");
  assert_eq(span->int_tags["couchbase.server_duration"],
            std::uint64_t{ 9 },
            "the span records the sum over all frames");

  assert_true(req->internal_cancel(), "the final frame's handler completes the request");
  assert_eq(span->end_count, 1, "completion does not end the span again");
}

void
a_dispatch_span_without_a_response_ends_on_cancel_with_no_result([[maybe_unused]] context& ctx)
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });
  auto span = std::make_shared<recording_span>();
  req->set_dispatch_span(span, "peer", 11210, "server", 11210);

  req->cancel(couchbase::errc::common::unambiguous_timeout);
  assert_eq(span->end_count, 1, "cancellation ends the span");
  assert_true(span->int_tags.count("couchbase.server_duration") == 0,
              "an unanswered attempt records no server duration");

  auto late = std::make_shared<recording_span>();
  req->set_dispatch_span(late, "peer", 11210, "server", 11210);
  assert_eq(late->end_count, 1, "a span set after completion is ended at once");
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
      { CASE(a_set_unknown_collection_hook_is_held_and_forwards_its_verdict) },
      { CASE(an_unknown_collection_hook_that_declines_reports_false) },
      { CASE(a_persistent_request_delivers_every_successful_response) },
      { CASE(an_error_ends_a_persistent_request_after_delivering_it) },
      { CASE(a_dispatch_span_ends_on_the_response) },
      { CASE(a_persistent_dispatch_span_sums_every_frame_and_ends_at_the_final_one) },
      { CASE(a_dispatch_span_without_a_response_ends_on_cancel_with_no_result) },
    },
  };
}

} // namespace couchbase::test
