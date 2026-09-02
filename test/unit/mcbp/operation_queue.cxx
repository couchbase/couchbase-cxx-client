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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/mcbp/operation_queue.hxx"
#include "core/mcbp/queue_request.hxx"

#include <couchbase/error_codes.hxx>

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::mcbp::operation_queue;
using couchbase::core::mcbp::queue_request;

auto
make_request() -> std::shared_ptr<queue_request>
{
  return std::make_shared<queue_request>(
    couchbase::core::protocol::magic::client_request,
    couchbase::core::protocol::client_opcode::get,
    [](auto /* response */, auto /* request */, auto /* error */) {
    });
}

void
a_held_request_is_removed_and_can_be_pushed_again([[maybe_unused]] context& ctx)
{
  auto queue = std::make_shared<operation_queue>();
  auto request = make_request();

  assert_success(queue->push(request, 0), "the queue accepts the request");
  assert_true(queue->remove(request), "the queue holds the request it was given");

  // The queue no longer owns it, so pushing it again is accepted rather than rejected with
  // request_already_queued.
  assert_success(queue->push(request, 0), "a removed request may be queued again");
}

void
a_request_the_queue_does_not_hold_is_not_removed([[maybe_unused]] context& ctx)
{
  auto queue = std::make_shared<operation_queue>();
  assert_false(queue->remove(make_request()), "a request that was never pushed");
}

void
a_removed_request_is_released_by_the_queue([[maybe_unused]] context& ctx)
{
  // The regression this catches is a leak rather than a wrong answer: while remove() reported
  // failure it also skipped erasing, so the queue kept the last strong reference to the request,
  // and with it everything the request's callback captured.
  auto queue = std::make_shared<operation_queue>();
  std::weak_ptr<queue_request> observer;
  {
    auto request = make_request();
    observer = request;
    assert_success(queue->push(request, 0), "the queue accepts the request");
    assert_true(queue->remove(request), "the queue holds the request it was given");
  }
  assert_true(observer.expired(), "removing releases the queue's reference to the request");
}

void
a_closed_queue_refuses_a_push([[maybe_unused]] context& ctx)
{
  // This is what keeps a request from being parked in a queue that has already been drained: the
  // caller sees the error and completes the request itself.
  auto queue = std::make_shared<operation_queue>();
  queue->close();

  auto request = make_request();
  assert_eq(queue->push(request, 0),
            couchbase::errc::network::operation_queue_closed,
            "a push after close is refused rather than silently parked");
}

void
draining_a_closed_queue_hands_over_its_requests([[maybe_unused]] context& ctx)
{
  auto queue = std::make_shared<operation_queue>();
  auto request = make_request();
  assert_success(queue->push(request, 0), "the queue accepts the request");

  queue->close();
  std::vector<std::shared_ptr<queue_request>> drained;
  queue->drain([&drained](auto r) {
    drained.emplace_back(std::move(r));
  });
  assert_eq(drained.size(), std::size_t{ 1 }, "everything the queue held is drained");
  assert_true(drained.front() == request, "the drained request is the one that was pushed");

  // Ownership is handed over, not shared: a drained request can be pushed into another queue, and
  // the queue it came from no longer holds it.
  auto other = std::make_shared<operation_queue>();
  assert_success(other->push(request, 0), "a drained request may be queued elsewhere");
}

void
cancelling_a_queued_request_takes_it_out_of_the_queue([[maybe_unused]] context& ctx)
{
  // queue_request::internal_cancel() de-queues through the same remove(), which is how a request
  // completed by cancellation or by its deadline stops being owned by the queue it was parked in.
  auto queue = std::make_shared<operation_queue>();
  std::weak_ptr<queue_request> observer;
  {
    auto request = make_request();
    observer = request;
    assert_success(queue->push(request, 0), "the queue accepts the request");
    request->cancel(couchbase::errc::common::request_canceled);
  }
  assert_true(observer.expired(), "cancelling releases the queue's reference to the request");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_held_request_is_removed_and_can_be_pushed_again) },
      { CASE(a_request_the_queue_does_not_hold_is_not_removed) },
      { CASE(a_removed_request_is_released_by_the_queue) },
      { CASE(a_closed_queue_refuses_a_push) },
      { CASE(draining_a_closed_queue_hands_over_its_requests) },
      { CASE(cancelling_a_queued_request_takes_it_out_of_the_queue) },
    },
  };
}

} // namespace couchbase::test
