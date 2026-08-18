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

#include "test_helper.hxx"

#include "core/mcbp/operation_queue.hxx"
#include "core/mcbp/queue_request.hxx"

#include <couchbase/error_codes.hxx>

#include <memory>

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
} // namespace

TEST_CASE("unit: operation_queue removes a request it holds", "[unit]")
{
  auto queue = std::make_shared<operation_queue>();
  auto request = make_request();

  REQUIRE_SUCCESS(queue->push(request, 0));
  REQUIRE(queue->remove(request));

  // The queue no longer owns it, so pushing it again is accepted rather than
  // rejected with request_already_queued.
  REQUIRE_SUCCESS(queue->push(request, 0));
}

TEST_CASE("unit: operation_queue does not remove a request it does not hold", "[unit]")
{
  auto queue = std::make_shared<operation_queue>();
  REQUIRE_FALSE(queue->remove(make_request()));
}

TEST_CASE("unit: a removed request is released by the queue", "[unit]")
{
  // The regression this catches is a leak rather than a wrong answer: while
  // remove() reported failure it also skipped erasing, so the queue kept the
  // last strong reference to the request, and with it everything the request's
  // callback captured.
  auto queue = std::make_shared<operation_queue>();
  std::weak_ptr<queue_request> observer;
  {
    auto request = make_request();
    observer = request;
    REQUIRE_SUCCESS(queue->push(request, 0));
    REQUIRE(queue->remove(request));
  }
  REQUIRE(observer.expired());
}

TEST_CASE("unit: cancelling a queued request takes it out of the queue", "[unit]")
{
  // queue_request::internal_cancel() de-queues through the same remove(), which
  // is how a request completed by cancellation or by its deadline stops being
  // owned by the queue it was parked in.
  auto queue = std::make_shared<operation_queue>();
  std::weak_ptr<queue_request> observer;
  {
    auto request = make_request();
    observer = request;
    REQUIRE_SUCCESS(queue->push(request, 0));
    request->cancel(couchbase::errc::common::request_canceled);
  }
  REQUIRE(observer.expired());
}
