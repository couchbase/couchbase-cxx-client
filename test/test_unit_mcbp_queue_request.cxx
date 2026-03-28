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

#include "test_helper.hxx"

#include "core/mcbp/queue_request.hxx"
#include "core/mcbp/queue_response.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <atomic>
#include <future>
#include <memory>
#include <thread>

namespace
{
using couchbase::core::mcbp::queue_callback;
using couchbase::core::mcbp::queue_request;
using couchbase::core::mcbp::queue_response;

auto
make_get_request(queue_callback&& cb) -> std::shared_ptr<queue_request>
{
  return std::make_shared<queue_request>(couchbase::core::protocol::magic::client_request,
                                         couchbase::core::protocol::client_opcode::get,
                                         std::move(cb));
}
} // namespace

TEST_CASE("unit: queue_request cancel invokes callback exactly once", "[unit]")
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });

  req->cancel(couchbase::errc::common::request_canceled);

  REQUIRE(call_count.load() == 1);

  SECTION("second cancel is a no-op")
  {
    req->cancel(couchbase::errc::common::request_canceled);
    REQUIRE(call_count.load() == 1);
  }
}

TEST_CASE("unit: queue_request cancel passes error code to callback", "[unit]")
{
  std::error_code received_ec{};

  auto req = make_get_request([&received_ec](auto /*resp*/, auto /*req*/, std::error_code ec) {
    received_ec = ec;
  });

  req->cancel(couchbase::errc::common::request_canceled);

  REQUIRE(received_ec == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: queue_request cancel() without error uses request_canceled", "[unit]")
{
  std::error_code received_ec{};

  auto req = make_get_request([&received_ec](auto /*resp*/, auto /*req*/, std::error_code ec) {
    received_ec = ec;
  });

  req->cancel(); // the pending_operation override — uses request_canceled

  REQUIRE(received_ec == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: queue_request try_callback invokes callback exactly once for non-persistent",
          "[unit]")
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });

  req->try_callback(nullptr, {});

  REQUIRE(call_count.load() == 1);

  SECTION("second try_callback is a no-op after completion")
  {
    req->try_callback(nullptr, {});
    REQUIRE(call_count.load() == 1);
  }
}

TEST_CASE("unit: queue_request try_callback races with cancel — exactly one wins", "[unit]")
{
  // Stress test: fire try_callback and cancel concurrently many times and verify
  // the callback is invoked exactly once per request.
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

    CHECK(call_count.load() == 1);
  }
}

TEST_CASE("unit: queue_request callback is moved out on cancel to release captured state", "[unit]")
{
  // Verify that after cancel() the callback_ has been moved out, so any
  // shared_ptr captured inside the lambda is released promptly.
  auto shared_resource = std::make_shared<int>(42);
  std::weak_ptr<int> weak_resource = shared_resource;

  {
    // The callback captures shared_resource by value.
    auto req =
      make_get_request([captured = shared_resource](auto /*resp*/, auto /*req*/, auto /*ec*/) {
        // Use captured to prevent the compiler optimising it away.
        (void)captured;
      });

    // Drop our own copy — now only the captured lambda holds a reference.
    shared_resource.reset();
    REQUIRE_FALSE(weak_resource.expired());

    req->cancel(couchbase::errc::common::request_canceled);

    // After cancel the callback lambda (and its captured shared_ptr) must have
    // been released, so the resource should be destroyed.
    REQUIRE(weak_resource.expired());
  }
}

TEST_CASE("unit: queue_request retry tracking", "[unit]")
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  REQUIRE(req->retry_attempts() == 0);
  REQUIRE(req->retry_reasons().empty());

  req->record_retry_attempt(couchbase::retry_reason::node_not_available);
  REQUIRE(req->retry_attempts() == 1);
  REQUIRE(req->retry_reasons().count(couchbase::retry_reason::node_not_available) == 1);

  req->record_retry_attempt(couchbase::retry_reason::key_value_temporary_failure);
  REQUIRE(req->retry_attempts() == 2);
  REQUIRE(req->retry_reasons().count(couchbase::retry_reason::key_value_temporary_failure) == 1);

  auto [count, reasons] = req->retries();
  REQUIRE(count == 2);
  REQUIRE(reasons.size() == 2);
}

TEST_CASE("unit: queue_request is_cancelled reflects completion state", "[unit]")
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  REQUIRE_FALSE(req->is_cancelled());

  req->cancel(couchbase::errc::common::request_canceled);

  REQUIRE(req->is_cancelled());
}

TEST_CASE("unit: queue_request span fields default to null", "[unit]")
{
  auto req = make_get_request([](auto /*resp*/, auto /*req*/, auto /*ec*/) {
  });

  REQUIRE(req->parent_span_ == nullptr);
  REQUIRE(req->dispatch_span_ == nullptr);
}

TEST_CASE("unit: queue_request persistent — callback invoked for each successful response",
          "[unit]")
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });
  req->persistent_ = true;

  // Successful responses should keep being delivered.
  req->try_callback(nullptr, {});
  req->try_callback(nullptr, {});
  req->try_callback(nullptr, {});

  REQUIRE(call_count.load() == 3);
}

TEST_CASE("unit: queue_request persistent — error cancels and invokes callback once", "[unit]")
{
  std::atomic<int> call_count{ 0 };

  auto req = make_get_request([&call_count](auto /*resp*/, auto /*req*/, auto /*ec*/) {
    ++call_count;
  });
  req->persistent_ = true;

  // A successful delivery first.
  req->try_callback(nullptr, {});
  REQUIRE(call_count.load() == 1);

  // An error delivery should cancel.
  req->try_callback(nullptr, couchbase::errc::common::request_canceled);
  REQUIRE(call_count.load() == 2);

  // Subsequent calls should be no-ops.
  req->try_callback(nullptr, {});
  REQUIRE(call_count.load() == 2);
}
