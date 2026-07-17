/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "core/io/mcbp_output_queue.hxx"

#include <cstddef>
#include <vector>

namespace
{
auto
byte_buffer(std::byte marker, std::size_t size = 1) -> std::vector<std::byte>
{
  return std::vector<std::byte>(size, marker);
}
} // namespace

TEST_CASE("unit: an empty output queue does not request a dispatch on flush", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;
  REQUIRE_FALSE(queue.mark_for_dispatch());
  REQUIRE_FALSE(queue.begin_writing());
}

TEST_CASE("unit: the first enqueue requests a dispatch, subsequent ones do not", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;

  // idle -> scheduled: the caller must post do_write.
  REQUIRE(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  // already scheduled: a second enqueue must not request another post.
  REQUIRE_FALSE(queue.enqueue(byte_buffer(std::byte{ 0x02 })));
  REQUIRE_FALSE(queue.enqueue(byte_buffer(std::byte{ 0x03 })));
}

TEST_CASE("unit: begin_writing moves everything queued into the writing batch", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x02 })));

  REQUIRE(queue.begin_writing());
  REQUIRE(queue.writing().size() == 2);
  REQUIRE(queue.writing()[0][0] == std::byte{ 0x01 });
  REQUIRE(queue.writing()[1][0] == std::byte{ 0x02 });
}

TEST_CASE("unit: a write in flight is never dispatched again until it finishes", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  REQUIRE(queue.begin_writing()); // batch #1 in flight

  // While the batch is in flight begin_writing must not hand out a second batch (issuing a
  // concurrent async_write on the same stream would corrupt the connection).
  REQUIRE_FALSE(queue.begin_writing());
  REQUIRE(queue.writing().size() == 1);
}

TEST_CASE("unit: a buffer enqueued during an in-flight write is not lost", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;

  REQUIRE(queue.enqueue(byte_buffer(std::byte{ 0x01 }))); // idle -> scheduled, caller posts
  REQUIRE(queue.begin_writing());                         // batch #1 in flight

  // A concurrent enqueue arrives while the write is in flight; it must NOT request a post (the
  // in-flight write's completion is responsible for re-dispatching), but it must be retained.
  REQUIRE_FALSE(queue.enqueue(byte_buffer(std::byte{ 0x02 })));

  queue.finish_writing(); // batch #1 completed; caller re-posts do_write

  // The re-dispatch must pick up the buffer that arrived mid-flight.
  REQUIRE(queue.begin_writing());
  REQUIRE(queue.writing().size() == 1);
  REQUIRE(queue.writing()[0][0] == std::byte{ 0x02 });
}

TEST_CASE("unit: draining to empty returns the queue to idle so the next enqueue re-arms", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  REQUIRE(queue.begin_writing());
  queue.finish_writing();

  // Nothing left to write: begin_writing reports no batch and the queue goes idle.
  REQUIRE_FALSE(queue.begin_writing());

  // Because it is idle again, the next enqueue must request a fresh dispatch.
  REQUIRE(queue.enqueue(byte_buffer(std::byte{ 0x04 })));
}

TEST_CASE("unit: mark_for_dispatch schedules queued data exactly once", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;

  // Data staged without requesting a dispatch (mirrors the pending-buffer drain path, which
  // stages many buffers and flushes once).
  queue.stage(byte_buffer(std::byte{ 0x01 }));
  queue.stage(byte_buffer(std::byte{ 0x02 }));

  REQUIRE(queue.mark_for_dispatch());       // idle + non-empty -> schedule
  REQUIRE_FALSE(queue.mark_for_dispatch()); // already scheduled -> no second post

  REQUIRE(queue.begin_writing());
  REQUIRE(queue.writing().size() == 2);
}

TEST_CASE("unit: reset clears both buffers and returns the queue to idle", "[unit]")
{
  couchbase::core::io::mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  static_cast<void>(queue.begin_writing());
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x02 })));

  queue.reset();

  REQUIRE(queue.writing().empty());
  REQUIRE_FALSE(queue.mark_for_dispatch()); // nothing queued
  // After a reset the queue is idle, so a new enqueue re-arms the dispatch.
  REQUIRE(queue.enqueue(byte_buffer(std::byte{ 0x03 })));
}
