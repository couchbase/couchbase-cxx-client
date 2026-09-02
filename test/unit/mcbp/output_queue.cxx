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

#include "framework/test_registry.hxx"

#include "core/io/mcbp_output_queue.hxx"

#include <cstddef>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::mcbp_output_queue;

auto
byte_buffer(std::byte marker, std::size_t size = 1) -> std::vector<std::byte>
{
  return std::vector<std::byte>(size, marker);
}

void
an_empty_queue_requests_no_dispatch([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;
  assert_false(queue.mark_for_dispatch(), "an empty queue has nothing to schedule");
  assert_false(queue.begin_writing(), "an empty queue has no batch to write");
}

void
only_the_first_enqueue_requests_a_dispatch([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;

  // idle -> scheduled: the caller must post do_write.
  assert_true(queue.enqueue(byte_buffer(std::byte{ 0x01 })), "the first enqueue arms the dispatch");
  // already scheduled: a second enqueue must not request another post.
  assert_false(queue.enqueue(byte_buffer(std::byte{ 0x02 })),
               "a second enqueue does not request another post");
  assert_false(queue.enqueue(byte_buffer(std::byte{ 0x03 })),
               "a third enqueue does not request another post");
}

void
begin_writing_moves_everything_queued_into_the_batch([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x02 })));

  assert_true(queue.begin_writing(), "a queue with data hands out a batch");
  assert_eq(queue.writing().size(), std::size_t{ 2 }, "the batch holds everything queued");
  assert_eq(queue.writing()[0][0], std::byte{ 0x01 }, "the first buffer enqueued leads the batch");
  assert_eq(queue.writing()[1][0], std::byte{ 0x02 }, "the second buffer follows it");
}

void
a_write_in_flight_blocks_a_second_batch([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  assert_true(queue.begin_writing(), "the first batch is handed out");

  // Issuing a concurrent async_write on the same stream would corrupt the connection.
  assert_false(queue.begin_writing(), "a second batch is refused while the first is in flight");
  assert_eq(queue.writing().size(), std::size_t{ 1 }, "the batch in flight is left untouched");
}

void
a_buffer_enqueued_during_an_in_flight_write_is_not_lost([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;

  assert_true(queue.enqueue(byte_buffer(std::byte{ 0x01 })), "the first enqueue arms the dispatch");
  assert_true(queue.begin_writing(), "the first batch is handed out");

  // A concurrent enqueue arrives while the write is in flight; the in-flight write's completion is
  // responsible for re-dispatching, so this one must not request a post -- but it must be retained.
  assert_false(queue.enqueue(byte_buffer(std::byte{ 0x02 })),
               "an enqueue during a write does not request another post");

  queue.finish_writing(); // batch #1 completed; caller re-posts do_write

  assert_true(queue.begin_writing(), "the re-dispatch hands out a second batch");
  assert_eq(queue.writing().size(), std::size_t{ 1 }, "the batch holds the buffer that arrived");
  assert_eq(queue.writing()[0][0],
            std::byte{ 0x02 },
            "the buffer that arrived mid-flight is the one written");
}

void
draining_to_empty_returns_the_queue_to_idle([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  assert_true(queue.begin_writing(), "the batch is handed out");
  queue.finish_writing();

  assert_false(queue.begin_writing(), "nothing is left to write");
  assert_true(queue.enqueue(byte_buffer(std::byte{ 0x04 })),
              "the queue is idle again, so the next enqueue re-arms the dispatch");
}

void
mark_for_dispatch_schedules_staged_data_exactly_once([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;

  // Data staged without requesting a dispatch (mirrors the pending-buffer drain path, which
  // stages many buffers and flushes once).
  queue.stage(byte_buffer(std::byte{ 0x01 }));
  queue.stage(byte_buffer(std::byte{ 0x02 }));

  assert_true(queue.mark_for_dispatch(), "staged data on an idle queue is scheduled");
  assert_false(queue.mark_for_dispatch(), "an already scheduled queue is not scheduled again");

  assert_true(queue.begin_writing(), "the staged data is handed out as a batch");
  assert_eq(queue.writing().size(), std::size_t{ 2 }, "the batch holds everything staged");
}

void
reset_clears_both_buffers_and_returns_the_queue_to_idle([[maybe_unused]] context& ctx)
{
  mcbp_output_queue queue;
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x01 })));
  static_cast<void>(queue.begin_writing());
  static_cast<void>(queue.enqueue(byte_buffer(std::byte{ 0x02 })));

  queue.reset();

  assert_true(queue.writing().empty(), "the batch in flight is discarded");
  assert_false(queue.mark_for_dispatch(), "nothing is left queued to schedule");
  assert_true(queue.enqueue(byte_buffer(std::byte{ 0x03 })),
              "the queue is idle again, so the next enqueue re-arms the dispatch");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_empty_queue_requests_no_dispatch) },
      { CASE(only_the_first_enqueue_requests_a_dispatch) },
      { CASE(begin_writing_moves_everything_queued_into_the_batch) },
      { CASE(a_write_in_flight_blocks_a_second_batch) },
      { CASE(a_buffer_enqueued_during_an_in_flight_write_is_not_lost) },
      { CASE(draining_to_empty_returns_the_queue_to_idle) },
      { CASE(mark_for_dispatch_schedules_staged_data_exactly_once) },
      { CASE(reset_clears_both_buffers_and_returns_the_queue_to_idle) },
    },
  };
}

} // namespace couchbase::test
