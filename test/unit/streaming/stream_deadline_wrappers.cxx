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

// Integration cases for the two shapes a language binding takes:
//
//   * synchronous API (Ruby, PHP) -- the io_context runs on a thread the binding owns, and the
//     caller's thread blocks on each row.
//   * event loop (Node.js, Python asyncio) -- rows arrive as callbacks on the loop thread;
//     iteration pauses by not requesting the next row.
//
// Both can hold a stream handle and stop iterating. Neither can bound that from the language
// side: finalizer timing is not under the binding's control.

#include "framework/test_registry.hxx"

#include "core/query_stream.hxx"
#include "core/row_streamer.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace utils = ::test::utils;

using namespace std::chrono_literals;

constexpr auto short_deadline = 50ms;
// Any wait that reaches this has failed; it exists so a broken case reports which step hung
// instead of expiring against the harness budget with no detail.
constexpr auto patience = 4s;
// Keeps a marker's expiry strictly after the deadline it follows: equal expiries are unordered.
constexpr auto marker_offset = 5ms;

auto
deadline_in(std::chrono::milliseconds d) -> std::chrono::steady_clock::time_point
{
  return std::chrono::steady_clock::now() + d;
}

auto
result_document(int row_count) -> std::string
{
  std::string doc = R"({"results":[)";
  for (int i = 0; i < row_count; ++i) {
    if (i != 0) {
      doc += ",";
    }
    doc += R"({"n":)" + std::to_string(i) + "}";
  }
  doc += R"(],"status":"success"})";
  return doc;
}

// Small watermarks so a consumer that stops after a handful of rows leaves the streamer parked
// above the high-water mark, which is where a real result set of any size puts it.
auto
back_pressured_options() -> couchbase::core::row_streamer_options
{
  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  return opts;
}

auto
stalling_body(asio::io_context& io) -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, /*terminal_ec*/ {}, /*stall*/ true);
}

// The io_context a binding owns: run on its own thread, held open by a work guard so it does not
// exit between operations.
class binding_runtime
{
public:
  binding_runtime()
    : work_{ asio::make_work_guard(io_) }
    , thread_{ [this]() {
      io_.run();
    } }
  {
  }

  binding_runtime(const binding_runtime&) = delete;
  binding_runtime(binding_runtime&&) = delete;
  auto operator=(const binding_runtime&) -> binding_runtime& = delete;
  auto operator=(binding_runtime&&) -> binding_runtime& = delete;

  ~binding_runtime()
  {
    work_.reset();
    thread_.join();
  }

  auto io() -> asio::io_context&
  {
    return io_;
  }

private:
  asio::io_context io_{};
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  std::thread thread_;
};

// Blocks until the io thread has run everything due strictly before `when`. Waits are ordered by
// expiry, and equal expiries have no defined order, so the marker is offset past the deadline it
// follows. A fixed sleep would instead depend on runner speed.
void
await_io_past(asio::io_context& io, std::chrono::steady_clock::time_point when)
{
  asio::steady_timer marker{ io };
  marker.expires_at(when + marker_offset);
  std::promise<void> reached;
  auto done = reached.get_future();
  marker.async_wait([reached = std::move(reached)](std::error_code) mutable {
    reached.set_value();
  });
  if (done.wait_for(patience) != std::future_status::ready) {
    assert_true(false, "the io thread reaches the marker");
  }
}

struct pulled_row {
  std::optional<std::string> row{};
  std::error_code ec{};
};

// Blocking bridge: pass a callback to the core, then block the caller's thread until it runs.
// An unbounded stream therefore blocks the interpreter thread indefinitely.
auto
pull_blocking(couchbase::core::row_streamer& streamer) -> pulled_row
{
  std::promise<pulled_row> promise;
  auto future = promise.get_future();
  streamer.next_row([promise = std::move(promise)](std::string row, std::error_code ec) mutable {
    pulled_row result{};
    result.ec = ec;
    if (!ec && !row.empty()) {
      result.row = std::move(row);
    }
    promise.set_value(std::move(result));
  });
  if (future.wait_for(patience) != std::future_status::ready) {
    assert_true(false, "a blocking pull returns rather than hanging the caller's thread");
    return {};
  }
  return future.get();
}

void
a_blocking_binding_iterates_every_row_under_a_deadline([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  auto body = utils::make_chunked_response_body(runtime.io(), result_document(500), 256);
  couchbase::core::row_streamer streamer{
    runtime.io(), std::move(body), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });

  // Armed once, covering the whole iteration. A per-row timeout would bound the gap between
  // rows, which the caller controls.
  streamer.set_deadline(
    deadline_in(std::chrono::duration_cast<std::chrono::milliseconds>(patience)));

  int rows = 0;
  pulled_row last{};
  while (true) {
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++rows;
  }

  assert_eq(rows, 500, "a user iterating to the end sees every row");
  assert_eq(last.ec, std::error_code{}, "a deadline the user stayed inside never shows up");
}

void
a_blocking_binding_that_stops_iterating_is_released_at_the_deadline([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  auto body = utils::make_chunked_response_body(runtime.io(), result_document(2000), 256);
  couchbase::core::row_streamer streamer{
    runtime.io(), std::move(body), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });
  const auto deadline = deadline_in(short_deadline);
  streamer.set_deadline(deadline);

  // `rows.first(10)` in Ruby, `break` out of a foreach in PHP: iteration stops, the handle stays
  // alive.
  for (int i = 0; i < 10; ++i) {
    assert_true(pull_blocking(streamer).row.has_value(), "the prefix the user asked for arrives");
  }

  // The socket is reclaimed by the deadline, independently of when the handle is collected.
  await_io_past(runtime.io(), deadline);

  // A later pull -- an explicit `#close`, or resumed iteration -- reports the terminal. Rows
  // buffered before the deadline are valid and drain first.
  int buffered = 0;
  pulled_row last{};
  while (true) {
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++buffered;
  }

  assert_eq(last.ec,
            couchbase::errc::common::ambiguous_timeout,
            "the abandoned stream reports the deadline rather than a clean end");
  assert_ne(buffered, 0, "rows buffered before the deadline are still handed over");
  assert_true(buffered < 2000, "the stream did not run to completion");
}

void
a_blocking_binding_re_arms_the_deadline_per_operation([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  // Large enough that the body is still being read while the user is in their own block. A result
  // that fits under the high-water mark is buffered whole and would survive a broken deadline.
  auto body = utils::make_chunked_response_body(runtime.io(), result_document(2000), 256);
  couchbase::core::row_streamer streamer{
    runtime.io(), std::move(body), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });

  // A binding that exposes a per-call timeout -- the shape Ruby and PHP users expect from every
  // other operation -- re-arms before each pull. Re-arming replaces the previous deadline, so a
  // caller-side work between rows does not count against a single deadline.
  // One deadline per pull. The pauses sum to well past a single deadline, so an implementation
  // that arms once and never replaces it expires part-way through; each pause on its own is a
  // small fraction of one, so a correct implementation is never close to the boundary. The margin
  // is wide in both directions because a loaded runner stretches the pauses, which only has to
  // keep them under per_call.
  constexpr auto pause = short_deadline / 4;    // 12ms
  constexpr auto per_call = short_deadline * 4; // 200ms
  constexpr int pauses = 24;                    // 288ms of pausing
  static_assert(pause * pauses > per_call, "a stale first deadline must expire");
  static_assert(pause * 8 < per_call, "one pause must stay far inside a live deadline");

  int rows = 0;
  int paused = 0;
  pulled_row last{};
  while (true) {
    streamer.set_deadline(deadline_in(per_call));
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++rows;
    // Caller-side work between rows. Waits on the io thread reaching a marker rather than
    // sleeping: test/README.md forbids a fixed sleep standing in for a condition.
    if (paused < pauses && rows % 80 == 0) {
      ++paused;
      await_io_past(runtime.io(), std::chrono::steady_clock::now() + pause);
    }
  }
  assert_eq(paused, pauses, "the case paused for longer in total than one deadline allows");

  assert_eq(rows, 2000, "time spent in the user's own block does not end the stream");
  assert_eq(last.ec, std::error_code{}, "the stream ends cleanly");
}

void
a_blocking_binding_closing_its_handle_reports_a_cancel([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  couchbase::core::row_streamer streamer{ runtime.io(), stalling_body(runtime.io()), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });
  streamer.set_deadline(
    deadline_in(std::chrono::duration_cast<std::chrono::milliseconds>(patience)));

  // Explicit `#close`, or the binding's teardown on scope exit, racing the armed deadline. A
  // retry layer keys off cancel-versus-timeout, so the two must stay distinct.
  streamer.cancel();

  const auto last = pull_blocking(streamer);
  assert_false(last.row.has_value(), "the stream is over");
  assert_eq(last.ec,
            couchbase::errc::common::request_canceled,
            "an explicit close reports a cancel even with a deadline armed");
}

// Event-loop shape: every callback below runs on the io_context thread, and nothing blocks.
void
an_event_loop_binding_delivers_rows_without_blocking([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, result_document(500), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };

  int rows = 0;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  // `for await (const row of result.rows)`: each row resolves a promise whose continuation
  // requests the next.
  std::function<void()> resume = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        ended = true;
        return;
      }
      ++rows;
      resume();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    resume();
  });
  streamer.set_deadline(
    deadline_in(std::chrono::duration_cast<std::chrono::milliseconds>(patience)));
  io.run();

  assert_true(ended, "the iteration completes");
  assert_eq(rows, 500, "every row reaches the loop");
  assert_eq(end_ec, std::error_code{}, "a deadline the consumer stayed inside never shows up");
}

void
an_event_loop_binding_that_pauses_is_released_at_the_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, result_document(2000), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };

  int rows = 0;
  bool paused = false;
  std::function<void()> resume = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        return;
      }
      ++rows;
      // `readable.pause()`, or an async generator no longer awaited: no further request, no
      // error, and no callback.
      if (rows == 10) {
        paused = true;
        return;
      }
      resume();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    resume();
  });
  streamer.set_deadline(deadline_in(short_deadline));
  // run_for rather than run: the feed parks on a full row channel, which is pending work while
  // nothing consumes.
  io.run_for(short_deadline * 8);
  io.restart();

  assert_true(paused, "the consumer stopped asking for rows");

  // `resume()`, or the binding's cleanup.
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  std::function<void()> drain = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        ended = true;
        return;
      }
      ++rows;
      drain();
    });
  };
  drain();
  io.run();

  assert_true(ended, "the stream terminates rather than leaving the consumer without a callback");
  assert_eq(end_ec,
            couchbase::errc::common::ambiguous_timeout,
            "the paused consumer is told the stream timed out");
  assert_true(rows < 2000, "the stream did not run to completion while paused");
}

void
an_event_loop_binding_aborts_ahead_of_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::query_stream stream{ io, stalling_body(io) };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  stream.start([&](std::error_code) {
    stream.next_row([&](std::optional<std::string> /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  stream.set_deadline(deadline_in(std::chrono::duration_cast<std::chrono::milliseconds>(patience)));
  // `AbortController` firing, or `asyncio.Task.cancel()`, with the deadline still armed: it must
  // neither fire nor hold the response alive.
  stream.cancel();

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the pending pull is resolved");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "an abort surfaces as a cancellation, not as a timeout");
  assert_true(elapsed < patience, "the aborted stream leaves no timer holding the loop open");
}

void
a_binding_arms_the_deadline_from_the_thread_it_was_called_on([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  couchbase::core::row_streamer streamer{ runtime.io(), stalling_body(runtime.io()), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });

  // A synchronous binding arms on the caller's thread while its io thread runs the stream.
  // set_deadline names no thread requirement, so any thread must work.
  streamer.set_deadline(deadline_in(short_deadline));

  const auto last = pull_blocking(streamer);
  assert_false(last.row.has_value(), "the stream is over");
  assert_eq(last.ec,
            couchbase::errc::common::ambiguous_timeout,
            "a deadline armed off the io thread still fires");
}

void
concurrent_arming_from_two_threads_is_safe([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto work = asio::make_work_guard(io);
  // More than one runner thread, which is what makes the ordering of two posted arms undefined.
  std::vector<std::thread> runners;
  runners.reserve(4);
  for (int i = 0; i < 4; ++i) {
    runners.emplace_back([&io]() {
      io.run();
    });
  }

  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::atomic_bool ended{ false };
  streamer.next_row([&](std::string /* row */, std::error_code ec) {
    end_ec = ec;
    ended = true;
  });

  // Two threads re-arm repeatedly against a multi-threaded io_context. Every deadline is far
  // enough away that none may fire, so this pins that concurrent arming leaves the stream usable
  // and gives the sanitizers the path to inspect. It does not establish which of two racing arms
  // wins; the sequence guard in row_streamer::set_deadline has no deterministic test.
  std::vector<std::thread> arming;
  arming.reserve(2);
  for (int t = 0; t < 2; ++t) {
    arming.emplace_back([&streamer]() {
      for (int i = 0; i < 200; ++i) {
        streamer.set_deadline(deadline_in(patience));
      }
    });
  }
  for (auto& th : arming) {
    th.join();
  }

  streamer.cancel();
  work.reset();
  for (auto& th : runners) {
    th.join();
  }

  assert_true(ended.load(), "the stream terminates");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "concurrent arming leaves the stream cancellable, not timed out");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_blocking_binding_iterates_every_row_under_a_deadline) },
      { CASE(a_blocking_binding_that_stops_iterating_is_released_at_the_deadline) },
      { CASE(a_blocking_binding_re_arms_the_deadline_per_operation) },
      { CASE(a_blocking_binding_closing_its_handle_reports_a_cancel) },
      { CASE(an_event_loop_binding_delivers_rows_without_blocking) },
      { CASE(an_event_loop_binding_that_pauses_is_released_at_the_deadline) },
      { CASE(an_event_loop_binding_aborts_ahead_of_its_deadline) },
      { CASE(a_binding_arms_the_deadline_from_the_thread_it_was_called_on) },
      { CASE(concurrent_arming_from_two_threads_is_safe) },
    },
  };
}

} // namespace couchbase::test
