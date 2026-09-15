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
// Both can hold a stream handle and stop iterating, which the language side cannot bound:
// finalizer timing is not under the binding's control.

#include "framework/context.hxx"
#include "framework/test_registry.hxx"

#include "core/analytics_stream.hxx"
#include "core/query_stream.hxx"
#include "core/row_streamer.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
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

constexpr auto short_deadline_unscaled = 50ms;
// Any wait that reaches this has failed. Under the case budget, so the failure names the step.
constexpr auto patience_unscaled = 2s;

// The harness scales each case budget by CB_TEST_TIMEOUT_MULTIPLIER but does not reach constants
// inside a case. Scaling here by the same factor keeps every wait under the budget it is meant to
// sit under, instead of failing inside an instrumented case that still has budget left.
// Scale an unscaled duration by the suite multiplier. Multiples are taken of the unscaled value
// and scaled once: scale_budget() saturates at milliseconds::max() for a large accepted factor,
// and multiplying a saturated result again is meaningless.
auto
scaled_budget(std::chrono::milliseconds unscaled) -> std::chrono::milliseconds
{
  return scale_budget(unscaled, timeout_multiplier(safe_getenv(timeout_multiplier_variable)));
}

auto
patience() -> std::chrono::milliseconds
{
  static const auto scaled =
    scaled_budget(std::chrono::duration_cast<std::chrono::milliseconds>(patience_unscaled));
  return scaled;
}
// Scaled for the same reason as patience(): a fixed 50ms is reached by a loaded or instrumented
// run before the case has set its scenario up, and the deadline then fires on correct behaviour.
auto
short_deadline() -> std::chrono::milliseconds
{
  static const auto scaled =
    scaled_budget(std::chrono::duration_cast<std::chrono::milliseconds>(short_deadline_unscaled));
  return scaled;
}

// Keeps a marker's expiry strictly after the deadline it follows: equal expiries are unordered.
// Not scaled, unlike the durations above: asio orders timers by expiry, so any positive offset
// gives the ordering this needs, at any multiplier.
constexpr auto marker_offset = 5ms;

// Saturating add on a time point. Any deadline here may already be time_point::max() under a
// large accepted multiplier, and adding to that wraps into the past -- turning a deadline that
// should never fire into one that fires at once. Nothing in this file adds to a time point
// directly; every site goes through this.
auto
past(std::chrono::steady_clock::time_point tp, std::chrono::milliseconds d)
  -> std::chrono::steady_clock::time_point
{
  const auto limit = std::chrono::steady_clock::time_point::max();
  // Compared in milliseconds. `limit - tp` is the clock's own duration, finer than milliseconds on
  // every platform here, so comparing the two directly converts `d` to that unit -- and a `d` of
  // milliseconds::max(), which scale_budget returns for a saturating multiplier, overflows in that
  // conversion. The guard then reads false and the addition below wraps into the past, firing at
  // once the deadline that was meant never to fire. duration_cast to the coarser unit truncates
  // towards zero, cannot overflow, and only ever understates the headroom.
  const auto headroom = std::chrono::duration_cast<std::chrono::milliseconds>(limit - tp);
  return d > headroom ? limit : tp + d;
}

auto
deadline_in(std::chrono::milliseconds d) -> std::chrono::steady_clock::time_point
{
  return past(std::chrono::steady_clock::now(), d);
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
// above the high-water mark.
auto
back_pressured_options() -> couchbase::core::row_streamer_options
{
  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  return opts;
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
    join();
  }

  // Stops the io thread and waits for it. A case holding a loopback_stream calls this before the
  // server leaves scope: ~loopback_stream stops the session and closes the listening socket from
  // the calling thread, neither of which is safe while the io thread still runs. Idempotent, so
  // the destructor may call it again.
  //
  // stop() and not just work_.reset(). Releasing the work guard lets run() return once outstanding
  // operations finish, and these cases park socket reads that nothing completes until
  // ~loopback_stream closes the socket -- which happens after this. Waiting for them would hang
  // teardown rather than race it.
  void join()
  {
    io_.stop();
    work_.reset();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  auto io() -> asio::io_context&
  {
    return io_;
  }

  // Joins on scope exit, including while an assertion unwinds. Declared after the loopback_stream
  // it protects, so it runs first and the io thread is stopped before ~loopback_stream touches the
  // session and the listening socket. A trailing join() call cannot do this: unwinding skips it,
  // and a failed assertion then becomes a race instead of a legible failure.
  class scoped_join
  {
  public:
    explicit scoped_join(binding_runtime& runtime)
      : runtime_{ runtime }
    {
    }

    scoped_join(const scoped_join&) = delete;
    scoped_join(scoped_join&&) = delete;
    auto operator=(const scoped_join&) -> scoped_join& = delete;
    auto operator=(scoped_join&&) -> scoped_join& = delete;

    ~scoped_join()
    {
      runtime_.join();
    }

  private:
    binding_runtime& runtime_;
  };

  [[nodiscard]] auto joiner() -> scoped_join
  {
    return scoped_join{ *this };
  }

private:
  asio::io_context io_{};
  asio::executor_work_guard<asio::io_context::executor_type> work_;
  std::thread thread_;
};

// Stops an io_context and joins the threads running it, on scope exit and while an assertion
// unwinds alike. Declared after the loopback_stream it protects, so the threads are gone before
// ~loopback_stream touches the session and the listening socket, and no joinable std::thread is
// destroyed by unwinding -- which calls std::terminate and replaces the failure message.
class scoped_runners
{
public:
  scoped_runners(asio::io_context& io,
                 asio::executor_work_guard<asio::io_context::executor_type>& work,
                 std::vector<std::thread>& threads)
    : io_{ io }
    , work_{ work }
    , threads_{ threads }
  {
  }

  scoped_runners(const scoped_runners&) = delete;
  scoped_runners(scoped_runners&&) = delete;
  auto operator=(const scoped_runners&) -> scoped_runners& = delete;
  auto operator=(scoped_runners&&) -> scoped_runners& = delete;

  ~scoped_runners()
  {
    io_.stop();
    work_.reset();
    for (auto& th : threads_) {
      if (th.joinable()) {
        th.join();
      }
    }
  }

private:
  asio::io_context& io_;
  asio::executor_work_guard<asio::io_context::executor_type>& work_;
  std::vector<std::thread>& threads_;
};

// Blocks until the io thread has run everything due strictly before `when`. Equal expiries have
// no defined order, so the marker is offset past the deadline it follows.
void
await_io_past(asio::io_context& io, std::chrono::steady_clock::time_point when)
{
  // Shared, not stack: a wait that times out below leaves the handler pending on the io thread,
  // and a stack timer would be destroyed under it.
  auto marker = std::make_shared<asio::steady_timer>(io);
  // `when` is the deadline itself; the offset past it belongs here, once, and saturating.
  // Callers pass the deadline and do not pre-add.
  marker->expires_at(past(when, marker_offset));
  std::promise<void> reached;
  auto done = reached.get_future();
  marker->async_wait([marker, reached = std::move(reached)](std::error_code) mutable {
    reached.set_value();
  });
  if (done.wait_for(patience()) != std::future_status::ready) {
    fail("the io thread reaches the marker");
  }
}

// Blocks until the loopback session hands over a live body.
auto
body_over_loopback(utils::loopback_stream& server) -> couchbase::core::http_response_body
{
  // The promise outlives this frame: a failing wait leaves the callback pending, and teardown can
  // still run it.
  auto ready = std::make_shared<std::promise<couchbase::core::http_response_body>>();
  auto body = ready->get_future();
  server.connect([ready](couchbase::core::http_response_body b) mutable {
    ready->set_value(std::move(b));
  });
  if (body.wait_for(patience()) != std::future_status::ready) {
    fail("the loopback session delivers a response body");
  }
  return body.get();
}

struct pulled_row {
  std::optional<std::string> row{};
  std::error_code ec{};
};

// Blocking bridge: pass a callback to the core, then block the caller's thread until it runs.
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
  if (future.wait_for(patience()) != std::future_status::ready) {
    assert_true(false, "a blocking pull returns rather than hanging the caller's thread");
    return {};
  }
  return future.get();
}

void
a_blocking_binding_iterates_every_row_under_a_deadline([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  auto body = utils::make_chunked_response_body(runtime.io(), result_document(2000), 256);
  couchbase::core::row_streamer streamer{
    runtime.io(), std::move(body), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });

  // Armed once, covering the whole iteration. A per-row timeout would bound the gap between
  // rows, which the caller controls.
  assert_eq(streamer.set_deadline(deadline_in(patience())),
            couchbase::core::io::deadline_state::armed,
            "a live stream arms");

  int rows = 0;
  pulled_row last{};
  while (true) {
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++rows;
  }

  assert_eq(rows, 2000, "a user iterating to the end sees every row");
  assert_eq(last.ec, std::error_code{}, "a deadline the user stayed inside never shows up");
  // A drained body refuses a deadline, which pins that the clean end left the body terminal. It
  // says nothing about the timer: set_deadline decides from the terminal state alone. The disarm
  // is bounded by an_event_loop_binding_delivers_rows_without_blocking, and cannot be shown here,
  // because binding_runtime runs the loop on its own thread behind a work guard and a timer left
  // armed costs this case no wall clock.
  assert_eq(streamer.set_deadline(deadline_in(patience())),
            couchbase::core::io::deadline_state::body_already_ended,
            "the drained stream refuses a further deadline");
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
  const auto deadline = deadline_in(short_deadline());
  assert_eq(streamer.set_deadline(deadline),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");

  // `rows.first(10)` in Ruby, `break` out of a foreach in PHP: iteration stops, the handle stays
  // alive.
  for (int i = 0; i < 10; ++i) {
    assert_true(pull_blocking(streamer).row.has_value(), "the prefix the user asked for arrives");
  }

  // The deadline closes the body, independently of when the handle is collected.
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
  // Counted across both loops. Bounding `buffered` alone could never reach 2000, because ten rows
  // were consumed before it started, so that form of the assertion passed whatever happened.
  assert_true(10 + buffered < 2000, "the deadline cut the stream short of its last row");
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

  // The deadline rolls: each arm replaces the one before it, so the bound covers a single pull and
  // the caller-side work that follows it, never the accumulated run. The pauses sum past one
  // deadline while each pause alone stays far inside one. Both are fractions of patience(), so the
  // static_assert on the unscaled values certifies the two relations for any multiplier of one
  // or more. Below one they do not survive scaling: scale_budget() floors at a millisecond,
  // which collapses per_call, pause and the case budget onto the same value.
  constexpr int pauses = 24;
  constexpr auto per_call_unscaled =
    std::chrono::duration_cast<std::chrono::milliseconds>(patience_unscaled) / 4;
  constexpr auto pause_unscaled = per_call_unscaled / 16;
  static_assert(pause_unscaled * pauses > per_call_unscaled, "a stale first deadline must expire");
  static_assert(pause_unscaled * 8 < per_call_unscaled, "one pause stays far inside a live one");
  const auto per_call = scaled_budget(per_call_unscaled);
  const auto pause = scaled_budget(pause_unscaled);

  int rows = 0;
  int paused = 0;
  int armed = 0;
  bool body_ended = false;
  bool armed_after_the_body_ended = false;
  pulled_row last{};
  while (true) {
    // The body ends while rows parsed from it are still being handed out, so the last operations
    // of every stream arm nothing. What must hold is that arming works while the body is live,
    // and never resumes once it has ended.
    const auto state = streamer.set_deadline(
      deadline_in(std::chrono::duration_cast<std::chrono::milliseconds>(per_call)));
    if (state == couchbase::core::io::deadline_state::armed) {
      ++armed;
      if (body_ended) {
        armed_after_the_body_ended = true;
      }
    } else {
      body_ended = true;
    }
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++rows;
    // Caller-side work between rows.
    if (paused < pauses && rows % 80 == 0) {
      ++paused;
      await_io_past(runtime.io(), deadline_in(pause));
    }
  }
  assert_eq(paused, pauses, "the case paused for longer in total than one deadline allows");
  assert_true(armed > 0, "a live body arms");
  assert_false(armed_after_the_body_ended, "arming does not resume once the body has ended");

  assert_eq(rows, 2000, "time spent in the user's own block does not end the stream");
  assert_eq(last.ec, std::error_code{}, "the stream ends cleanly");
}

void
a_blocking_binding_closing_its_handle_reports_a_cancel([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  utils::loopback_stream server{ runtime.io() };
  const auto joined = runtime.joiner();
  couchbase::core::row_streamer streamer{ runtime.io(), body_over_loopback(server), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });
  // Explicit `#close`, or the binding's teardown on scope exit.
  streamer.cancel();

  const auto last = pull_blocking(streamer);
  assert_false(last.row.has_value(), "the stream is over");
  assert_eq(
    last.ec, couchbase::errc::common::request_canceled, "an explicit close reports a cancel");
}

// Event-loop shape: every callback below runs on the io_context thread, and nothing blocks.
void
an_event_loop_binding_delivers_rows_without_blocking([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, result_document(2000), 256);
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
  assert_eq(streamer.set_deadline(deadline_in(patience())),
            couchbase::core::io::deadline_state::armed,
            "a live stream arms");
  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the iteration completes");
  assert_eq(rows, 2000, "every row reaches the loop");
  assert_eq(end_ec, std::error_code{}, "a deadline the consumer stayed inside never shows up");
  // run() returns only when the io_context runs out of work, and an armed timer is work. A clean
  // end that failed to disarm would hold the loop open until the deadline instead.
  assert_true(elapsed < patience(), "the completed stream leaves no timer holding the loop open");
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
  const auto deadline = deadline_in(short_deadline());
  assert_eq(streamer.set_deadline(deadline),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  // Bounded by the expiry rather than a multiple of it: asio dispatches a timer whose time has
  // come before run_until returns. Plain run() would not return -- the feed parks on a full row
  // channel, which stays pending work while nothing consumes.
  io.run_until(past(deadline, marker_offset));
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
  utils::loopback_stream server{ io };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  auto armed_in_the_callback = couchbase::core::io::deadline_state::body_already_ended;
  std::optional<couchbase::core::query_stream> stream{};
  // Everything below runs on the loop thread.
  server.connect([&](couchbase::core::http_response_body body) {
    stream.emplace(io, std::move(body));
    stream->start([&](std::error_code) {
      stream->next_row([&](std::optional<std::string> /* row */, std::error_code ec) {
        end_ec = ec;
        ended = true;
      });
    });
    // Recorded, not asserted: a throw inside an asio handler unwinds out of io.run() while the
    // server, the stream and these locals are still captured by reference with handlers pending.
    armed_in_the_callback = stream->set_deadline(deadline_in(patience()));
    // `AbortController` firing, or `asyncio.Task.cancel()`, with the deadline still armed: it must
    // neither fire nor hold the response alive.
    stream->cancel();
  });

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_eq(armed_in_the_callback,
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  assert_true(ended, "the pending pull is resolved");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "an abort surfaces as a cancellation, not as a timeout");
  assert_true(elapsed < patience(), "the aborted stream leaves no timer holding the loop open");
}

void
a_binding_arms_the_deadline_from_the_thread_it_was_called_on([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  utils::loopback_stream server{ runtime.io() };
  const auto joined = runtime.joiner();
  couchbase::core::row_streamer streamer{ runtime.io(), body_over_loopback(server), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });

  // A synchronous binding arms on the caller's thread while its io thread runs the stream.
  // set_deadline names no thread requirement, so any thread must work.
  assert_eq(streamer.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");

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
  // More than one runner thread: arming, expiry and the read completion can then be in flight at
  // once.
  // The server is built before any runner, and the joiner before the loop that starts them: a
  // throw part way through would otherwise unwind past threads already running.
  utils::loopback_stream server{ io };
  std::vector<std::thread> runners;
  runners.reserve(4);
  const scoped_runners joined{ io, work, runners };
  for (int i = 0; i < 4; ++i) {
    runners.emplace_back([&io]() {
      io.run();
    });
  }

  couchbase::core::row_streamer streamer{ io, body_over_loopback(server), "/results/^" };
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
  // and gives the sanitizers the path to inspect. Which of two racing arms wins is not a
  // contract: the caller that arms last is the caller that ordered last.
  std::atomic_int not_armed{ 0 };
  {
    std::vector<std::thread> arming;
    arming.reserve(2);
    const utils::scoped_threads joined_arming{ arming };
    for (int t = 0; t < 2; ++t) {
      arming.emplace_back([&streamer, &not_armed]() {
        for (int i = 0; i < 200; ++i) {
          if (streamer.set_deadline(deadline_in(patience())) !=
              couchbase::core::io::deadline_state::armed) {
            ++not_armed;
          }
        }
      });
    }
  }

  streamer.cancel();
  work.reset();
  for (auto& th : runners) {
    th.join();
  }

  // Asserted only once every thread is joined. Unwinding past a joinable std::thread calls
  // std::terminate, so an assertion placed above these joins reports "terminate called" instead
  // of naming what failed -- in the one case whose purpose is to hand the sanitizers a race.
  assert_eq(not_armed.load(), 0, "every arm on a live stream takes");
  assert_true(ended.load(), "the stream terminates");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "concurrent arming leaves the stream cancellable, not timed out");
}

// Reports the terminal a consumer sees once `after` has elapsed on a stream with no more rows
// coming. Stream is query_stream or analytics_stream: both wrap row_streamer and expose the three
// calls used here.
//
// The stream is shared so the outer handler can hold it. The inner pull keeps the impl alive
// itself -- next_row captures shared_from_this -- which is what makes a wait that times out
// safe while that handler is still pending. This is also how query_stream_component builds one.
template<typename Stream>
auto
terminal_of_a_parked_stream(std::shared_ptr<Stream> stream, std::chrono::milliseconds after)
  -> std::optional<std::error_code>
{
  auto ended = std::make_shared<std::promise<std::error_code>>();
  auto done = ended->get_future();
  stream->start([stream, ended](std::error_code) {
    stream->next_row([ended](std::optional<std::string> /* row */, std::error_code ec) {
      ended->set_value(ec);
    });
  });
  assert_eq(stream->set_deadline(deadline_in(after)),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  if (done.wait_for(patience()) != std::future_status::ready) {
    return {};
  }
  return done.get();
}

// The deadline over the body the query service hands out: a real session and socket, reached
// through core::query_stream rather than row_streamer. The loopback server writes a prefix under
// a Content-Length promising more and then sends nothing, which parks the stream where a consumer
// that stopped pulling leaves it.
void
query_stream_over_a_socket_terminates_at_its_deadline([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  utils::loopback_stream server{ runtime.io() };
  const auto joined = runtime.joiner();
  auto stream =
    std::make_shared<couchbase::core::query_stream>(runtime.io(), body_over_loopback(server));

  const auto terminal = terminal_of_a_parked_stream(stream, short_deadline());
  assert_true(terminal.has_value(), "the stream terminates");
  assert_eq(terminal.value(),
            couchbase::errc::common::ambiguous_timeout,
            "a parked query stream reports its deadline");
}

// The analytics counterpart, over the same socket-backed body.
void
analytics_stream_over_a_socket_terminates_at_its_deadline([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  utils::loopback_stream server{ runtime.io() };
  const auto joined = runtime.joiner();
  auto stream =
    std::make_shared<couchbase::core::analytics_stream>(runtime.io(), body_over_loopback(server));

  const auto terminal = terminal_of_a_parked_stream(stream, short_deadline());
  assert_true(terminal.has_value(), "the stream terminates");
  assert_eq(terminal.value(),
            couchbase::errc::common::ambiguous_timeout,
            "a parked analytics stream reports its deadline");
}

void
the_runtime_joiner_does_not_wait_for_a_parked_read([[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  std::chrono::steady_clock::time_point parked{};
  {
    // The server writes a prefix under a Content-Length promising more, so this read parks and
    // nothing completes it until ~loopback_stream closes the socket -- which runs after the guard.
    utils::loopback_stream server{ runtime.io() };
    const auto joined = runtime.joiner();
    couchbase::core::row_streamer streamer{ runtime.io(),
                                            body_over_loopback(server),
                                            "/results/^" };
    streamer.start([](std::string, std::error_code) {
    });
    // Measured from here, not from the top: opening the loopback connection can itself take most
    // of patience() on a loaded or instrumented runner, and what this case bounds is the teardown
    // below.
    parked = std::chrono::steady_clock::now();
  }
  const auto elapsed = std::chrono::steady_clock::now() - parked;
  // A guard that only released the work guard would wait here for a read that cannot complete,
  // turning any failing assertion in these cases into a hang instead of a reported failure.
  assert_true(elapsed < patience(), "teardown does not wait for the parked read");
}

// Rows enough to carry a socket-backed stream past the back-pressure high-water mark, left
// unterminated so the response stays incomplete and the lexer waits for the rest.
auto
rows_past_the_high_water_mark() -> std::string
{
  std::string prefix = R"({"requestID":"r1","signature":{},"results":[)";
  for (int i = 0; i < 2000; ++i) {
    if (i != 0) {
      prefix += ",";
    }
    prefix += R"({"n":)" + std::to_string(i) + "}";
  }
  return prefix;
}

void
a_socket_stream_abandoned_above_the_high_water_mark_releases_its_session(
  [[maybe_unused]] context& ctx)
{
  binding_runtime runtime;
  utils::loopback_stream server{ runtime.io(), rows_past_the_high_water_mark() };
  const auto joined = runtime.joiner();
  couchbase::core::row_streamer streamer{
    runtime.io(), body_over_loopback(server), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });

  // Take a prefix of the rows and stop. Above the high-water mark the streamer arms no read, so
  // the inter-read idle timer never runs and nothing else reclaims the connection.
  for (int i = 0; i < 5; ++i) {
    assert_true(pull_blocking(streamer).row.has_value(), "the rows the consumer asked for arrive");
  }

  assert_false(server.session_stopped(), "the session is live while the consumer holds the stream");
  const auto deadline = deadline_in(short_deadline());
  assert_eq(streamer.set_deadline(deadline),
            couchbase::core::io::deadline_state::armed,
            "a parked socket stream still takes a deadline");

  // Let the expiry land before any pull resumes reading. Draining first would let buffered_bytes_
  // fall under the low-water mark, arm a read, and have the deadline close a body with a read
  // outstanding -- the state the other socket cases already cover, not this one.
  await_io_past(runtime.io(), deadline);

  // The body ends when its bytes are read; rows already parsed from them are handed out first,
  // so the terminal arrives after the buffered prefix rather than on the next pull.
  pulled_row last{};
  int buffered = 0;
  while (true) {
    last = pull_blocking(streamer);
    if (!last.row.has_value()) {
      break;
    }
    ++buffered;
  }
  assert_eq(last.ec,
            couchbase::errc::common::ambiguous_timeout,
            "the abandoned socket stream reports the deadline");
  assert_ne(buffered, 0, "rows buffered before the deadline are still handed over");
  // The point of the feature. A consumer that stops pulling holds the socket, and only the
  // deadline hands it back; asserting the error code alone would pass with the connection leaked.
  assert_true(server.session_stopped(), "the deadline released the connection");
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
      { CASE(query_stream_over_a_socket_terminates_at_its_deadline) },
      { CASE(analytics_stream_over_a_socket_terminates_at_its_deadline) },
      { CASE(a_socket_stream_abandoned_above_the_high_water_mark_releases_its_session) },
      { CASE(the_runtime_joiner_does_not_wait_for_a_parked_read) },
    },
  };
}

} // namespace couchbase::test
