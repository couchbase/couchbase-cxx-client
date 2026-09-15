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

#include "framework/context.hxx"
#include "framework/test_registry.hxx"

#include "framework/errors.hxx"

#include "core/analytics_stream.hxx"
#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/free_form_http_request.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_session.hxx"
#include "core/io/query_cache.hxx"
#include "core/origin.hxx"
#include "core/query_stream.hxx"
#include "core/row_streamer.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <asio/write.hpp>

#include <chrono>
#include <cstddef>
#include <functional>
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

// Short enough to keep the suite quick, long enough that a scheduling hiccup on a loaded machine
// does not fire it before the case has set its scenario up. Scaled for the same reason as
// far_deadline(): a fixed value is reached by an instrumented run before the scenario is ready.
constexpr auto short_deadline_unscaled = 50ms;

// Scale an unscaled duration by the suite multiplier. Multiples are taken of the unscaled value
// and scaled once: scale_budget() saturates at milliseconds::max() for a large accepted factor,
// and multiplying a saturated result again is meaningless.
auto
scaled_budget(std::chrono::milliseconds unscaled) -> std::chrono::milliseconds
{
  return scale_budget(unscaled, timeout_multiplier(safe_getenv(timeout_multiplier_variable)));
}

auto
short_deadline() -> std::chrono::milliseconds
{
  static const auto scaled =
    scaled_budget(std::chrono::duration_cast<std::chrono::milliseconds>(short_deadline_unscaled));
  return scaled;
}
// A deadline no case may reach, kept under the case budget on purpose. A bound at or above the
// budget can never fail an assertion: the harness kills the case first, and reports a timeout
// naming nothing. The gap holds for any multiplier of one or more, which is what the variable
// is for; below one, scale_budget()'s one-millisecond floor closes it.
constexpr auto far_deadline_unscaled = 2s;

// The harness scales each case budget by CB_TEST_TIMEOUT_MULTIPLIER but does not reach constants
// inside a case. Scaling here by the same factor keeps the bound under the budget it is meant to
// sit under, instead of firing inside an instrumented case that still has budget left.
auto
far_deadline() -> std::chrono::milliseconds
{
  static const auto scaled =
    scaled_budget(std::chrono::duration_cast<std::chrono::milliseconds>(far_deadline_unscaled));
  return scaled;
}

// Keeps a wait strictly past the expiry it follows: equal time points are unordered.
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

// Rows totalling several times the high-water mark below, so a consumer that stops pulling leaves
// the streamer parked above it with no read outstanding.
auto
large_result_document() -> std::string
{
  std::string doc = R"({"results":[)";
  for (int i = 0; i < 2000; ++i) {
    if (i != 0) {
      doc += ",";
    }
    doc += R"({"n":)" + std::to_string(i) + "}";
  }
  doc += R"(],"status":"success"})";
  return doc;
}

auto
back_pressured_options() -> couchbase::core::row_streamer_options
{
  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  return opts;
}

// Drives a streamer to the point where back-pressure has stopped the reads, then hands control
// back. Models a consumer that took a prefix of the rows and stopped pulling.
void
fill_to_the_high_water_mark(couchbase::core::row_streamer& streamer, asio::io_context& io)
{
  streamer.start([](std::string, std::error_code) {
  });
  io.poll();
  // poll() leaves the io_context stopped once it runs out of work; restart() before run().
  io.restart();
}

// Pulls rows until the stream terminates, returning how many arrived and the terminal error.
auto
drain(couchbase::core::row_streamer& streamer, asio::io_context& io)
  -> std::pair<int, std::error_code>
{
  int rows = 0;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        return;
      }
      ++rows;
      pump();
    });
  };
  pump();
  io.run();
  return { rows, end_ec };
}

// Parks every pull and never completes it. With idle_timeout unset nothing in the body ends such a
// stream: the terminal has to come from outside it, either a whole-stream deadline or an explicit
// cancel.
auto
stalling_body(asio::io_context& io) -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, /*terminal_ec*/ {}, /*stall*/ true);
}

void
closes_a_stream_abandoned_above_the_high_water_mark([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto opts = back_pressured_options();
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  fill_to_the_high_water_mark(streamer, io);
  assert_ne(streamer.buffered_bytes(),
            std::size_t{ 0 },
            "the case only means anything if the streamer stopped with rows buffered");

  const auto deadline = deadline_in(short_deadline());
  assert_eq(streamer.set_deadline(deadline),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  // No consumer: back-pressure has stopped the reads, so the idle timer has no pull to guard.
  // Bounded by the expiry itself rather than a multiple of it -- asio dispatches a timer whose
  // time has come before run_until returns, so this waits on the event, not on a guess. Plain
  // run() would not return: the feed parks on a full row channel with nothing consuming.
  io.run_until(past(deadline, marker_offset));
  io.restart();

  auto [rows, end_ec] = drain(streamer, io);

  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "a later pull reports the timeout");
  assert_ne(rows, 0, "rows buffered before the deadline are still delivered");
  assert_ne(rows, 2000, "the stream did not run to completion");
}

void
terminates_a_consumer_parked_on_a_stalled_pull([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer_options opts{};
  opts.idle_timeout = std::chrono::milliseconds{ 0 }; // the idle timer must not be what ends this
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^", opts };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(streamer.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();

  assert_true(ended, "the parked consumer is released rather than waiting forever");
  assert_eq(end_ec,
            couchbase::errc::common::ambiguous_timeout,
            "the parked pull reports the deadline, not the abort that delivered it");
}

void
a_stream_with_no_deadline_is_never_timed_out([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };
  fill_to_the_high_water_mark(streamer, io);

  auto [rows, end_ec] = drain(streamer, io);

  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_eq(rows, 2000, "every row is delivered");
}

void
a_stream_that_ends_first_does_not_fire_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };
  fill_to_the_high_water_mark(streamer, io);
  assert_eq(streamer.set_deadline(deadline_in(far_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");

  const auto started = std::chrono::steady_clock::now();
  auto [rows, end_ec] = drain(streamer, io);
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly, not with a timeout");
  assert_eq(rows, 2000, "every row is delivered");
  // run() returns only when the io_context has no work left. An armed deadline is work, and holds
  // a strong reference to the body. A case that leaves one behind blocks here until it passes.
  assert_true(elapsed < far_deadline(), "the finished stream leaves no timer armed");
}

void
cancel_wins_over_an_armed_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(streamer.set_deadline(deadline_in(far_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  streamer.cancel();

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "an explicit cancel is reported as a cancel, not as a timeout");
  assert_true(elapsed < far_deadline(), "the cancelled stream leaves no timer armed");
}

void
cancelling_a_streaming_body_cancels_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A cached body: this exercises the io layer's close path.
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };
  fill_to_the_high_water_mark(streamer, io);
  assert_eq(streamer.set_deadline(deadline_in(far_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  streamer.cancel();

  const auto started = std::chrono::steady_clock::now();
  auto [rows, end_ec] = drain(streamer, io);
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "the cancel is what terminates the stream, not the deadline");
  assert_ne(rows, 2000, "the stream did not run to completion");
  assert_true(elapsed < far_deadline(), "the cancel leaves no timer behind");
}

void
a_deadline_already_past_closes_the_stream([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(streamer.set_deadline(std::chrono::steady_clock::now() - 1s),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();

  assert_true(ended, "a deadline in the past closes the stream rather than leaving it open");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
}

// The classification a retry layer keys off. Matches the idle timer's.
auto
deadline_terminal(bool is_read_only) -> std::error_code
{
  asio::io_context io;
  couchbase::core::row_streamer_options opts{};
  opts.is_read_only = is_read_only;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^", opts };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(streamer.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();
  assert_true(ended, "the deadline produces a terminal rather than parking the consumer");
  return end_ec;
}

void
deadline_on_a_read_only_request_is_unambiguous([[maybe_unused]] context& ctx)
{
  assert_eq(deadline_terminal(true),
            couchbase::errc::common::unambiguous_timeout,
            "a read-only request is idempotent, so its timeout is unambiguous");
}

void
deadline_on_a_mutating_request_is_ambiguous([[maybe_unused]] context& ctx)
{
  assert_eq(deadline_terminal(false),
            couchbase::errc::common::ambiguous_timeout,
            "a mutating request may have partially executed");
}

void
re_arming_extends_the_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  // The second arming replaces the first, so the stream outlives the first deadline.
  assert_eq(streamer.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  assert_eq(streamer.set_deadline(deadline_in(scaled_budget(short_deadline_unscaled * 8))),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream still terminates at the deadline that replaced the first");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
  assert_true(elapsed >= scaled_budget(short_deadline_unscaled * 2),
              "the replaced deadline did not fire at its original time");
}

void
re_arming_shortens_the_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(streamer.set_deadline(deadline_in(far_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  assert_eq(streamer.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
  assert_true(elapsed < far_deadline(), "the shorter deadline replaced the longer one");
}

void
arming_from_another_thread_is_safe([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  // Keeps the io_context from running out of work when the pull parks, before the other thread
  // has armed anything.
  auto work = asio::make_work_guard(io);

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
      work.reset();
    });
  });

  // Armed from a thread other than the one running the io_context. The result is recorded and
  // asserted after the join: an exception escaping a thread's entry function calls
  // std::terminate, which would replace this case's failure with "terminate called".
  auto armed_from_other_thread = couchbase::core::io::deadline_state::body_already_ended;
  {
    std::thread arming{ [&]() {
      armed_from_other_thread = streamer.set_deadline(deadline_in(short_deadline()));
    } };
    // Owned rather than joined by hand: io.run() propagates an exception thrown by a handler, and
    // unwinding past the still-joinable thread would call std::terminate.
    const utils::scoped_thread joined{ arming };
    io.run();
  }

  assert_eq(armed_from_other_thread,
            couchbase::core::io::deadline_state::armed,
            "a deadline armed off the io thread takes");
  assert_true(ended, "the stream terminates");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
}

void
arming_after_the_stream_ended_is_a_no_op([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };
  streamer.start([](std::string, std::error_code) {
  });
  auto [rows, end_ec] = drain(streamer, io);
  assert_eq(end_ec, std::error_code{}, "the stream drained cleanly before anything was armed");
  assert_eq(rows, 2000, "every row is delivered");

  io.restart();
  const auto armed = streamer.set_deadline(deadline_in(far_deadline()));
  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_eq(armed,
            couchbase::core::io::deadline_state::body_already_ended,
            "a drained stream reports that it armed nothing");
  // Arming a drained stream must leave no timer holding it alive.
  assert_true(elapsed < far_deadline(), "arming a finished stream leaves no timer behind");
}

void
query_stream_terminates_at_its_deadline([[maybe_unused]] context& ctx)
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
  assert_eq(stream.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
}

void
analytics_stream_terminates_at_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::analytics_stream stream{ io, stalling_body(io) };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  stream.start([&](std::error_code) {
    stream.next_row([&](std::optional<std::string> /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  assert_eq(stream.set_deadline(deadline_in(short_deadline())),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
}

void
a_replayed_query_stream_ignores_a_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // The prepared-statement path replays a buffered response: no socket, and every pull
  // completes. Arming it is harmless.
  std::vector<std::string> rows{ R"({"a":1})", R"({"a":2})" };
  couchbase::core::operations::query_response::query_meta_data meta{};
  meta.status = "success";
  couchbase::core::query_stream stream{ io, rows, meta };
  assert_eq(stream.set_deadline(std::chrono::steady_clock::now() - 1s),
            couchbase::core::io::deadline_state::body_already_ended,
            "a replayed response holds nothing a deadline could bound");

  int seen = 0;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        return;
      }
      ++seen;
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  assert_eq(seen, 2, "every replayed row is delivered");
  assert_eq(end_ec, std::error_code{}, "a deadline in the past does not fail a buffered replay");
}

// Only a real session parks a read that something else aborts.
void
a_deadline_over_a_parked_socket_read_reports_a_timeout([[maybe_unused]] context& ctx)
{
  asio::io_context io;

  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  const auto port = acceptor.local_endpoint().port();
  asio::ip::tcp::socket server_socket{ io };

  // Headers promise more body than is sent, so the session keeps a read outstanding.
  const std::string response = "HTTP/1.1 200 OK\r\n"
                               "Content-Type: application/json\r\n"
                               "Content-Length: 4096\r\n"
                               "\r\n"
                               "0123456789";
  std::string request_buffer(4096, '\0');
  acceptor.async_accept(server_socket, [&](std::error_code accept_ec) {
    assert_success(accept_ec, "the loopback server accepts the connection");
    server_socket.async_read_some(
      asio::buffer(request_buffer), [&](std::error_code read_ec, std::size_t) {
        assert_success(read_ec, "the loopback server reads the request");
        asio::write(server_socket, asio::buffer(response), read_ec);
        assert_success(read_ec, "the loopback server writes the truncated response");
      });
  });

  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "pass";
  couchbase::core::cluster_options options{};
  couchbase::core::origin origin{ creds, "127.0.0.1", port, options };
  couchbase::core::topology::configuration config{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{ config, options,     cache, "127.0.0.1",
                                          port,   "127.0.0.1", port };

  auto session =
    std::make_shared<couchbase::core::io::http_session>(couchbase::core::service_type::query,
                                                        "client-id",
                                                        "node-uuid",
                                                        io,
                                                        origin,
                                                        "127.0.0.1",
                                                        std::to_string(port),
                                                        http_ctx);

  std::error_code parked_ec{ make_error_code(std::errc::operation_in_progress) };
  bool parked_completed = false;
  std::optional<couchbase::core::io::http_streaming_response_body> body{};
  std::function<void()> pull = [&]() {
    body->next([&](std::string, bool has_more, std::error_code ec) {
      if (ec || !has_more) {
        parked_ec = ec;
        parked_completed = true;
        session->stop();
        io.stop();
        return;
      }
      pull();
    });
  };

  auto armed_on_the_body = couchbase::core::io::deadline_state::body_already_ended;
  couchbase::core::io::http_request request{};
  request.type = couchbase::core::service_type::query;
  request.method = "GET";
  request.path = "/query/service";
  request.stream_response = true;
  session->connect([&]() {
    session->write_and_stream(
      request,
      [&](auto err, couchbase::core::io::http_streaming_response resp) {
        // A failed dispatch reports a default-constructed response with a null impl; reading its
        // body would crash instead of failing the case.
        if (utils::dispatch_failed(err)) {
          return;
        }
        body = resp.body();
        // Armed on the body the session handed over. Recorded rather than asserted here: a throw
        // inside an asio handler unwinds out of io.run() while the acceptor, socket, session and
        // body are still captured by reference with handlers pending, so teardown comes apart
        // instead of the case reporting what failed.
        armed_on_the_body = body->set_deadline(deadline_in(short_deadline()),
                                               couchbase::core::io::deadline_terminal::ambiguous);
        pull();
      },
      []() {
      });
  });

  // Bounds the case: a deadline that never fires fails here, not at the harness budget.
  asio::steady_timer give_up{ io };
  // expires_at with a saturating time point, not expires_after: the latter adds to now()
  // internally and would wrap the same way.
  give_up.expires_at(deadline_in(scaled_budget(short_deadline_unscaled * 40)));
  give_up.async_wait([&](std::error_code ec) {
    if (ec == asio::error::operation_aborted) {
      return;
    }
    session->stop();
    io.stop();
  });
  io.run();

  assert_eq(armed_on_the_body,
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on the body the session handed over");
  assert_true(parked_completed, "the parked read is released rather than waiting forever");
  assert_eq(parked_ec,
            couchbase::errc::common::ambiguous_timeout,
            "the parked read reports the deadline, not the socket abort that delivered it");
}

void
a_body_reports_the_deadline_to_a_pull_that_arrives_afterwards([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, /*terminal_ec*/ {}, /*stall*/ true);
  assert_eq(body.set_deadline(deadline_in(short_deadline()),
                              couchbase::core::io::deadline_terminal::unambiguous),
            couchbase::core::io::deadline_state::armed,
            "the deadline arms on a live stream");
  io.run();

  // No pull was outstanding when the deadline fired. A later pull must still report the
  // terminal.
  io.restart();
  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  bool has_more = true;
  body.next([&](std::string, bool more, std::error_code ec) {
    seen = ec;
    has_more = more;
  });
  io.run();

  assert_eq(seen, couchbase::errc::common::unambiguous_timeout, "the recorded terminal is kept");
  assert_false(has_more, "the body reports end-of-stream");
}

void
a_close_after_a_clean_end_keeps_the_clean_terminal([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, R"({"results":[],"status":"success"})", 0);

  bool has_more = true;
  std::error_code drained{ make_error_code(std::errc::operation_in_progress) };
  body.next([&](std::string, bool more, std::error_code ec) {
    has_more = more;
    drained = ec;
  });
  io.run();
  assert_false(has_more, "one pull drains a body that was parsed whole");
  assert_eq(drained, std::error_code{}, "the drain reports a clean end");

  // A binding closing its handle after iterating to the end. The body has already reached its
  // terminal, so the close has nothing left to reclaim and must not replace it.
  body.cancel();

  io.restart();
  std::error_code after{ make_error_code(std::errc::operation_in_progress) };
  body.next([&](std::string, bool, std::error_code ec) {
    after = ec;
  });
  io.run();
  assert_eq(after, std::error_code{}, "the clean terminal survives a later close");
}

void
a_close_after_the_fault_body_finished_keeps_its_terminal([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // Distinct from request_canceled, which is what the seam reports once cancelled.
  const auto terminal = make_error_code(std::errc::connection_reset);
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, terminal, /*stall*/ false);

  std::error_code first{ make_error_code(std::errc::operation_in_progress) };
  body.next([&](std::string, bool, std::error_code ec) {
    first = ec;
  });
  io.run();
  assert_eq(first, terminal, "the seam delivers the terminal it was built with");

  body.cancel();

  io.restart();
  std::error_code after{ make_error_code(std::errc::operation_in_progress) };
  body.next([&](std::string, bool, std::error_code ec) {
    after = ec;
  });
  io.run();
  assert_eq(after, terminal, "a close after the seam finished does not replace its terminal");
}

void
arming_after_a_cancel_is_a_no_op([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };
  streamer.start([](std::string, std::error_code) {
  });

  streamer.cancel();
  // Deliberately before io.run(): cancel() closes the row channel on this thread but posts the
  // body teardown, so this is the window in which the body would still arm. A caller that has
  // cancelled must not be told a deadline took, or an expiry could report a timeout for a stream
  // that was cancelled.
  assert_eq(streamer.set_deadline(deadline_in(far_deadline())),
            couchbase::core::io::deadline_state::body_already_ended,
            "a cancelled stream arms nothing, even before the posted teardown has run");

  io.run();
}

void
cancelling_a_deadline_leaves_the_body_usable([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body =
    utils::make_chunked_response_body(io, R"({"results":[{"n":0}],"status":"success"})", 0);
  assert_eq(body.set_deadline(deadline_in(short_deadline()),
                              couchbase::core::io::deadline_terminal::ambiguous),
            couchbase::core::io::deadline_state::armed,
            "the body takes a deadline");

  // What row_streamer::cancel() calls on the caller's thread before posting its teardown, so an
  // expiry cannot take the body's terminal in the gap. The deadline has not expired yet, so the
  // wait is still cancellable and its handler is aborted; a_superseded_expiry_leaves_the_body_open
  // covers the expiry that is already committed when the supersession lands.
  body.cancel_deadline();

  io.run();

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  bool has_more = true;
  body.next([&](std::string, bool more, std::error_code ec) {
    seen = ec;
    has_more = more;
  });
  io.restart();
  io.run();

  assert_eq(seen, std::error_code{}, "the superseded deadline did not close the body");
  assert_false(has_more, "the body still reports its own clean end");
}

void
a_superseded_expiry_leaves_the_body_open([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);

  // A deadline already in the past. Its expiry is committed when the scheduler first runs, and
  // from that point asio cannot withdraw it: cancel() reports nothing cancelled and the wait
  // completes with a falsy error_code rather than operation_aborted. The generation carried by
  // the completion is the only thing separating it from a live expiry.
  assert_eq(body.set_deadline(std::chrono::steady_clock::now() - 1s,
                              couchbase::core::io::deadline_terminal::ambiguous),
            couchbase::core::io::deadline_state::armed,
            "the body takes a deadline");
  // Posted rather than called here: a handler queued before the run executes ahead of the
  // expiry, which is the interleaving row_streamer::cancel() hits when its cancel_deadline()
  // lands after the deadline has already elapsed.
  asio::post(io, [&body]() {
    body.cancel_deadline();
  });
  io.run();

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  bool has_more = false;
  bool delivered = false;
  body.next([&](std::string, bool more, std::error_code ec) {
    seen = ec;
    has_more = more;
    delivered = true;
  });
  io.restart();
  io.run();

  assert_true(delivered, "the pull completes");
  assert_eq(seen, std::error_code{}, "a superseded expiry does not take the body's terminal");
  assert_true(has_more, "the body is still streaming");
}

void
an_expiry_superseded_by_a_re_arm_leaves_the_body_open([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);

  assert_eq(body.set_deadline(std::chrono::steady_clock::now() - 1s,
                              couchbase::core::io::deadline_terminal::ambiguous),
            couchbase::core::io::deadline_state::armed,
            "the body takes a deadline");
  // The rolling-deadline shape: a binding re-arms per call, and the arm it replaces has already
  // elapsed. Re-arming cannot withdraw the committed expiry either, so the body must reject it on
  // the generation alone and go on streaming until the new deadline.
  asio::post(io, [&body]() {
    assert_eq(body.set_deadline(deadline_in(far_deadline()),
                                couchbase::core::io::deadline_terminal::ambiguous),
              couchbase::core::io::deadline_state::armed,
              "the re-arm takes on a body the elapsed deadline has not closed");
  });
  // poll(), not run(): the re-arm leaves a deadline outstanding, and run() would wait for that one
  // too and close the body on its own terms. This drains the re-arm and the elapsed expiry behind
  // it, and nothing else.
  io.poll();

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  bool has_more = false;
  bool delivered = false;
  body.next([&](std::string, bool more, std::error_code ec) {
    seen = ec;
    has_more = more;
    delivered = true;
  });

  assert_true(delivered, "the pull completes");
  assert_eq(seen, std::error_code{}, "the replaced expiry does not take the body's terminal");
  assert_true(has_more, "the body is still streaming");

  // Release the deadline the re-arm installed, so the body does not outlive the case holding a
  // wait on an io_context nobody runs again.
  body.cancel_deadline();
}

void
closing_a_body_disarms_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);

  assert_eq(body.set_deadline(deadline_in(far_deadline()),
                              couchbase::core::io::deadline_terminal::ambiguous),
            couchbase::core::io::deadline_state::armed,
            "the body takes a deadline");
  body.cancel();

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  body.next([&](std::string, bool, std::error_code ec) {
    seen = ec;
  });
  assert_eq(seen, couchbase::errc::common::request_canceled, "the close takes the terminal");

  // An armed wait holds a strong reference to the body and is work on the loop, so a body closed
  // ahead of its deadline would stay alive until that deadline passed. Bounded well under the
  // deadline: the loop has to run out of work on its own, not be cut short by the bound.
  io.run_until(deadline_in(short_deadline()));
  assert_true(io.stopped(), "a closed body leaves its deadline disarmed and the loop with no work");
}

// http_session checks a completed response back into the keep-alive pool only after the body has
// observed the end of it. Before that ordering the connection was published while the body still
// held it with reading_complete_ false, which the body reads as a response abandoned mid-stream:
// an expiry landing there stopped a connection another request could already have taken out of
// the pool.
void
the_body_has_ended_before_its_connection_is_checked_in([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A response that can be completed: the header promises exactly the prefix plus the tail.
  utils::loopback_stream stream{ io,
                                 R"({"requestID":"r1","signature":{},"results":[{"n":0})",
                                 R"(],"status":"success"})" };

  auto body = std::make_shared<std::optional<couchbase::core::http_response_body>>();
  std::error_code final_ec{ make_error_code(std::errc::operation_in_progress) };
  bool final_has_more = true;
  std::string final_data;
  bool finished = false;

  // http_session runs this when it checks the connection back into the keep-alive pool. The body
  // has already observed the end of its response by then, so it refuses a deadline: there is
  // nothing left to bound, and nothing that would stop a connection the pool has republished.
  // An expired deadline is used because one that armed would fire on the drain below, which is
  // how the stop reached a pooled session before.
  auto on_stream_end = [&io, body]() {
    if (!body->has_value()) {
      return;
    }
    assert_eq(body->value().set_deadline(std::chrono::steady_clock::now() - 1s,
                                         couchbase::core::io::deadline_terminal::unambiguous),
              couchbase::core::io::deadline_state::body_already_ended,
              "the body has ended before its connection is checked in");
    // Nested drain on the same thread: an expiry that had armed would land before this returns.
    io.poll();
  };

  stream.connect(
    [&](couchbase::core::http_response_body received) {
      *body = std::move(received);
      // Drains the prefix buffered by the header parse; the next pull is the socket read the
      // tail completes.
      body->value().next([&](std::string, bool, std::error_code) {
        stream.send_tail();
        body->value().next([&](std::string data, bool more, std::error_code ec) {
          final_data = std::move(data);
          final_has_more = more;
          final_ec = ec;
          finished = true;
        });
      });
    },
    on_stream_end);

  io.run_until(deadline_in(far_deadline()));

  assert_true(finished, "the final pull completes");
  assert_eq(final_ec,
            std::error_code{},
            "a response whose last read succeeded is not reported as a timeout");
  assert_false(final_data.empty(), "the remainder of the response is handed to the consumer");
  assert_false(final_has_more, "the body reports end-of-stream");

  // The clean terminal has to hold for every later pull too. A body that delivered the response
  // and then reported a timeout would contradict itself one call apart.
  std::error_code after_ec{ make_error_code(std::errc::operation_in_progress) };
  bool after_has_more = true;
  bool after_done = false;
  body->value().next([&](std::string, bool more, std::error_code ec) {
    after_ec = ec;
    after_has_more = more;
    after_done = true;
  });
  io.restart();
  io.run_until(deadline_in(far_deadline()));

  assert_true(after_done, "the pull after the clean end completes");
  assert_eq(after_ec, std::error_code{}, "a pull after the clean end keeps the clean terminal");
  assert_false(after_has_more, "the body still reports end-of-stream");
}

// A streaming response can be complete before a single read_some: an empty body (204, or
// Content-Length: 0) is finished by the first parse, and http_session constructs the body with
// `parser.complete` set while still passing the session. Nothing further belongs to that message
// and the connection is already back in the keep-alive pool, so a read from this state parks on a
// socket serving someone else, and `set_deadline` refuses the body it would have to bound.
void
a_response_complete_before_any_read_reports_end_of_stream([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // Accepts and sends nothing: a read issued from this state never completes, so a pull that
  // returns proves no read was issued.
  asio::ip::tcp::acceptor acceptor{
    io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 }
  };
  asio::ip::tcp::socket server_socket{ io };
  acceptor.async_accept(server_socket, [](std::error_code) {
  });
  const auto port = acceptor.local_endpoint().port();

  couchbase::core::cluster_credentials creds{};
  creds.username = "user";
  creds.password = "pass";
  couchbase::core::cluster_options options{};
  couchbase::core::origin origin{ creds, "127.0.0.1", port, options };
  couchbase::core::topology::configuration config{};
  couchbase::core::query_cache cache{};
  couchbase::core::http_context http_ctx{ config, options,     cache, "127.0.0.1",
                                          port,   "127.0.0.1", port };
  auto session =
    std::make_shared<couchbase::core::io::http_session>(couchbase::core::service_type::query,
                                                        "client-id",
                                                        "node-uuid",
                                                        io,
                                                        origin,
                                                        "127.0.0.1",
                                                        std::to_string(port),
                                                        http_ctx);

  bool ended = false;
  bool has_more = true;
  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  std::optional<couchbase::core::io::http_streaming_response_body> body{};
  auto armed = couchbase::core::io::deadline_state::armed;
  session->connect([&]() {
    body.emplace(io, session, /*cached_data*/ std::string{}, /*reading_complete*/ true, 0);
    armed = body->set_deadline(deadline_in(far_deadline()),
                               couchbase::core::io::deadline_terminal::unambiguous);
    body->next([&](std::string, bool more, std::error_code ec) {
      ended = true;
      has_more = more;
      seen = ec;
    });
  });
  // far rather than short: this has to cover a resolve, a connect and the callback, and a bound
  // tight enough to be reached by a slow one reports a spurious park as the regression. A pull
  // that completes leaves no work and returns at once, so the bound is only paid on failure.
  io.run_until(deadline_in(far_deadline()));

  // Read before the teardown and asserted after it. Torn down first because a failure here is the
  // parked-read state, and unwinding past a live session, socket and acceptor with handlers
  // outstanding replaces the failure with whatever their destruction does. Snapshotted because
  // stopping the session completes that parked read, which runs the callback below and would
  // otherwise hand the assertions the very end-of-stream they are there to demand.
  const auto pull_completed = ended;
  const auto pull_has_more = has_more;
  const auto pull_ec = seen;

  session->stop();
  std::error_code ignored;
  server_socket.close(ignored);
  acceptor.close(ignored);
  io.restart();
  io.run();

  assert_true(pull_completed,
              "the pull completes rather than parking on a read of a pooled connection");
  assert_false(pull_has_more, "the response is over");
  assert_eq(pull_ec, std::error_code{}, "a clean end of stream, not a terminal");
  assert_eq(armed,
            couchbase::core::io::deadline_state::body_already_ended,
            "there is nothing left to bound");
}
// The fault seam stands in for the real body, so a case on it establishes nothing unless the two
// agree about when a body has ended. With no bytes to hand out and no stall, the seam's terminal
// is reached by its first pull, which makes it terminal before that pull ever runs -- the state
// the real body reports as reading_complete_ with an empty cache.
void
a_fault_body_terminal_at_construction_refuses_a_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io,
    /*data*/ {},
    /*cached_chunk_size*/ 0,
    make_error_code(std::errc::connection_reset),
    /*stall*/ false);

  assert_eq(body.set_deadline(deadline_in(far_deadline()),
                              couchbase::core::io::deadline_terminal::ambiguous),
            couchbase::core::io::deadline_state::body_already_ended,
            "a seam with nothing left to deliver refuses a deadline");

  const auto started = std::chrono::steady_clock::now();
  io.run();
  assert_true(std::chrono::steady_clock::now() - started < far_deadline(),
              "the refusal leaves no timer holding the loop open");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(closes_a_stream_abandoned_above_the_high_water_mark) },
      { CASE(terminates_a_consumer_parked_on_a_stalled_pull) },
      { CASE(a_stream_with_no_deadline_is_never_timed_out) },
      { CASE(a_stream_that_ends_first_does_not_fire_its_deadline) },
      { CASE(cancel_wins_over_an_armed_deadline) },
      { CASE(arming_after_a_cancel_is_a_no_op) },
      { CASE(cancelling_a_deadline_leaves_the_body_usable) },
      { CASE(a_superseded_expiry_leaves_the_body_open) },
      { CASE(an_expiry_superseded_by_a_re_arm_leaves_the_body_open) },
      { CASE(closing_a_body_disarms_its_deadline) },
      { CASE(the_body_has_ended_before_its_connection_is_checked_in) },
      { CASE(cancelling_a_streaming_body_cancels_its_deadline) },
      { CASE(a_deadline_already_past_closes_the_stream) },
      { CASE(deadline_on_a_read_only_request_is_unambiguous) },
      { CASE(deadline_on_a_mutating_request_is_ambiguous) },
      { CASE(re_arming_extends_the_deadline) },
      { CASE(re_arming_shortens_the_deadline) },
      { CASE(arming_from_another_thread_is_safe) },
      { CASE(arming_after_the_stream_ended_is_a_no_op) },
      { CASE(query_stream_terminates_at_its_deadline) },
      { CASE(analytics_stream_terminates_at_its_deadline) },
      { CASE(a_replayed_query_stream_ignores_a_deadline) },
      { CASE(a_body_reports_the_deadline_to_a_pull_that_arrives_afterwards) },
      { CASE(a_close_after_a_clean_end_keeps_the_clean_terminal) },
      { CASE(a_close_after_the_fault_body_finished_keeps_its_terminal) },
      { CASE(a_deadline_over_a_parked_socket_read_reports_a_timeout) },
      { CASE(a_response_complete_before_any_read_reports_end_of_stream) },
      { CASE(a_fault_body_terminal_at_construction_refuses_a_deadline) },
    },
  };
}

} // namespace couchbase::test
