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
// does not fire it before the case has set its scenario up.
constexpr auto short_deadline = 50ms;
// Long enough that any case reaching it has failed to cancel a timer it should have cancelled.
constexpr auto far_deadline = 30s;

auto
deadline_in(std::chrono::milliseconds d) -> std::chrono::steady_clock::time_point
{
  return std::chrono::steady_clock::now() + d;
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
// back. Mirrors a consumer that took a prefix of the rows and walked away.
void
fill_to_the_high_water_mark(couchbase::core::row_streamer& streamer, asio::io_context& io)
{
  streamer.start([](std::string, std::error_code) {
  });
  io.poll();
  // poll() leaves the io_context stopped once it runs out of work; without this every later run()
  // would return immediately and no timer would ever be serviced.
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

// Accepts the request and sends nothing, so a pull parks. With idle_timeout unset, only a
// whole-stream deadline terminates it.
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

  streamer.set_deadline(deadline_in(short_deadline));
  // No consumer. Back-pressure has stopped the reads, so the idle timer has no pull to guard.
  // run_for rather than run: the feed parks on a full row channel, which is pending work.
  io.run_for(short_deadline * 4);
  io.restart();

  auto [rows, end_ec] = drain(streamer, io);

  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "a later pull reports the timeout");
  assert_ne(rows, 0, "rows buffered before the deadline are still delivered");
  assert_true(rows < 2000, "the stream did not run to completion");
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
  streamer.set_deadline(deadline_in(short_deadline));
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
  streamer.set_deadline(deadline_in(far_deadline));

  const auto started = std::chrono::steady_clock::now();
  auto [rows, end_ec] = drain(streamer, io);
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly, not with a timeout");
  assert_eq(rows, 2000, "every row is delivered");
  // run() returns only when the io_context has no work left. A deadline still armed at the end of
  // the stream is work, and it holds a strong reference to the body for as long as it is armed, so
  // a case that leaves one behind blocks here until the far deadline passes.
  assert_true(elapsed < far_deadline, "the finished stream leaves no timer armed");
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
  streamer.set_deadline(deadline_in(far_deadline));
  streamer.cancel();

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec,
            couchbase::errc::common::request_canceled,
            "an explicit cancel is reported as a cancel, not as a timeout");
  assert_true(elapsed < far_deadline, "the cancelled stream leaves no timer armed");
}

void
cancelling_a_streaming_body_cancels_its_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A cached body, not the fault seam: this exercises the io layer's close path.
  auto body = utils::make_chunked_response_body(io, large_result_document(), 256);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", back_pressured_options()
  };
  fill_to_the_high_water_mark(streamer, io);
  streamer.set_deadline(deadline_in(far_deadline));
  streamer.cancel();

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(elapsed < far_deadline, "closing the body disarms the deadline it was given");
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
  streamer.set_deadline(std::chrono::steady_clock::now() - 1s);
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
  streamer.set_deadline(deadline_in(short_deadline));
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
  streamer.set_deadline(deadline_in(short_deadline));
  streamer.set_deadline(deadline_in(short_deadline * 8));

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream still terminates at the deadline that replaced the first");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
  assert_true(elapsed >= short_deadline * 2,
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
  streamer.set_deadline(deadline_in(far_deadline));
  streamer.set_deadline(deadline_in(short_deadline));

  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  assert_true(ended, "the stream terminates");
  assert_true(elapsed < far_deadline, "the shorter deadline replaced the longer one");
}

void
arming_from_another_thread_is_safe([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  couchbase::core::row_streamer streamer{ io, stalling_body(io), "/results/^" };

  // Without the guard the io_context runs out of work when the pull parks, before the other
  // thread arms anything, and run() returns having serviced no timer.
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

  // Armed from a thread other than the one running the io_context.
  std::thread arming{ [&]() {
    streamer.set_deadline(deadline_in(short_deadline));
  } };
  io.run();
  arming.join();

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
  streamer.set_deadline(deadline_in(far_deadline));
  const auto started = std::chrono::steady_clock::now();
  io.run();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  // Arming a drained stream must not leave a timer holding it alive.
  assert_true(elapsed < far_deadline, "arming a finished stream leaves no timer behind");
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
  stream.set_deadline(deadline_in(short_deadline));
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
  stream.set_deadline(deadline_in(short_deadline));
  io.run();

  assert_true(ended, "the stream terminates");
  assert_eq(end_ec, couchbase::errc::common::ambiguous_timeout, "the reported terminal");
}

void
a_replayed_query_stream_ignores_a_deadline([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // The prepared-statement path replays a buffered response: no socket, and every pull completes.
  // A caller cannot distinguish the two stream kinds, so arming this one must be harmless.
  std::vector<std::string> rows{ R"({"a":1})", R"({"a":2})" };
  couchbase::core::operations::query_response::query_meta_data meta{};
  meta.status = "success";
  couchbase::core::query_stream stream{ io, rows, meta };
  stream.set_deadline(std::chrono::steady_clock::now() - 1s);

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

// Loopback socket rather than the in-memory seam: only a real session parks a read that something
// else aborts.
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
        // and then sends nothing further, holding the connection open.
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

  couchbase::core::io::http_request request{};
  request.type = couchbase::core::service_type::query;
  request.method = "GET";
  request.path = "/query/service";
  request.stream_response = true;
  session->connect([&]() {
    session->write_and_stream(
      request,
      [&](auto /* err */, couchbase::core::io::http_streaming_response resp) {
        body = resp.body();
        // Armed on the body the session handed over.
        body->set_deadline(std::chrono::steady_clock::now() + short_deadline);
        pull();
      },
      []() {
      });
  });

  // Bounds the case: a deadline that never fires fails here, not at the harness budget.
  asio::steady_timer give_up{ io };
  give_up.expires_after(short_deadline * 40);
  give_up.async_wait([&](std::error_code ec) {
    if (ec == asio::error::operation_aborted) {
      return;
    }
    session->stop();
    io.stop();
  });
  io.run();

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
  body.set_deadline(deadline_in(short_deadline), couchbase::errc::common::unambiguous_timeout);
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
      { CASE(a_deadline_over_a_parked_socket_read_reports_a_timeout) },
    },
  };
}

} // namespace couchbase::test
