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

#include "core/row_streamer.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
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

// Drains a cached document through a row_streamer and returns the rows plus the terminal error.
auto
drain_rows(const std::string& doc) -> std::pair<std::vector<std::string>, std::error_code>
{
  asio::io_context io;
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^" };

  std::vector<std::string> rows;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        return;
      }
      rows.push_back(std::move(row));
      pump();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    pump();
  });
  io.run();
  return { std::move(rows), end_ec };
}

// Runs a stalling body (one that never completes a read until cancelled) through a row_streamer
// with a short idle timeout and the given read-only flag, and returns the terminal error the idle
// timer produces.
auto
idle_timeout_terminal(bool is_read_only) -> std::error_code
{
  asio::io_context io;
  // Empty data + stall == a server that accepted the request but then stops sending: the
  // row_streamer arms its idle timer around the (never-completing) read, and the timer fires.
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, /*terminal_ec*/ {}, /*stall*/ true);
  couchbase::core::row_streamer_options opts{};
  opts.idle_timeout = std::chrono::milliseconds{ 20 };
  opts.is_read_only = is_read_only;
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        ended = true;
        return;
      }
      pump();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    pump();
  });
  io.run();
  assert_true(ended, "the idle timer produces a terminal rather than parking the consumer");
  return end_ec;
}

void
row_streamer_options_has_streaming_defaults([[maybe_unused]] context& ctx)
{
  couchbase::core::row_streamer_options opts{};
  assert_eq(opts.lexer_depth, std::uint32_t{ 4 }, "the depth the lexer descends to");
  assert_eq(opts.high_water_bytes, std::size_t{ 2 } * 1024 * 1024, "the high-water mark");
  assert_eq(opts.low_water_bytes, std::size_t{ 512 } * 1024, "the low-water mark");
  assert_eq(opts.max_row_bytes, std::size_t{ 64 } * 1024 * 1024, "the single-row ceiling");
  assert_eq(
    opts.row_buffer_size, std::size_t{ 100 }, "the number of rows held ahead of the consumer");
  assert_eq(opts.idle_timeout, std::chrono::milliseconds{ 0 }, "no idle timeout unless asked for");
}

void
fails_a_single_row_larger_than_the_ceiling([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // One row whose padding exceeds a deliberately tiny ceiling.
  std::string big(std::size_t{ 64 } * 1024, 'X');
  std::string doc = R"({"results":[{"p":")" + big + R"("}],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer_options opts{};
  opts.max_row_bytes = std::size_t{ 4 } * 1024; // tiny ceiling
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  std::error_code seen_ec{};
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec) {
        seen_ec = ec;
        return;
      }
      if (row.empty()) {
        return;
      }
      pump();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    pump();
  });
  io.run();

  assert_ne(seen_ec, std::error_code{}, "exceeding the ceiling is a terminal error, not an OOM");
}

void
resolves_start_on_a_non_object_or_empty_response([[maybe_unused]] context& ctx)
{
  // A body that is valid JSON but not an object (e.g. a proxy emitting a bare array), an empty
  // body, or a whitespace-only body never drives the lexer's metadata-header callback. start()'s
  // handler must still be resolved with an error so the caller is not parked forever.
  const std::vector<std::string> payloads{ "[]", "42", "", "   " };
  for (const auto& payload : payloads) {
    asio::io_context io;
    auto body = utils::make_cached_response_body(io, payload);
    couchbase::core::row_streamer streamer{
      io, std::move(body), "/results/^", couchbase::core::row_streamer_options{}
    };

    bool start_called = false;
    std::error_code start_ec{};
    streamer.start([&](std::string, std::error_code ec) {
      start_called = true;
      start_ec = ec;
    });
    io.run();

    assert_true(start_called, "the preamble handler is always resolved");
    assert_ne(start_ec, std::error_code{}, "it is resolved with an error, not a phantom success");
  }
}

void
next_row_after_cancel_reports_request_canceled([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"results":[{"a":1},{"a":2}],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", couchbase::core::row_streamer_options{}
  };
  streamer.start([](std::string, std::error_code) {
  });
  streamer.cancel(); // closes the row channel

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  streamer.next_row([&](std::string /* row */, std::error_code ec) {
    seen = ec;
  });
  io.run();

  assert_eq(seen, couchbase::errc::common::request_canceled, "the reported terminal");
}

void
yields_rows_then_clean_end_over_cached_body([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A small N1QL-shaped document with two rows and a trailing status.
  std::string doc = R"({"requestID":"x","signature":{"a":"number"},)"
                    R"("results":[{"a":1},{"a":2}],"status":"success","metrics":{}})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^" };

  std::vector<std::string> rows;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        return;
      }
      rows.push_back(std::move(row));
      pump();
    });
  };
  streamer.start([&](std::string /*preamble*/, std::error_code) {
    pump();
  });
  io.run();

  assert_eq(rows.size(), std::size_t{ 2 }, "every row in the result set is delivered");
  assert_contains(rows[0], R"("a":1)", "the first row is handed over verbatim");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
}

void
surfaces_a_parsing_error_when_the_body_ends_mid_document([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // The body ends before the JSON document is complete (truncated response). The consumer must
  // receive a terminal error rather than blocking forever waiting for a completion that never
  // arrives.
  std::string doc = R"({"requestID":"x","results":[{"a":1},{"a":2)";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^" };

  std::error_code end_ec{};
  bool ended = false;
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        ended = true;
        return;
      }
      pump();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    pump();
  });
  io.run();

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_ne(end_ec, std::error_code{}, "a truncated document is a terminal parsing error");
}

// Builds a result set of row_count small rows. Against the deliberately tiny watermarks the cases
// below configure, the payload runs to several times the high-water mark, so the streamer has to
// pause and resume reading to deliver it.
auto
watermark_exercising_document(int row_count) -> std::string
{
  std::string doc = R"({"results":[)";
  for (int i = 0; i < row_count; ++i) {
    if (i != 0) {
      doc += ',';
    }
    doc += R"({"n":)" + std::to_string(i) + "}";
  }
  doc += R"(],"status":"success"})";
  return doc;
}

void
bounds_buffered_bytes_with_byte_watermarks([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  constexpr int row_count = 2000;
  const std::string doc = watermark_exercising_document(row_count);

  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  // Dribble the body out in small slices so reads are demand-gated, as a real socket would be.
  constexpr std::size_t chunk_size = 256;
  auto body = utils::make_chunked_response_body(io, doc, chunk_size);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  streamer.start([](std::string, std::error_code) {
  });
  // Consume nothing and let the streamer read as far as back-pressure permits.
  io.poll();
  // The context stops for good if its last outstanding operation ever completes, and a stopped
  // context runs no handler until it is restarted.
  io.restart();

  // Without watermarks the entire ~16 KB body would be buffered; with them, reads pause soon after
  // the high-water mark is crossed. The overshoot is bounded by the last in-flight read; allow a
  // few chunks of slack so the assertion does not couple to lexer row-framing details.
  assert_ne(
    streamer.buffered_bytes(), std::size_t{ 0 }, "the streamer reads ahead of the consumer");
  assert_true(streamer.buffered_bytes() <= opts.high_water_bytes + (4 * chunk_size),
              "reads pause within a few chunks of the high-water mark");

  // Draining releases the budget and yields every row exactly once.
  int seen = 0;
  bool ended = false;
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        ended = true;
        return;
      }
      ++seen;
      pump();
    });
  };
  pump();
  io.run();

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(seen, row_count, "every row is yielded exactly once");
  assert_eq(streamer.buffered_bytes(), std::size_t{ 0 }, "draining releases the whole budget");
}

void
refills_the_buffer_after_a_drain_below_the_low_water_mark([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  constexpr int row_count = 2000;
  const std::string doc = watermark_exercising_document(row_count);

  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  constexpr std::size_t chunk_size = 256;
  auto body = utils::make_chunked_response_body(io, doc, chunk_size);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  streamer.start([](std::string, std::error_code) {
  });
  io.poll();
  const auto paused = streamer.buffered_bytes();
  assert_true(paused > opts.high_water_bytes,
              "reading pauses once the buffered budget passes the high-water mark");

  // Drain more bytes than the budget held at the pause, one pull at a time. Reading stays stopped
  // until a release drops the budget to or under the low-water mark, so passing that total is
  // possible only if the resume refilled: a lost resume leaves the budget falling to zero and the
  // pull past that point unanswered.
  //
  // Polled rather than run: a row handed to a full channel stays queued as a pending send, which
  // is outstanding work that only a receiver completes, and this loop supplies one at a time. A
  // run() would never return. restart() precedes each poll because the context stops for good if
  // its last outstanding operation ever completes.
  std::size_t released = 0;
  int seen = 0;
  while (released <= paused && seen < row_count) {
    bool delivered = false;
    std::error_code pull_ec{};
    streamer.next_row([&](std::string row, std::error_code ec) {
      pull_ec = ec;
      released += row.size();
      delivered = !row.empty();
      ++seen;
    });
    for (int spin = 0; spin < 20 && !delivered; ++spin) {
      io.restart();
      io.poll();
    }
    assert_eq(pull_ec, std::error_code{}, "the stream is still mid-body while rows remain");
    assert_true(delivered, "every pull is answered while rows remain");
  }
  assert_true(released > paused, "the drain released more than the budget held at the pause");
  assert_ne(streamer.buffered_bytes(),
            std::size_t{ 0 },
            "the producer resumed reading rather than leaving the buffer drained");
}

void
resumes_reading_when_a_drain_races_the_closing_read_window([[maybe_unused]] context& ctx)
{
  // The back-pressure handshake driven from more than one thread. The consumer releases a row's
  // budget and then tries to claim the feed gate; the producer closes its read window and then
  // re-checks the same budget. A side that decides on a stale view of the other declines a resume
  // the other has already declined, leaving the stream with no read outstanding and the consumer
  // parked in next_row. The watermarks force repeated pause/resume cycles, and no idle timeout is
  // configured, so nothing exists that would end such a stall.
  //
  // On x86-TSO both sides are ordered whatever the accesses declare, so this cannot distinguish
  // the handshake's memory ordering. What it pins is that a drain below the low-water mark is
  // followed by a resume, which a lost resume fails on any target.
  constexpr int row_count = 512;
  constexpr int iterations = 4;
  constexpr int io_threads = 4;
  const std::string doc = watermark_exercising_document(row_count);

  for (int iteration = 0; iteration < iterations; ++iteration) {
    asio::io_context io;
    couchbase::core::row_streamer_options opts{};
    opts.high_water_bytes = std::size_t{ 2 } * 1024;
    opts.low_water_bytes = std::size_t{ 512 };
    auto body = utils::make_chunked_response_body(io, doc, 128);
    couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

    std::atomic_int seen{ 0 };
    std::promise<std::error_code> terminal;
    auto terminal_reached = terminal.get_future();
    std::function<void()> pump = [&]() {
      streamer.next_row([&](std::string row, std::error_code ec) {
        if (ec || row.empty()) {
          terminal.set_value(ec);
          return;
        }
        seen.fetch_add(1);
        pump();
      });
    };
    streamer.start([&](std::string, std::error_code) {
      pump();
    });

    std::vector<std::thread> runners;
    // Declared after the thread vector, so it runs first on the way out and leaves no joinable
    // thread for the vector's destructor to terminate the process over -- including when a later
    // thread construction throws.
    struct stop_and_join {
      asio::io_context& io;
      std::vector<std::thread>& threads;
      ~stop_and_join()
      {
        io.stop();
        for (auto& thread : threads) {
          if (thread.joinable()) {
            thread.join();
          }
        }
      }
    } const joiner{ io, runners };
    runners.reserve(io_threads);
    for (int i = 0; i < io_threads; ++i) {
      runners.emplace_back([&io]() {
        io.run();
      });
    }

    // Bounded so a stall is reported by the assertion below rather than as an expired case. The
    // case is registered with a budget that outlasts every iteration waiting this long.
    const auto reached =
      terminal_reached.wait_for(std::chrono::seconds{ 3 }) == std::future_status::ready;
    assert_true(reached,
                "the stream reaches its terminal rather than stalling with no read outstanding");
    assert_eq(terminal_reached.get(), std::error_code{}, "the stream ends cleanly");
    assert_eq(seen.load(), row_count, "every row is yielded exactly once");
  }
}

void
resumes_reading_when_the_low_water_mark_is_zero([[maybe_unused]] context& ctx)
{
  // buffered_bytes_ is unsigned, so a strict comparison against a zero low-water mark is
  // unsatisfiable: the consumer would never ask the producer to resume, and reading would stop for
  // good at the first pause. Whatever the mark is set to, a buffer drained empty has to resume.
  asio::io_context io;
  constexpr int row_count = 1000;
  const std::string doc = watermark_exercising_document(row_count);

  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 2 } * 1024;
  opts.low_water_bytes = 0;
  auto body = utils::make_chunked_response_body(io, doc, 256);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  streamer.start([](std::string, std::error_code) {
  });

  // One pull at a time, polled rather than run, for the reason given in the case above. A resume
  // that never fires leaves the pull after the buffer empties unanswered.
  int seen = 0;
  bool ended = false;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  while (!ended && seen <= row_count) {
    bool answered = false;
    streamer.next_row([&](std::string row, std::error_code ec) {
      answered = true;
      if (ec || row.empty()) {
        ended = true;
        end_ec = ec;
        return;
      }
      ++seen;
    });
    for (int spin = 0; spin < 20 && !answered; ++spin) {
      io.restart();
      io.poll();
    }
    assert_true(answered, "every pull is answered, so reading resumed at an empty buffer");
  }
  assert_true(ended, "the stream reaches its terminal");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_eq(seen, row_count, "every row is yielded exactly once");
}

void
streams_to_completion_with_both_watermarks_at_zero([[maybe_unused]] context& ctx)
{
  // The degenerate setting: read ahead of the consumer by nothing at all. The producer declines
  // the gate while any row is buffered and the consumer asks for a resume once none is, so the
  // two marks meet at an empty buffer and the stream advances one read at a time. Neither side
  // may decline there, or the stream ends at its first pause.
  asio::io_context io;
  constexpr int row_count = 200;
  const std::string doc = watermark_exercising_document(row_count);

  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = 0;
  opts.low_water_bytes = 0;
  auto body = utils::make_chunked_response_body(io, doc, 256);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  streamer.start([](std::string, std::error_code) {
  });

  int seen = 0;
  bool ended = false;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  while (!ended && seen <= row_count) {
    bool answered = false;
    streamer.next_row([&](std::string row, std::error_code ec) {
      answered = true;
      if (ec || row.empty()) {
        ended = true;
        end_ec = ec;
        return;
      }
      ++seen;
    });
    for (int spin = 0; spin < 20 && !answered; ++spin) {
      io.restart();
      io.poll();
    }
    assert_true(answered, "every pull is answered, so neither side declines at an empty buffer");
  }
  assert_true(ended, "the stream reaches its terminal");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_eq(seen, row_count, "every row is yielded exactly once");
}

void
handles_an_empty_result_set([[maybe_unused]] context& ctx)
{
  auto [rows, end_ec] = drain_rows(R"({"requestID":"x","results":[],"status":"success"})");
  assert_true(rows.empty(), "an empty result set yields no rows");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
}

void
yields_scalar_and_null_row_values([[maybe_unused]] context& ctx)
{
  auto [rows, end_ec] = drain_rows(R"({"results":[null,42,"hi",true,3.5],"status":"success"})");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_eq(rows.size(), std::size_t{ 5 }, "every row in the result set is delivered");
  assert_eq(rows[0], "null", "a null row");
  assert_eq(rows[1], "42", "an integer row");
  assert_eq(rows[2], R"("hi")", "a string row keeps its quotes");
  assert_eq(rows[3], "true", "a boolean row");
  assert_eq(rows[4], "3.5", "a floating point row");
}

void
preserves_embedded_brackets_and_unicode_in_a_row([[maybe_unused]] context& ctx)
{
  // The row value contains characters that the lexer must treat as string content, not structure:
  // a closing bracket/brace and a multi-byte UTF-8 sequence (é).
  const std::string doc = "{\"results\":[{\"s\":\"a]b}c\xC3\xA9\"}],\"status\":\"success\"}";
  auto [rows, end_ec] = drain_rows(doc);
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_eq(rows.size(), std::size_t{ 1 }, "the row is not split at the embedded bracket");
  assert_eq(rows[0], "{\"s\":\"a]b}c\xC3\xA9\"}", "the row is handed over byte for byte");
}

void
idle_timeout_on_a_mutating_request_is_ambiguous([[maybe_unused]] context& ctx)
{
  // A mid-stream idle timeout on a non-read-only (mutating) query must be reported as
  // ambiguous_timeout: the mutation may have partially applied, so a retry layer must not treat it
  // as safe to replay.
  assert_eq(idle_timeout_terminal(/*is_read_only*/ false),
            couchbase::errc::common::ambiguous_timeout,
            "the reported terminal");
}

void
idle_timeout_on_a_read_only_request_is_unambiguous([[maybe_unused]] context& ctx)
{
  // A read-only request is idempotent, so its idle timeout is unambiguous (definitely not applied).
  assert_eq(idle_timeout_terminal(/*is_read_only*/ true),
            couchbase::errc::common::unambiguous_timeout,
            "the reported terminal");
}

void
surfaces_a_mid_stream_transport_error_verbatim([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // Deliver a valid preamble and one row, then inject a transport-level failure (as a reset socket
  // would). Unlike a JSON parse failure — which the streaming query/analytics layer normalizes to
  // parsing_failure — a transport error must surface as-is so the caller can tell a network failure
  // apart from a malformed body.
  const std::string data = R"({"results":[{"a":1})";
  const auto injected = make_error_code(std::errc::connection_reset);
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io, data, /*cached_chunk_size*/ 0, injected, /*stall*/ false);
  couchbase::core::row_streamer streamer{
    io, std::move(body), "/results/^", couchbase::core::row_streamer_options{}
  };

  int rows = 0;
  std::error_code end_ec{};
  bool ended = false;
  std::function<void()> pump = [&]() {
    streamer.next_row([&](std::string row, std::error_code ec) {
      if (ec || row.empty()) {
        end_ec = ec;
        ended = true;
        return;
      }
      ++rows;
      pump();
    });
  };
  streamer.start([&](std::string, std::error_code) {
    pump();
  });
  io.run();

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(rows, 1, "the row delivered before the failure is still seen");
  assert_eq(end_ec, injected, "the transport error passes through unchanged");
}

void
cancel_wins_over_an_armed_idle_timer([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A stalling body with a generous idle timeout: the inter-read idle timer is armed while the
  // (never-completing) read is outstanding, but cancel() is called long before it could fire.
  // cancel() must produce request_canceled — not an (un)ambiguous timeout — even though the timer
  // is armed; otherwise a user cancellation that raced the timer would be misreported as a timeout.
  auto body = couchbase::core::http_response_body::create_in_memory_faulty(
    io, /*data*/ {}, /*cached_chunk_size*/ 0, /*terminal_ec*/ {}, /*stall*/ true);
  couchbase::core::row_streamer_options opts{};
  opts.idle_timeout = std::chrono::seconds{ 30 }; // far longer than the test; must never fire
  opts.is_read_only = false; // would classify as ambiguous_timeout if misreported
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  streamer.start([&](std::string, std::error_code) {
    streamer.next_row([&](std::string /* row */, std::error_code ec) {
      end_ec = ec;
      ended = true;
    });
  });
  streamer.cancel();
  io.run();

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(end_ec, couchbase::errc::common::request_canceled, "the reported terminal");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(row_streamer_options_has_streaming_defaults) },
      { CASE(fails_a_single_row_larger_than_the_ceiling) },
      { CASE(resolves_start_on_a_non_object_or_empty_response) },
      { CASE(next_row_after_cancel_reports_request_canceled) },
      { CASE(yields_rows_then_clean_end_over_cached_body) },
      { CASE(surfaces_a_parsing_error_when_the_body_ends_mid_document) },
      { CASE(bounds_buffered_bytes_with_byte_watermarks) },
      { CASE(refills_the_buffer_after_a_drain_below_the_low_water_mark) },
      // timeout::slow, not network: the case bounds each stall at three seconds of its own, and
      // the budget has to outlast those bounds for the failure to be reported as one.
      { CASE(resumes_reading_when_a_drain_races_the_closing_read_window), {}, timeout::slow },
      { CASE(resumes_reading_when_the_low_water_mark_is_zero) },
      { CASE(streams_to_completion_with_both_watermarks_at_zero) },
      { CASE(handles_an_empty_result_set) },
      { CASE(yields_scalar_and_null_row_values) },
      { CASE(preserves_embedded_brackets_and_unicode_in_a_row) },
      { CASE(idle_timeout_on_a_mutating_request_is_ambiguous) },
      { CASE(idle_timeout_on_a_read_only_request_is_unambiguous) },
      { CASE(surfaces_a_mid_stream_transport_error_verbatim) },
      { CASE(cancel_wins_over_an_armed_idle_timer) },
    },
  };
}

} // namespace couchbase::test
