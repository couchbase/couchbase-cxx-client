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

#include "core/row_streamer.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <functional>
#include <string>
#include <system_error>
#include <vector>

TEST_CASE("unit: row_streamer_options has streaming defaults", "[unit]")
{
  couchbase::core::row_streamer_options opts{};
  REQUIRE(opts.lexer_depth == 4);
  REQUIRE(opts.high_water_bytes == std::size_t{ 2 } * 1024 * 1024);
  REQUIRE(opts.low_water_bytes == std::size_t{ 512 } * 1024);
  REQUIRE(opts.max_row_bytes == std::size_t{ 64 } * 1024 * 1024);
  REQUIRE(opts.row_buffer_size == 100);
  REQUIRE(opts.idle_timeout == std::chrono::milliseconds{ 0 });
}

TEST_CASE("unit: row_streamer fails a single row larger than the ceiling", "[unit]")
{
  asio::io_context io;
  // One row whose padding exceeds a deliberately tiny ceiling.
  std::string big(std::size_t{ 64 } * 1024, 'X');
  std::string doc = R"({"results":[{"p":")" + big + R"("}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
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

  REQUIRE(seen_ec); // ceiling exceeded => non-empty terminal error, no OOM
}

TEST_CASE("unit: row_streamer resolves start() on a non-object or empty response", "[unit]")
{
  // A body that is valid JSON but not an object (e.g. a proxy emitting a bare array), an empty
  // body, or a whitespace-only body never drives the lexer's metadata-header callback. start()'s
  // handler must still be resolved with an error so the caller is not parked forever.
  const std::vector<std::string> payloads{ "[]", "42", "", "   " };
  for (const auto& payload : payloads) {
    asio::io_context io;
    auto body = test::utils::make_cached_response_body(io, payload);
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

    REQUIRE(start_called); // must not hang: the preamble handler is always resolved
    REQUIRE(start_ec);     // resolved with an error, not a phantom success
  }
}

TEST_CASE("unit: row_streamer next_row after cancel reports request_canceled", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"results":[{"a":1},{"a":2}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
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

  REQUIRE(seen == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: row_streamer yields rows then clean end over cached body", "[unit]")
{
  asio::io_context io;
  // A small N1QL-shaped document with two rows and a trailing status.
  std::string doc = R"({"requestID":"x","signature":{"a":"number"},)"
                    R"("results":[{"a":1},{"a":2}],"status":"success","metrics":{}})";
  auto body = test::utils::make_cached_response_body(io, doc);
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

  REQUIRE(rows.size() == 2);
  REQUIRE(rows[0].find("\"a\":1") != std::string::npos);
  REQUIRE(!end_ec); // clean end => empty error
}

TEST_CASE("unit: row_streamer surfaces a parsing error when the body ends mid-document", "[unit]")
{
  asio::io_context io;
  // The body ends before the JSON document is complete (truncated response). The consumer must
  // receive a terminal error rather than blocking forever waiting for a completion that never
  // arrives.
  std::string doc = R"({"requestID":"x","results":[{"a":1},{"a":2)";
  auto body = test::utils::make_cached_response_body(io, doc);
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

  REQUIRE(ended);  // must terminate, not hang
  REQUIRE(end_ec); // truncated document => terminal parsing error
}

TEST_CASE("unit: row_streamer bounds buffered bytes with byte watermarks", "[unit]")
{
  asio::io_context io;
  // A document whose total row payload (~16 KB) is several times the high-water mark, so a working
  // back-pressure implementation must stop reading long before the whole body is buffered.
  constexpr int row_count = 2000;
  std::string doc = R"({"results":[)";
  for (int i = 0; i < row_count; ++i) {
    if (i != 0) {
      doc += ",";
    }
    doc += R"({"n":)" + std::to_string(i) + "}";
  }
  doc += R"(],"status":"success"})";

  couchbase::core::row_streamer_options opts{};
  opts.high_water_bytes = std::size_t{ 4 } * 1024;
  opts.low_water_bytes = std::size_t{ 1 } * 1024;
  // Dribble the body out in small slices so reads are demand-gated, as a real socket would be.
  constexpr std::size_t chunk_size = 256;
  auto body = test::utils::make_chunked_response_body(io, doc, chunk_size);
  couchbase::core::row_streamer streamer{ io, std::move(body), "/results/^", opts };

  streamer.start([](std::string, std::error_code) {
  });
  // Consume nothing and let the streamer read as far as back-pressure permits.
  io.poll();

  // Without watermarks the entire ~16 KB body would be buffered; with them, reads pause soon after
  // the high-water mark is crossed. The overshoot is bounded by the last in-flight read; allow a
  // few chunks of slack so the assertion does not couple to lexer row-framing details.
  REQUIRE(streamer.buffered_bytes() > 0);
  REQUIRE(streamer.buffered_bytes() <= opts.high_water_bytes + (4 * chunk_size));

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

  REQUIRE(ended);
  REQUIRE(seen == row_count);
  REQUIRE(streamer.buffered_bytes() == 0);
}

namespace
{
// Drains a cached document through a row_streamer and returns the rows plus the terminal error.
auto
drain_rows(const std::string& doc) -> std::pair<std::vector<std::string>, std::error_code>
{
  asio::io_context io;
  auto body = test::utils::make_cached_response_body(io, doc);
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
} // namespace

TEST_CASE("unit: row_streamer handles an empty result set", "[unit]")
{
  auto [rows, end_ec] = drain_rows(R"({"requestID":"x","results":[],"status":"success"})");
  REQUIRE(rows.empty());
  REQUIRE(!end_ec); // clean end, no rows
}

TEST_CASE("unit: row_streamer yields scalar and null row values", "[unit]")
{
  auto [rows, end_ec] = drain_rows(R"({"results":[null,42,"hi",true,3.5],"status":"success"})");
  REQUIRE(!end_ec);
  REQUIRE(rows.size() == 5);
  REQUIRE(rows[0] == "null");
  REQUIRE(rows[1] == "42");
  REQUIRE(rows[2] == R"("hi")");
  REQUIRE(rows[3] == "true");
  REQUIRE(rows[4] == "3.5");
}

TEST_CASE("unit: row_streamer preserves embedded brackets and unicode in a row", "[unit]")
{
  // The row value contains characters that the lexer must treat as string content, not structure:
  // a closing bracket/brace and a multi-byte UTF-8 sequence (é).
  const std::string doc = "{\"results\":[{\"s\":\"a]b}c\xC3\xA9\"}],\"status\":\"success\"}";
  auto [rows, end_ec] = drain_rows(doc);
  REQUIRE(!end_ec);
  REQUIRE(rows.size() == 1);
  REQUIRE(rows[0] == "{\"s\":\"a]b}c\xC3\xA9\"}");
}

namespace
{
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
  REQUIRE(ended); // the idle timer must produce a terminal, not hang
  return end_ec;
}
} // namespace

TEST_CASE("unit: row_streamer idle timeout on a mutating request is ambiguous", "[unit]")
{
  // A mid-stream idle timeout on a non-read-only (mutating) query must be reported as
  // ambiguous_timeout: the mutation may have partially applied, so a retry layer must not treat it
  // as safe to replay. This is the buffered path's http_command behavior, now mirrored by the
  // streaming path via row_streamer_options::is_read_only.
  REQUIRE(idle_timeout_terminal(/*is_read_only*/ false) ==
          couchbase::errc::common::ambiguous_timeout);
}

TEST_CASE("unit: row_streamer idle timeout on a read-only request is unambiguous", "[unit]")
{
  // A read-only request is idempotent, so its idle timeout is unambiguous (definitely not applied).
  REQUIRE(idle_timeout_terminal(/*is_read_only*/ true) ==
          couchbase::errc::common::unambiguous_timeout);
}

TEST_CASE("unit: row_streamer surfaces a mid-stream transport error verbatim", "[unit]")
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

  REQUIRE(ended);
  REQUIRE(rows == 1);          // the row delivered before the failure is still seen
  REQUIRE(end_ec == injected); // transport error passed through unchanged (not parsing_failure)
}

TEST_CASE("unit: row_streamer cancel wins over an armed idle timer", "[unit]")
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

  REQUIRE(ended);
  REQUIRE(end_ec == couchbase::errc::common::request_canceled);
}
