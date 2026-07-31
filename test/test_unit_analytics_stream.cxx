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

// Deterministic unit coverage for core::analytics_stream, mirroring test_unit_query_stream.cxx.
// analytics_stream previously had no unit test — its logic was exercised only by integration tests
// that SKIP without an analytics service, so a CI lane without one covered none of it. These tests
// drive the state machine over in-memory bodies (no server, no analytics service required).

#include "core/analytics_stream.hxx"
#include "core/impl/internal_analytics_stream_result.hxx"
#include "core/impl/observability_recorder.hxx"
#include "core/operations/document_analytics.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <catch2/catch_test_macros.hpp>
#include <tao/json/from_string.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace
{
using analytics_status = couchbase::core::operations::analytics_response::analytics_status;
} // namespace

TEST_CASE("unit: analytics_stream yields rows then exposes late metadata", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r1","signature":{"a":"number"},)"
                    R"("results":[{"a":1},{"a":2}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  std::vector<std::string> rows;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        return;
      }
      rows.push_back(*row);
      pump();
    });
  };
  std::error_code early{};
  stream.start([&](std::error_code e) {
    early = e;
    pump();
  });
  io.run();

  REQUIRE(!early);
  REQUIRE(rows.size() == 2);
  REQUIRE(!end_ec); // clean success end
  REQUIRE(stream.signature().has_value());
  REQUIRE(stream.meta_data().has_value());
  REQUIRE(stream.meta_data()->status == analytics_status::success);
}

TEST_CASE("unit: analytics_stream surfaces a trailing analytics error after rows", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r2","results":[{"a":1}],)"
                    R"("status":"fatal","errors":[{"code":24000,"msg":"boom"}]})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  int row_count = 0;
  std::error_code end_ec{};
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        return;
      }
      ++row_count;
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  REQUIRE(row_count == 1);
  REQUIRE(end_ec); // trailing error delivered as the terminal next_row result
}

TEST_CASE("unit: analytics_stream reports a clean end for an empty result set", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r3","signature":{"a":"number"},)"
                    R"("results":[],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  int row_count = 0;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  bool ended = false;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        ended = true;
        return;
      }
      ++row_count;
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  REQUIRE(ended);
  REQUIRE(row_count == 0);
  REQUIRE(!end_ec); // clean success end with zero rows
  REQUIRE(stream.meta_data().has_value());
  REQUIRE(stream.meta_data()->status == analytics_status::success);
}

TEST_CASE("unit: analytics_stream normalizes a malformed body to parsing_failure", "[unit]")
{
  asio::io_context io;
  // A bare unquoted token where a value is expected makes the streaming lexer abort with a
  // streaming_json_lexer::* code; analytics_stream must normalize it to parsing_failure to match
  // the buffered analytics_query() contract.
  std::string doc = R"({"requestID":"r","results":[{"a":1},xxx],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  std::error_code end_ec{};
  bool ended = false;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        ended = true;
        return;
      }
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  REQUIRE(ended);
  REQUIRE(end_ec == couchbase::errc::common::parsing_failure);
}

TEST_CASE("unit: analytics_stream normalizes an oversized row to parsing_failure", "[unit]")
{
  asio::io_context io;
  std::string big(std::size_t{ 64 } * 1024, 'X');
  std::string doc = R"({"results":[{"p":")" + big + R"("}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer_options opts{};
  opts.max_row_bytes = std::size_t{ 4 } * 1024; // tiny ceiling so the row overflows it
  couchbase::core::analytics_stream stream{ io, std::move(body), opts };

  std::error_code end_ec{};
  bool ended = false;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        ended = true;
        return;
      }
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  REQUIRE(ended);
  REQUIRE(end_ec == couchbase::errc::common::parsing_failure);
}

TEST_CASE("unit: analytics_stream re-delivers the terminal on pulls after the end", "[unit]")
{
  // Terminal-sticky contract: once the stream has ended, every later next_row re-delivers the
  // terminal instead of parking forever on the drained channel.
  asio::io_context io;
  std::string doc = R"({"requestID":"r","results":[{"a":1}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  int terminals = 0;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code) {
      if (!row.has_value()) {
        if (++terminals < 4) {
          pump();
        }
        return;
      }
      pump();
    });
  };
  stream.start([&](std::error_code) {
    pump();
  });
  io.run();

  REQUIRE(terminals == 4);
}

TEST_CASE("unit: analytics_stream reports request_canceled after cancel", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"results":[{"a":1},{"a":2}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  stream.start([](std::error_code) {
  });
  stream.cancel();

  std::error_code seen{ make_error_code(std::errc::operation_in_progress) };
  stream.next_row([&](std::optional<std::string> /* row */, std::error_code ec) {
    seen = ec;
  });
  io.run();

  REQUIRE(seen == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: analytics_stream reports no signature when absent", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r5","results":[{"a":1}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  bool resolved = false;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code) {
      if (row.has_value()) {
        pump();
      }
    });
  };
  stream.start([&](std::error_code) {
    resolved = true;
    pump();
  });
  io.run();

  REQUIRE(resolved);
  REQUIRE_FALSE(stream.signature().has_value());
}

TEST_CASE("unit: a mid-stream analytics terminal error is reported with the request context",
          "[unit]")
{
  // Counterpart of the query case in test_unit_query_stream.cxx: rows followed by a trailing error,
  // so the terminal arrives from the stream and reaches the first observing next().
  asio::io_context io;
  const std::string doc = R"({"requestID":"r-ctx","results":[{"a":1}],)"
                          R"("status":"fatal","errors":[{"code":24000,"msg":"boom"}]})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::analytics_stream stream{ io, std::move(body) };

  std::error_code start_ec{ make_error_code(std::errc::operation_in_progress) };
  stream.start([&start_ec](std::error_code ec) {
    start_ec = ec;
  });
  io.run();
  REQUIRE_FALSE(start_ec); // the error is in the trailer, so starting succeeds

  couchbase::core::error_context::analytics ctx{};
  ctx.statement = "SELECT a FROM x";
  ctx.client_context_id = "cid-1";
  ctx.method = "POST";
  ctx.path = "/analytics/service";
  ctx.hostname = "node.example";
  ctx.port = 8095;
  auto internal = std::make_shared<couchbase::internal_analytics_stream_result>(
    std::move(stream), nullptr, std::move(ctx));

  io.restart();
  int rows = 0;
  std::optional<couchbase::error> terminal;
  std::function<void()> pump = [&]() {
    internal->next([&](couchbase::error err, std::optional<couchbase::analytics_row> row) {
      if (!row.has_value()) {
        terminal = std::move(err);
        return;
      }
      ++rows;
      pump();
    });
  };
  pump();
  io.run();

  REQUIRE(rows == 1);
  REQUIRE(terminal.has_value());
  REQUIRE(terminal->ec());
  const auto reported = tao::json::from_string(terminal->ctx().to_json());
  REQUIRE(reported.at("statement").as<std::string>() == "SELECT a FROM x");
  REQUIRE(reported.at("client_context_id").as<std::string>() == "cid-1");
  REQUIRE(reported.at("hostname").as<std::string>() == "node.example");
  REQUIRE(reported.at("port").as<std::uint16_t>() == 8095);
  REQUIRE(reported.at("first_error_code").as<std::uint64_t>() == 24000);
  REQUIRE(reported.at("first_error_message").as<std::string>() == "boom");
}
