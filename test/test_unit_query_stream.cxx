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

#include "core/cluster.hxx"
#include "core/query_stream.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <catch2/catch_test_macros.hpp>

#include <functional>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

TEST_CASE("unit: query_stream yields rows then exposes late metadata", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r1","signature":{"a":"number"},)"
                    R"("results":[{"a":1},{"a":2}],"status":"success",)"
                    R"("metrics":{"resultCount":2,"resultSize":10,)"
                    R"("elapsedTime":"1ms","executionTime":"1ms"}})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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
  REQUIRE(stream.meta_data()->status == "success");
}

TEST_CASE("unit: query_stream surfaces a trailing query error after rows", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r2","results":[{"a":1}],)"
                    R"("status":"fatal","errors":[{"code":5000,"msg":"boom"}]})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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

TEST_CASE("unit: cluster exposes a core query_stream entry point", "[unit]")
{
  // Compile-only: the symbol exists with the expected signature.
  using fn =
    void (couchbase::core::cluster::*)(couchbase::core::operations::query_request,
                                       couchbase::core::utils::movable_function<void(
                                         couchbase::core::query_stream, std::error_code)>&&) const;
  constexpr fn p = &couchbase::core::cluster::query_stream;
  STATIC_REQUIRE(p != nullptr); // compile-time contract: the overload resolves to exactly `fn`
}

TEST_CASE("unit: query_stream buffered replay yields rows and exposes metadata immediately",
          "[unit]")
{
  // The buffered (prepared-statement, adhoc=false) path replays already-parsed rows + metadata
  // without touching the JSON lexer. Unlike the streaming path, signature()/meta_data() are
  // available before draining.
  asio::io_context io;
  couchbase::core::operations::query_response::query_meta_data meta{};
  meta.request_id = "r-buffered";
  meta.status = "success";
  meta.signature = R"({"a":"number"})";
  const std::vector<std::string> rows{ R"({"a":1})", R"({"a":2})", R"({"a":3})" };
  couchbase::core::query_stream stream{ io, rows, meta };

  REQUIRE(stream.signature().has_value());
  REQUIRE(stream.meta_data().has_value());
  REQUIRE(stream.meta_data()->status == "success");

  std::vector<std::string> drained;
  std::error_code end_ec{ make_error_code(std::errc::operation_in_progress) };
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
      if (!row.has_value()) {
        end_ec = ec;
        return;
      }
      drained.push_back(*row);
      pump();
    });
  };
  pump();
  io.run();

  REQUIRE(drained == rows); // in-order, verbatim replay
  REQUIRE(!end_ec);         // success metadata => clean terminal
}

TEST_CASE("unit: query_stream buffered replay reports request_canceled after cancel", "[unit]")
{
  asio::io_context io;
  couchbase::core::operations::query_response::query_meta_data meta{};
  meta.status = "success";
  const std::vector<std::string> rows{ R"({"a":1})", R"({"a":2})" };
  couchbase::core::query_stream stream{ io, rows, meta };

  stream.cancel();
  bool got_row = false;
  std::error_code seen{};
  stream.next_row([&](std::optional<std::string> row, std::error_code ec) {
    got_row = row.has_value();
    seen = ec;
  });
  io.run();

  REQUIRE_FALSE(got_row);
  REQUIRE(seen == couchbase::errc::common::request_canceled);
}

TEST_CASE("unit: query_stream reports a clean end for an empty result set", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r3","signature":{"a":"number"},)"
                    R"("results":[],"status":"success",)"
                    R"("metrics":{"resultCount":0,"resultSize":0,)"
                    R"("elapsedTime":"1ms","executionTime":"1ms"}})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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
  REQUIRE(stream.meta_data()->status == "success");
}

TEST_CASE("unit: query_stream surfaces a trailing error with zero rows", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r4","results":[],)"
                    R"("status":"fatal","errors":[{"code":5000,"msg":"boom"}]})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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

  REQUIRE(row_count == 0);
  REQUIRE(end_ec); // error trailer surfaced even though no rows were produced
}

TEST_CASE("unit: query_stream reports no signature when absent", "[unit]")
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r5","results":[{"a":1}],"status":"success"})";
  auto body = test::utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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
  REQUIRE_FALSE(stream.signature().has_value()); // no signature key in the response
}
