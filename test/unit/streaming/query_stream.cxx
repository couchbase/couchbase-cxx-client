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

#include "core/cluster.hxx"
#include "core/impl/internal_query_stream_result.hxx"
#include "core/impl/observability_recorder.hxx"
#include "core/query_stream.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <tao/json/from_string.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace utils = ::test::utils;

void
yields_rows_then_exposes_late_metadata([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r1","signature":{"a":"number"},)"
                    R"("results":[{"a":1},{"a":2}],"status":"success",)"
                    R"("metrics":{"resultCount":2,"resultSize":10,)"
                    R"("elapsedTime":"1ms","executionTime":"1ms"}})";
  auto body = utils::make_cached_response_body(io, doc);
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

  assert_eq(early, std::error_code{}, "starting the stream succeeds");
  assert_eq(rows.size(), std::size_t{ 2 }, "every row in the result set is delivered");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_true(stream.signature().has_value(),
              "the signature is available once the body is drained");
  assert_true(stream.meta_data().has_value(), "the metadata is available once the body is drained");
  assert_eq(stream.meta_data()->status, "success", "the reported status");
}

void
surfaces_a_trailing_query_error_after_rows([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r2","results":[{"a":1}],)"
                    R"("status":"fatal","errors":[{"code":5000,"msg":"boom"}]})";
  auto body = utils::make_cached_response_body(io, doc);
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

  assert_eq(row_count, 1, "the row delivered ahead of the trailer is still seen");
  assert_ne(end_ec, std::error_code{}, "the trailing error is the terminal next_row result");
}

void
cluster_exposes_a_core_query_stream_entry_point([[maybe_unused]] context& ctx)
{
  // Compile-only: the initialisation below resolves &cluster::query_stream against exactly `fn`
  // and does not compile if no overload matches. The handler receives an error context rather than
  // a bare error_code, so a streaming failure carries the same diagnostics (statement, service
  // error, endpoint) the buffered path reports.
  using fn = void (couchbase::core::cluster::*)(
    couchbase::core::operations::query_request,
    couchbase::core::utils::movable_function<void(couchbase::core::query_stream,
                                                  couchbase::core::error_context::query)>&&) const;
  [[maybe_unused]] constexpr fn entry_point = &couchbase::core::cluster::query_stream;
}

void
buffered_replay_yields_rows_and_exposes_metadata_immediately([[maybe_unused]] context& ctx)
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

  assert_true(stream.signature().has_value(), "the signature is available before draining");
  assert_true(stream.meta_data().has_value(), "the metadata is available before draining");
  assert_eq(stream.meta_data()->status, "success", "the reported status");

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

  assert_true(drained == rows, "the buffered rows are replayed in order and verbatim");
  assert_eq(end_ec, std::error_code{}, "success metadata ends the replay cleanly");
}

void
buffered_replay_reports_request_canceled_after_cancel([[maybe_unused]] context& ctx)
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

  assert_false(got_row, "a cancelled replay hands out no further rows");
  assert_eq(seen, couchbase::errc::common::request_canceled, "the reported terminal");
}

void
reports_a_clean_end_for_an_empty_result_set([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r3","signature":{"a":"number"},)"
                    R"("results":[],"status":"success",)"
                    R"("metrics":{"resultCount":0,"resultSize":0,)"
                    R"("elapsedTime":"1ms","executionTime":"1ms"}})";
  auto body = utils::make_cached_response_body(io, doc);
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

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(row_count, 0, "an empty result set yields no rows");
  assert_eq(end_ec, std::error_code{}, "the stream ends cleanly");
  assert_true(stream.meta_data().has_value(), "the metadata is available once the body is drained");
  assert_eq(stream.meta_data()->status, "success", "the reported status");
}

void
surfaces_a_trailing_error_with_zero_rows([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r4","results":[],)"
                    R"("status":"fatal","errors":[{"code":5000,"msg":"boom"}]})";
  auto body = utils::make_cached_response_body(io, doc);
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

  assert_eq(row_count, 0, "the failing statement yields no rows");
  assert_ne(end_ec, std::error_code{}, "the error trailer is surfaced with no rows to carry it");
}

void
normalizes_a_malformed_body_to_parsing_failure([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A valid preamble and one row, then a bare unquoted token where a value is expected: the
  // streaming lexer aborts with a streaming_json_lexer::* code. query_stream must normalize that to
  // errc::common::parsing_failure so it matches the buffered query() contract (which reports
  // parsing_failure for any body-parse failure) instead of leaking the internal lexer code.
  std::string doc = R"({"requestID":"r","results":[{"a":1},xxx],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

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

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(end_ec, couchbase::errc::common::parsing_failure, "the reported terminal");
}

void
normalizes_an_oversized_row_to_parsing_failure([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // A single row larger than the max_row_bytes ceiling trips the lexer's max_buffer abort. That
  // abort must also be normalized to parsing_failure rather than surfacing the raw
  // streaming_json_lexer code.
  std::string big(std::size_t{ 64 } * 1024, 'X');
  std::string doc = R"({"results":[{"p":")" + big + R"("}],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::row_streamer_options opts{};
  opts.max_row_bytes = std::size_t{ 4 } * 1024; // tiny ceiling so the row overflows it
  couchbase::core::query_stream stream{ io, std::move(body), opts };

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

  assert_true(ended, "the stream terminates rather than parking the consumer");
  assert_eq(end_ec, couchbase::errc::common::parsing_failure, "the reported terminal");
}

void
re_delivers_the_terminal_on_pulls_after_the_end([[maybe_unused]] context& ctx)
{
  // Terminal-sticky contract: once the stream has ended, every later next_row must re-deliver the
  // same terminal instead of parking forever on the drained channel.
  asio::io_context io;
  std::string doc = R"({"requestID":"r","results":[{"a":1}],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

  int terminals = 0;
  std::function<void()> pump = [&]() {
    stream.next_row([&](std::optional<std::string> row, std::error_code) {
      if (!row.has_value()) {
        // Pull three more times after the terminal; each must resolve (not hang).
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

  assert_eq(terminals, 4, "every pull after the end resolves with the terminal");
}

void
reports_no_signature_when_absent([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  std::string doc = R"({"requestID":"r5","results":[{"a":1}],"status":"success"})";
  auto body = utils::make_cached_response_body(io, doc);
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

  assert_true(resolved, "the start handler is resolved");
  assert_false(stream.signature().has_value(), "a response with no signature key reports none");
}

void
a_mid_stream_query_terminal_error_is_reported_with_the_request_context(
  [[maybe_unused]] context& ctx)
{
  // Rows followed by a trailing error, so the terminal arrives from the stream itself. That is the
  // path which hands the failure to the caller's first observing next(), and it is the only one
  // that exercises it: cancel() publishes the terminal before next() runs, so a cancelled stream is
  // served by the idempotent re-delivery guard instead.
  asio::io_context io;
  const std::string doc = R"({"requestID":"r-ctx","results":[{"a":1}],)"
                          R"("status":"fatal","errors":[{"code":5000,"msg":"boom"}]})";
  auto body = utils::make_cached_response_body(io, doc);
  couchbase::core::query_stream stream{ io, std::move(body) };

  std::error_code start_ec{ make_error_code(std::errc::operation_in_progress) };
  stream.start([&start_ec](std::error_code ec) {
    start_ec = ec;
  });
  io.run();
  assert_eq(start_ec, std::error_code{}, "the error is in the trailer, so starting succeeds");

  couchbase::core::error_context::query error_ctx{};
  error_ctx.statement = "SELECT a FROM x";
  error_ctx.client_context_id = "cid-1";
  error_ctx.method = "POST";
  error_ctx.path = "/query/service";
  error_ctx.hostname = "node.example";
  error_ctx.port = 8093;
  auto internal = std::make_shared<couchbase::internal_query_stream_result>(
    std::move(stream), nullptr, std::move(error_ctx));

  io.restart();
  int rows = 0;
  std::optional<couchbase::error> terminal;
  std::function<void()> pump = [&]() {
    internal->next([&](couchbase::error err, std::optional<couchbase::query_row> row) {
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

  assert_eq(rows, 1, "the row delivered ahead of the trailer is still seen");
  assert_true(terminal.has_value(), "the consumer observes a terminal");
  assert_ne(terminal->ec(), std::error_code{}, "the terminal carries an error");
  // The request-side fields come from the context the handle was built with, and the service-side
  // ones from the trailer. A bare error built at the delivery site would carry neither.
  const auto reported = tao::json::from_string(terminal->ctx().to_json());
  assert_eq(reported.at("statement").as<std::string>(), "SELECT a FROM x", "the statement");
  assert_eq(reported.at("client_context_id").as<std::string>(), "cid-1", "the context id");
  assert_eq(reported.at("hostname").as<std::string>(), "node.example", "the endpoint hostname");
  assert_eq(reported.at("port").as<std::uint16_t>(), std::uint16_t{ 8093 }, "the endpoint port");
  assert_eq(reported.at("first_error_code").as<std::uint64_t>(),
            std::uint64_t{ 5000 },
            "the service error code");
  assert_eq(
    reported.at("first_error_message").as<std::string>(), "boom", "the service error message");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(yields_rows_then_exposes_late_metadata) },
      { CASE(surfaces_a_trailing_query_error_after_rows) },
      { CASE(cluster_exposes_a_core_query_stream_entry_point) },
      { CASE(buffered_replay_yields_rows_and_exposes_metadata_immediately) },
      { CASE(buffered_replay_reports_request_canceled_after_cancel) },
      { CASE(reports_a_clean_end_for_an_empty_result_set) },
      { CASE(surfaces_a_trailing_error_with_zero_rows) },
      { CASE(normalizes_a_malformed_body_to_parsing_failure) },
      { CASE(normalizes_an_oversized_row_to_parsing_failure) },
      { CASE(re_delivers_the_terminal_on_pulls_after_the_end) },
      { CASE(reports_no_signature_when_absent) },
      { CASE(a_mid_stream_query_terminal_error_is_reported_with_the_request_context) },
    },
  };
}

} // namespace couchbase::test
