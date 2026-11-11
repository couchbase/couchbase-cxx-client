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

#include "core/error_context/http.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_error.hxx"
#include "core/io/http_message.hxx"
#include "core/io/http_parser.hxx"
#include "core/io/http_streaming_parser.hxx"
#include "core/io/http_streaming_response.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace couchbase::test
{
namespace
{
struct stub_response {
  couchbase::core::error_context::http ctx{};
};

// make_response throws `first` on the first call and, when `throws_again` is set, on every call
// after it.
template<typename Exception>
struct stub_request {
  using response_type = stub_response;

  bool throws_again{ false };
  int* calls{ nullptr };

  [[nodiscard]] auto make_response(couchbase::core::error_context::http&& ctx,
                                   const couchbase::core::io::http_response& /* encoded */) const
    -> stub_response
  {
    if (++*calls == 1 || throws_again) {
      throw Exception{};
    }
    return stub_response{ std::move(ctx) };
  }
};

struct decode_error : std::runtime_error {
  decode_error()
    : std::runtime_error{ "not decodable" }
  {
  }
};

template<typename Request>
auto
complete(const Request& request, std::uint32_t status) -> stub_response
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append("not json");
  return couchbase::core::io::complete_http_response(
    request,
    [] {
      return couchbase::core::error_context::http{};
    },
    encoded);
}

void
a_retry_request_propagates_to_the_caller([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<couchbase::core::priv::retry_http_request> request{ false, &calls };
  assert_throws<couchbase::core::priv::retry_http_request>(
    [&] {
      static_cast<void>(complete(request, 200));
    },
    "the command retries the request, so the exception is not swallowed");
  assert_eq(calls, 1, "make_response is not called again");
}

void
an_undecodable_response_other_than_400_is_a_parsing_failure([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<decode_error> request{ false, &calls };
  assert_eq(complete(request, 500).ctx.ec,
            couchbase::errc::common::parsing_failure,
            "only a 400 is the caller's error");
  assert_eq(calls, 2, "the response is built again from the error context");
}

void
a_second_throw_completes_from_the_error_context([[maybe_unused]] context& ctx)
{
  int calls = 0;
  const stub_request<decode_error> request{ true, &calls };
  assert_eq(complete(request, 400).ctx.ec,
            couchbase::errc::common::invalid_argument,
            "a 400 still completes, with invalid_argument");
  calls = 0;
  assert_eq(complete(request, 500).ctx.ec,
            couchbase::errc::common::parsing_failure,
            "any other status still completes, with parsing_failure");
}

void
a_reset_http_parser_parses_a_subsequent_response([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_parser parser{};
  constexpr std::string_view first = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
  auto res1 = parser.feed(first.data(), first.size());
  assert_false(res1.failure, "first response parses without error");
  assert_true(res1.complete, "first response completes");
  assert_eq(parser.response.status_code, std::uint32_t{ 200 }, "first status code");
  assert_eq(parser.response.body.data(), std::string{ "ok" }, "first body");

  parser.reset();

  constexpr std::string_view second = "HTTP/1.1 201 Created\r\nContent-Length: 4\r\n\r\ndone";
  auto res2 = parser.feed(second.data(), second.size());
  assert_false(res2.failure, "second response parses after reset");
  assert_true(res2.complete, "second response completes after reset");
  assert_eq(parser.response.status_code, std::uint32_t{ 201 }, "second status code");
  assert_eq(parser.response.body.data(), std::string{ "done" }, "second body");
}

void
a_reset_http_streaming_parser_parses_a_subsequent_response([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_streaming_parser parser{};
  constexpr std::string_view first = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
  auto res1 = parser.feed(first.data(), first.size());
  assert_false(res1.failure, "first streaming response parses without error");
  assert_true(res1.complete, "first streaming response completes");
  assert_eq(parser.status_code, std::uint32_t{ 200 }, "first status code");
  assert_eq(parser.body_chunk, std::string{ "ok" }, "first body chunk");

  parser.reset();

  constexpr std::string_view second = "HTTP/1.1 202 Accepted\r\nContent-Length: 4\r\n\r\nnext";
  auto res2 = parser.feed(second.data(), second.size());
  assert_false(res2.failure, "second streaming response parses after reset");
  assert_true(res2.complete, "second streaming response completes after reset");
  assert_eq(parser.status_code, std::uint32_t{ 202 }, "second status code");
  assert_eq(parser.body_chunk, std::string{ "next" }, "second body chunk");
}

void
a_move_constructed_http_streaming_parser_retains_its_parser_state([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_streaming_parser source{};
  constexpr std::string_view head = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
  auto res1 = source.feed(head.data(), head.size());
  assert_false(res1.failure, "header prefix parses");
  assert_false(res1.complete, "response is still in progress");

  couchbase::core::io::http_streaming_parser moved{ std::move(source) };
  constexpr std::string_view tail = "\r\nhello";
  auto res2 = moved.feed(tail.data(), tail.size());
  assert_false(res2.failure, "moved-to parser finishes parsing");
  assert_true(res2.complete, "moved-to parser completes response");
  assert_eq(moved.status_code, std::uint32_t{ 200 }, "status code preserved");
  assert_eq(moved.body_chunk, std::string{ "hello" }, "body delivered to moved-to parser");
}

void
a_move_assigned_http_streaming_parser_adopts_the_source_parser_state([[maybe_unused]] context& ctx)
{
  couchbase::core::io::http_streaming_parser source{};
  constexpr std::string_view head = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n";
  auto res1 = source.feed(head.data(), head.size());
  assert_false(res1.failure, "source header prefix parses");

  couchbase::core::io::http_streaming_parser target{};
  target = std::move(source);

  constexpr std::string_view tail = "\r\nworld";
  auto res2 = target.feed(tail.data(), tail.size());
  assert_false(res2.failure, "move-assigned parser finishes parsing");
  assert_true(res2.complete, "move-assigned parser completes response");
  assert_eq(target.status_code, std::uint32_t{ 200 }, "status code preserved");
  assert_eq(target.body_chunk, std::string{ "world" }, "body delivered to move-assigned parser");
}

void
moved_from_parsers_can_be_reset_and_reused([[maybe_unused]] context& ctx)
{
  // 1. Move-construct and move-assign http_parser mid-stream, then reset moved-from instance.
  {
    couchbase::core::io::http_parser src{};
    constexpr std::string_view head = "HTTP/1.1 200 OK\r\nX-First: 1\r\nContent-Length: 4\r\n";
    assert_false(src.feed(head.data(), head.size()).failure, "src header prefix parses");

    couchbase::core::io::http_parser moved_ctor{ std::move(src) };
    assert_false(src.complete, "moved-from http_parser resets complete flag");
#if defined(__clang__) && defined(__clang_analyzer__)
    [[clang::suppress]]
#endif
    assert_true(src.feed("HTTP/1.1 200 OK\r\n\r\n", 19).failure,
                "feeding moved-from http_parser before reset reports failure without crashing");

    src.reset();
    constexpr std::string_view full = "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nhi";
    auto res_reused = src.feed(full.data(), full.size());
    assert_false(res_reused.failure, "moved-from http_parser parses after reset");
    assert_true(res_reused.complete, "moved-from http_parser completes after reset");
    assert_eq(src.response.status_code, std::uint32_t{ 201 }, "reused http_parser status code");

    couchbase::core::io::http_parser moved_assign{};
    moved_assign = std::move(moved_ctor);
    constexpr std::string_view tail = "\r\ntest";
    auto res_tail = moved_assign.feed(tail.data(), tail.size());
    assert_false(res_tail.failure, "move-assigned http_parser finishes parsing");
    assert_true(res_tail.complete, "move-assigned http_parser completes");
    assert_eq(moved_assign.response.body.data(), std::string{ "test" }, "move-assigned body");
  }

  // 2. Reset and reuse moved-from http_streaming_parser.
  {
    couchbase::core::io::http_streaming_parser src{};
    couchbase::core::io::http_streaming_parser dst{ std::move(src) };
    constexpr std::string_view empty_ok = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    assert_true(dst.feed(empty_ok.data(), empty_ok.size()).complete,
                "move-constructed streaming parser completes");
#if defined(__clang__) && defined(__clang_analyzer__)
    [[clang::suppress]]
#endif
    assert_true(src.feed(empty_ok.data(), empty_ok.size()).failure,
                "feeding moved-from streaming parser before reset reports failure");
    src.reset();
    constexpr std::string_view full = "HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\nok";
    auto res = src.feed(full.data(), full.size());
    assert_false(res.failure, "moved-from streaming parser parses after reset");
    assert_true(res.complete, "moved-from streaming parser completes after reset");
    assert_eq(src.status_code, std::uint32_t{ 202 }, "reused streaming parser status code");
  }
}

void
responses_with_empty_reason_phrase_and_split_headers_populate_status_and_headers(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view chunk1 = "HTTP/1.1 204 \r\nX-Custom-Hea";
  constexpr std::string_view chunk2 = "der: val-";
  constexpr std::string_view chunk3 = "part2\r\n"
                                      "X-Custom-Header: second\r\n"
                                      "Set-Cookie: a=1; Path=/\r\n"
                                      "Set-Cookie: b=2; Pa";
  constexpr std::string_view chunk4 = "th=/\r\nContent-Length: 0\r\n\r\n";

  // 1. http_parser
  {
    couchbase::core::io::http_parser parser{};
    assert_false(parser.feed(chunk1.data(), chunk1.size()).failure, "chunk1");
    assert_false(parser.feed(chunk2.data(), chunk2.size()).failure, "chunk2");
    assert_false(parser.feed(chunk3.data(), chunk3.size()).failure, "chunk3");
    auto res = parser.feed(chunk4.data(), chunk4.size());
    assert_false(res.failure, "chunk4");
    assert_true(res.complete, "complete");
    assert_eq(parser.response.status_code,
              std::uint32_t{ 204 },
              "status_code populated even when reason phrase is empty");
    assert_eq(parser.response.headers["x-custom-header"],
              std::string{ "val-part2, second" },
              "split header name and value accumulated and duplicate headers comma-joined");
    assert_eq(parser.response.headers["set-cookie"],
              std::string{ "b=2; Path=/" },
              "repeated Set-Cookie retains last value while accumulating split chunks");

    parser.reset();
    assert_true(parser.response.headers.empty(), "reset clears headers");
  }

  // 2. http_streaming_parser
  {
    couchbase::core::io::http_streaming_parser parser{};
    assert_false(parser.feed(chunk1.data(), chunk1.size()).failure, "streaming chunk1");
    assert_false(parser.feed(chunk2.data(), chunk2.size()).failure, "streaming chunk2");
    assert_false(parser.feed(chunk3.data(), chunk3.size()).failure, "streaming chunk3");
    auto res = parser.feed(chunk4.data(), chunk4.size());
    assert_false(res.failure, "streaming chunk4");
    assert_true(res.complete, "streaming complete");
    assert_eq(parser.status_code,
              std::uint32_t{ 204 },
              "streaming status_code populated even when reason phrase is empty");
    assert_eq(
      parser.headers["x-custom-header"],
      std::string{ "val-part2, second" },
      "streaming split header name and value accumulated and duplicate headers comma-joined");
    assert_eq(parser.headers["set-cookie"],
              std::string{ "b=2; Path=/" },
              "streaming repeated Set-Cookie retains last value while accumulating split chunks");

    parser.reset();
    assert_true(parser.headers.empty(), "streaming reset clears headers");
  }
}

void
an_interim_100_continue_followed_by_a_final_response_replaces_the_reason_phrase(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view interim = "HTTP/1.1 100 Continue\r\n\r\n";
  constexpr std::string_view final_prefix = "HTTP/1.1 201 Cr";
  constexpr std::string_view part1 = "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Cr";
  constexpr std::string_view part2 = "eated\r\nContent-Length: 2\r\n\r\nhi";

  // 1. http_parser (feed boundary right after 100 Continue, and combined feed)
  {
    couchbase::core::io::http_parser parser{};
    const auto res0 = parser.feed(interim.data(), interim.size());
    assert_false(res0.failure, "http_parser parses standalone interim response");
    assert_false(res0.complete, "interim 100 Continue must not mark response complete");
    const auto res1 = parser.feed(final_prefix.data(), final_prefix.size());
    assert_false(res1.failure, "http_parser parses split final status prefix");
    assert_false(res1.complete, "final response is still incomplete after prefix feed");
    const auto res2 = parser.feed(part2.data(), part2.size());
    assert_false(res2.failure, "http_parser parses split final status suffix and body");
    assert_true(res2.complete, "http_parser completes final response");
    assert_eq(parser.response.status_code, std::uint32_t{ 201 }, "final status code");
    assert_eq(
      parser.response.status_message, std::string{ "Created" }, "split final reason phrase");
    assert_eq(parser.response.body.data(), std::string{ "hi" }, "final body");
  }
  {
    couchbase::core::io::http_parser parser{};
    const auto res1 = parser.feed(part1.data(), part1.size());
    assert_false(res1.failure, "http_parser parses interim response and split final status prefix");
    assert_false(res1.complete, "final response is still incomplete after first feed");
    auto res = parser.feed(part2.data(), part2.size());
    assert_false(res.failure, "http_parser parses split final status suffix and body");
    assert_true(res.complete, "http_parser completes final response");
    assert_eq(parser.response.status_code, std::uint32_t{ 201 }, "final status code");
    assert_eq(
      parser.response.status_message, std::string{ "Created" }, "split final reason phrase");
    assert_eq(parser.response.body.data(), std::string{ "hi" }, "final body");
  }

  // 2. http_streaming_parser (feed boundary right after 100 Continue, and combined feed)
  {
    couchbase::core::io::http_streaming_parser parser{};
    const auto res0 = parser.feed(interim.data(), interim.size());
    assert_false(res0.failure, "http_streaming_parser parses standalone interim response");
    assert_false(res0.complete, "streaming interim 100 Continue must not mark response complete");
    assert_false(res0.headers_complete,
                 "streaming interim 100 Continue must not mark headers complete");
    const auto res1 = parser.feed(final_prefix.data(), final_prefix.size());
    assert_false(res1.failure, "http_streaming_parser parses split final status prefix");
    assert_false(res1.complete, "streaming final response is still incomplete after prefix feed");
    assert_false(res1.headers_complete,
                 "streaming final headers are still incomplete after prefix feed");
    const auto res2 = parser.feed(part2.data(), part2.size());
    assert_false(res2.failure, "http_streaming_parser parses split final status suffix and body");
    assert_true(res2.complete, "http_streaming_parser completes final response");
    assert_true(res2.headers_complete, "http_streaming_parser completes final headers");
    assert_eq(parser.status_code, std::uint32_t{ 201 }, "streaming final status code");
    assert_eq(
      parser.status_message, std::string{ "Created" }, "streaming split final reason phrase");
    assert_eq(parser.body_chunk, std::string{ "hi" }, "streaming final body");
  }
  {
    couchbase::core::io::http_streaming_parser parser{};
    const auto res1 = parser.feed(part1.data(), part1.size());
    assert_false(res1.failure,
                 "http_streaming_parser parses interim response and split final status prefix");
    assert_false(res1.complete, "streaming final response is still incomplete after first feed");
    assert_false(res1.headers_complete,
                 "streaming final headers are still incomplete after first feed");
    auto res = parser.feed(part2.data(), part2.size());
    assert_false(res.failure, "http_streaming_parser parses split final status suffix and body");
    assert_true(res.complete, "http_streaming_parser completes final response");
    assert_eq(parser.status_code, std::uint32_t{ 201 }, "streaming final status code");
    assert_eq(
      parser.status_message, std::string{ "Created" }, "streaming split final reason phrase");
    assert_eq(parser.body_chunk, std::string{ "hi" }, "streaming final body");
  }
}

void
repeated_and_compound_connection_close_headers_mark_the_connection_for_closing(
  [[maybe_unused]] context& ctx)
{
  constexpr std::string_view repeated_close = "HTTP/1.1 200 OK\r\n"
                                              "Connection: close\r\n"
                                              "Connection: Close\r\n"
                                              "Content-Length: 0\r\n\r\n";
  constexpr std::string_view compound_close = "HTTP/1.1 200 OK\r\n"
                                              "Connection: keep-alive, Close\r\n"
                                              "Content-Length: 0\r\n\r\n";
  constexpr std::string_view keep_alive_only = "HTTP/1.1 200 OK\r\n"
                                               "Connection: keep-alive, close-later\r\n"
                                               "Content-Length: 0\r\n\r\n";

  // 1. http_parser + http_response::must_close_connection()
  {
    couchbase::core::io::http_parser parser{};
    assert_true(parser.feed(repeated_close.data(), repeated_close.size()).complete,
                "repeated Connection: close parses");
    assert_eq(parser.response.headers["connection"],
              std::string{ "close, Close" },
              "duplicate Connection headers are comma-joined");
    assert_true(parser.response.must_close_connection(),
                "repeated Connection: close requires closing the connection");

    parser.reset();
    assert_true(parser.feed(compound_close.data(), compound_close.size()).complete,
                "compound Connection: keep-alive, Close parses");
    assert_true(parser.response.must_close_connection(),
                "compound Connection header containing Close token requires closing");

    parser.reset();
    assert_true(parser.feed(keep_alive_only.data(), keep_alive_only.size()).complete,
                "Connection: keep-alive, close-later parses");
    assert_false(parser.response.must_close_connection(),
                 "non-close tokens do not mark the connection for closing");
  }

  // 2. http_streaming_parser + http_streaming_response::must_close_connection()
  {
    asio::io_context io{};
    couchbase::core::io::http_streaming_parser parser{};
    assert_true(parser.feed(repeated_close.data(), repeated_close.size()).complete,
                "streaming repeated Connection: close parses");
    assert_eq(parser.headers["connection"],
              std::string{ "close, Close" },
              "streaming duplicate Connection headers are comma-joined");
    const couchbase::core::io::http_streaming_response resp1{ io, parser, nullptr };
    assert_true(resp1.must_close_connection(),
                "streaming repeated Connection: close requires closing the connection");

    parser.reset();
    assert_true(parser.feed(compound_close.data(), compound_close.size()).complete,
                "streaming compound Connection: keep-alive, Close parses");
    const couchbase::core::io::http_streaming_response resp2{ io, parser, nullptr };
    assert_true(resp2.must_close_connection(),
                "streaming compound Connection header containing Close token requires closing");

    parser.reset();
    assert_true(parser.feed(keep_alive_only.data(), keep_alive_only.size()).complete,
                "streaming Connection: keep-alive, close-later parses");
    const couchbase::core::io::http_streaming_response resp3{ io, parser, nullptr };
    assert_false(resp3.must_close_connection(),
                 "streaming non-close tokens do not mark the connection for closing");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_retry_request_propagates_to_the_caller) },
      { CASE(an_undecodable_response_other_than_400_is_a_parsing_failure) },
      { CASE(a_second_throw_completes_from_the_error_context) },
      { CASE(a_reset_http_parser_parses_a_subsequent_response) },
      { CASE(a_reset_http_streaming_parser_parses_a_subsequent_response) },
      { CASE(a_move_constructed_http_streaming_parser_retains_its_parser_state) },
      { CASE(a_move_assigned_http_streaming_parser_adopts_the_source_parser_state) },
      { CASE(moved_from_parsers_can_be_reset_and_reused) },
      { CASE(responses_with_empty_reason_phrase_and_split_headers_populate_status_and_headers) },
      { CASE(an_interim_100_continue_followed_by_a_final_response_replaces_the_reason_phrase) },
      { CASE(repeated_and_compound_connection_close_headers_mark_the_connection_for_closing) },
    },
  };
}

} // namespace couchbase::test
