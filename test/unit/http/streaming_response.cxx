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
#include "test_helper_streaming.hxx"

#include "core/free_form_http_request.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
namespace utils = ::test::utils;

void
a_closed_body_surfaces_the_terminal_error_rather_than_cached_bytes([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // 10 bytes handed out 4 at a time, so the buffer parsed alongside the response headers is not
  // drained by a single pull -- bytes remain cached when the body is closed below.
  auto body = utils::make_chunked_response_body(io, "abcdefghij", 4);

  std::string first;
  bool first_has_more = false;
  std::error_code first_ec{};
  body.next([&](std::string data, bool has_more, std::error_code ec) {
    first = std::move(data);
    first_has_more = has_more;
    first_ec = ec;
  });
  assert_eq(first, "abcd", "the first cached chunk is delivered normally");
  assert_true(first_has_more, "the stream still has data to give");
  assert_success(first_ec, "a pull before the close reports no error");

  // cancel() closes the body while "efghij" is still buffered.
  body.cancel();

  std::string second{ "sentinel" };
  bool second_has_more = true;
  std::error_code second_ec{};
  body.next([&](std::string data, bool has_more, std::error_code ec) {
    second = std::move(data);
    second_has_more = has_more;
    second_ec = ec;
  });
  assert_true(second.empty(), "no residual body bytes leak out of a closed stream");
  assert_false(second_has_more, "a closed stream reports no more data");
  assert_error(
    second_ec, errc::common::request_canceled, "the recorded terminal error is surfaced");
}

void
closing_a_body_twice_keeps_the_first_terminal_error([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_cached_response_body(io, "abcdefghij");

  // cancel() closes the body; the second close must be a no-op rather than a second teardown.
  body.cancel();
  body.cancel();

  std::string data{ "sentinel" };
  bool has_more = true;
  std::error_code ec{};
  body.next([&](std::string chunk, bool more, std::error_code chunk_ec) {
    data = std::move(chunk);
    has_more = more;
    ec = chunk_ec;
  });
  assert_true(data.empty(), "no body bytes are delivered after the close");
  assert_false(has_more, "a closed stream reports no more data");
  assert_error(ec, errc::common::request_canceled, "the first close's terminal error survives");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      // The body is served from memory, so nothing here waits on a socket.
      { CASE(a_closed_body_surfaces_the_terminal_error_rather_than_cached_bytes),
        {},
        timeout::instant },
      { CASE(closing_a_body_twice_keeps_the_first_terminal_error), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
