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

#include "core/free_form_http_request.hxx"
#include "test_helper_streaming.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <system_error>

TEST_CASE("unit: streaming body surfaces the terminal error after close, not cached data", "[unit]")
{
  asio::io_context io;
  // 10 bytes handed out 4 at a time, so the buffer parsed alongside the response headers is not
  // drained by a single pull — bytes remain cached when the body is closed below.
  auto body = test::utils::make_chunked_response_body(io, "abcdefghij", 4);

  std::string d1;
  bool more1 = false;
  std::error_code ec1{};
  body.next([&](std::string d, bool has_more, std::error_code ec) {
    d1 = std::move(d);
    more1 = has_more;
    ec1 = ec;
  });
  REQUIRE(d1 == "abcd"); // first cached chunk delivered normally
  REQUIRE(more1);
  REQUIRE_FALSE(ec1);

  // cancel() closes the body (request_canceled) while "efghij" is still buffered. A subsequent
  // next() must surface the recorded terminal error and stop — not deliver the leftover body bytes
  // (which would make a cancelled stream look like it is still producing data), and not report a
  // clean end-of-stream either.
  body.cancel();

  std::string d2{ "sentinel" };
  bool more2 = true;
  std::error_code ec2{};
  body.next([&](std::string d, bool has_more, std::error_code ec) {
    d2 = std::move(d);
    more2 = has_more;
    ec2 = ec;
  });
  REQUIRE(d2.empty());                                       // no residual body bytes leaked
  REQUIRE_FALSE(more2);                                      // stream is done
  REQUIRE(ec2 == couchbase::errc::common::request_canceled); // terminal error surfaced
}

TEST_CASE("unit: streaming body close is idempotent", "[unit]")
{
  asio::io_context io;
  auto body = test::utils::make_cached_response_body(io, "abcdefghij");

  // Close twice (cancel() closes the body). The second close must be a no-op — not a double
  // teardown — and the terminal error recorded by the first close must be preserved.
  body.cancel();
  body.cancel();

  std::string data{ "sentinel" };
  bool more = true;
  std::error_code ec{};
  body.next([&](std::string d, bool has_more, std::error_code e) {
    data = std::move(d);
    more = has_more;
    ec = e;
  });
  REQUIRE(data.empty());
  REQUIRE_FALSE(more);
  REQUIRE(ec == couchbase::errc::common::request_canceled);
}
