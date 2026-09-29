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

#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

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
a_second_cancel_leaves_the_body_closed([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto body = utils::make_cached_response_body(io, "abcdefghij");

  // cancel() closes the body; the second close must be a no-op rather than a second teardown.
  // That the *first* terminal error survives a later one is not observable from here: cancel() is
  // the only close the body exposes and it always reports request_canceled, and the in-memory
  // fault seam treats a delivered terminal error as an ordinary end rather than a close. So the
  // rule held here is the narrower one -- a repeated cancel changes nothing.
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
  assert_error(ec, errc::common::request_canceled, "the terminal error is still reported");
}

struct consumer_failure : std::exception {
};

// Regression: the stream-end handler, which checks the connection back into the pool, is skipped
// when the final body callback throws, leaving the session checked out.
void
a_throwing_final_body_callback_still_ends_the_stream([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  // Declared ahead of `stream`: its destructor stops the session, which runs the handlers still
  // installed there.
  auto body = std::make_shared<std::optional<couchbase::core::http_response_body>>();
  bool stream_end_ran = false;

  utils::loopback_stream stream{ io,
                                 R"({"requestID":"r1","signature":{},"results":[{"n":0})",
                                 R"(],"status":"success"})" };
  stream.connect(
    [&](couchbase::core::http_response_body received) {
      *body = std::move(received);
      // Drains the prefix buffered by the header parse; the next pull is the socket read the
      // tail completes.
      body->value().next([&](std::string, bool, std::error_code) {
        stream.send_tail();
        body->value().next([](std::string, bool has_more, std::error_code) {
          if (!has_more) {
            throw consumer_failure{};
          }
        });
      });
    },
    [&stream_end_ran]() {
      stream_end_ran = true;
    });

  const auto bound = utils::deadline_in(scaled_budget(std::chrono::seconds(2)));
  bool thrown = false;
  try {
    io.run_until(bound);
  } catch (const consumer_failure&) {
    thrown = true;
  }
  // Drains whatever the unwound handler left queued.
  io.restart();
  io.run_until(bound);

  assert_true(thrown, "the body callback's exception propagates out of io_context::run");
  assert_true(stream_end_ran, "the stream-end handler runs although the body callback threw");
}

struct pull_result {
  bool completed{ false };
  std::error_code ec{};
};

// Calls `run`, which runs `io`, and reports whether it threw consumer_failure. Either way `io` is
// restarted, so it can be run again.
template<typename Run>
auto
throws_consumer_failure(asio::io_context& io, Run&& run) -> bool
{
  bool thrown = false;
  try {
    std::forward<Run>(run)();
  } catch (const consumer_failure&) {
    thrown = true;
  }
  io.restart();
  return thrown;
}

// Regression: a read callback that throws on a stopped session leaves the read in flight, so every
// later read_some() queues behind it and never completes.
void
a_throwing_read_callback_on_a_stopped_session_releases_the_read([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  utils::loopback_stream stream{ io };
  stream.connect([](couchbase::core::http_response_body) {
  });
  const auto session = stream.session();
  session->stop();

  session->read_some([](std::string, bool, std::error_code) {
    throw consumer_failure{};
  });
  const bool thrown = throws_consumer_failure(io, [&io]() {
    io.poll();
  });

  pull_result later{};
  session->read_some([&later](std::string, bool, std::error_code ec) {
    later = { true, ec };
  });
  io.poll();

  assert_true(thrown, "the read callback's exception propagates out of io_context::run");
  assert_true(later.completed, "a read issued after the throw completes");
  assert_error(later.ec, errc::common::request_canceled, "the stopped session cancels the read");
}

// Regression: a queued read callback that throws while the queue drains strands the callbacks
// queued behind it, and leaves the read in flight for every later read_some().
void
a_throwing_queued_read_callback_strands_no_later_read([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  utils::loopback_stream stream{ io };
  stream.connect([](couchbase::core::http_response_body) {
  });
  const auto session = stream.session();
  session->stop();

  pull_result behind{};
  session->read_some([&session, &behind](std::string, bool, std::error_code) {
    // Issued on the strand while this read is in flight, so both queue behind it.
    session->read_some([](std::string, bool, std::error_code) {
      throw consumer_failure{};
    });
    session->read_some([&behind](std::string, bool, std::error_code ec) {
      behind = { true, ec };
    });
  });
  const bool thrown = throws_consumer_failure(io, [&io]() {
    io.poll();
  });
  io.poll();

  pull_result later{};
  session->read_some([&later](std::string, bool, std::error_code ec) {
    later = { true, ec };
  });
  io.poll();

  assert_true(thrown, "the queued callback's exception propagates out of io_context::run");
  assert_true(behind.completed, "the callback queued behind the throwing one completes");
  assert_error(behind.ec, errc::common::request_canceled, "it completes as the queue drains");
  assert_true(later.completed, "a read issued after the drain completes");
  assert_error(later.ec, errc::common::request_canceled, "the stopped session cancels the read");
}

// Regression: a streaming response handler that throws skips reinstalling the response context, so
// the rest of the body is fed to an empty parser and the stream-end handler never runs.
void
a_throwing_streaming_response_handler_still_ends_the_stream([[maybe_unused]] context& ctx)
{
  const std::string prefix = R"({"requestID":"r1","signature":{},"results":[{"n":0})";
  const std::string tail = R"(],"status":"success"})";
  asio::io_context io;
  // Declared ahead of `stream`: its destructor stops the session, which runs the handlers still
  // installed there.
  auto body = std::make_shared<std::optional<couchbase::core::http_response_body>>();
  bool stream_end_ran = false;

  utils::loopback_stream stream{ io, prefix, tail };
  stream.connect(
    [&](couchbase::core::http_response_body received) {
      *body = std::move(received);
      throw consumer_failure{};
    },
    [&stream_end_ran]() {
      stream_end_ran = true;
    });
  const auto bound = utils::deadline_in(scaled_budget(std::chrono::seconds(2)));
  const bool thrown = throws_consumer_failure(io, [&io, bound]() {
    io.run_until(bound);
  });
  assert_true(thrown, "the response handler's exception propagates out of io_context::run");
  assert_true(body->has_value(), "the response handler received the body");

  std::string received;
  pull_result last{};
  std::function<void(std::string, bool, std::error_code)> pull;
  pull = [&](std::string data, bool has_more, std::error_code ec) {
    received += data;
    if (has_more && !ec) {
      return body->value().next([&pull](std::string d, bool m, std::error_code e) {
        pull(std::move(d), m, e);
      });
    }
    last = { true, ec };
  };
  stream.send_tail();
  body->value().next([&pull](std::string d, bool m, std::error_code e) {
    pull(std::move(d), m, e);
  });
  io.run_until(bound);

  assert_true(last.completed, "the body is read to its end");
  assert_success(last.ec, "the rest of the body parses after the throw");
  assert_eq(received, prefix + tail, "every byte of the body is delivered");
  assert_true(stream_end_ran, "the stream-end handler runs although the response handler threw");
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
      { CASE(a_second_cancel_leaves_the_body_closed), {}, timeout::instant },
      // A loopback connect and one response, bounded at two seconds inside the case.
      { CASE(a_throwing_final_body_callback_still_ends_the_stream), {}, timeout::network },
      { CASE(a_throwing_read_callback_on_a_stopped_session_releases_the_read),
        {},
        timeout::network },
      { CASE(a_throwing_queued_read_callback_strands_no_later_read), {}, timeout::network },
      { CASE(a_throwing_streaming_response_handler_still_ends_the_stream), {}, timeout::network },
    },
  };
}

} // namespace couchbase::test
