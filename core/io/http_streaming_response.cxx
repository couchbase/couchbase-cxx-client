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

#include "http_streaming_response.hxx"

#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/utils/movable_function.hxx"
#include "http_session.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/post.hpp>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core::io
{
auto
terminal_error_code(deadline_terminal terminal) -> std::error_code
{
  switch (terminal) {
    case deadline_terminal::unambiguous:
      return errc::common::unambiguous_timeout;
    case deadline_terminal::ambiguous:
      break;
  }
  return errc::common::ambiguous_timeout;
}

class http_streaming_response_body_impl
  : public std::enable_shared_from_this<http_streaming_response_body_impl>
{
public:
  http_streaming_response_body_impl(asio::io_context& io,
                                    std::shared_ptr<http_session> session,
                                    std::string cached_data,
                                    bool reading_complete,
                                    std::size_t cached_chunk_size)
    : session_{ std::move(session) }
    , endpoint_{ session_ ? session_->hostname() + ":" + session_->port() : std::string{} }
    , cached_data_{ std::move(cached_data) }
    , deadline_{ io }
    , reading_complete_{ reading_complete }
    , cached_chunk_size_{ cached_chunk_size }
  {
    // A response complete before a single read_some is constructed with the session still held.
    // http_session checks that connection in once the response handler returns, and nothing
    // further belongs to this message, so a read from that session would park on a connection
    // serving someone else. Release it here, which also establishes the invariant next(),
    // close_impl() and set_deadline() each assume: reading_complete_ means no live session.
    if (reading_complete_) {
      session_ = nullptr;
    }
  }

  // A pending read holds this body, so it is destroyed only with no pull in flight. An armed
  // deadline does not hold it. A body dropped before its response was fully read leaves its
  // connection mid-response, which otherwise only cluster close ends. Closing the body posts a stop
  // of its session to the session strand.
  ~http_streaming_response_body_impl()
  {
    close_impl(errc::common::request_canceled, std::nullopt);
  }

  void close(std::error_code ec)
  {
    close_impl(ec, std::nullopt);
  }

  // Closes only if `generation` is still current. The check and the terminal transition share one
  // lock, so a re-arm or a clean completion cannot land between them.
  void close_at_deadline(deadline_terminal on_expiry, std::uint64_t generation)
  {
    const auto ec = terminal_error_code(on_expiry);
    if (close_impl(ec, generation)) {
      CB_LOG_DEBUG("streaming response deadline expired, closing the body: terminal={}, "
                   "endpoint=\"{}\"",
                   ec.message(),
                   logger::system_data(endpoint_));
    }
  }

  // Returns whether this call closed the body; false when it was already closed, drained, or the
  // generation was superseded.
  auto close_impl(std::error_code ec, std::optional<std::uint64_t> expect_generation) -> bool
  {
    // session_ and final_ec_ are written here and on the session's read completion, which runs on
    // the session strand. close() is reached off that strand: from row_streamer::cancel()'s post,
    // the idle-timer completion and the deadline completion, and from the destructor on whatever
    // thread drops the body. A mutex guards the shared teardown state. close() is
    // idempotent, so a second teardown (a cancel racing a transport error) neither stops the
    // session twice nor overwrites the first terminal error.
    std::shared_ptr<http_session> to_stop;
    {
      const std::scoped_lock lock{ mutex_ };
      if (expect_generation.has_value() && *expect_generation != deadline_generation_) {
        return false;
      }
      if (closed_ || (reading_complete_ && cached_data_.empty())) {
        // Drained is terminal too, the state set_deadline() also refuses. A clean end leaves
        // closed_ false, so without this a later close() overwrites final_ec_ and a pull after
        // end-of-stream reports request_canceled. Both clean-end paths disarm the timer, so
        // returning here leaves none armed.
        return false;
      }
      closed_ = true;
      // Only stop the session when the response was abandoned mid-body: such a connection is left
      // at an arbitrary offset and cannot be reused. A response that was already fully received
      // (reading_complete_) leaves the connection at a message boundary, and http_session checks
      // it back into the keep-alive pool via its stream-end handler
      // (http_session_manager::check_in) — stopping it here would evict a live pooled connection
      // and make the *next* unrelated request on it fail with request_canceled.
      //
      // reading_complete_ is what separates a connection this body may stop from one the pool
      // owns. http_session::read_some runs the stream-end handler only after invoking this body's
      // completion, so on one thread the flag is set by the time the connection is published.
      // Across threads a clean end can still land between the claim below and the stop that
      // executes it, which is what mark_stopping() covers.
      //
      // Hand the session out to be stopped below, then drop our reference. Written as two
      // statements rather than std::exchange because cppcheck's flow analysis mis-models the
      // std::exchange return value and wrongly reports the `if (to_stop)` guard as always-false.
      //
      // A session already stopped is skipped: the stop() call that set stopped_ runs the whole
      // teardown, and a stop posted after the cluster closed would hold the session until the
      // io_context is destroyed.
      if (!reading_complete_ && session_ && !session_->is_stopped()) {
        to_stop = session_;
        // Claim the stop while this lock is held. The clean-end branch of the read completion
        // clears closed_ and hands the connection to check_in, and it can run between the claim
        // and the stop that executes it; the claim is what check_in tests, so the pool refuses a
        // connection this body is about to stop.
        to_stop->mark_stopping();
      }
      session_ = nullptr;
      final_ec_ = ec;
      // Disarm. cancel() does not stop a completion already queued, hence the generation bump.
      deadline_.cancel();
      ++deadline_generation_;
      // Reached on error, cancel or destruction (a clean end sets reading_complete_ instead), so
      // any bytes still buffered from the initial parse are not handed out as data. Dropping them
      // makes next() surface the terminal error, not stale body bytes.
      cached_data_.clear();
    }
    // stop() runs inline -- it closes the stream, cancels the timers, cancels the current
    // response and invokes on_stop_handler_ -- so it has to run on the session strand, alongside
    // the read and write handlers it tears down. close() is reached off that strand from a
    // deadline expiry, an idle-timer completion, row_streamer::cancel()'s post and the destructor,
    // so the stop is posted there.
    //
    // The session is already claimed above, so the gap between deciding the stop and running it
    // stays invisible to the pool.
    if (to_stop) {
      asio::post(to_stop->get_executor(), [to_stop]() {
        to_stop->stop();
      });
    }
    return true;
  }

  void next(utils::movable_function<void(std::string, bool, std::error_code)>&& callback)
  {
    // Decide what to do under the lock (the state it reads is mutated by close() and by the read
    // completion below), then invoke the callback / start the read outside the lock so neither can
    // re-enter next() while the mutex is held. deadline_ is touched under the lock: an asio timer
    // is not thread-safe.
    std::string data;
    bool has_more = false;
    std::error_code deliver_ec;
    bool deliver_now = false;
    std::shared_ptr<http_session> to_read;
    {
      const std::scoped_lock lock{ mutex_ };
      if (closed_) {
        // The body was closed on error/cancel: surface the recorded terminal error and stop,
        // ahead of the cached-data branch, so a next() after close never delivers residual body
        // bytes (with a falsy ec) that would make a cancelled/failed stream look like it is still
        // producing rows.
        deliver_ec = final_ec_;
        deliver_now = true;
      } else if (!cached_data_.empty()) {
        // Hand back the data buffered during the initial parse. When a cached chunk size is set (a
        // test seam that simulates a socket that dribbles the body out), deliver at most that many
        // bytes per pull; otherwise hand back everything at once. There is more to come while
        // cached data remains or the response was not already fully read (reading_complete_).
        if (cached_chunk_size_ == 0 || cached_data_.size() <= cached_chunk_size_) {
          std::swap(data, cached_data_);
        } else {
          data = cached_data_.substr(0, cached_chunk_size_);
          cached_data_.erase(0, cached_chunk_size_);
        }
        has_more = !reading_complete_ || !cached_data_.empty();
        deliver_now = true;
        if (!has_more) {
          deadline_.cancel();
          ++deadline_generation_;
        }
      } else if (session_) {
        // A read is needed. session_ is non-null here: it is cleared alongside closed_ and
        // alongside reading_complete_, so a live session means the body is still streaming.
        to_read = session_;
      } else {
        // No cached data and no live session, and not closed: the body drained cleanly
        // (reading_complete_), so report end-of-stream with a falsy error.
        deliver_now = true;
      }
    }
    if (deliver_now) {
      callback(std::move(data), has_more, deliver_ec);
      return;
    }
    to_read->read_some([self = shared_from_this(), cb = std::move(callback)](
                         std::string data, bool has_more, std::error_code ec) mutable {
      if (ec) {
        // Error or cancellation: the connection is left mid-response and is not reusable, so stop
        // it. close() records the terminal in final_ec_, which this callback and any later
        // next() both report.
        self->close(ec);
      } else {
        const std::scoped_lock lock{ self->mutex_ };
        if (!has_more) {
          // Clean end-of-stream. http_session::read_some hands the session back to the keep-alive
          // pool via its stream-end handler (http_session_manager::check_in) once this completion
          // returns, and that handler stops the connection only when it is not reusable
          // (Connection: close, node gone, etc.). Calling stop() here would evict an
          // otherwise-reusable connection and defeat
          // keep-alive, adding avoidable connection churn, so just release our reference and mark
          // the body drained; subsequent next() calls report end-of-stream via reading_complete_.
          //
          // Tested ahead of closed_, so a response whose last read succeeded is delivered even
          // when a terminal was recorded while that read was in flight. This is the rule
          // close_impl already applies, refusing a close once the body is drained; the read
          // completion is where drained is established, so the two orders disagree only over the
          // instant that establishes it. The bytes are the whole remainder of a response the
          // server completed, and reporting a timeout for one held in hand tells a retry layer
          // the request may not have applied when it did.
          self->reading_complete_ = true;
          self->session_ = nullptr;
          self->deadline_.cancel();
          ++self->deadline_generation_;
          // Drop the terminal a close recorded while this read was in flight. Delivering the read
          // as a clean end and leaving that terminal in place would contradict it on the next
          // pull, which tests closed_ first and would report the timeout the body has just
          // decided against. The body is drained either way -- reading_complete_ with no cached
          // bytes is the state close_impl and set_deadline already refuse -- so clearing this
          // reopens nothing.
          self->closed_ = false;
          self->final_ec_ = {};
        } else if (self->closed_) {
          // Closed while a read that has more to come was completing. Report the recorded terminal
          // and drop the bytes: they are a fragment of a response that has already ended, and
          // handing them over with a falsy error_code would report a closed body as still
          // producing data. row_streamer::cancel closes the row channel itself, so it is a
          // deadline expiry that reaches here with a consumer still able to receive. The close
          // has to land between the read completing and this lock, which needs a second thread
          // running the io_context.
          ec = self->final_ec_;
          data.clear();
          has_more = false;
        }
      }
      if (ec) {
        // A failed read reports why the body closed, not what the read returned: an abort reports
        // request_canceled whatever ended the body, and a transport error arriving after a close
        // would otherwise mask the terminal.
        ec = self->terminal_error();
      }
      cb(std::move(data), has_more, ec);
    });
  }

  // Supersedes any armed deadline without ending the body. Taken under the same lock as arming
  // and the terminal transition, so an expiry already queued when the lock is taken is discarded
  // by the generation check rather than delivered. Lets a caller whose own teardown must be
  // posted stop the deadline on the calling thread first, so a cancel cannot lose to an expiry
  // landing in the gap.
  void cancel_deadline()
  {
    const std::scoped_lock lock{ mutex_ };
    deadline_.cancel();
    ++deadline_generation_;
  }

  auto set_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp,
                    deadline_terminal on_expiry) -> deadline_state
  {
    const std::scoped_lock lock{ mutex_ };
    if (closed_ || (reading_complete_ && cached_data_.empty())) {
      // Drained: nothing left to reclaim. reading_complete_ with cached_data_ still buffered is
      // not drained, and does take a deadline.
      return deadline_state::body_already_ended;
    }
    const auto generation = ++deadline_generation_;
    deadline_.expires_at(deadline_tp);
    // Weak: an armed deadline does not keep a dropped body, and through it the session, alive
    // until expiry. The destructor closes a body dropped before its deadline.
    deadline_.async_wait([weak = weak_from_this(), on_expiry, generation](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      if (const auto self = weak.lock(); self) {
        self->close_at_deadline(on_expiry, generation);
      }
    });
    return deadline_state::armed;
  }

  [[nodiscard]] auto terminal_error() -> std::error_code
  {
    const std::scoped_lock lock{ mutex_ };
    return final_ec_;
  }

private:
  std::mutex mutex_{};
  // Guarded by mutex_: written from the read completion (session strand) and from close()
  // (off-strand). cached_data_/cached_chunk_size_ are logically single-consumer but are read under
  // the same lock for uniformity. deadline_ is guarded because an asio timer may not be armed and
  // cancelled concurrently; deadline_generation_ supersedes a queued completion.
  std::shared_ptr<http_session> session_;
  // hostname:port of the session the body was built with, kept for logging after session_ is
  // released.
  std::string endpoint_;
  std::string cached_data_;
  std::error_code final_ec_;
  asio::steady_timer deadline_;
  std::uint64_t deadline_generation_{ 0 };
  bool reading_complete_{ false };
  bool closed_{ false };
  std::size_t cached_chunk_size_{ 0 };
};

http_streaming_response_body::http_streaming_response_body(asio::io_context& io,
                                                           std::shared_ptr<http_session> session,
                                                           std::string cached_data,
                                                           bool reading_complete,
                                                           std::size_t cached_chunk_size)
  : impl_{ std::make_shared<http_streaming_response_body_impl>(io,
                                                               std::move(session),
                                                               std::move(cached_data),
                                                               reading_complete,
                                                               cached_chunk_size) }
{
}

void
http_streaming_response_body::next(
  utils::movable_function<void(std::string, bool, std::error_code)>&& callback)
{
  impl_->next(std::move(callback));
}

void
http_streaming_response_body::close(std::error_code ec)
{
  impl_->close(ec);
}

auto
http_streaming_response_body::set_deadline(
  std::chrono::time_point<std::chrono::steady_clock> deadline_tp,
  deadline_terminal on_expiry) -> deadline_state
{
  return impl_->set_deadline(deadline_tp, on_expiry);
}

void
http_streaming_response_body::cancel_deadline()
{
  return impl_->cancel_deadline();
}

class http_streaming_response_impl
{
public:
  http_streaming_response_impl(std::uint32_t status_code,
                               std::string status_message,
                               std::map<std::string, std::string> headers,
                               http_streaming_response_body body,
                               http_dispatch_info dispatch_info)
    : status_code_{ status_code }
    , status_message_{ std::move(status_message) }
    , headers_{ std::move(headers) }
    , body_{ std::move(body) }
    , dispatch_info_{ std::move(dispatch_info) }
  {
  }

  [[nodiscard]] auto status_code() const -> const std::uint32_t&
  {
    return status_code_;
  }

  [[nodiscard]] auto status_message() const -> const std::string&
  {
    return status_message_;
  }

  [[nodiscard]] auto headers() const -> const std::map<std::string, std::string>&
  {
    return headers_;
  }

  [[nodiscard]] auto body() -> http_streaming_response_body&
  {
    return body_;
  }

  [[nodiscard]] auto must_close_connection() const -> bool
  {
    if (const auto it = headers_.find("connection"); it != headers_.end()) {
      return it->second == "close";
    }
    return false;
  }

  [[nodiscard]] auto dispatch_info() const -> const http_dispatch_info&
  {
    return dispatch_info_;
  }

private:
  std::uint32_t status_code_;
  std::string status_message_;
  std::map<std::string, std::string> headers_;
  http_streaming_response_body body_;
  http_dispatch_info dispatch_info_;
};

http_streaming_response::http_streaming_response(
  asio::io_context& io,
  const couchbase::core::io::http_streaming_parser& parser,
  std::shared_ptr<http_session> session)
  : impl_{ nullptr }
{
  http_dispatch_info dispatch_info{};
  if (session) {
    // Taken from the session's http_context, the same source io::http_command stamps onto a
    // buffered response, so both paths report the identical endpoint. The port is already a
    // std::uint16_t there; http_session::port() is that same value rendered as a string for
    // resolving and for the Host header, so parsing it back would only reintroduce a conversion.
    dispatch_info.hostname = session->http_context().hostname;
    dispatch_info.port = session->http_context().port;
    dispatch_info.remote_address = session->remote_address();
    dispatch_info.local_address = session->local_address();
  }
  impl_ = std::make_shared<http_streaming_response_impl>(
    parser.status_code,
    parser.status_message,
    parser.headers,
    http_streaming_response_body{ io, std::move(session), parser.body_chunk, parser.complete },
    std::move(dispatch_info));
}

auto
http_streaming_response::status_code() const -> const std::uint32_t&
{
  return impl_->status_code();
}

auto
http_streaming_response::status_message() const -> const std::string&
{
  return impl_->status_message();
}

auto
http_streaming_response::headers() const -> const std::map<std::string, std::string>&
{
  return impl_->headers();
}

auto
http_streaming_response::body() -> http_streaming_response_body&
{
  return impl_->body();
}

auto
http_streaming_response::must_close_connection() const -> bool
{
  return impl_->must_close_connection();
}

auto
http_streaming_response::dispatch_info() const -> const http_dispatch_info&
{
  return impl_->dispatch_info();
}
} // namespace couchbase::core::io
