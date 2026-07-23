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

#include "core/utils/movable_function.hxx"
#include "http_session.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core::io
{
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
    , cached_data_{ std::move(cached_data) }
    , deadline_{ io }
    , reading_complete_{ reading_complete }
    , cached_chunk_size_{ cached_chunk_size }
  {
  }

  void close(std::error_code ec)
  {
    // session_ and final_ec_ are written here and also mutated on the session's read completion.
    // Those paths can run on different executors once a consumer drives the io_context with more
    // than one thread: close() is reached off-strand from row_streamer::cancel()'s asio::post and
    // from the idle-timer completion, while the read completion runs on the session strand. Guard
    // the shared teardown state with a mutex, and make close() idempotent so a second teardown (a
    // cancel racing a transport error, say) neither double-stops the session nor overwrites the
    // first terminal error with a later, less meaningful one.
    std::shared_ptr<http_session> to_stop;
    {
      const std::scoped_lock lock{ mutex_ };
      if (closed_) {
        return;
      }
      closed_ = true;
      // Hand the session out to be stopped below, then drop our reference. Written as two
      // statements rather than std::exchange because cppcheck's flow analysis mis-models the
      // std::exchange return value and wrongly reports the `if (to_stop)` guard as always-false.
      to_stop = session_;
      session_ = nullptr;
      final_ec_ = ec;
      // close() is only reached on error/cancel (a clean end sets reading_complete_ instead), so
      // any bytes still buffered from the initial parse belong to an aborted response and must not
      // be handed out as data — drop them so next() surfaces the terminal error, not stale body
      // bytes.
      cached_data_.clear();
    }
    // stop() posts its own work to the session; call it outside the lock so teardown never
    // re-enters this body while the mutex is held.
    if (to_stop) {
      to_stop->stop();
    }
  }

  void next(utils::movable_function<void(std::string, bool, std::error_code)>&& callback)
  {
    // Decide what to do under the lock (the state it reads is mutated by close() and by the read
    // completion below), then invoke the callback / start the read outside the lock so neither can
    // re-enter next() while the mutex is held.
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
      } else if (session_) {
        // A read is needed. session_ is non-null here: it is cleared only alongside closed_
        // (handled above) or reading_complete_ (the clean-end case below), so a live session means
        // the body is still streaming.
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
        // it. close() also records ec in final_ec_ so a later next() (or an upper layer reading
        // final_ec_) reports the real failure rather than a clean end-of-stream.
        self->close(ec);
      } else if (!has_more) {
        // Clean end-of-stream. http_session::read_some has already handed the session back to the
        // keep-alive pool via its stream-end handler (http_session_manager::check_in), which stops
        // the connection only when it is not reusable (Connection: close, node gone, etc.). Calling
        // stop() here would evict an otherwise-reusable connection and defeat keep-alive, adding
        // avoidable connection churn, so just release our reference and mark the body drained;
        // subsequent next() calls report end-of-stream via reading_complete_.
        const std::scoped_lock lock{ self->mutex_ };
        self->reading_complete_ = true;
        self->session_ = nullptr;
      }
      cb(std::move(data), has_more, ec);
    });
  }

  void set_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp)
  {
    deadline_.expires_at(deadline_tp);
    deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->close(errc::common::ambiguous_timeout);
    });
  }

private:
  std::mutex mutex_{};
  // Guarded by mutex_: written from the read completion (session strand) and from close()
  // (off-strand). cached_data_/cached_chunk_size_ are logically single-consumer but are read under
  // the same lock for uniformity.
  std::shared_ptr<http_session> session_;
  std::string cached_data_;
  std::error_code final_ec_;
  asio::steady_timer deadline_;
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

void
http_streaming_response_body::set_deadline(
  std::chrono::time_point<std::chrono::steady_clock> deadline_tp)
{
  impl_->set_deadline(deadline_tp);
}

class http_streaming_response_impl
{
public:
  http_streaming_response_impl(std::uint32_t status_code,
                               std::string status_message,
                               std::map<std::string, std::string> headers,
                               http_streaming_response_body body)
    : status_code_{ status_code }
    , status_message_{ std::move(status_message) }
    , headers_{ std::move(headers) }
    , body_{ std::move(body) }
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

private:
  std::uint32_t status_code_;
  std::string status_message_;
  std::map<std::string, std::string> headers_;
  http_streaming_response_body body_;
};

http_streaming_response::http_streaming_response(
  asio::io_context& io,
  const couchbase::core::io::http_streaming_parser& parser,
  std::shared_ptr<http_session> session)
  : impl_{ std::make_shared<http_streaming_response_impl>(
      parser.status_code,
      parser.status_message,
      parser.headers,
      http_streaming_response_body{ io, std::move(session), parser.body_chunk, parser.complete }) }
{
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
} // namespace couchbase::core::io
