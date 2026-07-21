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

#include "row_streamer.hxx"

#include <couchbase/error_codes.hxx>

#include "free_form_http_request.hxx"
#include "logger/logger.hxx"
#include "utils/json.hxx"
#include "utils/json_stream_control.hxx"
#include "utils/json_streaming_lexer.hxx"
#include "utils/movable_function.hxx"

#include <asio/error.hpp>
#include <asio/experimental/channel_error.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <variant>

namespace couchbase::core
{
struct row_stream_end_signal {
  std::error_code ec{};
  std::string metadata{};
};

class row_streamer_impl : public std::enable_shared_from_this<row_streamer_impl>
{
public:
  row_streamer_impl(asio::io_context& io,
                    http_response_body body,
                    const std::string& pointer_expression,
                    row_streamer_options options)
    : io_{ io }
    , body_{ std::move(body) }
    // rows_ initializes before options_, so read the capacity from the ctor parameter.
    , rows_{ io_, options.row_buffer_size }
    , options_{ options }
    , idle_timer_{ io_ }
    , lexer_{ pointer_expression, options_.lexer_depth, options_.max_row_bytes }
  {
  }

  void start(utils::movable_function<void(std::string, std::error_code)>&& handler)
  {
    metadata_header_handler_ = std::move(handler);
    lexer_.on_metadata_header_complete(
      [weak = weak_from_this()](auto ec, auto meta_header) mutable {
        auto self = weak.lock();
        if (!self) {
          return;
        }
        // Trim any whitespace from the end
        auto last_non_ws = meta_header.find_last_not_of(" \t\f\v\n\r");
        if (last_non_ws != std::string::npos) {
          meta_header.erase(last_non_ws + 1);
        }

        // The metadata header can end with an open `[` (opening bracket for pointer expression
        // array). If that's the case, close the array and the response object
        if (!meta_header.empty() && meta_header[meta_header.length() - 1] == '[') {
          meta_header.append("]}");
        }
        self->deliver_metadata_header(ec, std::move(meta_header));
      });
    lexer_.on_row([weak = weak_from_this()](std::string&& row) -> utils::json::stream_control {
      auto self = weak.lock();
      if (!self) {
        return utils::json::stream_control::stop;
      }
      auto row_len = row.size();
      // Account the row against the back-pressure budget *synchronously*, as it is handed to the
      // channel. A concurrent_channel completes async_send immediately while it has spare capacity
      // and otherwise queues the send until a receiver drains a slot; if we only counted bytes in
      // the completion handler, a full channel would leave buffered_bytes_ pinned low and let
      // maybe_feed_lexer() keep pulling from the socket, piling up unbounded pending sends (each
      // owning a row). Counting here makes the high-water mark observe the true backlog.
      self->buffered_bytes_.fetch_add(row_len, std::memory_order_relaxed);
      self->rows_.async_send({}, std::move(row), [self, row_len](auto ec) {
        if (ec) {
          if (ec != asio::experimental::error::channel_closed &&
              ec != asio::experimental::error::channel_cancelled) {
            CB_LOG_WARNING(
              "unexpected error while sending to row channel: {} ({})", ec.value(), ec.message());
          }
          // The send never reached a receiver (channel closed/cancelled), so release its budget.
          self->release_buffered_bytes(row_len);
        }
      });
      return utils::json::stream_control::next_row;
    });
    lexer_.on_complete([weak = weak_from_this()](
                         std::error_code ec, std::size_t /*number_of_rows*/, std::string&& meta) {
      auto self = weak.lock();
      if (!self) {
        return;
      }
      self->lexer_completed_ = true;
      row_stream_end_signal signal{ ec, std::move(meta) };
      self->rows_.async_send({}, std::move(signal), [self](auto ec) {
        if (ec) {
          if (ec != asio::experimental::error::channel_closed &&
              ec != asio::experimental::error::channel_cancelled) {
            CB_LOG_WARNING(
              "unexpected error while sending to row channel: {} ({})", ec.value(), ec.message());
          }
          return;
        }
      });
    });
    maybe_feed_lexer();
  }

  void next_row(utils::movable_function<void(std::string, std::error_code)>&& handler)
  {
    if (!rows_.is_open()) {
      handler({}, errc::common::request_canceled);
      return;
    }
    rows_.async_receive(
      [self = shared_from_this(), handler = std::move(handler)](auto ec, auto row) mutable {
        if (ec) {
          if (ec == asio::experimental::error::channel_closed ||
              ec == asio::experimental::error::channel_cancelled) {
            return handler({}, errc::common::request_canceled);
          }
          return handler({}, ec);
        }
        if (std::holds_alternative<row_stream_end_signal>(row)) {
          auto& signal = std::get<row_stream_end_signal>(row);
          if (!signal.metadata.empty()) {
            const std::scoped_lock<std::mutex> lock{ self->metadata_mutex_ };
            self->metadata_ = std::move(signal.metadata);
          }
          return handler({}, signal.ec);
        }
        auto row_content = std::move(std::get<std::string>(row));
        const auto row_bytes = row_content.size();
        handler(std::move(row_content), {});

        // Release the row's budget and resume feeding if below the low-water mark.
        self->release_buffered_bytes(row_bytes);
        if (self->should_resume()) {
          self->maybe_feed_lexer();
        }
        return;
      });
  }

  void cancel()
  {
    // Tear the HTTP body (and the socket + timers it owns on the session) down on the io_context
    // thread, where every other body operation runs. Calling body_.cancel() directly from an
    // arbitrary caller thread — e.g. the thread dropping the public handle — races the io thread
    // that may be mid-read and corrupts non-thread-safe session state. The channel is
    // thread-safe, so cancel/close it synchronously to unblock a waiting consumer immediately.
    asio::post(io_, [self = shared_from_this()]() {
      self->body_.cancel();
    });
    rows_.cancel();
    rows_.close();
  }

  auto metadata() -> std::optional<std::string>
  {
    const std::scoped_lock<std::mutex> lock{ metadata_mutex_ };
    return metadata_;
  }

  [[nodiscard]] auto buffered_bytes() const -> std::size_t
  {
    return buffered_bytes_.load(std::memory_order_relaxed);
  }

private:
  // Resolve start()'s preamble handler exactly once. The lexer delivers it on a well-formed
  // response (rows-array open or clean root pop); the terminal paths in maybe_feed_lexer call this
  // with an error so a response that never produces a metadata header (empty body, or a valid but
  // non-object JSON root) still resolves the handler instead of hanging the caller forever.
  void deliver_metadata_header(std::error_code ec, std::string&& meta_header)
  {
    if (metadata_header_delivered_.exchange(true)) {
      return;
    }
    // Move the handler out before invoking so it is destroyed right after. The handler owns a
    // strong reference to the owning query/analytics stream (which in turn owns this row_streamer),
    // so keeping it in a member past delivery would form a reference cycle and leak the whole
    // stream.
    auto handler = std::move(metadata_header_handler_);
    if (handler) {
      handler(std::move(meta_header), ec);
    }
  }

  [[nodiscard]] auto should_resume() const -> bool
  {
    return buffered_bytes_.load(std::memory_order_relaxed) < options_.low_water_bytes;
  }

  // Release a row's byte budget, clamping at zero. Uses an atomic compare-exchange loop so the
  // subtract is a single read-modify-write: even if the io_context is driven by more than one
  // thread, a concurrent update to buffered_bytes_ cannot be lost (as it could with a separate
  // load + store). Relaxed ordering is sufficient because the channel handoff already provides the
  // ordering between producer and consumer.
  void release_buffered_bytes(std::size_t n)
  {
    auto current = buffered_bytes_.load(std::memory_order_relaxed);
    while (!buffered_bytes_.compare_exchange_weak(
      current, current >= n ? current - n : 0, std::memory_order_relaxed)) {
    }
  }

  // Arm the inter-read idle timer, if configured. It is armed only while a socket read is in
  // flight (see maybe_feed_lexer) so a legitimately slow consumer — which produces no socket
  // traffic — is never timed out; a fire means the server stalled mid-body.
  void arm_idle_timer()
  {
    if (options_.idle_timeout.count() <= 0) {
      return;
    }
    const auto generation = idle_generation_.fetch_add(1, std::memory_order_relaxed) + 1;
    idle_timer_.expires_after(options_.idle_timeout);
    idle_timer_.async_wait([weak = weak_from_this(), generation](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      auto self = weak.lock();
      if (!self) {
        return;
      }
      // Ignore a completion from a timer that has already been superseded: when a read completes it
      // cancels and bumps the generation, but a timer that fired at almost the same instant may
      // already be queued. Acting on such a stale fire would abort the healthy next read and
      // misreport it as a timeout.
      if (self->idle_generation_.load(std::memory_order_relaxed) != generation) {
        return;
      }
      // Server stalled while a read was in flight: abort it; the read completion delivers the
      // terminal with a timeout error (steered by timed_out_).
      self->timed_out_ = true;
      self->body_.cancel();
    });
  }

  void maybe_feed_lexer()
  {
    if (received_all_data_ ||
        buffered_bytes_.load(std::memory_order_relaxed) > options_.high_water_bytes) {
      return;
    }

    // Atomically claim the feeding gate. With a multi-threaded io_context two resume paths (a
    // next_row completion draining a slot and the feed chain re-entering) can reach here at once;
    // only one may hold an outstanding body_.next().
    bool expected = false;
    if (!feeding_.compare_exchange_strong(expected, true)) {
      return;
    }
    arm_idle_timer();

    body_.next([self = shared_from_this()](const auto& data, bool has_more, auto ec) mutable {
      // A read completed (or was aborted); the idle timer only guards an in-flight read. Cancel it
      // and bump the generation so a timer that fired at the same instant is treated as stale.
      self->idle_timer_.cancel();
      self->idle_generation_.fetch_add(1, std::memory_order_relaxed);
      if (ec) {
        self->received_all_data_ = true;
        // If the idle timer aborted the read, report a timeout rather than the raw cancel error.
        auto terminal_ec =
          self->timed_out_ ? std::error_code{ errc::common::unambiguous_timeout } : ec;
        // A read error before any metadata header (e.g. the connection is reset right after the
        // response headers) must still resolve start()'s handler rather than hang the caller.
        self->deliver_metadata_header(terminal_ec, {});
        // Only enqueue a terminal if the lexer has not already delivered one: on a keep-alive
        // socket a read can error after the JSON document already completed, which would otherwise
        // enqueue a second, never-consumed terminal.
        if (!self->lexer_completed_) {
          auto signal = row_stream_end_signal{ terminal_ec };
          return self->rows_.async_send({}, std::move(signal), [self](auto ec) {
            if (!ec || ec == asio::experimental::error::channel_cancelled ||
                ec == asio::experimental::error::channel_closed) {
              return;
            }
            CB_LOG_WARNING("unexpected error while sending row stream end signal: {} ({})",
                           ec.value(),
                           ec.message());
          });
        }
        return;
      }
      if (!data.empty()) {
        // The chunk may be empty even mid-stream (a socket read consumed entirely by HTTP chunk
        // framing); only feed the lexer when there are actual body bytes. End-of-stream is keyed
        // off has_more, never off emptiness, so a framing-only read never looks like completion.
        const std::scoped_lock<std::mutex> lock{ self->data_feed_mutex_ };
        self->lexer_.feed(data);
      }
      if (!has_more) {
        // Transport reached the end of the body. For a well-formed response the lexer's
        // on_complete already delivered the terminal row_stream_end_signal; if the body ended
        // before the JSON document completed (truncated/malformed response), no terminal would
        // otherwise reach the consumer and it would block forever — surface a parsing failure.
        self->received_all_data_ = true;
        if (!self->lexer_completed_) {
          // The body ended without the lexer completing the document: a truncated/malformed
          // response, an empty body, or a valid-JSON-but-not-an-object root that the lexer rejects
          // without ever emitting a metadata header. Resolve start()'s handler and enqueue a
          // terminal so a consumer parked in start() or next_row() is not stuck forever.
          self->deliver_metadata_header(errc::common::parsing_failure, {});
          auto signal = row_stream_end_signal{ errc::common::parsing_failure };
          self->rows_.async_send({}, std::move(signal), [self](auto ec) {
            if (ec && ec != asio::experimental::error::channel_cancelled &&
                ec != asio::experimental::error::channel_closed) {
              CB_LOG_WARNING("unexpected error while sending row stream end signal: {} ({})",
                             ec.value(),
                             ec.message());
            }
          });
        }
        return;
      }

      self->feeding_ = false;

      return self->maybe_feed_lexer();
    });
  }

  asio::io_context& io_;
  http_response_body body_;
  asio::experimental::concurrent_channel<void(std::error_code,
                                              std::variant<std::string, row_stream_end_signal>)>
    rows_;
  row_streamer_options options_;
  asio::steady_timer idle_timer_;
  std::atomic_size_t buffered_bytes_{ 0 };
  std::atomic_bool received_all_data_{ false };
  std::atomic_bool feeding_{ false };
  std::atomic_bool lexer_completed_{ false };
  std::atomic_bool timed_out_{ false };
  // Guards the one-shot delivery of the preamble handler (see deliver_metadata_header): the stream
  // must resolve start()'s handler on every terminal, even when the lexer never fires its metadata
  // callback (empty body, or a valid-JSON-but-not-an-object root).
  std::atomic_bool metadata_header_delivered_{ false };
  utils::movable_function<void(std::string, std::error_code)> metadata_header_handler_{};
  // Bumped on every arm/cancel of the idle timer so a superseded timer completion is ignored.
  std::atomic_uint64_t idle_generation_{ 0 };
  std::optional<std::string> metadata_;
  utils::json::streaming_lexer lexer_;
  std::mutex data_feed_mutex_{};
  std::mutex metadata_mutex_{};
};

row_streamer::row_streamer(asio::io_context& io,
                           couchbase::core::http_response_body body,
                           const std::string& pointer_expression,
                           row_streamer_options options)
  : impl_{ std::make_shared<row_streamer_impl>(io, std::move(body), pointer_expression, options) }
{
}

void
row_streamer::start(utils::movable_function<void(std::string, std::error_code)>&& handler)
{
  impl_->start(std::move(handler));
}

void
row_streamer::next_row(utils::movable_function<void(std::string, std::error_code)>&& handler)
{
  impl_->next_row(std::move(handler));
}

void
row_streamer::cancel()
{
  impl_->cancel();
}

auto
row_streamer::metadata() -> std::optional<std::string>
{
  return impl_->metadata();
}

auto
row_streamer::buffered_bytes() const -> std::size_t
{
  return impl_->buffered_bytes();
}
} // namespace couchbase::core
