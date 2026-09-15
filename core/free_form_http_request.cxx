/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "free_form_http_request.hxx"

#include "io/http_message.hxx"
#include "io/http_streaming_response.hxx"
#include "utils/movable_function.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <chrono>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core
{
class http_response_impl : public std::enable_shared_from_this<http_response_impl>
{
public:
  http_response_impl() = default;

  explicit http_response_impl(io::http_streaming_response streaming_resp)
    : streaming_resp_{ std::move(streaming_resp) }
  {
  }

  http_response_impl(asio::io_context& io, std::string cached_data, std::size_t cached_chunk_size)
    : cached_body_{ io::http_streaming_response_body{ io,
                                                      nullptr,
                                                      std::move(cached_data),
                                                      true,
                                                      cached_chunk_size } }
  {
  }

  // Fault-injecting in-memory body (test seam): see http_response_body::create_in_memory_faulty.
  http_response_impl(asio::io_context& io,
                     std::string fault_data,
                     std::size_t fault_chunk_size,
                     std::error_code fault_terminal_ec,
                     bool fault_stall)
    : fault_deadline_{ std::make_shared<asio::steady_timer>(io) }
    , fault_enabled_{ true }
    , fault_data_{ std::move(fault_data) }
    , fault_chunk_size_{ fault_chunk_size }
    , fault_terminal_ec_{ fault_terminal_ec }
    , fault_stall_{ fault_stall }
  {
  }

  [[nodiscard]] auto endpoint() const -> std::string
  {
    if (cached_body_ || fault_enabled_) {
      return {};
    }
    return streaming_resp_.dispatch_info().remote_address;
  }

  [[nodiscard]] auto dispatch_info() const -> io::http_dispatch_info
  {
    if (cached_body_ || fault_enabled_) {
      return {};
    }
    return streaming_resp_.dispatch_info();
  }

  [[nodiscard]] auto status_code() const -> std::uint32_t
  {
    if (cached_body_) {
      return 200;
    }
    return streaming_resp_.status_code();
  }

  [[nodiscard]] auto content_length() const -> std::size_t
  {
    if (cached_body_) {
      return 0;
    }
    if (streaming_resp_.headers().find("content-length") == streaming_resp_.headers().end()) {
      return 0;
    }
    return std::stoul(streaming_resp_.headers().at("content-length"));
  }

  void next_body(utils::movable_function<void(std::string, bool, std::error_code)> callback)
  {
    if (fault_enabled_) {
      // Handlers are invoked after the lock is dropped.
      std::unique_lock<std::mutex> lock{ fault_mutex_ };
      if (fault_cancelled_) {
        const auto ec = fault_closed_ec_;
        lock.unlock();
        return callback({}, false, ec);
      }
      if (!fault_data_.empty()) {
        std::string chunk;
        if (fault_chunk_size_ == 0 || fault_data_.size() <= fault_chunk_size_) {
          std::swap(chunk, fault_data_);
        } else {
          chunk = fault_data_.substr(0, fault_chunk_size_);
          fault_data_.erase(0, fault_chunk_size_);
        }
        // has_more is always true here: whatever comes after the data (a stall, an injected
        // terminal error, or a clean end) is delivered on a subsequent pull.
        lock.unlock();
        return callback(std::move(chunk), true, {});
      }
      if (fault_stall_) {
        // Simulate a server that stopped sending mid-body: park the pull and never complete it
        // until cancel() (close_body) or an expired deadline fires it.
        fault_parked_ = std::move(callback);
        return;
      }
      // Drained: no deadline is accepted from here on. The generation is bumped for the same
      // reason close_body() bumps it -- cancel() does not stop a completion already queued -- so
      // both drain paths supersede one identically rather than relying on fault_finished_ alone.
      fault_finished_ = true;
      ++fault_generation_;
      fault_deadline_->cancel();
      const auto ec = fault_terminal_ec_;
      lock.unlock();
      return callback({}, false, ec);
    }
    if (cached_body_) {
      return cached_body_->next(std::move(callback));
    }
    return streaming_resp_.body().next(std::move(callback));
  }

  void cancel_body_deadline()
  {
    if (fault_enabled_) {
      const std::scoped_lock lock{ fault_mutex_ };
      ++fault_generation_;
      fault_deadline_->cancel();
      return;
    }
    if (cached_body_) {
      return cached_body_->cancel_deadline();
    }
    return streaming_resp_.body().cancel_deadline();
  }

  auto set_body_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp,
                         io::deadline_terminal on_expiry) -> io::deadline_state
  {
    if (fault_enabled_) {
      const std::scoped_lock lock{ fault_mutex_ };
      // fault_finished_ is set by the drain branch of next_body(), so a seam whose terminal is
      // reached without any bytes to hand out first is terminal before that branch has ever run.
      // The real body answers body_already_ended in the same state -- reading_complete_ with an
      // empty cache -- and a seam that armed instead would hold the io_context open until it fired.
      if (fault_cancelled_ || fault_finished_ || (fault_data_.empty() && !fault_stall_)) {
        return io::deadline_state::body_already_ended;
      }
      const auto generation = ++fault_generation_;
      fault_deadline_->expires_at(deadline_tp);
      fault_deadline_->async_wait([self = shared_from_this(), on_expiry, generation](auto ec) {
        if (ec == asio::error::operation_aborted) {
          return;
        }
        self->close_body_at_deadline(on_expiry, generation);
      });
      return io::deadline_state::armed;
    }
    if (cached_body_) {
      return cached_body_->set_deadline(deadline_tp, on_expiry);
    }
    return streaming_resp_.body().set_deadline(deadline_tp, on_expiry);
  }

  void close_body_at_deadline(io::deadline_terminal on_expiry, std::uint64_t generation)
  {
    utils::movable_function<void(std::string, bool, std::error_code)> parked;
    {
      const std::scoped_lock lock{ fault_mutex_ };
      // expires_at/cancel do not stop a completion already queued; ignore a superseded one.
      if (generation != fault_generation_ || fault_cancelled_ || fault_finished_) {
        return;
      }
      fault_cancelled_ = true;
      fault_closed_ec_ = io::terminal_error_code(on_expiry);
      fault_deadline_->cancel();
      parked = std::move(fault_parked_);
    }
    if (parked) {
      parked({}, false, io::terminal_error_code(on_expiry));
    }
  }

  void close_body()
  {
    if (fault_enabled_) {
      std::unique_lock<std::mutex> lock{ fault_mutex_ };
      // First terminal wins, as in the real body. set_body_deadline() and
      // close_body_at_deadline() test the same pair; a cancel arriving after the terminal path
      // unlocked would otherwise change later pulls to request_canceled.
      if (fault_cancelled_ || fault_finished_) {
        return;
      }
      fault_cancelled_ = true;
      ++fault_generation_;
      fault_deadline_->cancel();
      auto cb = std::move(fault_parked_);
      const auto ec = fault_closed_ec_;
      lock.unlock();
      if (cb) {
        cb({}, false, ec);
      }
      return;
    }
    if (cached_body_) {
      return cached_body_->close();
    }
    return streaming_resp_.body().close();
  }

private:
  io::http_streaming_response streaming_resp_;
  std::optional<io::http_streaming_response_body> cached_body_{};
  // fault_mutex_ guards every fault_* member written after construction, including the timer:
  // asio timers are not safe under concurrent use. fault_enabled_ is set once by the
  // constructor and read unlocked. Only the fault seam owns a timer; the other bodies forward to an
  // existing one.
  mutable std::mutex fault_mutex_{};
  std::shared_ptr<asio::steady_timer> fault_deadline_{};
  // Fault-injection test seam (see the fault-injecting ctor and create_in_memory_faulty).
  bool fault_enabled_{ false };
  std::string fault_data_{};
  std::size_t fault_chunk_size_{ 0 };
  std::error_code fault_terminal_ec_{};
  bool fault_stall_{ false };
  bool fault_cancelled_{ false };
  bool fault_finished_{ false };
  std::uint64_t fault_generation_{ 0 };
  std::error_code fault_closed_ec_{ errc::common::request_canceled };
  utils::movable_function<void(std::string, bool, std::error_code)> fault_parked_{};
};

class buffered_http_response_impl
{
public:
  explicit buffered_http_response_impl(io::http_response resp)
    : resp_{ std::move(resp) }
  {
  }

  [[nodiscard]] auto endpoint() const -> std::string
  {
    return {};
  }

  [[nodiscard]] auto status_code() const -> std::uint32_t
  {
    return resp_.status_code;
  }

  [[nodiscard]] auto content_length() const -> std::size_t
  {
    if (resp_.headers.find("content-length") == resp_.headers.end()) {
      return 0;
    }
    return std::stoul(resp_.headers.at("content-length"));
  }

  [[nodiscard]] auto body() const -> std::string
  {
    return resp_.body.data();
  }

private:
  io::http_response resp_;
};

http_response::http_response(io::http_streaming_response resp)
  : impl_{ std::make_shared<http_response_impl>(std::move(resp)) }
{
}

auto
http_response::endpoint() const -> std::string
{
  return impl_->endpoint();
}
auto
http_response::dispatch_info() const -> io::http_dispatch_info
{
  return impl_->dispatch_info();
}
auto
http_response::status_code() const -> std::uint32_t
{
  return impl_->status_code();
}
auto
http_response::content_length() const -> std::size_t
{
  return impl_->content_length();
}

auto
http_response::body() const -> http_response_body
{
  return http_response_body{ impl_ };
}

void
http_response::close()
{
  return impl_->close_body();
}

http_response_body::http_response_body(std::shared_ptr<http_response_impl> impl)
  : impl_{ std::move(impl) }
{
}

auto
http_response_body::create_in_memory(asio::io_context& io,
                                     std::string data,
                                     std::size_t cached_chunk_size) -> http_response_body
{
  return http_response_body{ std::make_shared<http_response_impl>(
    io, std::move(data), cached_chunk_size) };
}

auto
http_response_body::create_in_memory_faulty(asio::io_context& io,
                                            std::string data,
                                            std::size_t cached_chunk_size,
                                            std::error_code terminal_ec,
                                            bool stall) -> http_response_body
{
  return http_response_body{ std::make_shared<http_response_impl>(
    io, std::move(data), cached_chunk_size, terminal_ec, stall) };
}

auto
http_response_body::set_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp,
                                 io::deadline_terminal on_expiry) -> io::deadline_state
{
  return impl_->set_body_deadline(deadline_tp, on_expiry);
}

void
http_response_body::cancel_deadline()
{
  return impl_->cancel_body_deadline();
}

void
http_response_body::cancel()
{
  return impl_->close_body();
}

void
http_response_body::next(utils::movable_function<void(std::string, bool, std::error_code)> callback)
{
  return impl_->next_body(std::move(callback));
}

buffered_http_response::buffered_http_response(io::http_response resp)
  : impl_{ std::make_shared<buffered_http_response_impl>(std::move(resp)) }
{
}

auto
buffered_http_response::endpoint() const -> std::string
{
  return impl_->endpoint();
}
auto
buffered_http_response::status_code() const -> std::uint32_t
{
  return impl_->status_code();
}
auto
buffered_http_response::content_length() const -> std::size_t
{
  return impl_->content_length();
}

auto
buffered_http_response::body() -> std::string
{
  return impl_->body();
}

} // namespace couchbase::core
