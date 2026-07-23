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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace couchbase::core
{
class http_response_impl
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
  http_response_impl(std::string fault_data,
                     std::size_t fault_chunk_size,
                     std::error_code fault_terminal_ec,
                     bool fault_stall)
    : fault_enabled_{ true }
    , fault_data_{ std::move(fault_data) }
    , fault_chunk_size_{ fault_chunk_size }
    , fault_terminal_ec_{ fault_terminal_ec }
    , fault_stall_{ fault_stall }
  {
  }

  [[nodiscard]] auto endpoint() const -> std::string
  {
    return {};
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
      if (fault_cancelled_) {
        return callback({}, false, errc::common::request_canceled);
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
        return callback(std::move(chunk), true, {});
      }
      if (fault_stall_) {
        // Simulate a server that stopped sending mid-body: park the pull and never complete it
        // until cancel() (close_body) fires it. The row_streamer idle timer is what cancels it.
        fault_parked_ = std::move(callback);
        return;
      }
      return callback({}, false, fault_terminal_ec_);
    }
    if (cached_body_) {
      return cached_body_->next(std::move(callback));
    }
    return streaming_resp_.body().next(std::move(callback));
  }

  void close_body()
  {
    if (fault_enabled_) {
      fault_cancelled_ = true;
      if (fault_parked_) {
        auto cb = std::move(fault_parked_);
        cb({}, false, errc::common::request_canceled);
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
  // Fault-injection test seam (see the four-arg ctor and create_in_memory_faulty).
  bool fault_enabled_{ false };
  std::string fault_data_{};
  std::size_t fault_chunk_size_{ 0 };
  std::error_code fault_terminal_ec_{};
  bool fault_stall_{ false };
  bool fault_cancelled_{ false };
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
http_response_body::create_in_memory_faulty(asio::io_context& /* io */,
                                            std::string data,
                                            std::size_t cached_chunk_size,
                                            std::error_code terminal_ec,
                                            bool stall) -> http_response_body
{
  return http_response_body{ std::make_shared<http_response_impl>(
    std::move(data), cached_chunk_size, terminal_ec, stall) };
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
