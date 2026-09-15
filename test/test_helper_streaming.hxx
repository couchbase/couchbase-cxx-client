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

#pragma once

#include "core/cluster_credentials.hxx"
#include "core/cluster_options.hxx"
#include "core/free_form_http_request.hxx"
#include "core/io/http_context.hxx"
#include "core/io/http_session.hxx"
#include "core/io/query_cache.hxx"
#include "core/origin.hxx"
#include "core/service_type.hxx"
#include "core/topology/configuration.hxx"

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/write.hpp>
#include <variant>

#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace test::utils
{
inline auto
make_cached_response_body(asio::io_context& io, std::string data)
  -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory(io, std::move(data));
}

// Like make_cached_response_body, but the body is handed out in chunk_size-byte slices (one per
// next() pull) so a consumer's read-vs-consume back-pressure can be exercised deterministically.
inline auto
make_chunked_response_body(asio::io_context& io, std::string data, std::size_t chunk_size)
  -> couchbase::core::http_response_body
{
  return couchbase::core::http_response_body::create_in_memory(io, std::move(data), chunk_size);
}
// write_and_stream reports a failed dispatch as std::error_code, or as error_union in a columnar
// build. Both shapes answer the same question, and a test only needs the answer.
[[nodiscard]] inline auto
dispatch_failed(const std::error_code& ec) -> bool
{
  return static_cast<bool>(ec);
}

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
[[nodiscard]] inline auto
dispatch_failed(const couchbase::core::error_union& err) -> bool
{
  return !std::holds_alternative<std::monostate>(err);
}
#endif

// Joins a thread on every path out of the scope, including one taken while unwinding. A
// std::thread still joinable when it is destroyed calls std::terminate, which replaces whatever
// the case was reporting -- and the exception that caused the unwind -- with "terminate called".
class scoped_thread
{
public:
  explicit scoped_thread(std::thread& thread)
    : thread_{ thread }
  {
  }

  scoped_thread(const scoped_thread&) = delete;
  scoped_thread(scoped_thread&&) = delete;
  auto operator=(const scoped_thread&) -> scoped_thread& = delete;
  auto operator=(scoped_thread&&) -> scoped_thread& = delete;

  ~scoped_thread()
  {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

private:
  std::thread& thread_;
};

// The same for a group built one thread at a time. Declared before the loop that fills it, so a
// construction that throws part way leaves the threads already started owned rather than
// abandoned.
class scoped_threads
{
public:
  explicit scoped_threads(std::vector<std::thread>& threads)
    : threads_{ threads }
  {
  }

  scoped_threads(const scoped_threads&) = delete;
  scoped_threads(scoped_threads&&) = delete;
  auto operator=(const scoped_threads&) -> scoped_threads& = delete;
  auto operator=(scoped_threads&&) -> scoped_threads& = delete;

  ~scoped_threads()
  {
    for (auto& thread : threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

private:
  std::vector<std::thread>& threads_;
};

// A loopback HTTP server and an `http_session` connected to it, yielding a `http_response_body`
// backed by a real socket. Without a tail the Content-Length promises more than the server ever
// sends, so pulls park once the prefix is drained.
class loopback_stream
{
public:
  // The default prefix opens a query response and its rows array without closing either, so the
  // streaming lexer accepts what it has and waits for the rest.
  // With a tail, the header promises exactly prefix + tail, so send_tail() completes the response
  // and http_session runs its stream-end handler. Without one the response can never complete.
  explicit loopback_stream(asio::io_context& io,
                           std::string prefix = R"({"requestID":"r1","signature":{},"results":[)",
                           std::string tail = {})
    : io_{ io }
    , state_{ std::make_shared<state>(io, std::move(prefix), std::move(tail)) }
  {
    // These handlers hold the state, so they stay valid if this object is destroyed while the io
    // thread is still running.
    auto st = state_;
    st->acceptor.async_accept(st->server_socket, [st](std::error_code accept_ec) {
      if (accept_ec) {
        return;
      }
      st->server_socket.async_read_some(
        asio::buffer(st->request_buffer), [st](std::error_code read_ec, std::size_t) {
          if (read_ec) {
            return;
          }
          std::error_code ignored;
          asio::write(st->server_socket, asio::buffer(st->response), ignored);
        });
    });
  }

  loopback_stream(const loopback_stream&) = delete;
  loopback_stream(loopback_stream&&) = delete;
  auto operator=(const loopback_stream&) -> loopback_stream& = delete;
  auto operator=(loopback_stream&&) -> loopback_stream& = delete;

  ~loopback_stream()
  {
    if (session_) {
      session_->stop();
    }
    std::error_code ignored;
    state_->server_socket.close(ignored);
    state_->acceptor.close(ignored);
  }

  // Whether the session behind this stream has been torn down. The deadline reclaiming an
  // abandoned connection is the point of the feature, and only this distinguishes it from a
  // consumer merely being handed a timeout.
  [[nodiscard]] auto session_stopped() const -> bool
  {
    return session_ != nullptr && session_->is_stopped();
  }

  // Writes the bytes the header promised beyond the prefix, completing the response. Only
  // meaningful when the stream was built with a tail.
  void send_tail()
  {
    std::error_code ignored;
    asio::write(state_->server_socket, asio::buffer(state_->tail), ignored);
  }

  // Resolves with a body backed by the live session once the response headers have been parsed.
  // `on_stream_end` is http_session's stream-end handler: it runs inside the read completion that
  // finishes the response, before the body learns the read is done.
  void connect(
    couchbase::core::utils::movable_function<void(couchbase::core::http_response_body)> on_body,
    couchbase::core::utils::movable_function<void()> on_stream_end = []() {
    })
  {
    const auto port = state_->acceptor.local_endpoint().port();
    credentials_.username = "user";
    credentials_.password = "pass";
    origin_ = couchbase::core::origin{ credentials_, "127.0.0.1", port, options_ };

    // http_session takes the context by value and keeps that copy, so there is nothing to store
    // here. The copy holds config_, options_ and cache_ by reference, which is why those three
    // have static storage duration: the session can outlive this object.
    session_ = std::make_shared<couchbase::core::io::http_session>(
      couchbase::core::service_type::query,
      "client-id",
      "node-uuid",
      io_,
      origin_,
      "127.0.0.1",
      std::to_string(port),
      couchbase::core::http_context{
        config_, options_, cache_, "127.0.0.1", port, "127.0.0.1", port });

    couchbase::core::io::http_request request{};
    request.type = couchbase::core::service_type::query;
    request.method = "GET";
    request.path = "/query/service";
    request.stream_response = true;

    // The handler owns the session and the request: this object may be destroyed while the
    // connect is still outstanding, which is what a case that gives up on a wait does.
    session_->connect([session = session_,
                       request = std::move(request),
                       on_body = std::move(on_body),
                       on_stream_end = std::move(on_stream_end)]() mutable {
      session->write_and_stream(
        request,
        [on_body = std::move(on_body)](auto err,
                                       couchbase::core::io::http_streaming_response resp) mutable {
          // write_and_stream reports a failed dispatch with a default-constructed response,
          // whose impl is null. Handing its body on would segfault at the first pull and
          // replace the case's assertion with a crash, so drop it and let the case time out
          // on the body that never arrives.
          if (dispatch_failed(err)) {
            return;
          }
          on_body(couchbase::core::http_response{ std::move(resp) }.body());
        },
        std::move(on_stream_end));
    });
  }

private:
  struct state {
    state(asio::io_context& io, std::string prefix, std::string tail)
      : acceptor{ io, asio::ip::tcp::endpoint{ asio::ip::make_address("127.0.0.1"), 0 } }
      , server_socket{ io }
      , response{ "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                  "Content-Length: " +
                  std::to_string(prefix.size() +
                                 (tail.empty() ? promised_but_unsent : tail.size())) +
                  "\r\n\r\n" + prefix }
      , tail{ std::move(tail) }
    {
    }

    // How many bytes the header promises beyond the prefix when no tail was given. The response
    // is then always incomplete, and pulls park once the prefix is drained. A fixed
    // Content-Length would also cap the body, which kept a prefix from ever exceeding a
    // back-pressure high-water mark. With a tail the header promises exactly prefix + tail
    // instead, and send_tail() completes the response.
    static constexpr std::size_t promised_but_unsent{ 4096 };

    asio::ip::tcp::acceptor acceptor;
    asio::ip::tcp::socket server_socket;
    std::string response;
    std::string tail;
    std::string request_buffer = std::string(4096, '\0');
  };

  asio::io_context& io_;
  std::shared_ptr<state> state_;
  couchbase::core::cluster_credentials credentials_{};
  // Static storage duration, as in test/utils/http_context.hxx. http_session keeps its own copy
  // of the http_context, and that copy holds all three of these by reference, so it can outlive
  // a loopback_stream destroyed while a handler still holds the session. None is ever mutated.
  static inline couchbase::core::cluster_options options_{};
  static inline couchbase::core::topology::configuration config_{};
  static inline couchbase::core::query_cache cache_{};
  couchbase::core::origin origin_{};
  std::shared_ptr<couchbase::core::io::http_session> session_{};
};
} // namespace test::utils
