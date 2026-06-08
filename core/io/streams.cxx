/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2024 Couchbase, Inc.
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

#include "streams.hxx"

#include "core/platform/uuid.h"

#include <asio.hpp>
#include <asio/error.hpp>
#include <asio/ssl.hpp>
#include <asio/ssl/host_name_verification.hpp>
// ERR_get_error()/SSL_* come from the OpenSSL or BoringSSL headers pulled in by
// <asio/ssl.hpp>; do not include <openssl/*.h> directly so the static BoringSSL
// build (which ships its own headers) keeps working, matching core/io/mcbp_session.cxx.

namespace couchbase::core::io
{
auto
configure_tls_handshake(asio::ssl::stream<asio::ip::tcp::socket>& stream,
                        const std::string& hostname) -> std::error_code
{
  if (hostname.empty()) {
    // Fail closed: a TLS client connection must have a hostname to verify the
    // server's identity against. Returning success here would silently fall back
    // to CA-only validation -- the CWE-297 problem this function exists to prevent.
    return asio::error::make_error_code(asio::error::invalid_argument);
  }

  // Send SNI so that name-based / multi-tenant TLS endpoints present the correct
  // certificate for this hostname.
  {
    // SSL_set_tlsext_host_name() is a macro that expands to an internal C-style
    // cast to void*, which trips -Wold-style-cast; suppress it locally.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
    ERR_clear_error();
    const auto sni_result = SSL_set_tlsext_host_name(stream.native_handle(), hostname.c_str());
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
    if (sni_result != 1) {
      // Prefer the underlying SSL reason (if any) so failures are diagnosable,
      // and fall back to a generic error when the SSL error queue is empty.
      if (const auto reason = ERR_get_error(); reason != 0) {
        return { static_cast<int>(reason), asio::error::get_ssl_category() };
      }
      return asio::error::make_error_code(asio::error::invalid_argument);
    }
  }

  // Verify that the presented certificate actually identifies `hostname`
  // (SAN/CN). asio::ssl::verify_peer on its own only validates the certificate
  // chain, not the name. In verify_none mode OpenSSL ignores the callback
  // result, so this has no effect there.
  std::error_code ec{};
  stream.set_verify_callback(asio::ssl::host_name_verification(hostname), ec);
  return ec;
}

stream_impl::stream_impl(asio::io_context& ctx, bool is_tls)
  : strand_(asio::make_strand(ctx))
  , tls_(is_tls)
  , id_(uuid::to_string(uuid::random()))
{
}

auto
stream_impl::log_prefix() const -> std::string_view
{
  return tls_ ? "tls" : "plain";
}

auto
stream_impl::id() const -> const std::string&
{
  return id_;
}

plain_stream_impl::plain_stream_impl(asio::io_context& ctx)
  : stream_impl(ctx, false)
  , stream_(std::make_shared<asio::ip::tcp::socket>(strand_))
{
}

auto
plain_stream_impl::local_endpoint() const -> asio::ip::tcp::endpoint
{
  if (!stream_) {
    return {};
  }
  std::error_code ec;
  auto res = stream_->local_endpoint(ec);
  if (ec) {
    return {};
  }
  return res;
}

auto
plain_stream_impl::is_open() const -> bool
{
  if (stream_) {
    return stream_->is_open();
  }
  return false;
}

void
plain_stream_impl::close(utils::movable_function<void(std::error_code)>&& handler)
{
  if (!stream_) {
    return handler(asio::error::bad_descriptor);
  }
  return asio::post(strand_, [stream = std::move(stream_), handler = std::move(handler)]() {
    asio::error_code ec{};
    stream->shutdown(asio::socket_base::shutdown_both, ec);
    stream->close(ec);
    handler(ec);
  });
}

void
plain_stream_impl::set_options()
{
  if (!is_open()) {
    return;
  }
  std::error_code ec{};
  stream_->set_option(asio::ip::tcp::no_delay{ true }, ec);
  stream_->set_option(asio::socket_base::keep_alive{ true }, ec);
}

void
plain_stream_impl::async_connect(
  const asio::ip::tcp::resolver::results_type::endpoint_type& endpoint,
  const std::string& /* hostname */,
  utils::movable_function<void(std::error_code)>&& handler)
{
  if (!stream_) {
    id_ = uuid::to_string(uuid::random());
    stream_ = std::make_shared<asio::ip::tcp::socket>(strand_);
  }
  return stream_->async_connect(endpoint,
                                [stream = stream_, handler = std::move(handler)](auto ec) {
                                  return handler(ec);
                                });
}

void
plain_stream_impl::async_write(
  std::vector<asio::const_buffer>& buffers,
  utils::movable_function<void(std::error_code, std::size_t)>&& handler)
{
  if (!is_open()) {
    return handler(asio::error::bad_descriptor, {});
  }
  return asio::async_write(
    *stream_,
    buffers,
    [stream = stream_, handler = std::move(handler)](auto ec, auto bytes_transferred) {
      return handler(ec, bytes_transferred);
    });
}

void
plain_stream_impl::async_read_some(
  asio::mutable_buffer buffer,
  utils::movable_function<void(std::error_code, std::size_t)>&& handler)
{
  if (!is_open()) {
    return handler(asio::error::bad_descriptor, {});
  }
  return stream_->async_read_some(buffer, std::move(handler));
}

tls_stream_impl::tls_stream_impl(asio::io_context& ctx, tls_context_provider& tls)
  : stream_impl(ctx, true)
  , tls_(tls)
  , stream_(
      std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(asio::ip::tcp::socket(strand_),
                                                                 *tls_.get_ctx()))
{
}

auto
tls_stream_impl::local_endpoint() const -> asio::ip::tcp::endpoint
{
  if (!stream_) {
    return {};
  }
  std::error_code ec;
  auto res = stream_->lowest_layer().local_endpoint(ec);
  if (ec) {
    return {};
  }
  return res;
}

auto
tls_stream_impl::is_open() const -> bool
{
  if (stream_) {
    return stream_->lowest_layer().is_open();
  }
  return false;
}

void
tls_stream_impl::close(utils::movable_function<void(std::error_code)>&& handler)
{
  if (!stream_) {
    return handler(asio::error::bad_descriptor);
  }
  return asio::post(strand_, [stream = std::move(stream_), handler = std::move(handler)]() {
    asio::error_code ec{};
    stream->lowest_layer().shutdown(asio::socket_base::shutdown_both, ec);
    stream->lowest_layer().close(ec);
    handler(ec);
  });
}

void
tls_stream_impl::set_options()
{
  if (!is_open()) {
    return;
  }
  std::error_code ec{};
  stream_->lowest_layer().set_option(asio::ip::tcp::no_delay{ true }, ec);
  stream_->lowest_layer().set_option(asio::socket_base::keep_alive{ true }, ec);
}

void
tls_stream_impl::async_connect(const asio::ip::tcp::resolver::results_type::endpoint_type& endpoint,
                               const std::string& hostname,
                               utils::movable_function<void(std::error_code)>&& handler)
{
  if (!stream_) {
    id_ = uuid::to_string(uuid::random());
    stream_ = std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(
      asio::ip::tcp::socket(strand_), *tls_.get_ctx());
  }
  return stream_->lowest_layer().async_connect(
    endpoint,
    // hostname is copied into the closure (owning std::string) so the async
    // handler does not depend on the caller's argument lifetime.
    [stream = stream_, hostname = hostname, handler = std::move(handler)](
      std::error_code ec_connect) mutable {
      if (ec_connect == asio::error::operation_aborted) {
        return;
      }
      if (ec_connect) {
        return handler(ec_connect);
      }
      // Verify the server's identity against `hostname` (SAN/CN) and set SNI
      // before the handshake. Without this, verify_peer only checks that the
      // certificate chains to a trusted CA, not that it identifies this host.
      if (auto ec_identity = configure_tls_handshake(*stream, hostname); ec_identity) {
        return handler(ec_identity);
      }
      stream->async_handshake(
        asio::ssl::stream_base::client,
        [stream, handler = std::move(handler)](std::error_code ec_handshake) mutable {
          if (ec_handshake == asio::error::operation_aborted) {
            return;
          }
          return handler(ec_handshake);
        });
    });
}

void
tls_stream_impl::async_write(std::vector<asio::const_buffer>& buffers,
                             utils::movable_function<void(std::error_code, std::size_t)>&& handler)
{
  if (!is_open()) {
    return handler(asio::error::bad_descriptor, {});
  }
  return asio::async_write(
    *stream_,
    buffers,
    [stream = stream_, handler = std::move(handler)](auto ec, auto bytes_transferred) {
      return handler(ec, bytes_transferred);
    });
}

void
tls_stream_impl::async_read_some(
  asio::mutable_buffer buffer,
  utils::movable_function<void(std::error_code, std::size_t)>&& handler)
{
  if (!is_open()) {
    return handler(asio::error::bad_descriptor, {});
  }
  return stream_->async_read_some(
    buffer, [stream = stream_, handler = std::move(handler)](auto ec, auto bytes_transferred) {
      return handler(ec, bytes_transferred);
    });
}
} // namespace couchbase::core::io
