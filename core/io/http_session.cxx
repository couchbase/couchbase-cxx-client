/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2024. Couchbase, Inc.
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

#include "http_session.hxx"

#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/meta/version.hxx"
#include "core/platform/base64.h"
#include "core/platform/uuid.h"
#include "core/service_type_fmt.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/ssl/error.hpp>
#include <gsl/util>
#include <spdlog/fmt/bin_to_hex.h>

#include <exception>
#include <utility>

namespace couchbase::core::io
{
namespace
{
// Between connection attempts, and before a resend after a refused client certificate.
constexpr std::chrono::milliseconds reconnect_backoff{ 500 };
} // namespace

http_session_info::http_session_info(const std::string& client_id, const std::string& session_id)
  : log_prefix_(fmt::format("[{}/{}]", client_id, session_id))
{
}

http_session_info::http_session_info(const std::string& client_id,
                                     const std::string& session_id,
                                     asio::ip::tcp::endpoint local_endpoint,
                                     const asio::ip::tcp::endpoint& remote_endpoint)
  : local_endpoint_(std::move(local_endpoint))
{

  local_endpoint_address_ = local_endpoint_.address().to_string();
  if (local_endpoint_.protocol() == asio::ip::tcp::v6()) {
    local_endpoint_address_ =
      fmt::format("[{}]:{}", local_endpoint_address_, local_endpoint_.port());
  } else {
    local_endpoint_address_ = fmt::format("{}:{}", local_endpoint_address_, local_endpoint_.port());
  }

  remote_endpoint_ = remote_endpoint;
  remote_endpoint_address_ = remote_endpoint_.address().to_string();
  if (remote_endpoint_.protocol() == asio::ip::tcp::v6()) {
    remote_endpoint_address_ =
      fmt::format("[{}]:{}", remote_endpoint_address_, remote_endpoint_.port());
  } else {
    remote_endpoint_address_ =
      fmt::format("{}:{}", remote_endpoint_address_, remote_endpoint_.port());
  }

  const auto endpoints = fmt::format("{}:{}:{}",
                                     local_endpoint_.port(),
                                     remote_endpoint_.address().to_string(),
                                     remote_endpoint_.port());
  log_prefix_ = fmt::format("[{}/{}] <{}>", client_id, session_id, logger::system_data(endpoints));
}

auto
http_session_info::remote_endpoint() const -> const asio::ip::tcp::endpoint&
{
  return remote_endpoint_;
}

auto
http_session_info::remote_address() const -> const std::string&
{
  return remote_endpoint_address_;
}

auto
http_session_info::local_endpoint() const -> const asio::ip::tcp::endpoint&
{
  return local_endpoint_;
}

auto
http_session_info::local_address() const -> const std::string&
{
  return local_endpoint_address_;
}

auto
http_session_info::log_prefix() const -> const std::string&
{
  return log_prefix_;
}

http_session::http_session(couchbase::core::service_type type,
                           std::string client_id,
                           std::string node_uuid,
                           asio::io_context& ctx,
                           origin& origin,
                           std::string hostname,
                           std::string service,
                           couchbase::core::http_context http_ctx,
                           std::uint64_t pool_generation)
  : type_(type)
  , client_id_(std::move(client_id))
  , node_uuid_(std::move(node_uuid))
  , id_(uuid::to_string(uuid::random()))
  , pool_generation_(pool_generation)
  , ctx_(ctx)
  , resolver_(ctx_)
  , stream_(std::make_unique<plain_stream_impl>(ctx_))
  , connect_deadline_timer_(stream_->get_executor())
  , idle_timer_(stream_->get_executor())
  , retry_backoff_(stream_->get_executor())
  , origin_(origin)
  , hostname_(std::move(hostname))
  , service_(std::move(service))
  , user_agent_(meta::user_agent_for_http(client_id_, id_, http_ctx.options.user_agent_extra))
  , info_(client_id_, id_)
  , http_ctx_(std::move(http_ctx))
{
}

http_session::http_session(couchbase::core::service_type type,
                           std::string client_id,
                           std::string node_uuid,
                           asio::io_context& ctx,
                           tls_context_provider& tls,
                           origin& origin,
                           std::string hostname,
                           std::string service,
                           couchbase::core::http_context http_ctx,
                           std::uint64_t pool_generation)
  : type_(type)
  , client_id_(std::move(client_id))
  , node_uuid_(std::move(node_uuid))
  , id_(uuid::to_string(uuid::random()))
  , pool_generation_(pool_generation)
  , ctx_(ctx)
  , resolver_(ctx_)
  , stream_(std::make_unique<tls_stream_impl>(ctx_, tls))
  , connect_deadline_timer_(stream_->get_executor())
  , idle_timer_(stream_->get_executor())
  , retry_backoff_(stream_->get_executor())
  , origin_(origin)
  , hostname_(std::move(hostname))
  , service_(std::move(service))
  , user_agent_(meta::user_agent_for_http(client_id_, id_, http_ctx.options.user_agent_extra))
  , info_(client_id_, id_)
  , http_ctx_(std::move(http_ctx))
{
}

http_session::~http_session()
{
  stop();
}

auto
http_session::get_executor() const -> asio::strand<asio::io_context::executor_type>
{
  return stream_->get_executor();
}

auto
http_session::http_context() -> couchbase::core::http_context&
{
  return http_ctx_;
}

auto
http_session::remote_address() -> std::string
{
  const std::scoped_lock lock(info_mutex_);
  return info_.remote_address();
}

auto
http_session::local_address() -> std::string
{
  const std::scoped_lock lock(info_mutex_);
  return info_.local_address();
}

auto
http_session::dispatched_endpoints() -> std::pair<std::string, std::string>
{
  const std::scoped_lock lock(info_mutex_);
  return { dispatched_from_, dispatched_to_ };
}

void
http_session::record_dispatch()
{
  const std::scoped_lock lock(info_mutex_);
  dispatched_from_ = info_.local_address();
  dispatched_to_ = info_.remote_address();
}

auto
http_session::remote_endpoint() -> const asio::ip::tcp::endpoint&
{
  const std::scoped_lock lock(info_mutex_);
  return info_.remote_endpoint();
}

auto
http_session::diag_info() -> diag::endpoint_diag_info
{
  const auto last_active = last_active_.load();
  return { type_,
           id_,
           last_active.time_since_epoch().count() == 0
             ? std::nullopt
             : std::make_optional(std::chrono::duration_cast<std::chrono::microseconds>(
                 std::chrono::steady_clock::now() - last_active)),
           remote_address(),
           local_address(),
           state_.load() };
}

auto
http_session::log_prefix() -> std::string
{
  const std::scoped_lock lock(info_mutex_);
  return info_.log_prefix();
}

auto
http_session::id() const -> const std::string&
{
  return id_;
}

auto
http_session::pool_generation() const -> std::uint64_t
{
  return pool_generation_;
}

auto
http_session::node_uuid() const -> const std::string&
{
  return node_uuid_;
}

auto
http_session::credentials() const -> cluster_credentials
{
  return origin_.credentials();
}

auto
http_session::is_connected() const -> bool
{
  return connected_;
}

auto
http_session::type() const -> service_type
{
  return type_;
}

auto
http_session::hostname() const -> const std::string&
{
  return hostname_;
}

auto
http_session::port() const -> const std::string&
{
  return service_;
}

auto
http_session::endpoint() -> const asio::ip::tcp::endpoint&
{
  const std::scoped_lock lock(info_mutex_);
  return info_.remote_endpoint();
}

void
http_session::connect(utils::movable_function<void()>&& callback)
{
  {
    const std::scoped_lock lock(connect_callback_mutex_);
    connect_callback_ = std::move(callback);
  }
  initiate_connect();
}

void
http_session::initiate_connect()
{
  if (stopped_) {
    // stop() has begun, so nothing connects. Its teardown may already have taken an earlier
    // callback, so the one just stored, which holds this session, is run here. Whichever of this
    // call and the teardown moves it out under connect_callback_mutex_ runs it.
    return invoke_connect_callback();
  }
  if (state_ != diag::endpoint_state::connecting) {
    CB_LOG_DEBUG("{} {} attempt to establish HTTP connection",
                 info_.log_prefix(),
                 logger::system_data(fmt::format("{}:{}", hostname_, service_)));
    state_ = diag::endpoint_state::connecting;
    async_resolve(
      http_ctx_.options.use_ip_protocol,
      resolver_,
      hostname_,
      service_,
      asio::bind_executor(get_executor(), [capture0 = shared_from_this()](auto&& PH1, auto&& PH2) {
        capture0->on_resolve(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
      }));
  } else {
    // reset state in case the session is being reused
    state_ = diag::endpoint_state::disconnected;
    auto backoff = reconnect_backoff;
    CB_LOG_DEBUG(
      "{} waiting for {}ms before trying to connect", info_.log_prefix(), backoff.count());
    retry_backoff_.expires_after(backoff);
    retry_backoff_.async_wait([self = shared_from_this()](std::error_code ec) mutable {
      if (ec == asio::error::operation_aborted || self->stopped_) {
        return;
      }
      self->invoke_connect_callback();
    });
    return;
  }
}

void
http_session::on_stop(std::function<void()> handler)
{
  on_stop_handler_ = std::move(handler);
}

void
http_session::cancel_current_response(std::error_code ec)
{
  const std::scoped_lock lock(current_response_mutex_);
  if (streaming_response_) {
    auto ctx = std::move(current_streaming_response_);
    if (auto handler = std::move(ctx.resp_handler); handler) {
      handler(ec, {});
    }
    if (auto handler = std::move(ctx.stream_end_handler); handler) {
      handler();
    }
  } else {
    if (auto ctx = std::move(current_response_); ctx.handler) {
      ctx.handler(ec, std::move(ctx.parser.response));
    }
  }
}

void
http_session::remember_request(bool uses_certificate)
{
  // Only a session with certificate credentials resends, so no other session keeps a copy.
  const std::scoped_lock lock(output_buffer_mutex_);
  if (fresh_connection_ && uses_certificate) {
    unanswered_request_ = output_buffer_;
  } else {
    unanswered_request_.clear();
  }
}

namespace
{
// The alerts a server sends when it refuses the client certificate. OpenSSL and BoringSSL report a
// received alert as the reason SSL_AD_REASON_OFFSET plus the alert's code.
auto
is_refused_certificate(std::error_code ec) -> bool
{
  if (ec.category() != asio::error::get_ssl_category()) {
    return false;
  }
  // OpenSSL 1.1 defines ERR_GET_REASON() as a macro with a C-style cast; OpenSSL 3.0 and later,
  // and BoringSSL, define it as an inline function.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
  const int alert = ERR_GET_REASON(static_cast<std::uint32_t>(ec.value())) - SSL_AD_REASON_OFFSET;
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
  switch (alert) {
    case SSL_AD_BAD_CERTIFICATE:
    case SSL_AD_UNSUPPORTED_CERTIFICATE:
    case SSL_AD_CERTIFICATE_REVOKED:
    case SSL_AD_CERTIFICATE_EXPIRED:
    case SSL_AD_CERTIFICATE_UNKNOWN:
    case SSL_AD_UNKNOWN_CA:
    case SSL_AD_ACCESS_DENIED:
#if defined(SSL_AD_CERTIFICATE_REQUIRED)
    case SSL_AD_CERTIFICATE_REQUIRED:
#endif
      return true;
    default:
      return false;
  }
}
} // namespace

// Under TLS 1.3 the server checks the client certificate after the client's handshake has
// completed. A refusal therefore arrives as an alert when the response to the first request on a
// new connection is read. The server has processed nothing on that connection, so the request
// is written again on a new one, with the current TLS context. This repeats until the request is
// answered or its command's deadline stops this session. Only a session with certificate
// credentials resends: other credentials present no client certificate, and a rotation cannot
// change that.
auto
http_session::resend_after_rejected_handshake(std::error_code ec) -> bool
{
  if (stopped_ || !fresh_connection_ || !is_refused_certificate(ec) ||
      !credentials().uses_certificate()) {
    return false;
  }
  {
    const std::scoped_lock lock(output_buffer_mutex_);
    if (unanswered_request_.empty()) {
      return false;
    }
  }
  CB_LOG_WARNING("{} client certificate refused by the server ({}), reconnecting",
                 info_.log_prefix(),
                 ec.message());
  connected_ = false;
  reading_ = false;
  {
    const std::scoped_lock lock(writing_buffer_mutex_);
    writing_buffer_.clear();
  }
  state_ = diag::endpoint_state::disconnected;
  stream_->close([self = shared_from_this()](std::error_code) {
    // A stop() that ran meanwhile cancelled retry_backoff_; arming it after that cancel would hold
    // this session for the backoff.
    if (self->stopped_) {
      return;
    }
    self->retry_backoff_.expires_after(reconnect_backoff);
    self->retry_backoff_.async_wait([self](std::error_code wait_ec) {
      if (wait_ec == asio::error::operation_aborted || self->stopped_) {
        return;
      }
      self->reconnect_to_resend();
    });
  });
  return true;
}

// initiate_connect() also runs the connect callback after a failed attempt and its backoff. The
// callback then connects again.
void
http_session::reconnect_to_resend()
{
  {
    const std::scoped_lock lock(connect_callback_mutex_);
    connect_callback_ = [self = shared_from_this()]() {
      if (self->stopped_) {
        return;
      }
      if (!self->connected_) {
        return self->reconnect_to_resend();
      }
      {
        const std::scoped_lock buffer_lock(self->output_buffer_mutex_);
        self->output_buffer_ = self->unanswered_request_;
      }
      self->flush();
    };
  }
  initiate_connect();
}

void
http_session::invoke_connect_callback()
{
  utils::movable_function<void()> cb;
  {
    const std::scoped_lock lock(connect_callback_mutex_);
    cb = std::move(connect_callback_);
  }
  if (cb) {
    cb();
  }
}

void
http_session::stop()
{
  // One caller wins. A stop() from a response handler the teardown below runs returns here; a
  // second teardown would lock current_response_mutex_ again inside cancel_current_response().
  // An exchange rather than a load and a store: two threads stopping at once would both pass a
  // load and run the teardown concurrently.
  if (stopped_.exchange(true)) {
    return;
  }
  // Every step runs although a callback throws, because a later stop() returns above and on_stop
  // is what removes the session from the manager's pools. The first exception is rethrown once the
  // teardown is complete; later ones are dropped. Not a scope guard: its destructor is noexcept.
  std::exception_ptr failure;
  const auto run_step = [&failure](auto&& step) {
    try {
      step();
    } catch (...) {
      if (!failure) {
        failure = std::current_exception();
      }
    }
  };
  state_ = diag::endpoint_state::disconnecting;
  stream_->close([](std::error_code) {
  });
  run_step([this]() {
    invoke_connect_callback();
  });
  connect_deadline_timer_.cancel();
  {
    const std::scoped_lock lock(idle_timer_mutex_);
    idle_timer_.cancel();
  }
  retry_backoff_.cancel();

  run_step([this]() {
    cancel_current_response(errc::common::request_canceled);
  });

  if (auto handler = std::move(on_stop_handler_); handler) {
    run_step(handler);
  }
  state_ = diag::endpoint_state::disconnected;
  if (failure) {
    std::rethrow_exception(failure);
  }
}

auto
http_session::keep_alive() const -> bool
{
  return keep_alive_;
}

auto
http_session::is_stopped() const -> bool
{
  return stopped_;
}

void
http_session::mark_stopping()
{
  stopping_ = true;
}

auto
http_session::is_stopping() const -> bool
{
  return stopping_;
}

void
http_session::write(const std::vector<std::uint8_t>& buf)
{
  if (stopped_) {
    return;
  }
  const std::scoped_lock lock(output_buffer_mutex_);
  output_buffer_.push_back(buf);
}

void
http_session::write(const std::string_view& buf)
{
  if (stopped_) {
    return;
  }
  const std::scoped_lock lock(output_buffer_mutex_);
  output_buffer_.emplace_back(buf.begin(), buf.end());
}

void
http_session::flush()
{
  if (!connected_) {
    return;
  }
  if (stopped_) {
    return;
  }
  asio::post(get_executor(), [self = shared_from_this()]() {
    self->do_write();
  });
}

void
http_session::write_and_stream(
  io::http_request& request,
  utils::movable_function<void(std::error_code, io::http_streaming_response)> resp_handler,
  utils::movable_function<void()> stream_end_handler)
{
  {
    streaming_response_context ctx{ std::move(resp_handler), std::move(stream_end_handler) };
    bool installed = false;
    {
      // As in write_and_subscribe: the stopped_ test shares the section that installs the
      // context, so a stop is either seen here or its cancel_current_response() ends the stream.
      const std::scoped_lock lock(current_response_mutex_);
      if (!stopped_) {
        std::swap(current_streaming_response_, ctx);
        streaming_response_ = true;
        installed = true;
      }
    }
    if (!installed) {
      ctx.resp_handler(errc::common::request_canceled, {});
      ctx.stream_end_handler();
      return;
    }
  }
  if (request.headers["connection"] == "keep-alive") {
    keep_alive_ = true;
  }
  request.headers["user-agent"] = user_agent_;
  auto creds = credentials();
  if (creds.uses_jwt()) {
    request.headers["authorization"] = fmt::format("Bearer {}", creds.jwt_token);
  } else {
    const auto credentials = fmt::format("{}:{}", creds.username, creds.password);
    request.headers["authorization"] = fmt::format(
      "Basic {}",
      base64::encode(gsl::as_bytes(gsl::span{ credentials.data(), credentials.size() })));
  }
  write(fmt::format(
    "{} {} HTTP/1.1\r\nhost: {}:{}\r\n", request.method, request.path, hostname_, service_));
  if (!request.body.empty()) {
    request.headers["content-length"] = std::to_string(request.body.size());
  }
  for (const auto& [name, value] : request.headers) {
    write(fmt::format("{}: {}\r\n", name, value));
  }
  write("\r\n");
  write(request.body);
  remember_request(creds.uses_certificate());
  flush();
}

void
http_session::set_idle(std::chrono::milliseconds timeout)
{
  const std::scoped_lock lock(idle_timer_mutex_);
  // check_in can call this off the strand after its is_stopped() test, while a teardown runs. The
  // teardown cancels the idle timer under this mutex after stopped_ is set, so either that cancel
  // finds the wait armed here, or this test sees the stop. Armed after the cancel, the wait would
  // hold the stopped session until the idle timeout.
  if (stopped_) {
    return;
  }
  idle_timer_.expires_after(timeout);
  // Bound to the session strand, so the idle-timeout stop() is serialised with the completion of
  // the liveness read armed below.
  idle_timer_.async_wait(
    asio::bind_executor(get_executor(), [self = shared_from_this()](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      CB_LOG_DEBUG("{} idle timeout expired, stopping session: \"{}\"",
                   self->info_.log_prefix(),
                   logger::system_data(fmt::format("{}:{}", self->hostname_, self->service_)));
      self->stop();
    }));
  // Keep a read armed while the connection is idle so that a peer-initiated
  // close (FIN/RST) is noticed promptly and the session removed from the pool,
  // instead of being handed to the next request as a dead socket that then
  // stalls until the service timeout.  Other SDKs get this from their HTTP
  // stacks (Netty channelInactive, Go net/http and Rust hyper liveness checks);
  // here the read completion (EOF/error, or unexpected data) tears the session
  // down -- see do_read().  Posted onto the session executor so it never races
  // with an in-flight read.
  idle_ = true;
  asio::post(get_executor(), [self = shared_from_this()]() {
    self->do_read();
  });
}

auto
http_session::reset_idle() -> bool
{
  // The session is leaving the pool to service a request, so the read armed by
  // set_idle() is no longer an idle liveness probe -- it becomes the read that
  // receives the response.  Clear the flag first so do_read() treats any
  // subsequent completion as normal request/response traffic, not a stale
  // connection.
  idle_ = false;
  // Return true if cancel() is successful. Since the idle_timer_ has a single pending
  // wait per session, we know the timer has already expired if cancel() returns 0.
  // stop() sets stopped_ before it cancels the idle timer under this mutex, so a stop whose cancel
  // has not run yet is seen here, and one whose cancel has run leaves nothing to cancel.
  const bool reset = [this]() {
    const std::scoped_lock lock(idle_timer_mutex_);
    return idle_timer_.cancel() != 0 && !stopped_;
  }();
  if (!reset) {
    // The idle timer already fired, and its (strand-bound) handler is about to stop() this
    // session, or a stop() has begun: the session is being torn down rather than checked out.
    // Restore idle_ so the still-armed liveness read stays on the idle path (tearing the
    // connection down) instead of being fed to the response parser as if it were request traffic.
    idle_ = true;
  }
  return reset;
}

void
http_session::read_some(
  utils::movable_function<void(std::string, bool, std::error_code)>&& callback)
{
  // Initiated on the strand: callers pull from arbitrary io threads, and the body's close() closes
  // stream_ through a stop() it posts to the strand.
  asio::dispatch(get_executor(),
                 [self = shared_from_this(), callback = std::move(callback)]() mutable {
                   self->do_read_some(std::move(callback));
                 });
}

void
http_session::do_read_some(read_callback&& callback)
{
  if (read_some_in_flight_) {
    queued_reads_.push_back(std::move(callback));
    return;
  }
  read_some_in_flight_ = true;
  if (stopped_ || !stream_->is_open()) {
    // Completed by the drain in finish_read_some(), never inline: this runs from that function's
    // first branch, which a noexcept scope guard reaches, so a callback throwing here would
    // terminate. At the front, since it was issued before anything still queued.
    queued_reads_.push_front(std::move(callback));
    return finish_read_some(errc::common::request_canceled, false);
  }
  return stream_->async_read_some(
    asio::buffer(input_buffer_),
    [self = shared_from_this(),
     callback = std::move(callback)](std::error_code ec, std::size_t bytes_transferred) mutable {
      // Declared first, so it runs last: after the callback and the stream-end handler.
      std::error_code queued_ec{};
      bool response_ended = false;
      const auto release = gsl::finally([&self, &queued_ec, &response_ended]() {
        self->finish_read_some(queued_ec, response_ended);
      });
      if (ec == asio::error::operation_aborted || self->stopped_) {
        CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}",
                        self->type_,
                        self->info_.remote_address(),
                        ec ? ec.message() : "ok",
                        bytes_transferred);
        queued_ec = errc::common::request_canceled;
        callback({}, {}, queued_ec);
        return;
      }
      CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}{:a}",
                      self->type_,
                      self->info_.remote_address(),
                      ec ? ec.message() : "ok",
                      bytes_transferred,
                      spdlog::to_hex(self->input_buffer_.data(),
                                     self->input_buffer_.data() +
                                       static_cast<std::ptrdiff_t>(bytes_transferred)));

      self->last_active_ = std::chrono::steady_clock::now();
      if (ec) {
        CB_LOG_ERROR(
          "{} IO error while reading from the socket: {}", self->info_.log_prefix(), ec.message());
        // Stopped before the callback, as on a parse failure: a throwing callback must not leave
        // the failed connection running and listed as busy.
        self->stop();
        queued_ec = ec;
        return callback({}, {}, ec);
      }
      http_streaming_parser::feeding_result res{};
      std::string data;
      streaming_response_context ctx{};
      bool stopped = false;
      {
        // One section, as in do_read(): stop() sets stopped_ before its cancel takes this mutex,
        // so the section either sees the stop or takes the parts of the response before the cancel.
        const std::scoped_lock lock(self->current_response_mutex_);
        stopped = self->stopped_;
        if (!stopped) {
          res = self->current_streaming_response_.parser.feed(
            reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred);
          if (!res.failure) {
            std::swap(data, self->current_streaming_response_.parser.body_chunk);
            if (res.complete) {
              std::swap(self->current_streaming_response_, ctx);
            }
          }
        }
      }
      if (stopped) {
        queued_ec = errc::common::request_canceled;
        return callback({}, {}, queued_ec);
      }
      if (res.failure) {
        self->stop();
        queued_ec = errc::common::parsing_failure;
        return callback({}, {}, queued_ec);
      }
      response_ended = res.complete;
      if (res.complete) {
        // Cleared with the end of the response: check_in then posts the liveness read's do_read().
        self->body_pulled_ = false;
        if (ctx.must_close_connection) {
          self->keep_alive_ = false;
        }
      }
      // The stream-end handler checks this connection back into the keep-alive pool, and must not
      // run until the body has observed the end of its response. Until it does,
      // http_streaming_response_body_impl still holds this session with reading_complete_ false,
      // and treats it as a response abandoned mid-body: a deadline expiry reaching close_impl in
      // that state stops the session. Checking in first publishes the connection while that is
      // still true, so the stop lands on whichever request took it out of the pool next.
      //
      // Check-in therefore waits for the whole body callback chain, which ends in consumer code: a
      // request issued from inside that callback does not find this connection pooled and opens
      // another. The guard runs the handler even if the callback throws, which would otherwise
      // leave the session checked out and never returned to the pool.
      const auto end_stream = gsl::finally([handler = std::move(ctx.stream_end_handler)]() mutable {
        if (handler) {
          handler();
        }
      });
      callback(std::move(data), !res.complete, {});
    });
}

void
http_session::finish_read_some(std::error_code ec, bool response_ended)
{
  if (!ec && !response_ended && !queued_reads_.empty()) {
    read_some_in_flight_ = false;
    auto next = std::move(queued_reads_.front());
    queued_reads_.pop_front();
    return do_read_some(std::move(next));
  }
  // Nothing more belongs to this response. Each queued read completes without touching the socket.
  if (queued_reads_.empty()) {
    read_some_in_flight_ = false;
    return notify_reads_drained();
  }
  if (!ec) {
    // A clean end: the stream-end handler has already checked the connection in, and another
    // request may have taken it out. Its reads must not queue behind these, which belong to the
    // response that ended, so they are moved out and the flag cleared before they drain.
    auto ended = std::make_shared<std::deque<read_callback>>();
    std::swap(*ended, queued_reads_);
    read_some_in_flight_ = false;
    ++ended_drains_;
    return drain_ended_reads(std::move(ended));
  }
  // After a failure the session is being torn down. The flag stays set until the queue is empty, so
  // a read_some() from one of these callbacks queues behind the rest and completes the same way.
  // One callback per handler posted to the strand, never inline: this function runs from noexcept
  // scope guards, where a throwing callback would terminate. The guard posts the next handler even
  // if the callback throws, so the exception reaches io_context::run() and the rest still complete
  // in order.
  asio::post(get_executor(), [self = shared_from_this(), ec]() {
    auto callback = std::move(self->queued_reads_.front());
    self->queued_reads_.pop_front();
    const auto next = gsl::finally([&self, ec]() {
      self->finish_read_some(ec, true);
    });
    callback({}, false, ec);
  });
}

void
http_session::on_reads_drained(utils::movable_function<void()>&& handler)
{
  reads_drained_.emplace_back(std::move(handler));
  notify_reads_drained();
}

void
http_session::notify_reads_drained()
{
  if (read_some_in_flight_ || ended_drains_ != 0) {
    return;
  }
  // Posted, never inline: this runs from noexcept scope guards.
  for (auto& handler : std::exchange(reads_drained_, {})) {
    asio::post(get_executor(), std::move(handler));
  }
}

void
http_session::drain_ended_reads(std::shared_ptr<std::deque<read_callback>> reads)
{
  if (reads->empty()) {
    --ended_drains_;
    return notify_reads_drained();
  }
  // Posted one per handler for the reason finish_read_some() posts its drain.
  asio::post(get_executor(), [self = shared_from_this(), reads]() {
    auto callback = std::move(reads->front());
    reads->pop_front();
    const auto next = gsl::finally([&self, &reads]() {
      self->drain_ended_reads(reads);
    });
    callback({}, false, {});
  });
}

void
http_session::on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
{
  if (ec == asio::error::operation_aborted || stopped_) {
    return;
  }
  if (ec) {
    CB_LOG_ERROR("{} error on resolve \"{}\": {}",
                 info_.log_prefix(),
                 logger::system_data(fmt::format("{}:{}", hostname_, service_)),
                 ec.message());
    return initiate_connect();
  }
  last_active_ = std::chrono::steady_clock::now();
  endpoints_ = endpoints;
  CB_LOG_TRACE("{} resolved \"{}\" to {} endpoint(s)",
               info_.log_prefix(),
               logger::system_data(fmt::format("{}:{}", hostname_, service_)),
               endpoints_.size());
  do_connect(endpoints_.begin());
}

void
http_session::do_connect(asio::ip::tcp::resolver::results_type::iterator it)
{
  if (stopped_) {
    return;
  }
  if (it != endpoints_.end()) {
    CB_LOG_DEBUG("{} connecting to {} (\"{}\"), timeout={}ms",
                 info_.log_prefix(),
                 logger::system_data(fmt::format(
                   "{}:{}", it->endpoint().address().to_string(), it->endpoint().port())),
                 logger::system_data(fmt::format("{}:{}", hostname_, service_)),
                 http_ctx_.options.connect_timeout.count());
    connect_deadline_timer_.expires_after(http_ctx_.options.connect_timeout);
    connect_deadline_timer_.async_wait(
      [self = shared_from_this(), it](const auto timer_ec) mutable {
        if (timer_ec == asio::error::operation_aborted || self->stopped_) {
          return;
        }
        CB_LOG_DEBUG("{} unable to connect to {} in time, reconnecting",
                     self->info_.log_prefix(),
                     logger::system_data(fmt::format("{}:{}", self->hostname_, self->service_)));
        return self->stream_->close([self, next_address = ++it](std::error_code ec) {
          if (ec) {
            CB_LOG_WARNING(
              "{} unable to close socket, but continue connecting attempt to {}: {}",
              self->info_.log_prefix(),
              logger::system_data(fmt::format("{}:{}",
                                              next_address->endpoint().address().to_string(),
                                              next_address->endpoint().port())),
              ec.value());
          }
          self->do_connect(next_address);
        });
      });

    stream_->async_connect(it->endpoint(), hostname_, [self = shared_from_this(), it](auto&& ec) {
      self->on_connect(std::forward<decltype(ec)>(ec), it);
    });
  } else {
    CB_LOG_ERROR("{} no more endpoints left to connect, \"{}\" is not reachable",
                 info_.log_prefix(),
                 logger::system_data(fmt::format("{}:{}", hostname_, service_)));
    return initiate_connect();
  }
}

void
http_session::on_connect(const std::error_code& ec,
                         asio::ip::tcp::resolver::results_type::iterator it)
{
  if (ec == asio::error::operation_aborted || stopped_) {
    return;
  }
  last_active_ = std::chrono::steady_clock::now();
  if (!stream_->is_open() || ec) {
    CB_LOG_WARNING("{} unable to connect to {}: {}{}",
                   info_.log_prefix(),
                   logger::system_data(fmt::format(
                     "{}:{}", it->endpoint().address().to_string(), it->endpoint().port())),
                   ec.message(),
                   (ec == asio::error::connection_refused)
                     ? ", check server ports and cluster encryption setting"
                     : "");
    if (stream_->is_open()) {
      stream_->close([self = shared_from_this(), next_address = ++it](std::error_code ec) {
        if (ec) {
          CB_LOG_WARNING(
            "{} unable to close socket, but continue connecting attempt to {}: {}",
            self->info_.log_prefix(),
            logger::system_data(fmt::format("{}:{}",
                                            next_address->endpoint().address().to_string(),
                                            next_address->endpoint().port())),
            ec.value());
        }
        self->do_connect(next_address);
      });
    } else {
      do_connect(++it);
    }
  } else {
    state_ = diag::endpoint_state::connected;
    connected_ = true;
    CB_LOG_DEBUG("{} connected to {}",
                 info_.log_prefix(),
                 logger::system_data(fmt::format(
                   "{}:{}", it->endpoint().address().to_string(), it->endpoint().port())));
    {
      const std::scoped_lock lock(info_mutex_);
      info_ = http_session_info(client_id_, id_, stream_->local_endpoint(), it->endpoint());
    }
    connect_deadline_timer_.cancel();
    fresh_connection_ = true;
    invoke_connect_callback();
    flush();
  }
}

void
http_session::do_read()
{
  // body_pulled_: past a streaming response's head the body belongs to read_some(), which reads the
  // same socket into the same buffer. A do_read() armed then, such as the one a write completion
  // starts after the head has already arrived, would take a part of that body and drop it.
  if (stopped_ || reading_ || body_pulled_ || !stream_->is_open()) {
    return;
  }
  reading_ = true;
  stream_->async_read_some(
    asio::buffer(input_buffer_),
    [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
      if (ec == asio::error::operation_aborted || self->stopped_) {
        CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}",
                        self->type_,
                        self->info_.remote_address(),
                        ec ? ec.message() : "ok",
                        bytes_transferred);
        return;
      }
      CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}{:a}",
                      self->type_,
                      self->info_.remote_address(),
                      ec ? ec.message() : "ok",
                      bytes_transferred,
                      spdlog::to_hex(self->input_buffer_.data(),
                                     self->input_buffer_.data() +
                                       static_cast<std::ptrdiff_t>(bytes_transferred)));

      self->last_active_ = std::chrono::steady_clock::now();
      if (ec) {
        if (self->idle_) {
          // Expected: the peer closed a pooled idle connection.  Tear it down
          // quietly so it is removed from the pool rather than reused dead.
          CB_LOG_DEBUG("{} idle HTTP connection closed by peer ({}), stopping session",
                       self->info_.log_prefix(),
                       ec.message());
        } else {
          if (self->resend_after_rejected_handshake(ec)) {
            return;
          }
          CB_LOG_ERROR("{} IO error while reading from the socket: {}",
                       self->info_.log_prefix(),
                       ec.message());
        }
        return self->stop();
      }
      if (self->idle_) {
        // A pooled keep-alive connection must not receive data until it is
        // checked out for a request (which clears idle_ via reset_idle()).
        // Unsolicited bytes mean the connection is in an unexpected state, so
        // drop it rather than feed them into an empty response parser.
        CB_LOG_DEBUG("{} unexpected data on idle HTTP connection, stopping session",
                     self->info_.log_prefix());
        return self->stop();
      }
      if (self->fresh_connection_.exchange(false)) {
        const std::scoped_lock lock(self->output_buffer_mutex_);
        self->unanswered_request_.clear();
      }

      if (self->streaming_response_) {
        // If streaming the response, read at least the entire header and then call the
        // streaming handler
        http_streaming_parser::feeding_result res{};
        {
          const std::scoped_lock lock(self->current_response_mutex_);
          res = self->current_streaming_response_.parser.feed(
            reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred);
        }
        if (res.failure) {
          return self->stop();
        }
        if (res.complete || res.headers_complete) {
          streaming_response_context ctx{};
          {
            const std::scoped_lock lock(self->current_response_mutex_);
            std::swap(self->current_streaming_response_, ctx);
          }

          http_streaming_response resp{ self->ctx_, ctx.parser, self };
          ctx.must_close_connection = resp.must_close_connection();
          ctx.parser.body_chunk = "";

          if (res.complete && ctx.must_close_connection) {
            self->keep_alive_ = false;
          }
          self->body_pulled_ = !res.complete;
          self->reading_ = false;
          // Runs even if the response handler throws. Skipped, the context holding the parser
          // would be dropped, so the rest of the body is fed to an empty parser and the stream-end
          // handler never runs.
          const auto reinstall_or_end = gsl::finally([&self, &ctx, complete = res.complete]() {
            if (!complete) {
              // A stop() that ran while ctx was swapped out, from the response handler or from
              // another thread, ended nothing, and a context reinstalled into a stopped session is
              // never ended: its stream-end handler keeps this session alive. stop() sets
              // stopped_ before its teardown takes this mutex, so either the teardown finds the
              // reinstalled context or this test sees the stop.
              const std::scoped_lock lock(self->current_response_mutex_);
              if (!self->stopped_) {
                std::swap(self->current_streaming_response_, ctx);
                return;
              }
            }
            if (auto handler = std::move(ctx.stream_end_handler); handler) {
              handler();
            }
          });
          if (auto handler = std::move(ctx.resp_handler); handler) {
            handler({}, std::move(resp));
          }
          return;
        }
        self->reading_ = false;
        return self->do_read();
      }
      http_parser::feeding_result res{};
      response_context ctx{};
      {
        // One section for the feed and the take. stop() sets stopped_ before its
        // cancel_current_response() takes this mutex, so either the stop is seen here and its
        // cancel completes the response, or the cancel runs after the take and finds no handler.
        // A cancel between a feed and a separate take would leave an empty context, whose handler
        // throws std::bad_function_call on the io thread.
        const std::scoped_lock lock(self->current_response_mutex_);
        if (self->stopped_) {
          return;
        }
        res = self->current_response_.parser.feed(
          reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred);
        if (res.complete) {
          std::swap(self->current_response_, ctx);
        }
      }
      if (res.failure) {
        return self->stop();
      }
      if (res.complete) {
        if (ctx.parser.response.must_close_connection()) {
          self->keep_alive_ = false;
        }
        ctx.handler({}, std::move(ctx.parser.response));
        self->reading_ = false;
        return;
      }
      self->reading_ = false;
      return self->do_read();
    });
}

void
http_session::do_write()
{
  if (stopped_) {
    return;
  }
  const std::scoped_lock lock(writing_buffer_mutex_, output_buffer_mutex_);
  if (!writing_buffer_.empty() || output_buffer_.empty()) {
    return;
  }
  std::swap(writing_buffer_, output_buffer_);
  // Recorded where the request is handed to the socket. A request a stop() keeps from this point
  // is not reported as written on this connection.
  record_dispatch();
  std::vector<asio::const_buffer> buffers;
  buffers.reserve(writing_buffer_.size());
  for (auto& buf : writing_buffer_) {
    CB_LOG_PROTOCOL("[HTTP, OUT] type={}, host=\"{}\", buffer_size={}{:a}",
                    type_,
                    info_.remote_address(),
                    buf.size(),
                    spdlog::to_hex(buf));
    buffers.emplace_back(asio::buffer(buf));
  }
  stream_->async_write(
    buffers, [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
      CB_LOG_PROTOCOL("[HTTP, OUT] type={}, host=\"{}\", rc={}, bytes_sent={}",
                      self->type_,
                      self->info_.remote_address(),
                      ec ? ec.message() : "ok",
                      bytes_transferred);
      if (ec == asio::error::operation_aborted || self->stopped_) {
        return;
      }
      self->last_active_ = std::chrono::steady_clock::now();
      if (ec) {
        if (self->resend_after_rejected_handshake(ec)) {
          return;
        }
        CB_LOG_ERROR(
          "{} IO error while writing to the socket: {}", self->info_.log_prefix(), ec.message());
        return self->stop();
      }
      {
        const std::scoped_lock inner_lock(self->writing_buffer_mutex_);
        self->writing_buffer_.clear();
      }
      bool want_write = false;
      {
        const std::scoped_lock inner_lock(self->output_buffer_mutex_);
        want_write = !self->output_buffer_.empty();
      }
      if (want_write) {
        return self->do_write();
      }
      self->do_read();
    });
}
} // namespace couchbase::core::io
