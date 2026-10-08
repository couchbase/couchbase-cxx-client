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

#include "http_component.hxx"

#include "free_form_http_request.hxx"
#include "io/http_session_manager.hxx"
#include "logger/redaction.hxx"
#include "pending_operation.hxx"
#include "pending_operation_connection_info.hxx"
#include "tracing/constants.hxx"
#include "tracing/tracer_wrapper.hxx"

#include <asio/error.hpp>
#include <spdlog/fmt/bundled/chrono.h>
#include <tl/expected.hpp>

#include <memory>
#include <utility>

namespace couchbase::core
{
namespace
{
auto
encode_http_request(const http_request& req) -> io::http_request
{
  return io::http_request{
    req.service, req.method, req.path, req.headers, req.body, {}, req.client_context_id,
  };
}
} // namespace

class pending_http_operation
  : public std::enable_shared_from_this<pending_http_operation>
  , public pending_operation
  , public pending_operation_connection_info
{
public:
  pending_http_operation(asio::io_context& io, http_request request)
    : deadline_{ io }
    , request_{ std::move(request) }
    , encoded_{ encode_http_request(request_) }
  {
  }

  ~pending_http_operation() override = default;
  pending_http_operation(const pending_http_operation&) = delete;
  pending_http_operation(pending_http_operation&&) = delete;
  auto operator=(const pending_http_operation&) -> pending_http_operation = delete;
  auto operator=(pending_http_operation&&) -> pending_http_operation = delete;

  void start(free_form_http_request_callback&& callback)
  {
    callback_ = std::move(callback);
    encoded_.headers["client-context-id"] = request_.client_context_id;
    deadline_.expires_after(request_.timeout);
    deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      // deadline_.cancel() does not retract a completion already queued. A response that took
      // the callback first has handed the session to the stream or to the pool.
      if (!self->trigger_timeout()) {
        return;
      }
      CB_LOG_DEBUG(
        R"(HTTP request timed out: {}, method={}, path="{}", timeout={}, client_context_id={})",
        self->encoded_.type,
        self->encoded_.method,
        logger::user_data(self->encoded_.path),
        self->request_.timeout,
        self->encoded_.client_context_id);
    });
  }

  // Receives the session the stream was dispatched on, which a failover makes different from the
  // one checked out.
  void set_stream_end_callback(
    utils::movable_function<void(std::shared_ptr<io::http_session>)>&& stream_end_callback)
  {
    stream_end_callback_ = std::move(stream_end_callback);
  }

  void set_tracer(std::shared_ptr<tracing::tracer_wrapper> tracer)
  {
    tracer_ = std::move(tracer);
  }

  // Once the stream has ended its connection is back in the pool, possibly serving another
  // request, so cancel() leaves it alone. A cancel that comes first keeps the stream end from
  // checking the connection in.
  void cancel() override
  {
    std::shared_ptr<io::http_session> session;
    {
      const std::scoped_lock lock(callback_mutex_);
      if (stream_ended_) {
        return;
      }
      cancelled_ = true;
      session = session_.lock();
    }
    if (session) {
      session->stop();
    }
    invoke_response_handler(errc::common::request_canceled, {});
  }

  // Returns whether this call took the callback, which is the token for completing the request.
  auto invoke_response_handler(std::error_code err, io::http_streaming_response resp) -> bool
  {
    deadline_.cancel();
    free_form_http_request_callback callback{};
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span{};
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
      std::swap(dispatch_span, dispatch_span_);
    }
    // The dispatch span covers the request up to the response headers, not the streamed body.
    if (dispatch_span) {
      dispatch_span->end();
    }
    if (!callback) {
      return false;
    }
    callback(http_response{ std::move(resp) }, err);
    return true;
  }

  void send_to(std::shared_ptr<io::http_session> session)
  {
    {
      // The deadline or a cancel can complete the operation concurrently. Checking the callback and
      // storing the span under the lock that invoke_response_handler takes means the span is either
      // stored before the handler runs, and ended by it, or never created.
      std::unique_lock lock(callback_mutex_);
      if (!callback_) {
        // The session is listed busy and nothing checks it in; on_stop removes it.
        lock.unlock();
        return session->stop();
      }
      session_ = session;
      dispatched_to_ = session->remote_address();
      dispatched_from_ = session->local_address();
      dispatched_to_host_ = fmt::format("{}:{}", session->hostname(), session->port());
      dispatch_span_ = create_dispatch_span(*session);
    }

    auto start_op = [self = shared_from_this(), session]() {
      session->write_and_stream(
        self->encoded_,
        [self](std::error_code ec, io::http_streaming_response resp) {
          if (ec == asio::error::operation_aborted) {
            return;
          }
          self->invoke_response_handler(ec, std::move(resp));
        },
        [self]() {
          self->stream_end_callback_(self->session_at_stream_end());
        });
    };

    start_op();
  }

  [[nodiscard]] auto deadline_expiry() const -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return deadline_.expiry();
  }

  [[nodiscard]] auto request() const -> http_request
  {
    return request_;
  }

  [[nodiscard]] auto dispatched_to() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_to_;
  }

  [[nodiscard]] auto dispatched_from() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_from_;
  }

  [[nodiscard]] auto dispatched_to_host() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_to_host_;
  }

private:
  // Null before send_to() runs or once the session is gone: the operation does not keep its
  // session alive.
  [[nodiscard]] auto session() const -> std::shared_ptr<io::http_session>
  {
    const std::scoped_lock lock(callback_mutex_);
    return session_.lock();
  }

  // The session to check in at stream end: null if cancel() or the deadline came first, which stops
  // it.
  [[nodiscard]] auto session_at_stream_end() -> std::shared_ptr<io::http_session>
  {
    const std::scoped_lock lock(callback_mutex_);
    stream_ended_ = true;
    if (cancelled_) {
      return nullptr;
    }
    return session_.lock();
  }

  // Ends the request with a timeout unless a response took the callback first. The session is
  // stopped before the callback runs, and a stream end does not check it in. Returns whether this
  // call took the callback.
  auto trigger_timeout() -> bool
  {
    // TODO(JC):  if triggered from the dispatch timeout, should only be
    // errc::common::unambiguous_timeout?
    auto ec =
      request_.is_read_only ? errc::common::unambiguous_timeout : errc::common::ambiguous_timeout;
    free_form_http_request_callback callback{};
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span{};
    std::shared_ptr<io::http_session> session;
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
      std::swap(dispatch_span, dispatch_span_);
      if (callback) {
        cancelled_ = true;
      }
      session = session_.lock();
    }
    if (dispatch_span) {
      dispatch_span->end();
    }
    if (!callback) {
      return false;
    }
    if (session) {
      session->stop();
    }
    callback(http_response{ io::http_streaming_response{} }, ec);
    return true;
  }

  // Carries the same tags as http_command::create_dispatch_span, so a streamed request reports the
  // same dispatch_to_server span as a buffered one. It is parented to the caller's span because no
  // operation span exists in core for a streamed request.
  [[nodiscard]] auto create_dispatch_span(io::http_session& session) const
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
    if (!tracer_) {
      return {};
    }
    auto dispatch_span =
      tracer_->create_span(tracing::operation::step_dispatch, request_.parent_span);
    if (dispatch_span->uses_tags()) {
      dispatch_span->add_tag(tracing::attributes::dispatch::network_transport, "tcp");
      dispatch_span->add_tag(tracing::attributes::dispatch::operation_id,
                             request_.client_context_id);
      dispatch_span->add_tag(tracing::attributes::dispatch::local_id, session.id());
      dispatch_span->add_tag(tracing::attributes::dispatch::server_address,
                             session.http_context().canonical_hostname);
      dispatch_span->add_tag(tracing::attributes::dispatch::server_port,
                             session.http_context().canonical_port);

      const auto& peer_endpoint = session.remote_endpoint();
      dispatch_span->add_tag(tracing::attributes::dispatch::peer_address,
                             peer_endpoint.address().to_string());
      dispatch_span->add_tag(tracing::attributes::dispatch::peer_port, peer_endpoint.port());
    }
    return dispatch_span;
  }

  asio::steady_timer deadline_;
  http_request request_;
  io::http_request encoded_;
  free_form_http_request_callback callback_;
  std::shared_ptr<tracing::tracer_wrapper> tracer_{};
  std::shared_ptr<couchbase::tracing::request_span> dispatch_span_{};
  utils::movable_function<void(std::shared_ptr<io::http_session>)> stream_end_callback_;
  std::weak_ptr<io::http_session> session_;
  // Endpoints of the session send_to() dispatched on, guarded by callback_mutex_. Recorded because
  // session_ expires once the session is released.
  std::string dispatched_to_;
  std::string dispatched_from_;
  std::string dispatched_to_host_;
  // Guarded by callback_mutex_.
  bool stream_ended_{ false };
  bool cancelled_{ false };
  mutable std::mutex callback_mutex_;
};

class pending_buffered_http_operation
  : public std::enable_shared_from_this<pending_buffered_http_operation>
  , public pending_operation
  , public pending_operation_connection_info
{
public:
  pending_buffered_http_operation(asio::io_context& io, http_request request)
    : deadline_{ io }
    , request_{ std::move(request) }
    , encoded_{ encode_http_request(request_) }
  {
  }

  ~pending_buffered_http_operation() override = default;
  pending_buffered_http_operation(const pending_buffered_http_operation&) = delete;
  pending_buffered_http_operation(pending_buffered_http_operation&&) = delete;
  auto operator=(const pending_buffered_http_operation&)
    -> pending_buffered_http_operation = delete;
  auto operator=(pending_buffered_http_operation&&) -> pending_buffered_http_operation = delete;

  void start(buffered_free_form_http_request_callback&& callback)
  {
    callback_ = std::move(callback);
    encoded_.headers["client-context-id"] = request_.client_context_id;
    deadline_.expires_after(request_.timeout);
    deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      // deadline_.cancel() does not retract a completion already queued. A response that took
      // the callback first has handed the session back to the pool.
      if (!self->trigger_timeout()) {
        return;
      }
      CB_LOG_DEBUG(
        R"(HTTP request timed out: {}, method={}, path="{}", timeout={}, client_context_id={})",
        self->encoded_.type,
        self->encoded_.method,
        logger::user_data(self->encoded_.path),
        self->request_.timeout,
        self->encoded_.client_context_id);
    });
  }

  // Takes the callback, then stops the session before running it: the callback checks the session
  // in, and check_in refuses a stopped one. With no callback left the operation has completed, and
  // its connection may be serving another request.
  void cancel() override
  {
    buffered_free_form_http_request_callback callback{};
    std::shared_ptr<io::http_session> session;
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
      session = session_.lock();
    }
    if (!callback) {
      return;
    }
    deadline_.cancel();
    if (session) {
      session->stop();
    }
    callback(buffered_http_response{ io::http_response{} }, errc::common::request_canceled);
  }

  // Returns whether this call took the callback, which is the token for completing the request.
  auto invoke_response_handler(std::error_code ec, io::http_response resp) -> bool
  {
    deadline_.cancel();
    buffered_free_form_http_request_callback callback{};
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
    }
    if (!callback) {
      return false;
    }
    callback(buffered_http_response{ std::move(resp) }, ec);
    return true;
  }

  void send_to(std::shared_ptr<io::http_session> session)
  {
    {
      // Checked and stored under the lock invoke_response_handler takes the callback with: a
      // deadline completion that took the callback reads this session, and after it took the
      // callback nothing is stored.
      std::unique_lock lock(callback_mutex_);
      if (!callback_) {
        // The completion checks in only a session stored here, so this one stays listed busy
        // until on_stop removes it.
        lock.unlock();
        return session->stop();
      }
      session_ = session;
      dispatched_to_ = session->remote_address();
      dispatched_from_ = session->local_address();
      dispatched_to_host_ = fmt::format("{}:{}", session->hostname(), session->port());
    }

    session->write_and_subscribe(
      encoded_, [self = shared_from_this()](std::error_code ec, io::http_response resp) {
        if (ec == asio::error::operation_aborted) {
          return;
        }
        self->invoke_response_handler(ec, std::move(resp));
      });
  }

  // The session send_to() dispatched on, which a failover makes different from the one checked
  // out. Null if send_to() never ran or the session is gone: the operation does not keep its
  // session alive.
  [[nodiscard]] auto session() const -> std::shared_ptr<io::http_session>
  {
    const std::scoped_lock lock(callback_mutex_);
    return session_.lock();
  }

  [[nodiscard]] auto deadline_expiry() const -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return deadline_.expiry();
  }

  [[nodiscard]] auto request() const -> http_request
  {
    return request_;
  }

  [[nodiscard]] auto dispatched_to() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_to_;
  }

  [[nodiscard]] auto dispatched_from() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_from_;
  }

  [[nodiscard]] auto dispatched_to_host() const -> std::string override
  {
    const std::scoped_lock lock(callback_mutex_);
    return dispatched_to_host_;
  }

private:
  // Ends the request with a timeout unless a response took the callback first. The session is
  // stopped before the callback runs: the callback checks the session in, and check_in refuses a
  // stopped one. Returns whether this call took the callback.
  auto trigger_timeout() -> bool
  {
    // TODO(JC):  if triggered from the dispatch timeout, should only be
    // errc::common::unambiguous_timeout?
    auto ec =
      request_.is_read_only ? errc::common::unambiguous_timeout : errc::common::ambiguous_timeout;
    buffered_free_form_http_request_callback callback{};
    std::shared_ptr<io::http_session> session;
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
      session = session_.lock();
    }
    if (!callback) {
      return false;
    }
    if (session) {
      session->stop();
    }
    callback(buffered_http_response{ io::http_response{} }, ec);
    return true;
  }

  asio::steady_timer deadline_;
  http_request request_;
  io::http_request encoded_;
  buffered_free_form_http_request_callback callback_;
  std::weak_ptr<io::http_session> session_;
  // Endpoints of the session send_to() dispatched on, guarded by callback_mutex_. Recorded because
  // session_ expires once the session is released.
  std::string dispatched_to_;
  std::string dispatched_from_;
  std::string dispatched_to_host_;
  mutable std::mutex callback_mutex_;
};

class http_component_impl
{
public:
  http_component_impl(asio::io_context& io,
                      core_sdk_shim shim,
                      std::shared_ptr<retry_strategy> default_retry_strategy)
    : io_{ io }
    , shim_{ std::move(shim) }
    , default_retry_strategy_{ std::move(default_retry_strategy) }
  {
  }

  auto do_http_request(const http_request& request, free_form_http_request_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    std::shared_ptr<io::http_session_manager> session_manager;
    {
      auto [ec, sm] = shim_.cluster.http_session_manager();
      if (ec) {
        return tl::unexpected(ec);
      }
      session_manager = std::move(sm);
    }
    auto op = std::make_shared<pending_http_operation>(io_, request);

    send_http_operation(op, session_manager, std::move(callback));
    return op;
  }

  auto do_http_request_buffered(const http_request& request,
                                buffered_free_form_http_request_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    std::shared_ptr<io::http_session_manager> session_manager;
    {
      auto [ec, sm] = shim_.cluster.http_session_manager();
      if (ec) {
        return tl::unexpected(ec);
      }
      session_manager = std::move(sm);
    }

    auto op = std::make_shared<pending_buffered_http_operation>(io_, request);

    send_http_operation(op, session_manager, std::move(callback));
    return op;
  }

private:
  void send_http_operation(const std::shared_ptr<pending_http_operation>& op,
                           const std::shared_ptr<io::http_session_manager>& session_manager,
                           free_form_http_request_callback&& callback)
  {
    op->start([callback = std::move(callback)](auto resp, auto ec) mutable {
      callback(std::move(resp), ec);
    });
    std::shared_ptr<io::http_session> session;
    {
      auto [check_out_ec, s] = session_manager->check_out(
        op->request().service, op->request().endpoint, op->request().internal.undesired_endpoint);
      if (check_out_ec) {
        op->invoke_response_handler(check_out_ec, {});
        return;
      }
      session = std::move(s);
    }
    // Not an owner: a completed operation that never reaches the stream end must not keep the
    // manager alive.
    op->set_stream_end_callback(
      [manager = std::weak_ptr{ session_manager },
       service = op->request().service](std::shared_ptr<io::http_session> served) {
        if (const auto m = manager.lock(); m) {
          m->check_in(service, std::move(served));
        }
      });
    op->set_tracer(session_manager->tracer());
    if (!session->is_connected()) {
      session_manager->connect_then_send_pending_op(
        session,
        {},
        op->deadline_expiry(),
        [op](std::error_code ec, std::shared_ptr<io::http_session> http_session) {
          if (ec) {
            op->invoke_response_handler(ec, {});
            return;
          }
          op->send_to(std::move(http_session));
        });
    } else {
      op->send_to(session);
    }
  }

  void send_http_operation(const std::shared_ptr<pending_buffered_http_operation>& op,
                           const std::shared_ptr<io::http_session_manager>& session_manager,
                           buffered_free_form_http_request_callback&& callback)
  {
    std::shared_ptr<io::http_session> session;
    {
      auto [check_out_ec, s] = session_manager->check_out(
        op->request().service, op->request().endpoint, op->request().internal.undesired_endpoint);
      if (check_out_ec) {
        op->invoke_response_handler(check_out_ec, {});
        return;
      }
      session = std::move(s);
    }
    // Checks in only the session send_to() stored. A completion before send_to() leaves the
    // checked-out session to send_to(), which stops it. Checked in here, it could be taken by
    // another request before that stop.
    op->start([callback = std::move(callback),
               session_manager,
               weak_op = std::weak_ptr<pending_buffered_http_operation>{ op },
               service = op->request().service](auto resp, auto ec) mutable {
      callback(std::move(resp), ec);
      if (auto self = weak_op.lock(); self) {
        session_manager->check_in(service, self->session());
      }
    });

    if (!session->is_connected()) {
      session_manager->connect_then_send_pending_op(
        session,
        {},
        op->deadline_expiry(),
        [op](std::error_code ec, std::shared_ptr<io::http_session> http_session) {
          if (ec) {
            op->invoke_response_handler(ec, {});
            return;
          }
          op->send_to(std::move(http_session));
        });
    } else {
      op->send_to(session);
    }
  }

  asio::io_context& io_;
  core_sdk_shim shim_;
  std::shared_ptr<retry_strategy> default_retry_strategy_;
};

http_component::http_component(asio::io_context& io,
                               core_sdk_shim shim,
                               std::shared_ptr<retry_strategy> default_retry_strategy)
  : impl_{
    std::make_shared<http_component_impl>(io, std::move(shim), std::move(default_retry_strategy))
  }
{
}

auto
http_component::do_http_request(const http_request& request,
                                free_form_http_request_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->do_http_request(request, std::move(callback));
}

auto
http_component::do_http_request_buffered(const couchbase::core::http_request& request,
                                         buffered_free_form_http_request_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->do_http_request_buffered(request, std::move(callback));
}

} // namespace couchbase::core
