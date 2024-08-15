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
#include "pending_operation.hxx"
#include "pending_operation_connection_info.hxx"

#include <couchbase/build_config.hxx>

#include <asio/error.hpp>
#include <tl/expected.hpp>

#include <memory>
#include <utility>

namespace couchbase::core
{
class pending_http_operation
  : public std::enable_shared_from_this<pending_http_operation>
  , public pending_operation
  , public pending_operation_connection_info
{
public:
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  pending_http_operation(asio::io_context& io,
                         http_request request,
                         std::chrono::milliseconds dispatch_timeout)
    : deadline_{ io }
    , dispatch_deadline_{ io }
    , dispatch_timeout_{ dispatch_timeout }
    , request_{ std::move(request) }
    , encoded_{ io::http_request {
      request_.service,
      request_.method,
      request_.path,
      request_.headers,
      request_.body,
      {},
      request_.client_context_id,
    } }
  {
  }
#else
  pending_http_operation(asio::io_context& io, http_request request)
    : deadline_{ io }
    , request_{ std::move(request) }
    , encoded_{ io::http_request{
        request_.service,
        request_.method,
        request_.path,
        request_.headers,
        request_.body,
        {},
        {},
      } }
  {
  }
#endif

  ~pending_http_operation() override = default;
  pending_http_operation(const pending_http_operation&) = delete;
  pending_http_operation(pending_http_operation&&) = delete;
  auto operator=(const pending_http_operation&) -> pending_http_operation = delete;
  auto operator=(pending_http_operation&&) -> pending_http_operation = delete;

  void start(free_form_http_request_callback&& callback)
  {
    callback_ = std::move(callback);
    encoded_.headers["client-context-id"] = request_.client_context_id;
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    dispatch_deadline_.expires_after(dispatch_timeout_);
    dispatch_deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      CB_LOG_DEBUG(R"(HTTP request timed out: {}, method={}, path="{}", client_context_id={})",
                   self->encoded_.type,
                   self->encoded_.method,
                   self->encoded_.path,
                   self->encoded_.client_context_id);
      self->trigger_timeout();
      if (self->session_) {
        self->session_->stop();
      }
    });
#endif
    if (request_.timeout.has_value()) {
      deadline_.expires_after(request_.timeout.value());
      deadline_.async_wait([self = shared_from_this()](auto ec) {
        if (ec == asio::error::operation_aborted) {
          return;
        }
        CB_LOG_DEBUG(R"(HTTP request timed out: {}, method={}, path="{}", client_context_id={})",
                     self->encoded_.type,
                     self->encoded_.method,
                     self->encoded_.path,
                     self->encoded_.client_context_id);
        self->trigger_timeout();
        if (self->session_) {
          self->session_->stop();
        }
      });
    }
  }

  void set_stream_end_callback(utils::movable_function<void()>&& stream_end_callback)
  {
    stream_end_callback_ = std::move(stream_end_callback);
  }

  void cancel() override
  {
    if (session_) {
      session_->stop();
    }
    invoke_response_handler(errc::common::request_canceled, {});
  }

  void invoke_response_handler(std::error_code ec, io::http_streaming_response resp)
  {
    deadline_.cancel();
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    dispatch_deadline_.cancel();
#endif
    free_form_http_request_callback callback{};
    {
      const std::scoped_lock lock(callback_mutex_);
      std::swap(callback, callback_);
    }
    if (callback) {
      callback(http_response{ std::move(resp) }, ec);
    }
  }

  void send_to(std::shared_ptr<io::http_session> session)
  {
    if (!callback_) {
      return;
    }
    session_ = std::move(session);

    auto start_op = [self = shared_from_this()]() {
      self->session_->write_and_stream(
        self->encoded_,
        [self](std::error_code ec, io::http_streaming_response resp) {
          if (ec == asio::error::operation_aborted) {
            return;
          }
          self->invoke_response_handler(ec, std::move(resp));
        },
        [self]() {
          self->stream_end_callback_();
        });
    };

    start_op();
  }

  [[nodiscard]] auto deadline_expiry() const -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return deadline_.expiry();
  }

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR

  [[nodiscard]] auto dispatch_deadline_expiry() const
    -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return dispatch_deadline_.expiry();
  }

  [[nodiscard]] auto request() const -> http_request
  {
    return request_;
  }
#endif

  [[nodiscard]] auto dispatched_to() const -> std::string override
  {
    return session_->remote_address();
  }

  [[nodiscard]] auto dispatched_from() const -> std::string override
  {
    return session_->local_address();
  }

  [[nodiscard]] auto dispatched_to_host() const -> std::string override
  {
    return fmt::format("{}:{}", session_->hostname(), session_->port());
  }

private:
  void trigger_timeout()
  {
    // TODO(JC):  if triggered from the dispatch timeout, should only be
    // errc::common::unambiguous_timeout?
    auto ec =
      request_.is_read_only ? errc::common::unambiguous_timeout : errc::common::ambiguous_timeout;
    invoke_response_handler(ec, {});
  }

  asio::steady_timer deadline_;
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  asio::steady_timer dispatch_deadline_;
  std::chrono::milliseconds dispatch_timeout_;
#endif
  http_request request_;
  io::http_request encoded_;
  free_form_http_request_callback callback_;
  utils::movable_function<void()> stream_end_callback_;
  std::shared_ptr<io::http_session> session_;
  std::mutex callback_mutex_;
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

    cluster_credentials credentials;
    if (request.username.empty() && request.password.empty()) {
      auto [ec, origin] = shim_.cluster.origin();
      if (ec) {
        return tl::unexpected(ec);
      }
      credentials = origin.credentials();
    } else {
      credentials = cluster_credentials{ request.username, request.password };
    }

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    auto op =
      std::make_shared<pending_http_operation>(io_, request, session_manager->dispatch_timeout());
    if (!session_manager->is_configured()) {
      return defer_command(op, session_manager, credentials, std::move(callback));
    }
#else
    auto op = std::make_shared<pending_http_operation>(io_, request);
#endif

    std::shared_ptr<io::http_session> session;
    {
      auto [ec, s] = session_manager->check_out(
        request.service, credentials, request.endpoint, request.internal.undesired_endpoint);
      if (ec) {
        return tl::unexpected(ec);
      }
      session = std::move(s);
    }

    op->start([callback = std::move(callback)](auto resp, auto ec) mutable {
      callback(std::move(resp), ec);
    });
    op->set_stream_end_callback([session_manager, session, service = request.service]() mutable {
      session_manager->check_in(service, session);
    });

    if (!session->is_connected()) {
      session_manager->connect_then_send_pending_op(
        session,
        {},
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
        op->dispatch_deadline_expiry(),
#endif
        op->deadline_expiry(),
        [op](std::error_code ec, std::shared_ptr<io::http_session> http_session) {
          if (ec) {
            return op->invoke_response_handler(ec, {});
          }
          op->send_to(std::move(http_session));
        });
    } else {
      op->send_to(session);
    }
    return op;
  }

private:
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  auto defer_command(std::shared_ptr<pending_http_operation> pending_op,
                     const std::shared_ptr<io::http_session_manager>& session_manager,
                     const couchbase::core::cluster_credentials& credentials,
                     free_form_http_request_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
  {
    if (auto last_error = session_manager->last_bootstrap_error(); last_error.has_value()) {
      return tl::unexpected(last_error->ec);
    }
    pending_op->start([callback = std::move(callback)](auto resp, auto ec) mutable {
      callback(std::move(resp), ec);
    });
    CB_LOG_DEBUG(
      R"(Adding pending HTTP operation to deferred queue: service={}, client_context_id={})",
      pending_op->request().service,
      pending_op->request().client_context_id);
    session_manager->add_to_deferred_queue(
      [op = pending_op, session_manager, credentials](std::error_code ec) mutable {
        if (ec) {
          // The deferred operation was cancelled - currently this can happen due to closing the
          // cluster
          return op->invoke_response_handler(ec, {});
        }

        // don't do anything if the op wasn't dispatched or has already timed out
        auto now = std::chrono::steady_clock::now();
        if (op->dispatch_deadline_expiry() < now || op->deadline_expiry() < now) {
          return;
        }
        std::shared_ptr<io::http_session> session;
        {
          auto [check_out_ec, s] =
            session_manager->check_out(op->request().service,
                                       credentials,
                                       op->request().endpoint,
                                       op->request().internal.undesired_endpoint);
          if (check_out_ec) {
            return op->invoke_response_handler(check_out_ec, {});
          }
          session = std::move(s);
        }
        op->set_stream_end_callback(
          [session_manager, session, service = op->request().service]() mutable {
            session_manager->check_in(service, session);
          });
        if (!session->is_connected()) {
          session_manager->connect_then_send_pending_op(
            session,
            {},
            op->dispatch_deadline_expiry(),
            op->deadline_expiry(),
            [op](std::error_code ec, std::shared_ptr<io::http_session> http_session) {
              if (ec) {
                return op->invoke_response_handler(ec, {});
              }
              op->send_to(std::move(http_session));
            });
        } else {
          op->send_to(session);
        }
      });
    return pending_op;
  }
#endif

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

} // namespace couchbase::core
