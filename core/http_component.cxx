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

#include <asio/error.hpp>
#include <tl/expected.hpp>

#include <memory>
#include <utility>

namespace couchbase::core
{
class pending_http_operation
  : public std::enable_shared_from_this<pending_http_operation>
  , public pending_operation
{
public:
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
        request_.timeout,
      } }
  {
  }

  ~pending_http_operation() override = default;

  void start(free_form_http_request_callback&& callback,
             utils::movable_function<void()>&& stream_end_callback)
  {
    callback_ = std::move(callback);
    stream_end_callback_ = std::move(stream_end_callback);
    deadline_.expires_after(request_.timeout);
    deadline_.async_wait([self = shared_from_this()](auto ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      self->trigger_timeout();
      if (self->session_) {
        self->session_->stop();
      }
    });
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
    std::scoped_lock lock(callback_mutex_);
    if (callback_) {
      callback_(http_response{ std::move(resp) }, ec);
    }
    callback_ = nullptr;
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

    // TODO(dimitris): Connecting should be retried.
    if (!session_->is_connected()) {
      session_->connect([self = shared_from_this(), start_op = std::move(start_op)]() {
        if (!self->session_->is_connected()) {
          self->invoke_response_handler(couchbase::errc::common::request_canceled, {});
          return;
        }
        start_op();
      });
    } else {
      start_op();
    }
  }

private:
  void trigger_timeout()
  {
    auto ec =
      request_.is_read_only ? errc::common::unambiguous_timeout : errc::common::ambiguous_timeout;
    invoke_response_handler(ec, {});
  }

  asio::steady_timer deadline_;
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

  auto do_http_request(http_request request, free_form_http_request_callback&& callback)
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

    std::shared_ptr<io::http_session> session;
    {
      auto [ec, s] = session_manager->check_out(request.service, credentials, request.endpoint);
      if (ec) {
        return tl::unexpected(ec);
      }
      session = std::move(s);
    }

    auto op = std::make_shared<pending_http_operation>(io_, request);
    op->start(
      [callback = std::move(callback)](auto resp, auto ec) mutable {
        callback(std::move(resp), ec);
      },
      [session_manager, session, service = request.service]() mutable {
        session_manager->check_in(service, session);
      });
    op->send_to(session);

    return op;
  }

private:
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
http_component::do_http_request(http_request request, free_form_http_request_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
  return impl_->do_http_request(std::move(request), std::move(callback));
}

} // namespace couchbase::core
