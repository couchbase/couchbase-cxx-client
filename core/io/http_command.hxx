/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/build_config.hxx>

#include "core/app_telemetry_meter.hxx"
#include "core/impl/bootstrap_error.hxx"
#include "core/logger/redaction.hxx"
#include "core/metrics/meter_wrapper.hxx"
#include "core/service_type_fmt.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/movable_function.hxx"
#include "http_session.hxx"
#include "http_traits.hxx"

#include <couchbase/tracing/request_tracer.hxx>

#include <mutex>
#include <string_view>
#include <utility>

namespace couchbase::core::operations
{
template<typename Request>
struct http_command : public std::enable_shared_from_this<http_command<Request>> {
  using encoded_request_type = typename Request::encoded_request_type;
  using encoded_response_type = typename Request::encoded_response_type;
  using error_context_type = typename Request::error_context_type;
  using response_type = typename Request::response_type;
  using handler_type = utils::movable_function<void(response_type&&)>;

  asio::steady_timer deadline;
  Request request;
  encoded_request_type encoded;
  std::shared_ptr<tracing::tracer_wrapper> tracer_;
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
  std::shared_ptr<couchbase::tracing::request_span> span_{ nullptr };
#endif
  std::shared_ptr<metrics::meter_wrapper> meter_{};
  std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter_{ nullptr };
  // Not an owner: null once nothing else holds the session.
  std::weak_ptr<io::http_session> session_{};
  handler_type handler_{};
  std::mutex handler_mutex_{};
  // Set by send_to() if the request had not completed, guarded by handler_mutex_. Until then
  // session_for_check_in() returns null, so a completion before send_to() never pools the session.
  bool sent_{ false };
  // Guarded by handler_mutex_. complete() reports these, so its report does not depend on the
  // session still being alive. The target fields belong to the session set_command_session()
  // last assigned. The dispatched fields stay empty until send() dispatches.
  std::string target_node_uuid_{};
  std::string dispatched_from_{};
  std::string dispatched_to_{};
  std::string target_hostname_{};
  std::uint16_t target_port_{};
  std::chrono::milliseconds timeout_{};
  std::string client_context_id_;
  std::shared_ptr<couchbase::tracing::request_span> parent_span_{ nullptr };
  http_command(asio::io_context& ctx,
               Request req,
               std::shared_ptr<tracing::tracer_wrapper> tracer,
               std::shared_ptr<metrics::meter_wrapper> meter,
               std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter,
               std::chrono::milliseconds default_timeout)
    : deadline(ctx)
    , request(req)
    , tracer_(std::move(tracer))
    , meter_(std::move(meter))
    , app_telemetry_meter_(std::move(app_telemetry_meter))
    , timeout_(request.timeout.value_or(default_timeout))
    , client_context_id_(request.client_context_id.value_or(uuid::to_string(uuid::random())))
    , parent_span_(request.parent_span)
  {
  }

  void start(handler_type&& handler)
  {
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
    span_ = tracer_->create_span(tracing::span_name_for_http_service(request.type), parent_span_);
    if (span_->uses_tags()) {
      span_->add_tag(tracing::attributes::op::service,
                     tracing::service_name_for_http_service(request.type));
    }
#endif

    handler_ = std::move(handler);
    deadline.expires_after(timeout_);
    deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      std::error_code timeout = errc::common::ambiguous_timeout;
      if constexpr (io::http_traits::supports_readonly_v<Request>) {
        if (self->request.readonly) {
          timeout = errc::common::unambiguous_timeout;
        }
      }
      if (!self->cancel(timeout)) {
        return;
      }
      CB_LOG_DEBUG(R"(HTTP request timed out: {}, client_context_id="{}")",
                   self->request.type,
                   self->client_context_id_);
    });
  }

  // Stops the session, then completes the request with `ec`: the completion checks the session in,
  // and check_in refuses a stopped one. Returns false, and stops nothing, when the handler was
  // already taken, as by a response handled before a deadline completion already queued:
  // deadline.cancel() does not retract it, and the session then belongs to the pool.
  auto cancel(std::error_code ec) -> bool
  {
    handler_type handler{};
    std::shared_ptr<io::http_session> session;
    {
      const std::scoped_lock lock(handler_mutex_);
      handler = std::move(handler_);
      session = session_.lock();
    }
    if (!handler) {
      return false;
    }
    if (session) {
      session->stop();
    }
    complete(std::move(handler), ec, {});
    return true;
  }

  void invoke_handler(std::error_code ec, io::http_response&& msg)
  {
    complete(take_handler(), ec, std::move(msg));
  }

  // The handler is the token for completing the request. The deadline completion runs on the
  // io_context and the response on the session strand, so it is taken under handler_mutex_.
  auto take_handler() -> handler_type
  {
    const std::scoped_lock lock(handler_mutex_);
    return std::move(handler_);
  }

  void complete(handler_type handler, std::error_code ec, io::http_response&& msg)
  {
    if (handler) {
      std::string node_uuid;
      {
        const std::scoped_lock lock(handler_mutex_);
        node_uuid = target_node_uuid_;
      }
      auto telemetry_recorder = app_telemetry_meter_->value_recorder(node_uuid, {});
      telemetry_recorder->update_counter(total_counter_for_service_type(request.type));
      if (ec == errc::common::ambiguous_timeout || ec == errc::common::unambiguous_timeout) {
        telemetry_recorder->update_counter(timedout_counter_for_service_type(request.type));
      } else if (ec == errc::common::request_canceled) {
        telemetry_recorder->update_counter(canceled_counter_for_service_type(request.type));
      }
      encoded_response_type encoded_resp{ std::move(msg) };
      error_context_type ctx{};
      ctx.ec = ec;
      ctx.client_context_id = client_context_id_;
      ctx.method = encoded.method;
      ctx.path = encoded.path;
      ctx.http_status = encoded_resp.status_code;
      ctx.http_body = encoded_resp.body.data();
      {
        const std::scoped_lock lock(handler_mutex_);
        // A request completed before any dispatch leaves last_dispatched_from/to unset.
        if (!dispatched_to_.empty()) {
          // A resend after a refused client certificate writes the request on a new connection.
          if (const auto session = session_.lock(); session) {
            if (auto [from, to] = session->dispatched_endpoints(); !to.empty()) {
              dispatched_from_ = std::move(from);
              dispatched_to_ = std::move(to);
            }
          }
          ctx.last_dispatched_from = dispatched_from_;
          ctx.last_dispatched_to = dispatched_to_;
        }
        ctx.hostname = target_hostname_;
        ctx.port = target_port_;
      }

      // Can raise priv::retry_http_request when a retry is required
      auto resp = request.make_response(std::move(ctx), std::move(encoded_resp));

#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
      span_->end();
      span_ = nullptr;
#endif

      handler(std::move(resp));
    }
    deadline.cancel();
  }

  void send_to()
  {
    bool completed = false;
    {
      const std::scoped_lock lock(handler_mutex_);
      completed = !handler_;
      sent_ = !completed;
    }
    if (completed) {
      // The completion ran with sent_ false, so session_for_check_in() gave it no session. Stopped
      // here unless cancel() already stopped it; on_stop takes it off the busy list.
      if (const auto session = session_.lock(); session) {
        session->stop();
      }
      return;
    }
    send();
  }

  void set_command_session(const std::shared_ptr<io::http_session>& session)
  {
    const std::scoped_lock lock(handler_mutex_);
    session_ = session;
    target_node_uuid_ = session->node_uuid();
    target_hostname_ = session->http_context().hostname;
    target_port_ = session->http_context().port;
  }

  // The session the completion checks in: the one send_to() sent on, or null. A completion before
  // send_to() gets null, so a session send_to() is about to stop is never pooled first.
  [[nodiscard]] auto session_for_check_in() -> std::shared_ptr<io::http_session>
  {
    const std::scoped_lock lock(handler_mutex_);
    return sent_ ? session_.lock() : nullptr;
  }

  [[nodiscard]] auto deadline_expiry() const -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return deadline.expiry();
  }

private:
  void send()
  {
    encoded.type = request.type;
    encoded.client_context_id = client_context_id_;
    encoded.timeout = timeout_;
    const auto session = session_.lock();
    if (!session) {
      return invoke_handler(errc::common::request_canceled, {});
    }
    if (auto ec = request.encode_to(encoded, session->http_context()); ec) {
      return invoke_handler(ec, {});
    }
    encoded.headers["client-context-id"] = client_context_id_;

    CB_LOG_TRACE(
      R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms)",
      session->log_prefix(),
      encoded.type,
      encoded.method,
      logger::user_data(encoded.path),
      client_context_id_,
      timeout_.count());

    auto dispatch_span = create_dispatch_span(*session);

    {
      const std::scoped_lock lock(handler_mutex_);
      dispatched_from_ = session->local_address();
      dispatched_to_ = session->remote_address();
    }

    session->write_and_subscribe(
      encoded,
      [self = this->shared_from_this(),
       session,
       dispatch_span = std::move(dispatch_span),
       start = std::chrono::steady_clock::now()](std::error_code ec, io::http_response&& msg) {
        if (ec == asio::error::operation_aborted) {
          dispatch_span->end();
          return self->invoke_handler(errc::common::ambiguous_timeout, std::move(msg));
        }

        dispatch_span->end();

        {
          auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
          self->app_telemetry_meter_->value_recorder(session->node_uuid(), {})
            ->record_latency(latency_for_service_type(self->request.type), latency);
        }

#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
        if (self->meter_) {
          metrics::metric_attributes attrs{
            tracing::service_name_for_http_service(self->request.type),
            self->request.observability_identifier,
            metrics::standardized_error_type(ec),
          };
          self->meter_->record_value(std::move(attrs), start);
        }
#endif

        self->deadline.cancel();
        CB_LOG_TRACE(R"({} HTTP response: {}, client_context_id="{}", ec={}, status={}, body={})",
                     session->log_prefix(),
                     self->request.type,
                     self->client_context_id_,
                     ec.message(),
                     msg.status_code,
                     // Views, not strings: the arms have different types, so a plain ternary
                     // built a std::string and copied the whole body in on every non-200.
                     // Only the arm that carries a body is tagged. "[hidden]" is a constant the
                     // SDK substituted, and hashing it would leave a reader unable to tell a body
                     // the SDK withheld from one the redaction tool replaced.
                     logger::user_data_if(msg.status_code != 200,
                                          msg.status_code == 200
                                            ? std::string_view{ "[hidden]" }
                                            : std::string_view{ msg.body.data() }));
        if (auto parser_ec = msg.body.ec(); !ec && parser_ec) {
          ec = parser_ec;
        }
        try {
          self->invoke_handler(ec, std::move(msg));
        } catch (const priv::retry_http_request&) {
          self->send();
        }
      });
  }

  [[nodiscard]] auto create_dispatch_span(io::http_session& session) const
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
#ifdef COUCHBASE_CXX_CLIENT_CREATE_OPERATION_SPAN_IN_CORE
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span =
      tracer_->create_span(tracing::operation::step_dispatch, span_);
#else
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span =
      tracer_->create_span(tracing::operation::step_dispatch, parent_span_);
#endif
    if (dispatch_span->uses_tags()) {
      dispatch_span->add_tag(tracing::attributes::dispatch::network_transport, "tcp");
      dispatch_span->add_tag(tracing::attributes::dispatch::operation_id, client_context_id_);
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
};

} // namespace couchbase::core::operations
