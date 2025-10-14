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
#include "core/metrics/meter_wrapper.hxx"
#include "core/service_type_fmt.hxx"
#include "core/tracing/constants.hxx"
#include "core/tracing/tracer_wrapper.hxx"
#include "core/utils/movable_function.hxx"
#include "http_session.hxx"
#include "http_traits.hxx"

#include <couchbase/tracing/request_tracer.hxx>

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
  std::shared_ptr<couchbase::tracing::request_span> span_{ nullptr };
  std::shared_ptr<metrics::meter_wrapper> meter_{};
  std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter_{ nullptr };
  std::shared_ptr<io::http_session> session_{};
  handler_type handler_{};
  std::chrono::milliseconds timeout_{};
  std::string client_context_id_;
  std::shared_ptr<couchbase::tracing::request_span> parent_span_{ nullptr };
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  std::chrono::milliseconds dispatch_timeout_{};
  asio::steady_timer dispatch_deadline_;

  http_command(asio::io_context& ctx,
               Request req,
               std::shared_ptr<tracing::tracer_wrapper> tracer,
               std::shared_ptr<metrics::meter_wrapper> meter,
               std::shared_ptr<core::app_telemetry_meter> app_telemetry_meter,
               std::chrono::milliseconds default_timeout,
               std::chrono::milliseconds dispatch_timeout)
    : deadline(ctx)
    , request(req)
    , tracer_(std::move(tracer))
    , meter_(std::move(meter))
    , app_telemetry_meter_(std::move(app_telemetry_meter))
    , timeout_(request.timeout.value_or(default_timeout))
    , client_context_id_(request.client_context_id.value_or(uuid::to_string(uuid::random())))
    , parent_span_(request.parent_span)
    , dispatch_timeout_(dispatch_timeout)
    , dispatch_deadline_(ctx)
  {
  }
#else
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
#endif

  void start(handler_type&& handler)
  {
    span_ = tracer_->create_span(tracing::span_name_for_http_service(request.type), parent_span_);
    if (span_->uses_tags()) {
      span_->add_tag(tracing::attributes::service,
                     tracing::service_name_for_http_service(request.type));
    }

    handler_ = std::move(handler);
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    dispatch_deadline_.expires_after(dispatch_timeout_);
    dispatch_deadline_.async_wait([self = this->shared_from_this()](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      CB_LOG_DEBUG(R"(HTTP request timed out before dispatch: {}, client_context_id="{}")",
                   self->request.type,
                   self->client_context_id_);
      self->cancel(errc::common::unambiguous_timeout);
    });
#endif
    deadline.expires_after(timeout_);
    deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
      if (ec == asio::error::operation_aborted) {
        return;
      }
      CB_LOG_DEBUG(R"(HTTP request timed out: {}, client_context_id="{}")",
                   self->request.type,
                   self->client_context_id_);
      if constexpr (io::http_traits::supports_readonly_v<Request>) {
        if (self->request.readonly) {
          self->cancel(errc::common::unambiguous_timeout);
          return;
        }
      }
      self->cancel(errc::common::ambiguous_timeout);
    });
  }

  void cancel(std::error_code ec)
  {
    invoke_handler(ec, {});
    if (session_) {
      session_->stop();
    }
  }

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  void invoke_handler(error_union error, io::http_response&& msg)
#else
  void invoke_handler(std::error_code ec, io::http_response&& msg)
#endif
  {
    if (handler_type handler = std::move(handler_); handler) {
      const auto& node_uuid = session_ ? session_->node_uuid() : "";
      auto telemetry_recorder = app_telemetry_meter_->value_recorder(node_uuid, {});
      telemetry_recorder->update_counter(total_counter_for_service_type(request.type));
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
      std::error_code ec{};
      if (std::holds_alternative<std::error_code>(error)) {
        ec = std::get<std::error_code>(error);
      } else if (std::holds_alternative<impl::bootstrap_error>(error)) {
        ec = std::get<impl::bootstrap_error>(error).ec;
      }
#endif
      if (ec == errc::common::ambiguous_timeout || ec == errc::common::unambiguous_timeout) {
        telemetry_recorder->update_counter(timedout_counter_for_service_type(request.type));
      } else if (ec == errc::common::request_canceled) {
        telemetry_recorder->update_counter(canceled_counter_for_service_type(request.type));
      }
      encoded_response_type encoded_resp{ std::move(msg) };
      error_context_type ctx{};
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
      if (!std::holds_alternative<std::monostate>(error)) {
        if (std::holds_alternative<impl::bootstrap_error>(error)) {
          auto bootstrap_error = std::get<impl::bootstrap_error>(error);
          if (bootstrap_error.ec == errc::common::unambiguous_timeout) {
            CB_LOG_DEBUG("Timeout caused by bootstrap error. code={}, ec_message={}, message={}.",
                         bootstrap_error.ec.value(),
                         bootstrap_error.ec.message(),
                         bootstrap_error.error_message);
          }
          ctx.ec = bootstrap_error.ec;
        } else {
          ctx.ec = std::get<std::error_code>(error);
        }
      }
#else
      ctx.ec = ec;
#endif
      ctx.client_context_id = client_context_id_;
      ctx.method = encoded.method;
      ctx.path = encoded.path;
      ctx.http_status = encoded_resp.status_code;
      ctx.http_body = encoded_resp.body.data();
      ctx.last_dispatched_from = session_->local_address();
      ctx.last_dispatched_to = session_->remote_address();
      ctx.hostname = session_->http_context().hostname;
      ctx.port = session_->http_context().port;

      // Can raise priv::retry_http_request when a retry is required
      auto resp = request.make_response(std::move(ctx), std::move(encoded_resp));

      span_->end();
      span_ = nullptr;

      handler(std::move(resp));
    }
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    dispatch_deadline_.cancel();
#endif
    deadline.cancel();
  }

  void send_to()
  {
#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
    dispatch_deadline_.cancel();
#endif
    if (!handler_) {
      return;
    }
    send();
  }

  void set_command_session(std::shared_ptr<io::http_session> session)
  {
    session_.reset();
    session_ = std::move(session);
  }

  [[nodiscard]] auto deadline_expiry() const -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return deadline.expiry();
  }

#ifdef COUCHBASE_CXX_CLIENT_COLUMNAR
  [[nodiscard]] auto dispatch_deadline_expiry() const
    -> std::chrono::time_point<std::chrono::steady_clock>
  {
    return dispatch_deadline_.expiry();
  }
#endif

private:
  void send()
  {
    encoded.type = request.type;
    encoded.client_context_id = client_context_id_;
    encoded.timeout = timeout_;
    if (auto ec = request.encode_to(encoded, session_->http_context()); ec) {
      return invoke_handler(ec, {});
    }
    encoded.headers["client-context-id"] = client_context_id_;

    CB_LOG_TRACE(
      R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms)",
      session_->log_prefix(),
      encoded.type,
      encoded.method,
      encoded.path,
      client_context_id_,
      timeout_.count());

    auto dispatch_span = create_dispatch_span();

    session_->write_and_subscribe(
      encoded,
      [self = this->shared_from_this(),
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
          self->app_telemetry_meter_->value_recorder(self->session_->node_uuid(), {})
            ->record_latency(latency_for_service_type(self->request.type), latency);
        }

        if (self->meter_) {
          metrics::metric_attributes attrs{
            self->request.type,
            self->request.observability_identifier,
            ec,
          };
          self->meter_->record_value(std::move(attrs), start);
        }

        self->deadline.cancel();
        CB_LOG_TRACE(R"({} HTTP response: {}, client_context_id="{}", ec={}, status={}, body={})",
                     self->session_->log_prefix(),
                     self->request.type,
                     self->client_context_id_,
                     ec.message(),
                     msg.status_code,
                     msg.status_code == 200 ? "[hidden]" : msg.body.data());
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

  [[nodiscard]] auto create_dispatch_span() const
    -> std::shared_ptr<couchbase::tracing::request_span>
  {
    std::shared_ptr<couchbase::tracing::request_span> dispatch_span =
      tracer_->create_span(tracing::operation::step_dispatch, span_);
    if (dispatch_span->uses_tags()) {
      dispatch_span->add_tag(tracing::attributes::dispatch::network_transport, "tcp");
      dispatch_span->add_tag(tracing::attributes::dispatch::operation_id, client_context_id_);
      dispatch_span->add_tag(tracing::attributes::dispatch::local_id, session_->id());
      dispatch_span->add_tag(tracing::attributes::dispatch::server_address,
                             session_->http_context().canonical_hostname);
      dispatch_span->add_tag(tracing::attributes::dispatch::server_port,
                             session_->http_context().canonical_port);

      const auto& peer_endpoint = session_->remote_endpoint();
      dispatch_span->add_tag(tracing::attributes::dispatch::peer_address,
                             peer_endpoint.address().to_string());
      dispatch_span->add_tag(tracing::attributes::dispatch::peer_port, peer_endpoint.port());
    }
    return dispatch_span;
  }
};

} // namespace couchbase::core::operations
