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

#include <io/http_session.hxx>

#include <tracing/request_tracer.hxx>
#include <metrics/meter.hxx>

namespace couchbase::operations
{

template<typename Request>
struct http_command : public std::enable_shared_from_this<http_command<Request>> {
    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    using error_context_type = typename Request::error_context_type;
    asio::steady_timer deadline;
    asio::steady_timer retry_backoff;
    Request request;
    encoded_request_type encoded;
    tracing::request_tracer* tracer_;
    tracing::request_span* span_{ nullptr };
    metrics::meter* meter_;

    http_command(asio::io_context& ctx, Request req, tracing::request_tracer* tracer, metrics::meter* meter)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
      , tracer_(tracer)
      , meter_(meter)
    {
    }

    void finish_dispatch(const std::string& remote_address, const std::string& local_address)
    {
        if (span_ == nullptr) {
            return;
        }
        span_->add_tag(tracing::attributes::remote_socket, remote_address);
        span_->add_tag(tracing::attributes::local_socket, local_address);
        span_->end();
        span_ = nullptr;
    }

    template<typename Handler>
    void send_to(std::shared_ptr<io::http_session> session, Handler&& handler)
    {
        encoded.type = request.type;
        if (auto ec = request.encode_to(encoded, session->http_context()); ec) {
            error_context_type ctx{};
            ctx.ec = ec;
            ctx.client_context_id = request.client_context_id;
            return handler(make_response(std::move(ctx), request, {}));
        }
        encoded.headers["client-context-id"] = request.client_context_id;
        auto log_prefix = session->log_prefix();
        spdlog::trace(R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms)",
                      log_prefix,
                      encoded.type,
                      encoded.method,
                      encoded.path,
                      request.client_context_id,
                      request.timeout.count());
        SPDLOG_TRACE(R"({} HTTP request: {}, method={}, path="{}", client_context_id="{}", timeout={}ms{:a})",
                     log_prefix,
                     encoded.type,
                     encoded.method,
                     encoded.path,
                     request.client_context_id,
                     request.timeout.count(),
                     spdlog::to_hex(encoded.body));
        span_ = tracer_->start_span(tracing::span_name_for_http_service(request.type), nullptr);
        span_->add_tag(tracing::attributes::service, tracing::service_name_for_http_service(request.type));
        span_->add_tag(tracing::attributes::operation_id, request.client_context_id);
        span_->add_tag(tracing::attributes::local_id, session->id());
        session->write_and_subscribe(
          encoded,
          [self = this->shared_from_this(),
           log_prefix,
           session,
           handler = std::forward<Handler>(handler),
           start = std::chrono::steady_clock::now()](std::error_code ec, io::http_response&& msg) mutable {
              static std::string meter_name = "db.couchbase.operations";
              static std::map<std::string, std::string> tags = {
                  { "db.couchbase.service", fmt::format("{}", self->request.type) },
                  { "db.operation", self->encoded.path },
              };
              if(self->meter_) {
                  self->meter_->get_value_recorder(meter_name, tags)
                    ->record_value(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());
              }
              self->deadline.cancel();
              self->finish_dispatch(session->remote_address(), session->local_address());
              encoded_response_type resp(msg);
              spdlog::trace(R"({} HTTP response: {}, client_context_id="{}", status={})",
                            log_prefix,
                            self->request.type,
                            self->request.client_context_id,
                            resp.status_code);
              SPDLOG_TRACE(R"({} HTTP response: {}, client_context_id="{}", status={}{:a})",
                           log_prefix,
                           self->request.type,
                           self->request.client_context_id,
                           resp.status_code,
                           spdlog::to_hex(resp.body));
              try {
                  error_context_type ctx{};
                  ctx.ec = ec;
                  ctx.client_context_id = self->request.client_context_id;
                  ctx.method = self->encoded.method;
                  ctx.path = self->encoded.path;
                  ctx.last_dispatched_from = session->local_address();
                  ctx.last_dispatched_to = session->remote_address();
                  ctx.http_status = msg.status_code;
                  ctx.http_body = msg.body;
                  handler(make_response(std::move(ctx), self->request, std::move(msg)));
              } catch (const priv::retry_http_request&) {
                  self->send_to(session, std::forward<Handler>(handler));
              }
          });
        deadline.expires_after(request.timeout);
        deadline.async_wait([session](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            session->stop();
        });
    }
};

} // namespace couchbase::operations
