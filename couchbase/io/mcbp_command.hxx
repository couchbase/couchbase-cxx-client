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

#include <couchbase/document_id_fmt.hxx>
#include <couchbase/io/mcbp_session.hxx>
#include <couchbase/io/mcbp_traits.hxx>
#include <couchbase/io/retry_orchestrator.hxx>
#include <couchbase/metrics/meter.hxx>
#include <couchbase/platform/uuid.h>
#include <couchbase/protocol/cmd_get_collection_id.hxx>
#include <couchbase/protocol/durability_level.hxx>
#include <couchbase/tracing/request_tracer.hxx>
#include <couchbase/utils/movable_function.hxx>

#include <functional>
#include <utility>

namespace couchbase::operations
{

using mcbp_command_handler = utils::movable_function<void(std::error_code, std::optional<io::mcbp_message>)>;

template<typename Manager, typename Request>
struct mcbp_command : public std::enable_shared_from_this<mcbp_command<Manager, Request>> {
    static constexpr std::chrono::milliseconds durability_timeout_floor{ 1'500 };

    using encoded_request_type = typename Request::encoded_request_type;
    using encoded_response_type = typename Request::encoded_response_type;
    asio::steady_timer deadline;
    asio::steady_timer retry_backoff;
    Request request;
    encoded_request_type encoded;
    std::optional<std::uint32_t> opaque_{};
    std::shared_ptr<io::mcbp_session> session_{};
    mcbp_command_handler handler_{};
    std::shared_ptr<Manager> manager_{};
    std::chrono::milliseconds timeout_{};
    std::string id_{ uuid::to_string(uuid::random()) };
    std::shared_ptr<tracing::request_span> span_{ nullptr };

    mcbp_command(asio::io_context& ctx, std::shared_ptr<Manager> manager, Request req, std::chrono::milliseconds default_timeout)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
      , manager_(manager)
      , timeout_(request.timeout.value_or(default_timeout))
    {
        if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
            if (request.durability_level != protocol::durability_level::none && timeout_ < durability_timeout_floor) {
                LOG_DEBUG(
                  R"({} Timeout is too low for operation with durability, increasing to sensible value. timeout={}ms, floor={}ms, id="{}")",
                  session_->log_prefix(),
                  request.id,
                  timeout_.count(),
                  durability_timeout_floor.count(),
                  id_);
                timeout_ = durability_timeout_floor;
            }
        }
    }

    void start(mcbp_command_handler&& handler)
    {
        span_ = manager_->tracer()->start_span(tracing::span_name_for_mcbp_command(encoded_request_type::body_type::opcode), nullptr);
        span_->add_tag(tracing::attributes::service, tracing::service::key_value);
        span_->add_tag(tracing::attributes::instance, request.id.bucket());

        handler_ = std::move(handler);
        deadline.expires_after(timeout_);
        deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->cancel(io::retry_reason::do_not_retry);
        });
    }

    void cancel(io::retry_reason reason)
    {
        if (opaque_ && session_) {
            if (session_->cancel(opaque_.value(), asio::error::operation_aborted, reason)) {
                handler_ = nullptr;
            }
        }
        invoke_handler(request.retries.idempotent ? error::common_errc::unambiguous_timeout : error::common_errc::ambiguous_timeout);
    }

    void invoke_handler(std::error_code ec, std::optional<io::mcbp_message> msg = {})
    {
        retry_backoff.cancel();
        deadline.cancel();
        mcbp_command_handler handler{};
        std::swap(handler, handler_);
        if (span_ != nullptr) {
            if (msg) {
                auto server_duration_us = static_cast<std::uint64_t>(protocol::parse_server_duration_us(msg.value()));
                span_->add_tag(tracing::attributes::server_duration, server_duration_us);
            }
            span_->end();
            span_ = nullptr;
        }
        if (handler) {
            handler(ec, std::move(msg));
        }
    }

    void request_collection_id()
    {
        if (session_->is_stopped()) {
            return manager_->map_and_send(this->shared_from_this());
        }
        protocol::client_request<protocol::get_collection_id_request_body> req;
        req.opaque(session_->next_opaque());
        req.body().collection_path(request.id.collection_path());
        session_->write_and_subscribe(
          req.opaque(),
          req.data(session_->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this()](std::error_code ec, io::retry_reason /* reason */, io::mcbp_message&& msg) mutable {
              if (ec == asio::error::operation_aborted) {
                  return self->invoke_handler(error::common_errc::ambiguous_timeout);
              }
              if (ec == error::common_errc::collection_not_found) {
                  if (self->request.id.is_collection_resolved()) {
                      return self->invoke_handler(ec);
                  }
                  return self->handle_unknown_collection();
              }
              if (ec) {
                  return self->invoke_handler(ec);
              }
              protocol::client_response<protocol::get_collection_id_response_body> resp(std::move(msg));
              self->session_->update_collection_uid(self->request.id.collection_path(), resp.body().collection_uid());
              self->request.id.collection_uid(resp.body().collection_uid());
              return self->send();
          });
    }

    void handle_unknown_collection()
    {
        auto backoff = std::chrono::milliseconds(500);
        auto time_left = deadline.expiry() - std::chrono::steady_clock::now();
        LOG_DEBUG(R"({} unknown collection response for "{}", time_left={}ms, id="{}")",
                  session_->log_prefix(),
                  request.id,
                  std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count(),
                  id_);
        if (time_left < backoff) {
            request.retries.reasons.insert(couchbase::io::retry_reason::kv_collection_outdated);
            return invoke_handler(make_error_code(request.retries.idempotent ? error::common_errc::unambiguous_timeout
                                                                             : error::common_errc::ambiguous_timeout));
        }
        retry_backoff.expires_after(backoff);
        retry_backoff.async_wait([self = this->shared_from_this()](std::error_code ec) mutable {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->request_collection_id();
        });
    }

    void send()
    {
        opaque_ = session_->next_opaque();
        request.opaque = *opaque_;
        span_->add_tag(tracing::attributes::operation_id, fmt::format("0x{:x}", request.opaque));
        if (request.id.use_collections() && !request.id.is_collection_resolved()) {
            if (session_->supports_feature(protocol::hello_feature::collections)) {
                auto collection_id = session_->get_collection_uid(request.id.collection_path());
                if (collection_id) {
                    request.id.collection_uid(collection_id.value());
                } else {
                    LOG_DEBUG(R"({} no cache entry for collection, resolve collection id for "{}", timeout={}ms, id="{}")",
                              session_->log_prefix(),
                              request.id,
                              timeout_.count(),
                              id_);
                    return request_collection_id();
                }
            } else {
                if (!request.id.has_default_collection()) {
                    return invoke_handler(error::common_errc::unsupported_operation);
                }
            }
        }

        if (auto ec = request.encode_to(encoded, session_->context()); ec) {
            return invoke_handler(ec);
        }
        if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
            if (request.durability_level != protocol::durability_level::none) {
                encoded.body().durability(request.durability_level,
                                          static_cast<std::uint16_t>(static_cast<double>(timeout_.count()) * 0.9));
            }
        }

        session_->write_and_subscribe(
          request.opaque,
          encoded.data(session_->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this(),
           start = std::chrono::steady_clock::now()](std::error_code ec, io::retry_reason reason, io::mcbp_message&& msg) mutable {
              static std::string meter_name = "db.couchbase.operations";
              static std::map<std::string, std::string> tags = {
                  { "db.couchbase.service", "kv" },
                  { "db.operation", fmt::format("{}", encoded_request_type::body_type::opcode) },
              };
              self->manager_->meter()
                ->get_value_recorder(meter_name, tags)
                ->record_value(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count());

              self->retry_backoff.cancel();
              if (ec == asio::error::operation_aborted) {
                  self->span_->add_tag(tracing::attributes::orphan, "aborted");
                  return self->invoke_handler(make_error_code(self->request.retries.idempotent ? error::common_errc::unambiguous_timeout
                                                                                               : error::common_errc::ambiguous_timeout));
              }
              if (ec == error::common_errc::request_canceled) {
                  if (reason == io::retry_reason::do_not_retry) {
                      self->span_->add_tag(tracing::attributes::orphan, "canceled");
                      return self->invoke_handler(ec);
                  }
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
              protocol::status status = protocol::status::invalid;
              std::optional<error_map::error_info> error_code{};
              if (protocol::is_valid_status(msg.header.status())) {
                  status = protocol::status(msg.header.status());
              } else {
                  error_code = self->session_->decode_error_code(msg.header.status());
              }
              if (status == protocol::status::not_my_vbucket) {
                  self->session_->handle_not_my_vbucket(std::move(msg));
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, io::retry_reason::kv_not_my_vbucket, ec);
              }
              if (status == protocol::status::unknown_collection) {
                  return self->handle_unknown_collection();
              }
              if (error_code && error_code.value().has_retry_attribute()) {
                  reason = io::retry_reason::kv_error_map_retry_indicated;
              } else {
                  switch (status) {
                      case protocol::status::locked:
                          if constexpr (encoded_request_type::body_type::opcode != protocol::client_opcode::unlock) {
                              /**
                               * special case for unlock command, when it should not be retried, because it does not make sense
                               * (someone else unlocked the document)
                               */
                              reason = io::retry_reason::kv_locked;
                          }
                          break;
                      case protocol::status::temporary_failure:
                          reason = io::retry_reason::kv_temporary_failure;
                          break;
                      case protocol::status::sync_write_in_progress:
                          reason = io::retry_reason::kv_sync_write_in_progress;
                          break;
                      case protocol::status::sync_write_re_commit_in_progress:
                          reason = io::retry_reason::kv_sync_write_re_commit_in_progress;
                          break;
                      default:
                          break;
                  }
              }
              if (reason == io::retry_reason::do_not_retry) {
                  self->invoke_handler(ec, msg);
              } else {
                  io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
          });
    }

    void send_to(std::shared_ptr<io::mcbp_session> session)
    {
        if (!handler_ || !span_) {
            return;
        }
        session_ = std::move(session);
        span_->add_tag(tracing::attributes::remote_socket, session_->remote_address());
        span_->add_tag(tracing::attributes::local_socket, session_->local_address());
        span_->add_tag(tracing::attributes::local_id, session_->id());
        send();
    }
};

} // namespace couchbase::operations
