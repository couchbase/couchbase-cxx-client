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

#include "core/document_id_fmt.hxx"
#include "core/platform/uuid.h"
#include "core/protocol/client_request.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_get_collection_id.hxx"
#include "core/tracing/constants.hxx"
#include "core/utils/movable_function.hxx"
#include "couchbase/metrics/meter.hxx"
#include "couchbase/tracing/request_tracer.hxx"
#include "mcbp_session.hxx"
#include "mcbp_traits.hxx"
#include "retry_orchestrator.hxx"

#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/key_value_error_map_info.hxx>

#include <asio/steady_timer.hpp>

#include <functional>
#include <utility>

namespace couchbase::core::operations
{

using mcbp_command_handler = utils::movable_function<void(std::error_code, std::optional<io::mcbp_message>&&)>;

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
    std::optional<io::mcbp_session> session_{};
    mcbp_command_handler handler_{};
    std::shared_ptr<Manager> manager_{};
    std::chrono::milliseconds timeout_{};
    std::string id_{
        fmt::format("{:02x}/{}", static_cast<std::uint8_t>(encoded_request_type::body_type::opcode), uuid::to_string(uuid::random()))
    };
    std::shared_ptr<couchbase::tracing::request_span> span_{ nullptr };
    std::shared_ptr<couchbase::tracing::request_span> parent_span{ nullptr };
    std::optional<std::string> last_dispatched_from_{};
    std::optional<std::string> last_dispatched_to_{};

    mcbp_command(asio::io_context& ctx, std::shared_ptr<Manager> manager, Request req, std::chrono::milliseconds default_timeout)
      : deadline(ctx)
      , retry_backoff(ctx)
      , request(req)
      , manager_(manager)
      , timeout_(request.timeout.value_or(default_timeout))
    {
        if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
            if (request.durability_level != durability_level::none && timeout_ < durability_timeout_floor) {
                CB_LOG_DEBUG(
                  R"(Timeout is too low for operation with durability, increasing to sensible value. timeout={}ms, floor={}ms, id="{}")",
                  request.id,
                  timeout_.count(),
                  durability_timeout_floor.count(),
                  id_);
                timeout_ = durability_timeout_floor;
            }
        }
        if constexpr (io::mcbp_traits::supports_parent_span_v<Request>) {
            parent_span = request.parent_span;
        }
    }

    void start(mcbp_command_handler&& handler)
    {
        span_ = manager_->tracer()->start_span(tracing::span_name_for_mcbp_command(encoded_request_type::body_type::opcode), parent_span);
        span_->add_tag(tracing::attributes::service, tracing::service::key_value);
        span_->add_tag(tracing::attributes::instance, request.id.bucket());

        handler_ = std::move(handler);
        deadline.expires_after(timeout_);
        deadline.async_wait([self = this->shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->cancel(retry_reason::do_not_retry);
        });
    }

    void cancel(retry_reason reason)
    {
        if (opaque_ && session_) {
            if (session_->cancel(opaque_.value(), asio::error::operation_aborted, reason)) {
                handler_ = nullptr;
            }
        }
        invoke_handler(request.retries.idempotent() || !opaque_.has_value()
                         ? errc::common::unambiguous_timeout // safe to retry or has not been sent to the server
                         : errc::common::ambiguous_timeout   // non-idempotent and has been sent to the server
        );
    }

    void invoke_handler(std::error_code ec, std::optional<io::mcbp_message>&& msg = {})
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
        session_->write_and_subscribe(req.opaque(),
                                      req.data(session_->supports_feature(protocol::hello_feature::snappy)),
                                      [self = this->shared_from_this()](std::error_code ec,
                                                                        retry_reason /* reason */,
                                                                        io::mcbp_message&& msg,
                                                                        std::optional<key_value_error_map_info> /* error_info */) mutable {
                                          if (ec == asio::error::operation_aborted) {
                                              return self->invoke_handler(errc::common::ambiguous_timeout);
                                          }
                                          if (ec == errc::common::collection_not_found) {
                                              if (self->request.id.is_collection_resolved()) {
                                                  return self->invoke_handler(ec);
                                              }
                                              return self->handle_unknown_collection();
                                          }
                                          if (ec) {
                                              return self->invoke_handler(ec);
                                          }
                                          protocol::client_response<protocol::get_collection_id_response_body> resp(std::move(msg));
                                          self->session_->update_collection_uid(self->request.id.collection_path(),
                                                                                resp.body().collection_uid());
                                          self->request.id.collection_uid(resp.body().collection_uid());
                                          return self->send();
                                      });
    }

    void handle_unknown_collection()
    {
        auto backoff = std::chrono::milliseconds(500);
        auto time_left = deadline.expiry() - std::chrono::steady_clock::now();
        CB_LOG_DEBUG(R"({} unknown collection response for "{}", time_left={}ms, id="{}")",
                     session_->log_prefix(),
                     request.id,
                     std::chrono::duration_cast<std::chrono::milliseconds>(time_left).count(),
                     id_);
        request.retries.add_reason(retry_reason::key_value_collection_outdated);
        if (time_left < backoff) {
            return invoke_handler(
              make_error_code(request.retries.idempotent() ? errc::common::unambiguous_timeout : errc::common::ambiguous_timeout));
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
                    CB_LOG_DEBUG(R"({} no cache entry for collection, resolve collection id for "{}", timeout={}ms, id="{}")",
                                 session_->log_prefix(),
                                 request.id,
                                 timeout_.count(),
                                 id_);
                    return request_collection_id();
                }
            } else {
                if (!request.id.has_default_collection()) {
                    return invoke_handler(errc::common::unsupported_operation);
                }
            }
        }

        if (auto ec = request.encode_to(encoded, session_->context()); ec) {
            return invoke_handler(ec);
        }
        if constexpr (io::mcbp_traits::supports_durability_v<Request>) {
            if (request.durability_level != durability_level::none) {
                encoded.body().durability(request.durability_level,
                                          static_cast<std::uint16_t>(static_cast<double>(timeout_.count()) * 0.9));
            }
        }

        session_->write_and_subscribe(
          request.opaque,
          encoded.data(session_->supports_feature(protocol::hello_feature::snappy)),
          [self = this->shared_from_this(),
           start = std::chrono::steady_clock::now()](std::error_code ec,
                                                     retry_reason reason,
                                                     io::mcbp_message&& msg,
                                                     std::optional<key_value_error_map_info> /* error_info */) mutable {
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
                  return self->invoke_handler(make_error_code(self->request.retries.idempotent() ? errc::common::unambiguous_timeout
                                                                                                 : errc::common::ambiguous_timeout));
              }
              if (ec == errc::common::request_canceled) {
                  if (reason == retry_reason::do_not_retry) {
                      self->span_->add_tag(tracing::attributes::orphan, "canceled");
                      return self->invoke_handler(ec);
                  }
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
              key_value_status_code status = key_value_status_code::invalid;
              std::optional<key_value_error_map_info> error_code{};
              if (protocol::is_valid_status(msg.header.status())) {
                  status = static_cast<key_value_status_code>(msg.header.status());
              } else {
                  error_code = self->session_->decode_error_code(msg.header.status());
              }
              if (status == key_value_status_code::not_my_vbucket) {
                  self->session_->handle_not_my_vbucket(msg);
                  return io::retry_orchestrator::maybe_retry(self->manager_, self, retry_reason::key_value_not_my_vbucket, ec);
              }
              if (status == key_value_status_code::unknown_collection) {
                  return self->handle_unknown_collection();
              }
              if (error_code && error_code.value().has_retry_attribute()) {
                  reason = retry_reason::key_value_error_map_retry_indicated;
              } else {
                  switch (status) {
                      case key_value_status_code::locked:
                          if constexpr (encoded_request_type::body_type::opcode != protocol::client_opcode::unlock) {
                              /**
                               * special case for unlock command, when it should not be retried, because it does not make sense
                               * (someone else unlocked the document)
                               */
                              reason = retry_reason::key_value_locked;
                          }
                          break;
                      case key_value_status_code::temporary_failure:
                          reason = retry_reason::key_value_temporary_failure;
                          break;
                      case key_value_status_code::sync_write_in_progress:
                          reason = retry_reason::key_value_sync_write_in_progress;
                          break;
                      case key_value_status_code::sync_write_re_commit_in_progress:
                          reason = retry_reason::key_value_sync_write_re_commit_in_progress;
                          break;
                      default:
                          break;
                  }
              }
              if (reason == retry_reason::do_not_retry) {
                  self->invoke_handler(ec, std::move(msg));
              } else {
                  io::retry_orchestrator::maybe_retry(self->manager_, self, reason, ec);
              }
          });
    }

    void send_to(io::mcbp_session session)
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

} // namespace couchbase::core::operations
