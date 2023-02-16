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

#include "mcbp_session.hxx"

#include "core/config_listener.hxx"
#include "core/diagnostics.hxx"
#include "core/impl/bootstrap_state_listener.hxx"
#include "core/logger/logger.hxx"
#include "core/mcbp/codec.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/meta/version.hxx"
#include "core/operation_map.hxx"
#include "core/origin.hxx"
#include "core/ping_reporter.hxx"
#include "core/protocol/client_request.hxx"
#include "core/protocol/cmd_cluster_map_change_notification.hxx"
#include "core/protocol/cmd_get.hxx"
#include "core/protocol/cmd_get_cluster_config.hxx"
#include "core/protocol/cmd_get_error_map.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/cmd_noop.hxx"
#include "core/protocol/cmd_sasl_auth.hxx"
#include "core/protocol/cmd_sasl_list_mechs.hxx"
#include "core/protocol/cmd_sasl_step.hxx"
#include "core/protocol/cmd_select_bucket.hxx"
#include "core/protocol/hello_feature_fmt.hxx"
#include "core/protocol/server_request.hxx"
#include "core/sasl/client.h"
#include "core/sasl/error_fmt.h"
#include "core/topology/capabilities_fmt.hxx"
#include "core/topology/configuration_fmt.hxx"
#include "mcbp_context.hxx"
#include "mcbp_message.hxx"
#include "mcbp_parser.hxx"
#include "retry_orchestrator.hxx"
#include "streams.hxx"

#include <couchbase/fmt/retry_reason.hxx>

#include <asio.hpp>
#include <spdlog/fmt/bin_to_hex.h>

#include <cstring>
#include <utility>

namespace couchbase::core::io
{
class collection_cache
{
  private:
    std::map<std::string, std::uint32_t> cid_map_{ { "_default._default", 0 } };

  public:
    [[nodiscard]] std::optional<std::uint32_t> get(const std::string& path)
    {
        Expects(!path.empty());
        if (auto ptr = cid_map_.find(path); ptr != cid_map_.end()) {
            return ptr->second;
        }
        return {};
    }

    void update(const std::string& path, std::uint32_t id)
    {
        Expects(!path.empty());
        cid_map_[path] = id;
    }

    void reset()
    {
        cid_map_.clear();
        cid_map_["_default._default"] = 0;
    }
};

class mcbp_session_impl
  : public std::enable_shared_from_this<mcbp_session_impl>
  , public operation_map
{
    class bootstrap_handler : public std::enable_shared_from_this<bootstrap_handler>
    {
      private:
        std::shared_ptr<mcbp_session_impl> session_;
        sasl::ClientContext sasl_;
        std::atomic_bool stopped_{ false };

      public:
        ~bootstrap_handler()
        {
            stop();
        }

        void stop()
        {
            bool expected_state{ false };
            stopped_.compare_exchange_strong(expected_state, true);
        }

        static auto sasl_mechanisms(const std::shared_ptr<mcbp_session_impl>& session) -> std::vector<std::string>
        {
            if (const auto user_mechanisms = session->origin_.credentials().allowed_sasl_mechanisms; user_mechanisms.has_value()) {
                return user_mechanisms.value();
            }
            if (session->is_tls_) {
                return { "PLAIN" };
            }
            return { "SCRAM-SHA512", "SCRAM-SHA256", "SCRAM-SHA1" };
        }

        explicit bootstrap_handler(std::shared_ptr<mcbp_session_impl> session)
          : session_(std::move(session))
          , sasl_([origin = session_->origin_]() { return origin.username(); },
                  [origin = session_->origin_]() { return origin.password(); },
                  sasl_mechanisms(session_))
        {
            protocol::client_request<protocol::hello_request_body> hello_req;
            if (session_->origin_.options().enable_unordered_execution) {
                hello_req.body().enable_unordered_execution();
            }
            if (session_->origin_.options().enable_clustermap_notification) {
                hello_req.body().enable_clustermap_change_notification();
            }
            if (session_->origin_.options().enable_compression) {
                hello_req.body().enable_compression();
            }
            if (session_->origin_.options().enable_mutation_tokens) {
                hello_req.body().enable_mutation_tokens();
            }
            hello_req.opaque(session_->next_opaque());
            auto user_agent =
              meta::user_agent_for_mcbp(session_->client_id_, session_->id_, session_->origin_.options().user_agent_extra, 250);
            hello_req.body().user_agent(user_agent);
            CB_LOG_DEBUG("{} user_agent={}, requested_features=[{}]",
                         session_->log_prefix_,
                         user_agent,
                         utils::join_strings_fmt("{}", hello_req.body().features(), ", "));
            session_->write(hello_req.data());

            if (!session_->origin_.credentials().uses_certificate()) {
                protocol::client_request<protocol::sasl_list_mechs_request_body> list_req;
                list_req.opaque(session_->next_opaque());
                session_->write(list_req.data());

                protocol::client_request<protocol::sasl_auth_request_body> auth_req;
                auto [sasl_code, sasl_payload] = sasl_.start();
                auth_req.opaque(session_->next_opaque());
                auth_req.body().mechanism(sasl_.get_name());
                auth_req.body().sasl_data(sasl_payload);
                session_->write(auth_req.data());
            }

            session_->flush();
        }

        void complete(std::error_code ec)
        {
            if (bool expected_state{ false }; stopped_.compare_exchange_strong(expected_state, true)) {
                session_->invoke_bootstrap_handler(ec);
            }
        }

        void auth_success()
        {
            session_->authenticated_ = true;
            if (session_->supports_feature(protocol::hello_feature::xerror)) {
                protocol::client_request<protocol::get_error_map_request_body> errmap_req;
                errmap_req.opaque(session_->next_opaque());
                session_->write(errmap_req.data());
            }
            if (session_->bucket_name_) {
                protocol::client_request<protocol::select_bucket_request_body> sb_req;
                sb_req.opaque(session_->next_opaque());
                sb_req.body().bucket_name(session_->bucket_name_.value());
                session_->write(sb_req.data());
            }
            protocol::client_request<protocol::get_cluster_config_request_body> cfg_req;
            cfg_req.opaque(session_->next_opaque());
            session_->write(cfg_req.data());
            session_->flush();
        }

        void handle(mcbp_message&& msg)
        {
            if (stopped_ || !session_) {
                return;
            }
            Expects(protocol::is_valid_magic(msg.header.magic));
            switch (auto magic = static_cast<protocol::magic>(msg.header.magic)) {
                case protocol::magic::client_response:
                case protocol::magic::alt_client_response:
                    Expects(protocol::is_valid_client_opcode(msg.header.opcode));
                    switch (auto status = static_cast<key_value_status_code>(msg.header.status())) {
                        case key_value_status_code::rate_limited_max_commands:
                        case key_value_status_code::rate_limited_max_connections:
                        case key_value_status_code::rate_limited_network_egress:
                        case key_value_status_code::rate_limited_network_ingress:
                            CB_LOG_DEBUG(
                              "{} unable to bootstrap MCBP session (bucket={}, opcode={}, status={}), the user has reached rate limit",
                              session_->log_prefix_,
                              session_->bucket_name_.value_or(""),
                              protocol::client_opcode(msg.header.opcode),
                              status);
                            return complete(errc::common::rate_limited);

                        case key_value_status_code::scope_size_limit_exceeded:
                            CB_LOG_DEBUG(
                              "{} unable to bootstrap MCBP session (bucket={}, opcode={}, status={}), the user has reached quota limit",
                              session_->log_prefix_,
                              session_->bucket_name_.value_or(""),
                              protocol::client_opcode(msg.header.opcode),
                              status);
                            return complete(errc::common::quota_limited);

                        default:
                            break;
                    }
                    switch (auto opcode = static_cast<protocol::client_opcode>(msg.header.opcode)) {
                        case protocol::client_opcode::hello: {
                            protocol::client_response<protocol::hello_response_body> resp(std::move(msg));
                            if (resp.status() == key_value_status_code::success) {
                                session_->supported_features_ = resp.body().supported_features();
                                CB_LOG_DEBUG("{} supported_features=[{}]",
                                             session_->log_prefix_,
                                             utils::join_strings_fmt("{}", session_->supported_features_, ", "));
                                if (session_->origin_.credentials().uses_certificate()) {
                                    CB_LOG_DEBUG("{} skip SASL authentication, because TLS certificate was specified",
                                                 session_->log_prefix_);
                                    return auth_success();
                                }
                            } else {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque());
                                return complete(errc::network::handshake_failure);
                            }
                        } break;
                        case protocol::client_opcode::sasl_list_mechs: {
                            protocol::client_response<protocol::sasl_list_mechs_response_body> resp(std::move(msg));
                            if (resp.status() != key_value_status_code::success) {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque());
                                return complete(errc::common::authentication_failure);
                            }
                        } break;
                        case protocol::client_opcode::sasl_auth: {
                            protocol::client_response<protocol::sasl_auth_response_body> resp(std::move(msg));
                            if (resp.status() == key_value_status_code::success) {
                                return auth_success();
                            }
                            if (resp.status() == key_value_status_code::auth_continue) {
                                auto [sasl_code, sasl_payload] = sasl_.step(resp.body().value());
                                if (sasl_code == sasl::error::OK) {
                                    return auth_success();
                                }
                                if (sasl_code == sasl::error::CONTINUE) {
                                    protocol::client_request<protocol::sasl_step_request_body> req;
                                    req.opaque(session_->next_opaque());
                                    req.body().mechanism(sasl_.get_name());
                                    req.body().sasl_data(sasl_payload);
                                    session_->write_and_flush(req.data());
                                } else {
                                    CB_LOG_ERROR("{} unable to authenticate: (sasl_code={}, opaque={})",
                                                 session_->log_prefix_,
                                                 sasl_code,
                                                 resp.opaque());
                                    return complete(errc::common::authentication_failure);
                                }
                            } else {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque());
                                return complete(errc::common::authentication_failure);
                            }
                        } break;
                        case protocol::client_opcode::sasl_step: {
                            protocol::client_response<protocol::sasl_step_response_body> resp(std::move(msg));
                            if (resp.status() == key_value_status_code::success) {
                                return auth_success();
                            }
                            return complete(errc::common::authentication_failure);
                        }
                        case protocol::client_opcode::get_error_map: {
                            protocol::client_response<protocol::get_error_map_response_body> resp(std::move(msg));
                            if (resp.status() == key_value_status_code::success) {
                                session_->error_map_.emplace(resp.body().errmap());
                            } else {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque(),
                                               spdlog::to_hex(resp.header()));
                                return complete(errc::network::protocol_error);
                            }
                        } break;
                        case protocol::client_opcode::select_bucket: {
                            protocol::client_response<protocol::select_bucket_response_body> resp(std::move(msg));
                            if (resp.status() == key_value_status_code::success) {
                                CB_LOG_DEBUG("{} selected bucket: {}", session_->log_prefix_, session_->bucket_name_.value_or(""));
                                session_->bucket_selected_ = true;
                            } else if (resp.status() == key_value_status_code::not_found) {
                                CB_LOG_DEBUG(
                                  "{} kv_engine node does not have configuration propagated yet (opcode={}, status={}, opaque={})",
                                  session_->log_prefix_,
                                  opcode,
                                  resp.status(),
                                  resp.opaque());
                                return complete(errc::network::configuration_not_available);
                            } else if (resp.status() == key_value_status_code::no_access) {
                                CB_LOG_DEBUG("{} unable to select bucket: {}, probably the bucket does not exist",
                                             session_->log_prefix_,
                                             session_->bucket_name_.value_or(""));
                                session_->bucket_selected_ = false;
                                return complete(errc::common::bucket_not_found);
                            } else {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque(),
                                               spdlog::to_hex(resp.header()));
                                return complete(errc::common::bucket_not_found);
                            }
                        } break;
                        case protocol::client_opcode::get_cluster_config: {
                            protocol::cmd_info info{ session_->endpoint_address_, session_->endpoint_.port() };
                            protocol::client_response<protocol::get_cluster_config_response_body> resp(std::move(msg), info);
                            if (resp.status() == key_value_status_code::success) {
                                session_->update_configuration(resp.body().config());
                                complete({});
                            } else if (resp.status() == key_value_status_code::not_found) {
                                CB_LOG_DEBUG(
                                  "{} kv_engine node does not have configuration propagated yet (opcode={}, status={}, opaque={})",
                                  session_->log_prefix_,
                                  opcode,
                                  resp.status(),
                                  resp.opaque());
                                return complete(errc::network::configuration_not_available);
                            } else if (resp.status() == key_value_status_code::no_bucket && !session_->bucket_name_) {
                                // bucket-less session, but the server wants bucket
                                session_->supports_gcccp_ = false;
                                CB_LOG_WARNING("{} this server does not support GCCCP, open bucket before making any cluster-level command",
                                               session_->log_prefix_);
                                session_->update_configuration(
                                  topology::make_blank_configuration(session_->endpoint_address_, session_->endpoint_.port(), 0));
                                complete({});
                            } else {
                                CB_LOG_WARNING("{} unexpected message status during bootstrap: {} (opaque={}, {:n})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque(),
                                               spdlog::to_hex(resp.header()));
                                return complete(errc::network::protocol_error);
                            }
                        } break;
                        default:
                            CB_LOG_WARNING("{} unexpected message during bootstrap: {}", session_->log_prefix_, opcode);
                            return complete(errc::network::protocol_error);
                    }
                    break;
                case protocol::magic::server_request:
                    Expects(protocol::is_valid_server_request_opcode(msg.header.opcode));
                    switch (static_cast<protocol::server_opcode>(msg.header.opcode)) {
                        case protocol::server_opcode::cluster_map_change_notification: {
                            protocol::cmd_info info{ session_->endpoint_address_, session_->endpoint_.port() };
                            protocol::server_request<protocol::cluster_map_change_notification_request_body> req(std::move(msg), info);
                            std::optional<topology::configuration> config = req.body().config();
                            if (session_ && config.has_value()) {
                                if ((!config->bucket.has_value() && req.body().bucket().empty()) ||
                                    (session_->bucket_name_.has_value() && !req.body().bucket().empty() &&
                                     session_->bucket_name_.value() == req.body().bucket())) {
                                    session_->update_configuration(std::move(config.value()));
                                }
                            }
                        } break;
                        default:
                            CB_LOG_WARNING("{} unexpected server request: opcode={:x}, opaque={}{:a}{:a}",
                                           session_->log_prefix_,
                                           msg.header.opcode,
                                           msg.header.opaque,
                                           spdlog::to_hex(msg.header_data()),
                                           spdlog::to_hex(msg.body));
                    }
                    break;
                case protocol::magic::client_request:
                case protocol::magic::alt_client_request:
                case protocol::magic::server_response:
                    CB_LOG_WARNING("{} unexpected magic: {} (opcode={:x}, opaque={}){:a}{:a}",
                                   session_->log_prefix_,
                                   magic,
                                   msg.header.opcode,
                                   msg.header.opaque,
                                   spdlog::to_hex(msg.header_data()),
                                   spdlog::to_hex(msg.body));
                    break;
            }
        }
    };

    class message_handler : public std::enable_shared_from_this<message_handler>
    {
      private:
        std::shared_ptr<mcbp_session_impl> session_;
        asio::steady_timer heartbeat_timer_;
        std::chrono::milliseconds heartbeat_interval_;
        std::atomic_bool stopped_{ false };

      public:
        explicit message_handler(std::shared_ptr<mcbp_session_impl> session)
          : session_(std::move(session))
          , heartbeat_timer_(session_->ctx_)
          , heartbeat_interval_{ session_->origin_.options().config_poll_floor > session_->origin_.options().config_poll_interval
                                   ? session_->origin_.options().config_poll_floor
                                   : session_->origin_.options().config_poll_interval }
        {
        }

        ~message_handler()
        {
            stop();
        }

        void start()
        {
            if (session_->supports_gcccp_) {
                fetch_config({});
            }
        }

        void stop()
        {
            if (bool expected_state{ false }; stopped_.compare_exchange_strong(expected_state, true)) {
                heartbeat_timer_.cancel();
            }
        }

        void handle(mcbp_message&& msg)
        {
            if (stopped_ || !session_) {
                return;
            }
            Expects(protocol::is_valid_magic(msg.header.magic));
            switch (auto magic = static_cast<protocol::magic>(msg.header.magic)) {
                case protocol::magic::client_response:
                case protocol::magic::alt_client_response:
                    Expects(protocol::is_valid_client_opcode(msg.header.opcode));
                    switch (auto opcode = static_cast<protocol::client_opcode>(msg.header.opcode)) {
                        case protocol::client_opcode::get_cluster_config: {
                            protocol::cmd_info info{ session_->endpoint_address_, session_->endpoint_.port() };
                            protocol::client_response<protocol::get_cluster_config_response_body> resp(std::move(msg), info);
                            if (resp.status() == key_value_status_code::success) {
                                if (session_) {
                                    session_->update_configuration(resp.body().config());
                                }
                            } else {
                                CB_LOG_WARNING("{} unexpected message status: {} (opaque={})",
                                               session_->log_prefix_,
                                               resp.error_message(),
                                               resp.opaque());
                            }
                        } break;
                        case protocol::client_opcode::noop:
                        case protocol::client_opcode::get_collections_manifest:
                        case protocol::client_opcode::get_collection_id:
                        case protocol::client_opcode::get:
                        case protocol::client_opcode::get_and_lock:
                        case protocol::client_opcode::get_and_touch:
                        case protocol::client_opcode::get_meta:
                        case protocol::client_opcode::get_replica:
                        case protocol::client_opcode::touch:
                        case protocol::client_opcode::insert:
                        case protocol::client_opcode::replace:
                        case protocol::client_opcode::upsert:
                        case protocol::client_opcode::append:
                        case protocol::client_opcode::prepend:
                        case protocol::client_opcode::remove:
                        case protocol::client_opcode::observe_seqno:
                        case protocol::client_opcode::unlock:
                        case protocol::client_opcode::increment:
                        case protocol::client_opcode::range_scan_create:
                        case protocol::client_opcode::range_scan_continue:
                        case protocol::client_opcode::range_scan_cancel:
                        case protocol::client_opcode::decrement:
                        case protocol::client_opcode::subdoc_multi_lookup:
                        case protocol::client_opcode::subdoc_multi_mutation: {
                            std::uint16_t status = utils::byte_swap(msg.header.specific);
                            if (status == static_cast<std::uint16_t>(key_value_status_code::not_my_vbucket)) {
                                session_->handle_not_my_vbucket(msg);
                            }

                            std::uint32_t opaque = utils::byte_swap(msg.header.opaque);
                            if (session_->handle_request(opcode, status, opaque, std::move(msg))) {
                                CB_LOG_TRACE("{} MCBP invoked operation handler: opcode={}, opaque={}, status={}",
                                             session_->log_prefix_,
                                             opcode,
                                             opaque,
                                             protocol::status_to_string(status));
                            } else {
                                CB_LOG_DEBUG("{} unexpected orphan response: opcode={}, opaque={}, status={}",
                                             session_->log_prefix_,
                                             opcode,
                                             opaque,
                                             protocol::status_to_string(status));
                            }
                        } break;
                        default:
                            CB_LOG_WARNING("{} unexpected client response: opcode={}, opaque={}{:a}{:a})",
                                           session_->log_prefix_,
                                           opcode,
                                           msg.header.opaque,
                                           spdlog::to_hex(msg.header_data()),
                                           spdlog::to_hex(msg.body));
                    }
                    break;
                case protocol::magic::server_request:
                    Expects(protocol::is_valid_server_request_opcode(msg.header.opcode));
                    switch (static_cast<protocol::server_opcode>(msg.header.opcode)) {
                        case protocol::server_opcode::cluster_map_change_notification: {
                            protocol::server_request<protocol::cluster_map_change_notification_request_body> req(std::move(msg));
                            std::optional<topology::configuration> config = req.body().config();
                            if (session_ && config.has_value()) {
                                if ((!config->bucket.has_value() && req.body().bucket().empty()) ||
                                    (session_->bucket_name_.has_value() && !req.body().bucket().empty() &&
                                     session_->bucket_name_.value() == req.body().bucket())) {
                                    session_->update_configuration(std::move(config.value()));
                                }
                            }
                        } break;
                        default:
                            CB_LOG_WARNING("{} unexpected server request: opcode={:x}, opaque={}{:a}{:a}",
                                           session_->log_prefix_,
                                           msg.header.opcode,
                                           msg.header.opaque,
                                           spdlog::to_hex(msg.header_data()),
                                           spdlog::to_hex(msg.body));
                    }
                    break;
                case protocol::magic::client_request:
                case protocol::magic::alt_client_request:
                case protocol::magic::server_response:
                    CB_LOG_WARNING("{} unexpected magic: {} (opcode={:x}, opaque={}){:a}{:a}",
                                   session_->log_prefix_,
                                   magic,
                                   msg.header.opcode,
                                   msg.header.opaque,
                                   spdlog::to_hex(msg.header_data()),
                                   spdlog::to_hex(msg.body));
                    break;
            }
        }

        void fetch_config(std::error_code ec)
        {
            if (ec == asio::error::operation_aborted || stopped_ || !session_) {
                return;
            }
            protocol::client_request<protocol::get_cluster_config_request_body> req;
            req.opaque(session_->next_opaque());
            session_->write_and_flush(req.data());
            heartbeat_timer_.expires_after(heartbeat_interval_);
            heartbeat_timer_.async_wait([self = shared_from_this()](std::error_code e) {
                if (e == asio::error::operation_aborted) {
                    return;
                }
                self->fetch_config(e);
            });
        }
    };

  public:
    mcbp_session_impl() = delete;
    mcbp_session_impl(std::string_view client_id,
                      asio::io_context& ctx,
                      couchbase::core::origin origin,
                      std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                      std::optional<std::string> bucket_name = {},
                      std::vector<protocol::hello_feature> known_features = {})
      : client_id_(client_id)
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<plain_stream_impl>(ctx_))
      , bootstrap_deadline_(ctx_)
      , connection_deadline_(ctx_)
      , retry_backoff_(ctx_)
      , origin_{ std::move(origin) }
      , bucket_name_{ std::move(bucket_name) }
      , supported_features_{ std::move(known_features) }
      , is_tls_{ false }
      , state_listener_{ std::move(state_listener) }
      , codec_{ { supported_features_.begin(), supported_features_.end() } }
    {
        log_prefix_ = fmt::format("[{}/{}/{}/{}]", client_id_, id_, stream_->log_prefix(), bucket_name_.value_or("-"));
    }

    mcbp_session_impl(std::string_view client_id,
                      asio::io_context& ctx,
                      asio::ssl::context& tls,
                      couchbase::core::origin origin,
                      std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                      std::optional<std::string> bucket_name = {},
                      std::vector<protocol::hello_feature> known_features = {})
      : client_id_(client_id)
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<tls_stream_impl>(ctx_, tls))
      , bootstrap_deadline_(ctx_)
      , connection_deadline_(ctx_)
      , retry_backoff_(ctx_)
      , origin_(std::move(origin))
      , bucket_name_(std::move(bucket_name))
      , supported_features_(std::move(known_features))
      , is_tls_{ true }
      , state_listener_{ std::move(state_listener) }
      , codec_{ { supported_features_.begin(), supported_features_.end() } }
    {
        log_prefix_ = fmt::format("[{}/{}/{}/{}]", client_id_, id_, stream_->log_prefix(), bucket_name_.value_or("-"));
    }

    ~mcbp_session_impl()
    {
        CB_LOG_DEBUG("{} destroy MCBP connection", log_prefix_);
        stop(retry_reason::do_not_retry);
    }

    [[nodiscard]] const std::string& log_prefix() const
    {
        return log_prefix_;
    }

    std::string remote_address() const
    {
        if (endpoint_.protocol() == asio::ip::tcp::v6()) {
            return fmt::format("[{}]:{}", endpoint_address_, endpoint_.port());
        }
        return fmt::format("{}:{}", endpoint_address_, endpoint_.port());
    }

    std::string local_address() const
    {
        if (endpoint_.protocol() == asio::ip::tcp::v6()) {
            return fmt::format("[{}]:{}", local_endpoint_address_, local_endpoint_.port());
        }
        return fmt::format("{}:{}", local_endpoint_address_, local_endpoint_.port());
    }

    [[nodiscard]] diag::endpoint_diag_info diag_info() const
    {
        return { service_type::key_value,
                 id_,
                 last_active_.time_since_epoch().count() == 0 ? std::nullopt
                                                              : std::make_optional(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - last_active_)),
                 remote_address(),
                 local_address(),
                 state_,
                 bucket_name_ };
    }

    void ping(std::shared_ptr<diag::ping_reporter> handler)
    {
        protocol::client_request<protocol::mcbp_noop_request_body> req;
        req.opaque(next_opaque());
        write_and_subscribe(req.opaque(),
                            req.data(false),
                            [start = std::chrono::steady_clock::now(), self = shared_from_this(), handler](
                              std::error_code ec,
                              retry_reason reason,
                              io::mcbp_message&& /* msg */,
                              std::optional<key_value_error_map_info> /* error_info */) {
                                diag::ping_state state = diag::ping_state::ok;
                                std::optional<std::string> error{};
                                if (ec) {
                                    state = diag::ping_state::error;
                                    error.emplace(fmt::format("code={}, message={}, reason={}", ec.value(), ec.message(), reason));
                                }
                                handler->report({
                                  service_type::key_value,
                                  self->id_,
                                  std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start),
                                  self->remote_address(),
                                  self->local_address(),
                                  state,
                                  self->bucket_name_,
                                  error,
                                });
                            });
    }

    [[nodiscard]] mcbp_context context() const
    {
        return { config_, supported_features_ };
    }

    void bootstrap(utils::movable_function<void(std::error_code, topology::configuration)>&& callback,
                   bool retry_on_bucket_not_found = false)
    {
        retry_bootstrap_on_bucket_not_found_ = retry_on_bucket_not_found;
        bootstrap_callback_ = std::move(callback);
        bootstrap_deadline_.expires_after(origin_.options().bootstrap_timeout);
        bootstrap_deadline_.async_wait([self = shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            if (!ec) {
                ec = errc::common::unambiguous_timeout;
            }
            if (self->state_listener_) {
                self->state_listener_->report_bootstrap_error(fmt::format("{}:{}", self->bootstrap_hostname_, self->bootstrap_port_), ec);
            }
            CB_LOG_WARNING("{} unable to bootstrap in time", self->log_prefix_);
            auto h = std::move(self->bootstrap_callback_);
            h(ec, {});
            self->stop(retry_reason::do_not_retry);
        });
        initiate_bootstrap();
    }

    void initiate_bootstrap()
    {
        if (stopped_) {
            return;
        }
        state_ = diag::endpoint_state::connecting;
        if (stream_->is_open()) {
            std::string old_id = stream_->id();
            stream_->reopen();
            CB_LOG_TRACE(R"({} reopen socket connection "{}" -> "{}", host="{}", port={})",
                         log_prefix_,
                         old_id,
                         stream_->id(),
                         bootstrap_hostname_,
                         bootstrap_port_);
        }
        if (origin_.exhausted()) {
            auto backoff = std::chrono::milliseconds(500);
            CB_LOG_DEBUG("{} reached the end of list of bootstrap nodes, waiting for {}ms before restart", log_prefix_, backoff.count());
            retry_backoff_.expires_after(backoff);
            retry_backoff_.async_wait([self = shared_from_this()](std::error_code ec) mutable {
                if (ec == asio::error::operation_aborted || self->stopped_) {
                    return;
                }
                self->origin_.restart();
                self->initiate_bootstrap();
            });
            return;
        }
        std::tie(bootstrap_hostname_, bootstrap_port_) = origin_.next_address();
        log_prefix_ = fmt::format("[{}/{}/{}/{}] <{}:{}>",
                                  client_id_,
                                  id_,
                                  stream_->log_prefix(),
                                  bucket_name_.value_or("-"),
                                  bootstrap_hostname_,
                                  bootstrap_port_);
        CB_LOG_DEBUG("{} attempt to establish MCBP connection", log_prefix_);

        async_resolve(origin_.options().use_ip_protocol,
                      resolver_,
                      bootstrap_hostname_,
                      bootstrap_port_,
                      std::bind(&mcbp_session_impl::on_resolve, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_;
    }

    [[nodiscard]] bool is_stopped() const
    {
        return stopped_;
    }

    void on_stop(utils::movable_function<void(retry_reason)> handler)
    {
        on_stop_handler_ = std::move(handler);
    }

    void stop(retry_reason reason)
    {
        if (stopped_) {
            return;
        }
        state_ = diag::endpoint_state::disconnecting;
        CB_LOG_DEBUG("{} stop MCBP connection, reason={}", log_prefix_, reason);
        stopped_ = true;
        bootstrap_deadline_.cancel();
        connection_deadline_.cancel();
        retry_backoff_.cancel();
        resolver_.cancel();
        stream_->close([](std::error_code) {});
        if (auto h = std::move(bootstrap_handler_); h) {
            h->stop();
        }
        if (auto h = std::move(handler_); h) {
            h->stop();
        }
        std::error_code ec = errc::common::request_canceled;
        if (!bootstrapped_) {
            if (auto h = std::move(bootstrap_callback_); h) {
                h(ec, {});
            }
        }
        {
            std::scoped_lock lock(command_handlers_mutex_);
            for (auto& [opaque, handler] : command_handlers_) {
                if (handler) {
                    CB_LOG_DEBUG("{} MCBP cancel operation during session close, opaque={}, ec={}", log_prefix_, opaque, ec.message());
                    auto fun = std::move(handler);
                    fun(ec, reason, {}, {});
                }
            }
            command_handlers_.clear();
        }
        {
            std::scoped_lock lock(operations_mutex_);
            auto operations = std::move(operations_);
            for (auto& [opaque, operation] : operations) {
                auto& [request, handler] = operation;
                if (handler) {
                    CB_LOG_DEBUG("{} MCBP cancel operation during session close, opaque={}, ec={}", log_prefix_, opaque, ec.message());
                    handler->handle_response(std::move(request), {}, reason, {}, {});
                }
            }
            operations_.clear();
        }
        config_listeners_.clear();
        state_ = diag::endpoint_state::disconnected;
        if (auto on_stop = std::move(on_stop_handler_); on_stop) {
            on_stop(reason);
        }
    }

    void write(std::vector<std::byte>&& buf)
    {
        if (stopped_) {
            return;
        }
        std::uint32_t opaque{ 0 };
        std::memcpy(&opaque, buf.data() + 12, sizeof(opaque));
        CB_LOG_TRACE("{} MCBP send, opaque={}, {:n}", log_prefix_, opaque, spdlog::to_hex(buf.begin(), buf.begin() + 24));
        std::scoped_lock lock(output_buffer_mutex_);
        output_buffer_.emplace_back(std::move(buf));
    }

    void flush()
    {
        if (stopped_) {
            return;
        }
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this()]() { self->do_write(); }));
    }

    void write_and_flush(std::vector<std::byte>&& buf)
    {
        if (stopped_) {
            return;
        }
        write(std::move(buf));
        flush();
    }

    void remove_request(std::shared_ptr<mcbp::queue_request> request) override
    {
        std::scoped_lock lock(operations_mutex_);
        if (auto iter = operations_.find(request->opaque_); iter != operations_.end()) {
            operations_.erase(iter);
        }
    }

    void enqueue_request(std::uint32_t opaque, std::shared_ptr<mcbp::queue_request> request, std::shared_ptr<response_handler> handler)
    {
        std::scoped_lock lock(operations_mutex_);
        request->waiting_in_ = this;
        operations_.try_emplace(opaque, std::move(request), std::move(handler));
    }

    auto handle_request(protocol::client_opcode opcode, std::uint16_t status, std::uint32_t opaque, mcbp_message&& msg) -> bool
    {
        // handle request old style
        command_handler fun{};
        {
            std::scoped_lock lock(command_handlers_mutex_);
            if (auto handler = command_handlers_.find(opaque); handler != command_handlers_.end() && handler->second) {
                fun = std::move(handler->second);
                command_handlers_.erase(handler);
            }
        }

        if (fun) {
            fun(protocol::map_status_code(opcode, status), retry_reason::do_not_retry, std::move(msg), decode_error_code(status));
            return true;
        }

        // handle request new style
        std::shared_ptr<mcbp::queue_request> request{};
        std::shared_ptr<response_handler> handler{};
        std::scoped_lock lock(operations_mutex_);
        {
            if (auto pair = operations_.find(opaque); pair != operations_.end() && pair->second.first) {
                request = pair->second.first;
                handler = pair->second.second;
                if (!request->persistent_) {
                    operations_.erase(pair);
                }
            }
        }
        if (request) {
            handler->handle_response(std::move(request),
                                     protocol::map_status_code(opcode, status),
                                     retry_reason::do_not_retry,
                                     std::move(msg),
                                     decode_error_code(status));
            return true;
        }
        return false;
    }

    void write_and_subscribe(std::shared_ptr<mcbp::queue_request> request, std::shared_ptr<response_handler> handler)
    {
        auto opaque = request->opaque_;
        auto data = codec_.encode_packet(*request);
        if (!data) {
            CB_LOG_DEBUG("unable to encode packet. opaque={}, ec={}", opaque, data.error().message());
            request->try_callback({}, data.error());
            return;
        }

        if (stopped_) {
            CB_LOG_WARNING("cancel operation while trying to write to closed mcbp session, opaque={}", opaque);
            handler->handle_response(request, errc::common::request_canceled, retry_reason::socket_closed_while_in_flight, {}, {});
            return;
        }
        enqueue_request(opaque, std::move(request), std::move(handler));
        if (bootstrapped_ && stream_->is_open()) {
            write_and_flush(std::move(data.value()));
        } else {
            CB_LOG_DEBUG("{} the stream is not ready yet, put the message into pending buffer, opaque={}", log_prefix_, opaque);
            std::scoped_lock lock(pending_buffer_mutex_);
            if (bootstrapped_ && stream_->is_open()) {
                write_and_flush(std::move(data.value()));
            } else {
                pending_buffer_.emplace_back(data.value());
            }
        }
    }

    void write_and_subscribe(std::uint32_t opaque, std::vector<std::byte>&& data, command_handler&& handler)
    {
        if (stopped_) {
            CB_LOG_WARNING("{} MCBP cancel operation, while trying to write to closed session, opaque={}", log_prefix_, opaque);
            handler(errc::common::request_canceled, retry_reason::socket_closed_while_in_flight, {}, {});
            return;
        }
        {
            std::scoped_lock lock(command_handlers_mutex_);
            command_handlers_.try_emplace(opaque, std::move(handler));
        }
        if (bootstrapped_ && stream_->is_open()) {
            write_and_flush(std::move(data));
        } else {
            CB_LOG_DEBUG("{} the stream is not ready yet, put the message into pending buffer, opaque={}", log_prefix_, opaque);
            std::scoped_lock lock(pending_buffer_mutex_);
            if (bootstrapped_ && stream_->is_open()) {
                write_and_flush(std::move(data));
            } else {
                pending_buffer_.emplace_back(data);
            }
        }
    }

    [[nodiscard]] bool cancel(std::uint32_t opaque, std::error_code ec, retry_reason reason)
    {
        if (stopped_) {
            return false;
        }
        command_handlers_mutex_.lock();
        if (auto handler = command_handlers_.find(opaque); handler != command_handlers_.end()) {
            CB_LOG_DEBUG("{} MCBP cancel operation, opaque={}, ec={} ({})", log_prefix_, opaque, ec.value(), ec.message());
            if (handler->second) {
                auto fun = std::move(handler->second);
                command_handlers_.erase(handler);
                command_handlers_mutex_.unlock();
                fun(ec, reason, {}, {});
                return true;
            }
        }
        command_handlers_mutex_.unlock();
        return false;
    }

    [[nodiscard]] bool supports_feature(protocol::hello_feature feature)
    {
        return std::find(supported_features_.begin(), supported_features_.end(), feature) != supported_features_.end();
    }

    [[nodiscard]] std::vector<protocol::hello_feature> supported_features() const
    {
        return supported_features_;
    }

    [[nodiscard]] bool supports_gcccp() const
    {
        return supports_gcccp_;
    }

    [[nodiscard]] bool has_config() const
    {
        return configured_;
    }

    [[nodiscard]] topology::configuration config()
    {
        std::scoped_lock lock(config_mutex_);
        return config_.value();
    }

    [[nodiscard]] std::size_t index() const
    {
        std::scoped_lock lock(config_mutex_);
        Expects(config_.has_value());
        return config_->index_for_this_node();
    }

    [[nodiscard]] const std::string& bootstrap_hostname() const
    {
        return bootstrap_hostname_;
    }

    [[nodiscard]] const std::string& bootstrap_port() const
    {
        return bootstrap_port_;
    }

    [[nodiscard]] std::uint32_t next_opaque()
    {
        return ++opaque_;
    }

    std::optional<key_value_error_map_info> decode_error_code(std::uint16_t code)
    {
        if (error_map_) {
            auto info = error_map_->errors.find(code);
            if (info != error_map_->errors.end()) {
                return info->second;
            }
        }
        return {};
    }

    void on_configuration_update(std::shared_ptr<config_listener> handler)
    {
        config_listeners_.emplace_back(std::move(handler));
    }

    void update_configuration(topology::configuration&& config)
    {
        if (stopped_) {
            return;
        }
        std::scoped_lock lock(config_mutex_);
        if (config_) {
            if (config_->vbmap && config.vbmap && config_->vbmap->size() != config.vbmap->size()) {
                CB_LOG_DEBUG("{} received a configuration with a different number of vbuckets, ignoring", log_prefix_);
                return;
            }
            if (config == config_) {
                CB_LOG_TRACE("{} received a configuration with identical revision (rev={}), ignoring", log_prefix_, config.rev_str());
                return;
            }
            if (config < config_) {
                CB_LOG_DEBUG("{} received a configuration with older revision, ignoring", log_prefix_);
                return;
            }
        }
        bool this_node_found = false;
        for (auto& node : config.nodes) {
            if (node.hostname.empty()) {
                node.hostname = bootstrap_hostname_;
            }
            if (node.this_node) {
                this_node_found = true;
            }
        }
        if (!this_node_found) {
            for (auto& node : config.nodes) {
                if (node.hostname == bootstrap_hostname_) {
                    if ((node.services_plain.key_value && std::to_string(node.services_plain.key_value.value()) == bootstrap_port_) ||
                        (node.services_tls.key_value && std::to_string(node.services_tls.key_value.value()) == bootstrap_port_)) {
                        node.this_node = true;
                    }
                }
            }
        }
        config_.emplace(std::move(config));
        configured_ = true;
        for (const auto& listener : config_listeners_) {
            asio::post(asio::bind_executor(
              ctx_, [listener, config = config_.value()]() mutable { return listener->update_config(std::move(config)); }));
        }
    }

    void handle_not_my_vbucket(const io::mcbp_message& msg)
    {
        if (stopped_) {
            return;
        }
        Expects(msg.header.magic == static_cast<std::uint8_t>(protocol::magic::alt_client_response) ||
                msg.header.magic == static_cast<std::uint8_t>(protocol::magic::client_response));
        if (protocol::has_json_datatype(msg.header.datatype)) {
            auto magic = static_cast<protocol::magic>(msg.header.magic);
            std::uint8_t extras_size = msg.header.extlen;
            std::uint8_t framing_extras_size = 0;
            std::uint16_t key_size = utils::byte_swap(msg.header.keylen);
            if (magic == protocol::magic::alt_client_response) {
                framing_extras_size = static_cast<std::uint8_t>(msg.header.keylen >> 8U);
                key_size = msg.header.keylen & 0xffU;
            }

            std::vector<std::uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
            if (utils::byte_swap(msg.header.bodylen) - offset > 0) {
                std::string_view config_text{ reinterpret_cast<const char*>(msg.body.data()) + offset,
                                              msg.body.size() - static_cast<std::size_t>(offset) };
                auto config = protocol::parse_config(config_text, endpoint_address_, endpoint_.port());
                CB_LOG_DEBUG("{} received not_my_vbucket status for {}, opaque={} with config rev={} in the payload",
                             log_prefix_,
                             protocol::client_opcode(msg.header.opcode),
                             msg.header.opaque,
                             config.rev_str());
                update_configuration(std::move(config));
            }
        }
    }

    std::optional<std::uint32_t> get_collection_uid(const std::string& collection_path)
    {
        return collection_cache_.get(collection_path);
    }

    void update_collection_uid(const std::string& path, std::uint32_t uid)
    {
        if (stopped_) {
            return;
        }
        collection_cache_.update(path, uid);
    }

  private:
    void invoke_bootstrap_handler(std::error_code ec)
    {
        retry_backoff_.cancel();

        if (ec && state_listener_) {
            state_listener_->report_bootstrap_error(fmt::format("{}:{}", bootstrap_hostname_, bootstrap_port_), ec);
        }
        if (ec == errc::network::configuration_not_available) {
            return initiate_bootstrap();
        }
        if (retry_bootstrap_on_bucket_not_found_ && ec == errc::common::bucket_not_found) {
            CB_LOG_DEBUG(R"({} server returned {} ({}), it must be transient condition, retrying)", log_prefix_, ec.value(), ec.message());
            return initiate_bootstrap();
        }

        if (!bootstrapped_ && bootstrap_callback_) {
            bootstrap_deadline_.cancel();
            if (config_ && state_listener_) {
                std::vector<std::string> endpoints;
                endpoints.reserve(config_.value().nodes.size());
                for (const auto& node : config_.value().nodes) {
                    if (auto endpoint = node.endpoint(origin_.options().network, service_type::key_value, is_tls_); endpoint) {
                        endpoints.push_back(endpoint.value());
                    }
                }
                state_listener_->report_bootstrap_success(endpoints);
            }
            auto h = std::move(bootstrap_callback_);
            h(ec, config_.value_or(topology::configuration{}));
        }
        if (ec) {
            return stop(retry_reason::node_not_available);
        }
        state_ = diag::endpoint_state::connected;
        std::scoped_lock lock(pending_buffer_mutex_);
        bootstrapped_ = true;
        bootstrap_handler_->stop();
        handler_ = std::make_shared<message_handler>(shared_from_this());
        handler_->start();
        if (!pending_buffer_.empty()) {
            for (auto& buf : pending_buffer_) {
                write(std::move(buf));
            }
            pending_buffer_.clear();
            flush();
        }
    }

    void on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (ec) {
            CB_LOG_ERROR("{} error on resolve: {} ({})", log_prefix_, ec.value(), ec.message());
            return initiate_bootstrap();
        }
        endpoints_ = endpoints;
        do_connect(endpoints_.begin());
        connection_deadline_.expires_after(origin_.options().resolve_timeout);
        connection_deadline_.async_wait([self = shared_from_this()](const auto timer_ec) {
            if (timer_ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            return self->stream_->close([self](std::error_code) { self->initiate_bootstrap(); });
        });
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (it != endpoints_.end()) {
            CB_LOG_DEBUG("{} connecting to {}:{}, timeout={}ms",
                         log_prefix_,
                         it->endpoint().address().to_string(),
                         it->endpoint().port(),
                         origin_.options().connect_timeout.count());
            connection_deadline_.expires_after(origin_.options().connect_timeout);
            connection_deadline_.async_wait([self = shared_from_this()](const auto timer_ec) {
                if (timer_ec == asio::error::operation_aborted || self->stopped_) {
                    return;
                }
                return self->stream_->close([self](std::error_code) { self->initiate_bootstrap(); });
            });
            stream_->async_connect(it->endpoint(),
                                   std::bind(&mcbp_session_impl::on_connect, shared_from_this(), std::placeholders::_1, it));
        } else {
            CB_LOG_ERROR("{} no more endpoints left to connect, will try another address", log_prefix_);
            if (state_listener_) {
                state_listener_->report_bootstrap_error(fmt::format("{}:{}", bootstrap_hostname_, bootstrap_port_),
                                                        errc::network::no_endpoints_left);
            }
            return initiate_bootstrap();
        }
    }

    void on_connect(const std::error_code& ec, asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (!stream_->is_open() || ec) {

            CB_LOG_WARNING("{} unable to connect to {}:{}: {} ({}){}. is_open={}",
                           log_prefix_,
                           it->endpoint().address().to_string(),
                           it->endpoint().port(),
                           ec.value(),
                           (ec.category() == asio::error::ssl_category) ? ERR_error_string(static_cast<unsigned long>(ec.value()), nullptr)
                                                                        : ec.message(),
                           (ec == asio::error::connection_refused) ? ", check server ports and cluster encryption setting" : "",
                           stream_->is_open());
            if (stream_->is_open()) {
                stream_->close(std::bind(&mcbp_session_impl::do_connect, shared_from_this(), ++it));
            } else {
                do_connect(++it);
            }
        } else {
            stream_->set_options();
            local_endpoint_ = stream_->local_endpoint();
            local_endpoint_address_ = local_endpoint_.address().to_string();
            endpoint_ = it->endpoint();
            endpoint_address_ = endpoint_.address().to_string();
            CB_LOG_DEBUG("{} connected to {}:{}", log_prefix_, endpoint_address_, it->endpoint().port());
            log_prefix_ = fmt::format("[{}/{}/{}/{}] <{}/{}:{}>",
                                      client_id_,
                                      id_,
                                      stream_->log_prefix(),
                                      bucket_name_.value_or("-"),
                                      bootstrap_hostname_,
                                      endpoint_address_,
                                      endpoint_.port());
            bootstrap_handler_ = std::make_shared<bootstrap_handler>(shared_from_this());
            connection_deadline_.cancel();
        }
    }

    void check_deadline(std::error_code ec)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        if (connection_deadline_.expiry() <= asio::steady_timer::clock_type::now()) {
            stream_->close([](std::error_code) {});
        }
        connection_deadline_.async_wait(std::bind(&mcbp_session_impl::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_ || reading_ || !stream_->is_open()) {
            return;
        }
        reading_ = true;
        stream_->async_read_some(
          asio::buffer(input_buffer_),
          [self = shared_from_this(), stream_id = stream_->id()](std::error_code ec, std::size_t bytes_transferred) {
              if (ec == asio::error::operation_aborted || self->stopped_) {
                  return;
              }
              self->last_active_ = std::chrono::steady_clock::now();
              if (ec) {
                  if (stream_id != self->stream_->id()) {
                      CB_LOG_ERROR(R"({} ignore IO error while reading from the socket: {} ({}), old_id="{}", new_id="{}")",
                                   self->log_prefix_,
                                   ec.value(),
                                   ec.message(),
                                   stream_id,
                                   self->stream_->id());
                      return;
                  }
                  CB_LOG_ERROR(R"({} IO error while reading from the socket("{}"): {} ({}))",
                               self->log_prefix_,
                               self->stream_->id(),
                               ec.value(),
                               ec.message());
                  return self->stop(retry_reason::socket_closed_while_in_flight);
              }
              self->parser_.feed(self->input_buffer_.data(), self->input_buffer_.data() + static_cast<std::ptrdiff_t>(bytes_transferred));

              for (;;) {
                  mcbp_message msg{};
                  switch (self->parser_.next(msg)) {
                      case mcbp_parser::result::ok: {
                          if (self->stopped_) {
                              return;
                          }
                          CB_LOG_TRACE(
                            "{} MCBP recv, opaque={}, {:n}", self->log_prefix_, msg.header.opaque, spdlog::to_hex(msg.header_data()));
                          if (self->bootstrapped_) {
                              self->handler_->handle(std::move(msg));
                          } else {
                              self->bootstrap_handler_->handle(std::move(msg));
                          }
                          if (self->stopped_) {
                              return;
                          }
                      } break;
                      case mcbp_parser::result::need_data:
                          self->reading_ = false;
                          if (!self->stopped_ && self->stream_->is_open()) {
                              self->do_read();
                          }
                          return;
                      case mcbp_parser::result::failure:
                          return self->stop(retry_reason::key_value_temporary_failure);
                  }
              }
          });
    }

    void do_write()
    {
        if (stopped_ || !stream_->is_open()) {
            return;
        }
        std::scoped_lock lock(writing_buffer_mutex_, output_buffer_mutex_);
        if (!writing_buffer_.empty() || output_buffer_.empty()) {
            return;
        }
        std::swap(writing_buffer_, output_buffer_);
        std::vector<asio::const_buffer> buffers;
        buffers.reserve(writing_buffer_.size());
        for (auto& buf : writing_buffer_) {
            buffers.emplace_back(asio::buffer(buf));
        }
        stream_->async_write(buffers, [self = shared_from_this()](std::error_code ec, std::size_t /*unused*/) {
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            self->last_active_ = std::chrono::steady_clock::now();
            if (ec) {
                CB_LOG_ERROR(R"({} IO error while writing to the socket("{}"): {} ({}))",
                             self->log_prefix_,
                             self->stream_->id(),
                             ec.value(),
                             ec.message());
                return self->stop(retry_reason::socket_closed_while_in_flight);
            }
            {
                std::scoped_lock inner_lock(self->writing_buffer_mutex_);
                self->writing_buffer_.clear();
            }
            asio::post(asio::bind_executor(self->ctx_, [self]() {
                self->do_write();
                self->do_read();
            }));
        });
    }

    const std::string client_id_;
    const std::string id_{ uuid::to_string(uuid::random()) };
    asio::io_context& ctx_;
    asio::ip::tcp::resolver resolver_;
    std::unique_ptr<stream_impl> stream_;
    asio::steady_timer bootstrap_deadline_;
    asio::steady_timer connection_deadline_;
    asio::steady_timer retry_backoff_;
    couchbase::core::origin origin_;
    std::optional<std::string> bucket_name_;
    mcbp_parser parser_;
    std::shared_ptr<bootstrap_handler> bootstrap_handler_{ nullptr };
    std::shared_ptr<message_handler> handler_{ nullptr };
    utils::movable_function<void(std::error_code, const topology::configuration&)> bootstrap_callback_{};
    std::mutex command_handlers_mutex_{};
    std::map<std::uint32_t, command_handler> command_handlers_{};
    std::vector<std::shared_ptr<config_listener>> config_listeners_{};
    utils::movable_function<void(retry_reason)> on_stop_handler_{};

    std::atomic_bool bootstrapped_{ false };
    std::atomic_bool stopped_{ false };
    bool authenticated_{ false };
    bool bucket_selected_{ false };
    bool supports_gcccp_{ true };
    bool retry_bootstrap_on_bucket_not_found_{ false };

    std::atomic<std::uint32_t> opaque_{ 0 };

    std::array<std::byte, 16384> input_buffer_{};
    std::vector<std::vector<std::byte>> output_buffer_{};
    std::vector<std::vector<std::byte>> pending_buffer_{};
    std::vector<std::vector<std::byte>> writing_buffer_{};
    std::mutex output_buffer_mutex_{};
    std::mutex pending_buffer_mutex_{};
    std::mutex writing_buffer_mutex_{};
    std::string bootstrap_hostname_{};
    std::string bootstrap_port_{};
    asio::ip::tcp::endpoint endpoint_{}; // connected endpoint
    std::string endpoint_address_{};     // cached string with endpoint address
    asio::ip::tcp::endpoint local_endpoint_{};
    std::string local_endpoint_address_{};
    asio::ip::tcp::resolver::results_type endpoints_;
    std::vector<protocol::hello_feature> supported_features_;
    std::optional<topology::configuration> config_;
    mutable std::mutex config_mutex_{};
    std::atomic_bool configured_{ false };
    std::optional<error_map> error_map_;
    collection_cache collection_cache_;

    const bool is_tls_;
    std::shared_ptr<impl::bootstrap_state_listener> state_listener_{ nullptr };

    mcbp::codec codec_;
    std::recursive_mutex operations_mutex_{};
    std::map<std::uint32_t, std::pair<std::shared_ptr<mcbp::queue_request>, std::shared_ptr<response_handler>>> operations_{};

    std::atomic_bool reading_{ false };

    std::string log_prefix_{};
    std::chrono::time_point<std::chrono::steady_clock> last_active_{};
    std::atomic<diag::endpoint_state> state_{ diag::endpoint_state::disconnected };
};

mcbp_session::mcbp_session(std::string client_id,
                           asio::io_context& ctx,
                           core::origin origin,
                           std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                           std::optional<std::string> bucket_name,
                           std::vector<protocol::hello_feature> known_features)
  : impl_{ std::make_shared<mcbp_session_impl>(std::move(client_id),
                                               ctx,
                                               std::move(origin),
                                               std::move(state_listener),
                                               std::move(bucket_name),
                                               std::move(known_features)) }
{
}

mcbp_session::mcbp_session(std::string client_id,
                           asio::io_context& ctx,
                           asio::ssl::context& tls,
                           core::origin origin,
                           std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                           std::optional<std::string> bucket_name,
                           std::vector<protocol::hello_feature> known_features)
  : impl_{ std::make_shared<mcbp_session_impl>(std::move(client_id),
                                               ctx,
                                               tls,
                                               std::move(origin),
                                               std::move(state_listener),
                                               std::move(bucket_name),
                                               std::move(known_features)) }
{
}

const std::string&
mcbp_session::log_prefix() const
{
    return impl_->log_prefix();
}

bool
mcbp_session::cancel(std::uint32_t opaque, std::error_code ec, retry_reason reason)
{
    return impl_->cancel(opaque, ec, reason);
}

bool
mcbp_session::is_stopped() const
{
    return impl_->is_stopped();
}

std::uint32_t
mcbp_session::next_opaque()
{
    return impl_->next_opaque();
}

std::optional<std::uint32_t>
mcbp_session::get_collection_uid(const std::string& collection_path)
{
    return impl_->get_collection_uid(collection_path);
}

mcbp_context
mcbp_session::context() const
{
    return impl_->context();
}

bool
mcbp_session::supports_feature(protocol::hello_feature feature)
{
    return impl_->supports_feature(feature);
}

// const std::string&
// mcbp_session::id() const
// {
//     return impl_->id();
// }
std::string
mcbp_session::id() const
{
    if (impl_) {
        return fmt::format("{}, {}, {}, refcnt={}",
                           reinterpret_cast<const void*>(this),
                           reinterpret_cast<const void*>(impl_.get()),
                           impl_->id(),
                           impl_.use_count());
    }
    return fmt::format("{}, nullptr", reinterpret_cast<const void*>(this));
}

std::string
mcbp_session::remote_address() const
{
    return impl_->remote_address();
}

std::string
mcbp_session::local_address() const
{
    return impl_->local_address();
}

const std::string&
mcbp_session::bootstrap_hostname() const
{
    return impl_->bootstrap_hostname();
}

const std::string&
mcbp_session::bootstrap_port() const
{
    return impl_->bootstrap_port();
}

void
mcbp_session::write_and_subscribe(std::uint32_t opaque, std::vector<std::byte>&& data, command_handler&& handler)
{
    return impl_->write_and_subscribe(opaque, std::move(data), std::move(handler));
}

void
mcbp_session::bootstrap(utils::movable_function<void(std::error_code, topology::configuration)>&& handler, bool retry_on_bucket_not_found)
{
    return impl_->bootstrap(std::move(handler), retry_on_bucket_not_found);
}

void
mcbp_session::on_stop(utils::movable_function<void(retry_reason)> handler)
{
    return impl_->on_stop(std::move(handler));
}

void
mcbp_session::stop(retry_reason reason)
{
    return impl_->stop(reason);
}

std::size_t
mcbp_session::index() const
{
    return impl_->index();
}

bool
mcbp_session::has_config() const
{
    return impl_->has_config();
}

diag::endpoint_diag_info
mcbp_session::diag_info() const
{
    return impl_->diag_info();
}

void
mcbp_session::on_configuration_update(std::shared_ptr<config_listener> handler)
{
    return impl_->on_configuration_update(std::move(handler));
}

std::vector<protocol::hello_feature>
mcbp_session::supported_features() const
{
    return impl_->supported_features();
}

void
mcbp_session::ping(std::shared_ptr<diag::ping_reporter> handler) const
{
    return impl_->ping(std::move(handler));
}

bool
mcbp_session::supports_gcccp() const
{
    return impl_->supports_gcccp();
}

std::optional<key_value_error_map_info>
mcbp_session::decode_error_code(std::uint16_t code)
{
    return impl_->decode_error_code(code);
}

void
mcbp_session::handle_not_my_vbucket(const mcbp_message& msg) const
{
    return impl_->handle_not_my_vbucket(msg);
}

void
mcbp_session::update_collection_uid(const std::string& path, std::uint32_t uid)
{
    return impl_->update_collection_uid(path, uid);
}

void
mcbp_session::write_and_subscribe(std::shared_ptr<mcbp::queue_request> request, std::shared_ptr<response_handler> handler)
{
    return impl_->write_and_subscribe(std::move(request), std::move(handler));
}

} // namespace couchbase::core::io
