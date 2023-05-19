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

#include "core/protocol/hello_feature.hxx"
#include "core/response_handler.hxx"
#include "core/utils/movable_function.hxx"
#include "mcbp_context.hxx"
#include "mcbp_message.hxx"

#include <cinttypes>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace asio
{
class io_context;
namespace ssl
{
class context;
} // namespace ssl
} // namespace asio

namespace couchbase::core
{
struct origin;
class config_listener;

namespace topology
{
struct configuration;
} // namespace topology

namespace diag
{
class ping_reporter;
struct endpoint_diag_info;
} // namespace diag

namespace impl
{
class bootstrap_state_listener;
} // namespace impl

namespace io
{
class mcbp_session_impl;

using command_handler =
  utils::movable_function<void(std::error_code, retry_reason, io::mcbp_message&&, std::optional<key_value_error_map_info>)>;

class mcbp_session
{
  public:
    mcbp_session() = delete;
    ~mcbp_session() = default;
    mcbp_session(const mcbp_session& other) = default;
    mcbp_session& operator=(const mcbp_session& other) = default;
    mcbp_session(mcbp_session&& other) = default;
    mcbp_session& operator=(mcbp_session&& other) = default;

    mcbp_session(std::string client_id,
                 asio::io_context& ctx,
                 couchbase::core::origin origin,
                 std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                 std::optional<std::string> bucket_name = {},
                 std::vector<protocol::hello_feature> known_features = {});

    mcbp_session(std::string client_id,
                 asio::io_context& ctx,
                 asio::ssl::context& tls,
                 couchbase::core::origin origin,
                 std::shared_ptr<impl::bootstrap_state_listener> state_listener,
                 std::optional<std::string> bucket_name = {},
                 std::vector<protocol::hello_feature> known_features = {});

    [[nodiscard]] const std::string& log_prefix() const;
    [[nodiscard]] bool cancel(std::uint32_t opaque, std::error_code ec, retry_reason reason);
    [[nodiscard]] bool is_stopped() const;
    [[nodiscard]] std::uint32_t next_opaque();
    [[nodiscard]] std::optional<std::uint32_t> get_collection_uid(const std::string& collection_path);
    [[nodiscard]] mcbp_context context() const;
    [[nodiscard]] bool supports_feature(protocol::hello_feature feature);
    [[nodiscard]] std::vector<protocol::hello_feature> supported_features() const;
    [[nodiscard]] const std::string& id() const;
    [[nodiscard]] std::string remote_address() const;
    [[nodiscard]] std::string local_address() const;
    [[nodiscard]] const std::string& bootstrap_address() const;
    [[nodiscard]] const std::string& bootstrap_hostname() const;
    [[nodiscard]] const std::string& bootstrap_port() const;
    [[nodiscard]] std::uint16_t bootstrap_port_number() const;
    void write_and_subscribe(std::shared_ptr<mcbp::queue_request>, std::shared_ptr<response_handler> handler);
    void write_and_subscribe(std::uint32_t opaque, std::vector<std::byte>&& data, command_handler&& handler);
    void bootstrap(utils::movable_function<void(std::error_code, topology::configuration)>&& handler,
                   bool retry_on_bucket_not_found = false);
    void on_stop(utils::movable_function<void()> handler);
    void stop(retry_reason reason);
    [[nodiscard]] std::size_t index() const;
    [[nodiscard]] bool has_config() const;
    [[nodiscard]] diag::endpoint_diag_info diag_info() const;
    void on_configuration_update(std::shared_ptr<config_listener> handler);
    void ping(std::shared_ptr<diag::ping_reporter> handler) const;
    [[nodiscard]] bool supports_gcccp() const;
    [[nodiscard]] std::optional<key_value_error_map_info> decode_error_code(std::uint16_t code);
    void handle_not_my_vbucket(const io::mcbp_message& msg) const;
    void update_collection_uid(const std::string& path, std::uint32_t uid);

  private:
    std::shared_ptr<mcbp_session_impl> impl_{ nullptr };
};
} // namespace io
} // namespace couchbase::core
