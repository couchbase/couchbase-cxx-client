/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include "agent.hxx"
#include "agent_group_config.hxx"
#include "analytics_query_options.hxx"
#include "diagntostics_options.hxx"
#include "free_form_http_request.hxx"
#include "n1ql_query_options.hxx"
#include "pending_operation.hxx"
#include "ping_options.hxx"
#include "search_query_options.hxx"
#include "wait_until_ready_options.hxx"

#include <tl/expected.hpp>

#include <memory>
#include <system_error>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class agent_group_impl;

class agent_group
{
  public:
    agent_group(asio::io_context& io, agent_group_config config);

    auto open_bucket(const std::string& bucket_name) -> std::error_code;

    auto get_agent(const std::string& bucket_name) -> tl::expected<agent, std::error_code>;

    auto close() -> std::error_code;

    auto n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto prepared_n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto analytics_query(analytics_query_options options, analytics_query_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto search_query(search_query_options options, search_query_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto free_form_http_request(http_request request, free_form_http_request_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto wait_until_ready(std::chrono::milliseconds timeout, wait_until_ready_options options, wait_until_ready_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto ping(ping_options options, ping_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

    auto diagnostics(diagnostics_options options) -> tl::expected<diagnostic_info, std::error_code>;

  private:
    std::shared_ptr<agent_group_impl> impl_;
};
} // namespace couchbase::core
