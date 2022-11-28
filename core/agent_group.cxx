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

#include "agent_group.hxx"
#include "agent.hxx"
#include "cluster_agent.hxx"
#include "logger/logger.hxx"
#include "meta/version.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

#include <mutex>

namespace couchbase::core
{
class agent_group_impl
{
  public:
    agent_group_impl(asio::io_context& io, agent_group_config config)
      : io_{ io }
      , config_(std::move(config))
      , cluster_agent_({
          config_.shim,
          config_.user_agent,
          config_.default_retry_strategy,
          config_.seed,
          config_.key_value,
        })
    {
        CB_LOG_DEBUG("SDK version: {}", meta::sdk_id());
        CB_LOG_DEBUG("creating new agent group: {}", config_.to_string());
    }

    auto open_bucket(const std::string& bucket_name) -> std::error_code
    {
        std::scoped_lock lock(bound_agents_mutex_);

        if (auto existing_agent = get_agent(bucket_name)) {
            return {};
        }

        bound_agents_.try_emplace(bucket_name,
                                  agent{
                                    io_,
                                    {
                                      config_.shim,
                                      bucket_name,
                                      config_.user_agent,
                                      config_.default_retry_strategy,
                                      config_.seed,
                                      config_.key_value,
                                    },
                                  });

        return {};
    }

    auto get_agent(const std::string& bucket_name) -> tl::expected<agent, std::error_code>
    {
        std::scoped_lock lock(bound_agents_mutex_);

        if (auto existing_agent = bound_agents_.find(bucket_name); existing_agent != bound_agents_.end()) {
            return existing_agent->second;
        }
        return tl::make_unexpected(errc::common::bucket_not_found);
    }

    auto close() -> std::error_code
    {
        return {};
    }

    auto n1ql_query(n1ql_query_options /* options */, n1ql_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto prepared_n1ql_query(n1ql_query_options /* options */, n1ql_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto analytics_query(analytics_query_options /* options */, analytics_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto search_query(search_query_options /* options */, search_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto free_form_http_request(http_request /* request */, free_form_http_request_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto wait_until_ready(std::chrono::milliseconds /* timeout */, wait_until_ready_options /* options */, wait_until_ready_callback &&
                          /* callback */) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto ping(ping_options /* options */, ping_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return {};
    }

    auto diagnostics(diagnostics_options /* options */) -> tl::expected<diagnostic_info, std::error_code>
    {
        return {};
    }

  private:
    asio::io_context& io_;
    const agent_group_config config_;
    cluster_agent cluster_agent_;
    std::map<std::string, agent, std::less<>> bound_agents_{};
    mutable std::recursive_mutex bound_agents_mutex_{};
};

agent_group::agent_group(asio::io_context& io, agent_group_config config)
  : impl_{ std::make_shared<agent_group_impl>(io, std::move(config)) }
{
}

auto
agent_group::open_bucket(const std::string& bucket_name) -> std::error_code
{
    return impl_->open_bucket(bucket_name);
}

auto
agent_group::get_agent(const std::string& bucket_name) -> tl::expected<agent, std::error_code>
{
    return impl_->get_agent(bucket_name);
}

auto
agent_group::close() -> std::error_code
{
    return impl_->close();
}

auto
agent_group::n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->n1ql_query(std::move(options), std::move(callback));
}

auto
agent_group::prepared_n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->prepared_n1ql_query(std::move(options), std::move(callback));
}

auto
agent_group::analytics_query(analytics_query_options options, analytics_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->analytics_query(std::move(options), std::move(callback));
}

auto
agent_group::search_query(search_query_options options, search_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->search_query(std::move(options), std::move(callback));
}

auto
agent_group::free_form_http_request(http_request request, free_form_http_request_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->free_form_http_request(std::move(request), std::move(callback));
}

auto
agent_group::wait_until_ready(std::chrono::milliseconds timeout, wait_until_ready_options options, wait_until_ready_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->wait_until_ready(timeout, std::move(options), std::move(callback));
}

auto
agent_group::ping(ping_options options, ping_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->ping(std::move(options), std::move(callback));
}

auto
agent_group::diagnostics(diagnostics_options options) -> tl::expected<diagnostic_info, std::error_code>
{
    return impl_->diagnostics(options);
}

} // namespace couchbase::core
