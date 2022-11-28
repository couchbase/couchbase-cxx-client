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

#include "agent.hxx"

#include "collections_component.hxx"
#include "core/logger/logger.hxx"
#include "core/meta/version.hxx"
#include "crud_component.hxx"
#include "dispatcher.hxx"

#include "agent_unit_test_api.hxx"
#include "collections_component_unit_test_api.hxx"

#include <couchbase/error_codes.hxx>

#include <asio/io_context.hpp>

namespace couchbase::core
{
class agent_impl
{
  public:
    agent_impl(asio::io_context& io, agent_config config)
      : io_{ io }
      , config_{ std::move(config) }
      , bucket_name_{ config_.bucket_name }
      , collections_{ io_, { bucket_name_, config_.shim }, { config_.key_value.max_queue_size, config_.default_retry_strategy } }
      , crud_{ io_, collections_, config_.default_retry_strategy }
    {
        CB_LOG_DEBUG("SDK version: {}", meta::sdk_id());
        CB_LOG_DEBUG("creating new agent: {}", config_.to_string());
    }

    [[nodiscard]] auto bucket_name() const -> const std::string&
    {
        return bucket_name_;
    }

    auto get(get_options /* options */, get_callback && /* callback */) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_and_touch(get_and_touch_options /* options */, get_and_touch_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_and_lock(get_and_lock_options /* options */, get_and_touch_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_one_replica(get_one_replica_options /* options */, get_one_replica_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto touch(touch_options /* options */, touch_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto unlock(unlock_options /* options */, unlock_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto remove(remove_options /* options */, remove_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto insert(insert_options /* options */, insert_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto upsert(upsert_options /* options */, upsert_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto replace(replace_options /* options */, replace_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto append(adjoin_options /* options */, adjoin_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto prepend(adjoin_options /* options */, adjoin_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto increment(counter_options /* options */, counter_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto decrement(counter_options /* options */, counter_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto lookup_in(lookup_in_options /* options */, lookup_in_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto mutate_in(mutate_in_options /* options */, mutate_in_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_random(get_random_options /* options */, get_random_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_with_meta(get_with_meta_options /* options */, get_with_meta_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto upsert_with_meta(upsert_with_meta_options /* options */, upsert_with_meta_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto remove_with_meta(remove_with_meta_options /* options */, remove_with_meta_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto n1ql_query(n1ql_query_options /* options */, n1ql_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto prepared_n1ql_query(n1ql_query_options /* options */, n1ql_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto analytics_query(analytics_query_options /* options */, analytics_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto search_query(search_query_options /* options */, search_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto view_query(view_query_options /* options */, view_query_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto free_form_http_request(http_request /* request */, free_form_http_request_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto get_collection_id(std::string scope_name,
                           std::string collection_name,
                           get_collection_id_options options,
                           get_collection_id_callback callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return collections_.get_collection_id(std::move(scope_name), std::move(collection_name), std::move(options), std::move(callback));
    }

    auto wait_until_ready(std::chrono::milliseconds /* timeout */, wait_until_ready_options /* options */, wait_until_ready_callback &&
                          /* callback */) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto ping(ping_options /* options */, ping_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto diagnostics(diagnostics_options /* options */) -> tl::expected<diagnostic_info, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto stats(stats_options /* options */, stats_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto observe(observe_options /* options */, observe_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto observe_seqno(observe_seqno_options /* options */, observe_seqno_callback && /* callback */)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return tl::unexpected(errc::common::unsupported_operation);
    }

    auto range_scan_create(std::uint16_t vbucket_id, range_scan_create_options options, range_scan_create_callback&& callback)
      -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return crud_.range_scan_create(vbucket_id, std::move(options), std::move(callback));
    }

    auto range_scan_continue(std::vector<std::byte> scan_uuid,
                             std::uint16_t vbucket_id,
                             range_scan_continue_options options,
                             range_scan_item_callback&& item_callback,
                             range_scan_continue_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return crud_.range_scan_continue(
          std::move(scan_uuid), vbucket_id, std::move(options), std::move(item_callback), std::move(callback));
    }

    auto range_scan_cancel(std::vector<std::byte> scan_uuid,
                           std::uint16_t vbucket_id,
                           range_scan_cancel_options options,
                           range_scan_cancel_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
    {
        return crud_.range_scan_cancel(std::move(scan_uuid), vbucket_id, std::move(options), std::move(callback));
    }

  private:
    friend class agent_unit_test_api;

    asio::io_context& io_;
    const agent_config config_;
    const std::string bucket_name_;
    collections_component collections_;
    crud_component crud_;
};

core::agent::agent(asio::io_context& io, agent_config config)
  : impl_{ std::make_shared<agent_impl>(io, std::move(config)) }
{
}

auto
agent::bucket_name() const -> const std::string&
{
    return impl_->bucket_name();
}

auto
agent::get(get_options options, get_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get(std::move(options), std::move(callback));
}

auto
agent::get_and_touch(get_and_touch_options options, get_and_touch_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_and_touch(std::move(options), std::move(callback));
}

auto
agent::get_and_lock(get_and_lock_options options, get_and_touch_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_and_lock(std::move(options), std::move(callback));
}

auto
agent::get_one_replica(get_one_replica_options options, get_one_replica_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_one_replica(std::move(options), std::move(callback));
}

auto
agent::touch(touch_options options, touch_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->touch(std::move(options), std::move(callback));
}

auto
agent::unlock(unlock_options options, unlock_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->unlock(std::move(options), std::move(callback));
}

auto
agent::remove(remove_options options, remove_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->remove(std::move(options), std::move(callback));
}

auto
agent::insert(insert_options options, insert_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->insert(std::move(options), std::move(callback));
}

auto
agent::upsert(upsert_options options, upsert_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->upsert(std::move(options), std::move(callback));
}

auto
agent::replace(replace_options options, replace_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->replace(std::move(options), std::move(callback));
}

auto
agent::append(adjoin_options options, adjoin_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->append(std::move(options), std::move(callback));
}

auto
agent::prepend(adjoin_options options, adjoin_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->prepend(std::move(options), std::move(callback));
}

auto
agent::increment(counter_options options, counter_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->increment(std::move(options), std::move(callback));
}

auto
agent::decrement(counter_options options, counter_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->decrement(std::move(options), std::move(callback));
}

auto
agent::get_random(get_random_options options, get_random_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_random(std::move(options), std::move(callback));
}

auto
agent::get_with_meta(get_with_meta_options options, get_with_meta_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_with_meta(std::move(options), std::move(callback));
}

auto
agent::upsert_with_meta(upsert_with_meta_options options, upsert_with_meta_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->upsert_with_meta(std::move(options), std::move(callback));
}

auto
agent::remove_with_meta(remove_with_meta_options options, remove_with_meta_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->remove_with_meta(std::move(options), std::move(callback));
}

auto
agent::n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->n1ql_query(std::move(options), std::move(callback));
}

auto
agent::prepared_n1ql_query(n1ql_query_options options, n1ql_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->prepared_n1ql_query(std::move(options), std::move(callback));
}

auto
agent::analytics_query(analytics_query_options options, analytics_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->analytics_query(std::move(options), std::move(callback));
}

auto
agent::search_query(search_query_options options, search_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->search_query(std::move(options), std::move(callback));
}

auto
agent::view_query(view_query_options options, view_query_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->view_query(std::move(options), std::move(callback));
}

auto
agent::free_form_http_request(http_request request, free_form_http_request_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->free_form_http_request(std::move(request), std::move(callback));
}

auto
agent::wait_until_ready(std::chrono::milliseconds timeout, wait_until_ready_options options, wait_until_ready_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->wait_until_ready(timeout, std::move(options), std::move(callback));
}

auto
agent::ping(ping_options options, ping_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->ping(std::move(options), std::move(callback));
}

auto
agent::diagnostics(diagnostics_options options) -> tl::expected<diagnostic_info, std::error_code>
{
    return impl_->diagnostics(options);
}

auto
agent::stats(stats_options options, stats_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->stats(std::move(options), std::move(callback));
}

auto
agent::get_collection_id(std::string scope_name,
                         std::string collection_name,
                         get_collection_id_options options,
                         get_collection_id_callback callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->get_collection_id(std::move(scope_name), std::move(collection_name), std::move(options), std::move(callback));
}

auto
agent::observe(observe_options options, observe_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->observe(std::move(options), std::move(callback));
}

auto
agent::observe_seqno(observe_seqno_options options, observe_seqno_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->observe_seqno(std::move(options), std::move(callback));
}

auto
agent::lookup_in(lookup_in_options options, lookup_in_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->lookup_in(std::move(options), std::move(callback));
}

auto
agent::mutate_in(mutate_in_options options, mutate_in_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->mutate_in(std::move(options), std::move(callback));
}

auto
agent::range_scan_create(std::uint16_t vbucket_id, range_scan_create_options options, range_scan_create_callback&& callback)
  -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->range_scan_create(vbucket_id, std::move(options), std::move(callback));
}

auto
agent::range_scan_continue(std::vector<std::byte> scan_uuid,
                           std::uint16_t vbucket_id,
                           range_scan_continue_options options,
                           range_scan_item_callback&& item_callback,
                           range_scan_continue_callback&& callback) -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->range_scan_continue(std::move(scan_uuid), vbucket_id, std::move(options), std::move(item_callback), std::move(callback));
}

auto
agent::range_scan_cancel(std::vector<std::byte> scan_uuid,
                         std::uint16_t vbucket_id,
                         range_scan_cancel_options options,
                         range_scan_cancel_callback&& callback) const -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>
{
    return impl_->range_scan_cancel(std::move(scan_uuid), vbucket_id, std::move(options), std::move(callback));
}

agent_unit_test_api::agent_unit_test_api(std::shared_ptr<agent_impl> impl)
  : impl_{ std::move(impl) }
{
}

auto
agent_unit_test_api::collections() -> collections_component_unit_test_api
{
    return impl_->collections_.unit_test_api();
}

auto
agent::unit_test_api() -> agent_unit_test_api
{
    return agent_unit_test_api(impl_);
}

} // namespace couchbase::core
