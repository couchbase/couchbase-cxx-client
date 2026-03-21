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

#include "pending_operation.hxx"

#include "crud_options.hxx"
#include "range_scan_options.hxx"
#include "subdoc_options.hxx"

#include <tl/expected.hpp>

#include <memory>
#include <system_error>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class collections_component;
class crud_component_impl;
class crud_component
{
public:
  crud_component(asio::io_context& io,
                 std::string bucket_name,
                 collections_component collections,
                 std::shared_ptr<retry_strategy> default_retry_strategy);

  auto get(std::string scope_name,
           std::string collection_name,
           std::vector<std::byte> key,
           const get_options& options,
           get_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto insert(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const insert_options& options,
              insert_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto upsert(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const upsert_options& options,
              upsert_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto replace(std::string scope_name,
               std::string collection_name,
               std::vector<std::byte> key,
               std::vector<std::byte> value,
               const replace_options& options,
               replace_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto remove(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              const remove_options& options,
              remove_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto touch(std::string scope_name,
             std::string collection_name,
             std::vector<std::byte> key,
             const touch_options& options,
             touch_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto get_and_touch(std::string scope_name,
                     std::string collection_name,
                     std::vector<std::byte> key,
                     const get_and_touch_options& options,
                     get_and_touch_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto get_and_lock(std::string scope_name,
                    std::string collection_name,
                    std::vector<std::byte> key,
                    const get_and_lock_options& options,
                    get_and_lock_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto unlock(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              const unlock_options& options,
              unlock_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto get_with_meta(std::string scope_name,
                     std::string collection_name,
                     std::vector<std::byte> key,
                     const get_with_meta_options& options,
                     get_with_meta_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto append(std::string scope_name,
              std::string collection_name,
              std::vector<std::byte> key,
              std::vector<std::byte> value,
              const adjoin_options& options,
              adjoin_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto prepend(std::string scope_name,
               std::string collection_name,
               std::vector<std::byte> key,
               std::vector<std::byte> value,
               const adjoin_options& options,
               adjoin_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto increment(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const counter_options& options,
                 counter_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto decrement(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const counter_options& options,
                 counter_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto lookup_in(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const lookup_in_options& options,
                 lookup_in_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto mutate_in(std::string scope_name,
                 std::string collection_name,
                 std::vector<std::byte> key,
                 const mutate_in_options& options,
                 mutate_in_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto range_scan_create(std::uint16_t vbucket_id,
                         const range_scan_create_options& options,
                         range_scan_create_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto range_scan_continue(const std::vector<std::byte>& scan_uuid,
                           std::uint16_t vbucket_id,
                           const range_scan_continue_options& options,
                           range_scan_item_callback&& item_callback,
                           range_scan_continue_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

  auto range_scan_cancel(std::vector<std::byte> scan_uuid,
                         std::uint16_t vbucket_id,
                         const range_scan_cancel_options& options,
                         range_scan_cancel_callback&& callback)
    -> tl::expected<std::shared_ptr<pending_operation>, std::error_code>;

private:
  std::shared_ptr<crud_component_impl> impl_;
};
} // namespace couchbase::core
