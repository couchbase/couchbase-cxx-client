/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/create_collection_options.hxx>
#include <couchbase/create_scope_options.hxx>
#include <couchbase/drop_collection_options.hxx>
#include <couchbase/drop_scope_options.hxx>
#include <couchbase/get_all_scopes_options.hxx>
#include <couchbase/management/collection_spec.hxx>
#include <couchbase/update_collection_options.hxx>

#include <future>
#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase::core
{
class cluster;
} // namespace couchbase::core
#endif

namespace couchbase
{
class bucket;

class collection_manager
{
  public:
    void get_all_scopes(const get_all_scopes_options& options, get_all_scopes_handler&& handler) const;

    [[nodiscard]] auto get_all_scopes(const get_all_scopes_options& options = {}) const
      -> std::future<std::pair<manager_error_context, std::vector<management::bucket::scope_spec>>>;

    void create_collection(std::string scope_name,
                           std::string collection_name,
                           const create_collection_settings& settings,
                           const create_collection_options& options,
                           create_collection_handler&& handler) const;

    [[nodiscard]] auto create_collection(std::string scope_name,
                                         std::string collection_name,
                                         const create_collection_settings& settings = {},
                                         const create_collection_options& options = {}) const -> std::future<manager_error_context>;

    void update_collection(std::string scope_name,
                           std::string collection_name,
                           const update_collection_settings& settings,
                           const update_collection_options& options,
                           update_collection_handler&& handler) const;

    [[nodiscard]] auto update_collection(std::string scope_name,
                                         std::string collection_name,
                                         const update_collection_settings& settings,
                                         const update_collection_options& options = {}) const -> std::future<manager_error_context>;

    void drop_collection(std::string scope_name,
                         std::string collection_name,
                         const drop_collection_options& options,
                         drop_collection_handler&& handler) const;

    [[nodiscard]] auto drop_collection(std::string scope_name,
                                       std::string collection_name,
                                       const drop_collection_options& options = {}) const -> std::future<manager_error_context>;

    void create_scope(std::string scope_name, const create_scope_options& options, create_scope_handler&& handler) const;

    [[nodiscard]] auto create_scope(std::string scope_name, const create_scope_options& options = {}) const
      -> std::future<manager_error_context>;

    void drop_scope(std::string scope_name, const drop_scope_options& options, drop_scope_handler&& handler) const;

    [[nodiscard]] auto drop_scope(std::string scope_name, const drop_scope_options& options = {}) const
      -> std::future<manager_error_context>;

  private:
    friend class bucket;

    explicit collection_manager(std::shared_ptr<couchbase::core::cluster> core, std::string bucket_name)
      : core_(std::move(core))
      , bucket_name_(std::move(bucket_name))
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
    std::string bucket_name_;
};
} // namespace couchbase
