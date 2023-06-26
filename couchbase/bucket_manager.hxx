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

#include <couchbase/create_bucket_options.hxx>
#include <couchbase/drop_bucket_options.hxx>
#include <couchbase/flush_bucket_options.hxx>
#include <couchbase/get_all_buckets_options.hxx>
#include <couchbase/get_bucket_options.hxx>
#include <couchbase/update_bucket_options.hxx>

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
class cluster;

class bucket_manager
{
  public:
    void get_bucket(std::string bucket_name, const get_bucket_options& options, get_bucket_handler&& handler) const;

    [[nodiscard]] auto get_bucket(std::string bucket_name, const get_bucket_options& options = {}) const
      -> std::future<std::pair<manager_error_context, management::cluster::bucket_settings>>;

    void get_all_buckets(const get_all_buckets_options& options, get_all_buckets_handler&& handler) const;

    [[nodiscard]] auto get_all_buckets(const get_all_buckets_options& options = {}) const
      -> std::future<std::pair<manager_error_context, std::vector<management::cluster::bucket_settings>>>;

    void create_bucket(const management::cluster::bucket_settings& bucket_settings,
                       const create_bucket_options& options,
                       create_bucket_handler&& handler) const;

    [[nodiscard]] auto create_bucket(const management::cluster::bucket_settings& bucket_settings,
                                     const create_bucket_options& options = {}) const -> std::future<manager_error_context>;

    void update_bucket(const management::cluster::bucket_settings& bucket_settings,
                       const update_bucket_options& options,
                       update_bucket_handler&& handler) const;

    [[nodiscard]] auto update_bucket(const management::cluster::bucket_settings& bucket_settings,
                                     const update_bucket_options& options = {}) const -> std::future<manager_error_context>;

    void drop_bucket(std::string bucket_name, const drop_bucket_options& options, drop_bucket_handler&& handler) const;

    [[nodiscard]] auto drop_bucket(std::string bucket_name, const drop_bucket_options& options = {}) const
      -> std::future<manager_error_context>;

    void flush_bucket(std::string bucket_name, const flush_bucket_options& options, flush_bucket_handler&& handler) const;

    [[nodiscard]] auto flush_bucket(std::string bucket_name, const flush_bucket_options& options = {}) const
      -> std::future<manager_error_context>;

  private:
    friend class cluster;

    explicit bucket_manager(std::shared_ptr<couchbase::core::cluster> core)
      : core_(std::move(core))
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
};
} // namespace couchbase
