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

#include <couchbase/build_query_index_options.hxx>
#include <couchbase/create_primary_query_index_options.hxx>
#include <couchbase/create_query_index_options.hxx>
#include <couchbase/drop_primary_query_index_options.hxx>
#include <couchbase/drop_query_index_options.hxx>
#include <couchbase/get_all_query_indexes_options.hxx>
#include <couchbase/watch_query_indexes_options.hxx>

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

/**
 * The Query Index Manager interface contains the means for managing indexes used for queries.
 *
 * @since 1.0.0
 * @committed
 */
class query_index_manager
{
  public:
    /**
     * Get all indexes within a bucket.
     *
     *
     * @param bucket_name specifies the bucket in which we look for the indexes
     * @param options optional parameters
     * @param handler the handler that implements @ref get_all_query_indexes_handler
     *
     * @since 1.0.0
     * @committed
     */
    void get_all_indexes(std::string bucket_name,
                         const get_all_query_indexes_options& options,
                         get_all_query_indexes_handler&& handler) const
    {
        return core::impl::initiate_get_all_query_indexes(
          core_, std::move(bucket_name), options.build(), std::forward<get_all_query_indexes_handler>(handler));
    }
    [[nodiscard]] auto get_all_indexes(std::string bucket_name, const get_all_query_indexes_options& options) const
      -> std::future<std::pair<manager_error_context, std::vector<couchbase::management::query::index>>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<manager_error_context, std::vector<couchbase::management::query::index>>>>();
        auto future = barrier->get_future();
        get_all_indexes(std::move(bucket_name), options, [barrier](auto ctx, auto resp) { barrier->set_value({ ctx, resp }); });
        return future;
    }
    /**
     * Create an index on a bucket.
     *
     * @param bucket_name specifies the bucket in which to create the index
     * @param index_name name of the index
     * @param fields the fields to create the index over
     * @param options optional parameters
     * @param handler the handler that implements @ref create_query_index_handler
     *
     * @since 1.0.0
     * @committed
     */
    void create_index(std::string bucket_name,
                      std::string index_name,
                      std::vector<std::string> fields,
                      const create_query_index_options& options,
                      create_query_index_handler&& handler) const
    {
        core::impl::initiate_create_query_index(
          core_, std::move(bucket_name), std::move(index_name), std::move(fields), options.build(), std::move(handler));
    }

    [[nodiscard]] auto create_index(std::string bucket_name,
                                    std::string index_name,
                                    std::vector<std::string> fields,
                                    const create_query_index_options& options) const -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        create_index(
          std::move(bucket_name), std::move(index_name), std::move(fields), options, [barrier](auto ctx) { barrier->set_value(ctx); });
        return future;
    }

    /**
     * Create a primary index on a bucket.
     *
     * @param bucket_name specifies the bucket in which to create the index
     * @param options optional parameters
     * @param handler the handler that implements @ref create_query_index_handler
     *
     * @since 1.0.0
     * @committed
     */
    void create_primary_index(std::string bucket_name,
                              const create_primary_query_index_options& options,
                              create_query_index_handler&& handler)
    {
        return core::impl::initiate_create_primary_query_index(core_, std::move(bucket_name), options.build(), std::move(handler));
    }

    [[nodiscard]] auto create_primary_index(std::string bucket_name, const create_primary_query_index_options& options)
      -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        create_primary_index(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(ctx); });
        return future;
    }
    /**
     * Drop primary index on a bucket.
     *
     * @param bucket_name name of the bucket in which to drop the primary index
     * @param options optional parameters
     * @param handler the handler that implements @ref drop_query_index_handler
     *
     * @since 1.0.0
     * @committed
     */
    void drop_primary_index(std::string bucket_name, const drop_primary_query_index_options& options, drop_query_index_handler&& handler)
    {
        return core::impl::initiate_drop_primary_query_index(core_, std::move(bucket_name), options.build(), std::move(handler));
    }

    [[nodiscard]] auto drop_primary_index(std::string bucket_name, const drop_primary_query_index_options& options)
      -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        drop_primary_index(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(ctx); });
        return future;
    }

    /**
     *
     * @param bucket_name name of the bucket in which to drop the index
     * @param index_name name of the index to drop
     * @param options optional parameters
     * @param handler handler that implements @ref drop_query_index_handler
     *
     * @since 1.0.0
     * @committed
     */
    void drop_index(std::string bucket_name,
                    std::string index_name,
                    const drop_query_index_options& options,
                    drop_query_index_handler&& handler)
    {
        return core::impl::initiate_drop_query_index(
          core_, std::move(bucket_name), std::move(index_name), options.build(), std::move(handler));
    }

    [[nodiscard]] auto drop_index(std::string bucket_name, std::string index_name, const drop_query_index_options& options)
      -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        drop_index(std::move(bucket_name), std::move(index_name), options, [barrier](auto ctx) { barrier->set_value(ctx); });
        return future;
    }
    /**
     * Builds all currently deferred indexes.
     *
     * By default, this method will build the indexes on the bucket.
     *
     * @param bucket_name name of the bucket
     * @param options the custom options
     * @param handler the handler that implements @ref build_deferred_query_indexes_handler
     *
     * @since 1.0.0
     * @committed
     */
    void build_deferred_indexes(std::string bucket_name,
                                const build_query_index_options& options,
                                build_deferred_query_indexes_handler&& handler) const
    {
        return core::impl::initiate_build_deferred_indexes(
          core_, std::move(bucket_name), options.build(), std::forward<build_deferred_query_indexes_handler>(handler));
    }

    [[nodiscard]] auto build_deferred_indexes(std::string bucket_name, const build_query_index_options& options) const
      -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        build_deferred_indexes(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
        return future;
    }

    /**
     * Polls the state of a set of indexes, until they all are online.
     *
     * @param bucket_name name of the bucket in which to look for the indexes
     * @param index_names names of the indexes to watch
     * @param options optional parameters
     * @param handler handler that implements @ref watch_query_indexes_handler
     *
     * @since 1.0.0
     * @committed
     */
    void watch_indexes(std::string bucket_name,
                       std::vector<std::string> index_names,
                       const watch_query_indexes_options& options,
                       watch_query_indexes_handler&& handler)
    {
        return core::impl::initiate_watch_query_indexes(
          core_, std::move(bucket_name), std::move(index_names), options.build(), std::move(handler));
    }

    [[nodiscard]] auto watch_indexes(std::string bucket_name,
                                     std::vector<std::string> index_names,
                                     const watch_query_indexes_options& options)
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        watch_indexes(std::move(bucket_name), std::move(index_names), options, [barrier](auto ctx) { barrier->set_value(ctx); });
        return future;
    }

  private:
    friend class cluster;

    explicit query_index_manager(std::shared_ptr<couchbase::core::cluster> core)
      : core_(std::move(core))
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
};
} // namespace couchbase
