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
     * Builds all currently deferred indexes.
     *
     * By default, this method will build the indexes on the bucket. If the indexes should be built on a collection, both
     * {@link build_query_index_options#scope_name} and {@link build_query_index_options#collection_name} must be set.
     *
     * @tparam Handler
     * @param bucket_name name of the bucket
     * @param options the custom options
     * @param handler the handler that implements @ref build_deferred_query_indexes_handler
     */
    template<typename Handler>
    void build_deferred_indexes(std::string bucket_name, const build_query_index_options& options, Handler&& handler) const
    {
        return core::impl::initiate_build_deferred_indexes(
          core_, std::move(bucket_name), std::move(options.build()), std::forward<Handler>(handler));
    }

    [[nodiscard]] auto build_deferred_indexes(std::string bucket_name, const build_query_index_options& options) const
      -> std::future<manager_error_context>
    {
        auto barrier = std::make_shared<std::promise<manager_error_context>>();
        auto future = barrier->get_future();
        build_deferred_indexes(std::move(bucket_name), options, [barrier](auto ctx) { barrier->set_value(std::move(ctx)); });
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
