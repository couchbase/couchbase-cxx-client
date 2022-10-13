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

#include <couchbase/bucket.hxx>
#include <couchbase/query_index_manager.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/transactions.hxx>

#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase::core
{
namespace transactions
{
class transactions;
} // namespace transactions

class cluster;
} // namespace couchbase::core
#endif

namespace couchbase
{
/**
 * The {@link cluster} is the main entry point when connecting to a Couchbase cluster.
 *
 * @since 1.0.0
 * @committed
 */
class cluster
{
  public:
    /**
     * Wraps low-level implementation of the SDK to provide common API.
     *
     * @param core pointer to the low-level SDK handle
     *
     * @since 1.0.0
     * @volatile
     */
    explicit cluster(std::shared_ptr<couchbase::core::cluster> core)
      : core_(std::move(core))
    {
    }

    /**
     * Opens a {@link bucket} with the given name.
     *
     * @param bucket_name the name of the bucket to open.
     * @return a {@link bucket} once opened.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket(std::string_view bucket_name) const -> bucket
    {
        return { core_, bucket_name };
    }

    /**
     * Performs a query against the query (N1QL) services.
     *
     * @tparam Handler callable type that implements @ref query_handler signature
     *
     * @param statement the N1QL query statement.
     * @param options options to customize the query request.
     * @param handler the handler that implements @ref query_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @since 1.0.0
     * @committed
     */
    template<typename Handler>
    void query(std::string statement, const query_options& options, Handler&& handler) const
    {
        return core::impl::initiate_query_operation(core_, std::move(statement), {}, {}, options.build(), std::forward<Handler>(handler));
    }

    /**
     * Performs a query against the query (N1QL) services.
     *
     * @param statement the N1QL query statement.
     * @param options options to customize the query request.
     * @return future object that carries result of the operation
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto query(std::string statement, const query_options& options) const
      -> std::future<std::pair<query_error_context, query_result>>
    {
        auto barrier = std::make_shared<std::promise<std::pair<query_error_context, query_result>>>();
        auto future = barrier->get_future();
        query(std::move(statement), options, [barrier](auto ctx, auto result) {
            barrier->set_value({ std::move(ctx), std::move(result) });
        });
        return future;
    }

    /**
     * Provides access to the N1QL index management services.
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto query_indexes() const -> query_index_manager
    {
        return query_index_manager{ core_ };
    }

    [[nodiscard]] auto transactions() -> std::shared_ptr<couchbase::transactions::transactions>;

  private:
    std::shared_ptr<couchbase::core::cluster> core_;
    std::shared_ptr<couchbase::core::transactions::transactions> transactions_;
};
} // namespace couchbase
