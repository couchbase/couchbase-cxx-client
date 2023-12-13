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

#include <couchbase/analytics_index_manager.hxx>
#include <couchbase/analytics_options.hxx>
#include <couchbase/bucket.hxx>
#include <couchbase/bucket_manager.hxx>
#include <couchbase/cluster_options.hxx>
#include <couchbase/query_index_manager.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/search_index_manager.hxx>
#include <couchbase/search_options.hxx>
#include <couchbase/search_query.hxx>
#include <couchbase/transactions.hxx>

#include <memory>

namespace couchbase
{
#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace core
{
class cluster;
} // namespace core
class cluster_impl;
#endif

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
     * Connect to a Couchbase cluster.
     *
     * @param io IO context
     * @param connection_string connection string used to locate the Couchbase cluster object.
     * @param options options to customize connection (note, that connection_string takes precedence over this options).
     * @param handler the handler
     *
     * @since 1.0.0
     * @committed
     */
    static void connect(asio::io_context& io,
                        const std::string& connection_string,
                        const cluster_options& options,
                        cluster_connect_handler&& handler);

    /**
     * Connect to a Couchbase cluster.
     *
     * @param io IO context
     * @param connection_string connection string used to locate the Couchbase cluster object.
     * @param options options to customize connection (note, that connection_string takes precedence over this options).
     *
     * @return future object that carries cluster object and operation status
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] static auto connect(asio::io_context& io, const std::string& connection_string, const cluster_options& options)
      -> std::future<std::pair<cluster, std::error_code>>;

    cluster() = default;
    cluster(const cluster& other) = default;
    cluster(cluster&& other) = default;
    auto operator=(const cluster& other) -> cluster& = default;
    auto operator=(cluster&& other) -> cluster& = default;

    void close() const;

    /**
     * Wraps low-level implementation of the SDK to provide common API.
     *
     * @param core pointer to the low-level SDK handle
     *
     * @since 1.0.0
     * @volatile
     */
    explicit cluster(core::cluster core);

    /**
     * Opens a {@link bucket} with the given name.
     *
     * @param bucket_name the name of the bucket to open.
     * @return a {@link bucket} once opened.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket(std::string_view bucket_name) const -> bucket;

    /**
     * Performs a query against the query (N1QL) services.
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
    void query(std::string statement, const query_options& options, query_handler&& handler) const;

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
      -> std::future<std::pair<query_error_context, query_result>>;

    /**
     * Performs a query against the full text search services.
     *
     * @param index_name name of the search index
     * @param query query object, see hierarchy of @ref search_query for more details.
     * @param options options to customize the query request.
     * @param handler the handler that implements @ref search_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @see https://docs.couchbase.com/server/current/fts/fts-introduction.html
     *
     * @since 1.0.0
     * @committed
     */
    void search_query(std::string index_name, const search_query& query, const search_options& options, search_handler&& handler) const;

    /**
     * Performs a query against the full text search services.
     *
     * @param index_name name of the search index
     * @param query query object, see hierarchy of @ref search_query for more details.
     * @param options options to customize the query request.
     * @return future object that carries result of the operation
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @see https://docs.couchbase.com/server/current/fts/fts-introduction.html
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto search_query(std::string index_name, const class search_query& query, const search_options& options = {}) const
      -> std::future<std::pair<search_error_context, search_result>>;

    /**
     * Performs a query against the analytics services.
     *
     * @param statement the query statement.
     * @param options options to customize the query request.
     * @param handler the handler that implements @ref query_handler
     *
     * @exception errc::common::ambiguous_timeout
     * @exception errc::common::unambiguous_timeout
     *
     * @see https://docs.couchbase.com/server/current/analytics/introduction.html
     *
     * @since 1.0.0
     * @committed
     */
    void analytics_query(std::string statement, const analytics_options& options, analytics_handler&& handler) const;

    /**
     * Performs a query against the analytics services.
     *
     * @param statement the query statement.
     * @param options options to customize the query request.
     * @return future object that carries result of the operation
     *
     * @see https://docs.couchbase.com/server/current/analytics/introduction.html
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto analytics_query(std::string statement, const analytics_options& options = {}) const
      -> std::future<std::pair<analytics_error_context, analytics_result>>;

    /**
     * Provides access to the N1QL index management services.
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto query_indexes() const -> query_index_manager;

    /**
     * Provides access ot the Analytics index management services.
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto analytics_indexes() const -> analytics_index_manager;

    /**
     * Provides access to the bucket management services.
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto buckets() const -> bucket_manager;

    /**
     * Provides access to search index management services
     *
     * @return a manager instance
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto search_indexes() const -> search_index_manager;

    /**
     * Provides access to transaction services.
     *
     * See {@link transactions} for details on using the transactions object.
     *
     * @return an {@link transactions} object
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto transactions() const -> std::shared_ptr<couchbase::transactions::transactions>;

  private:
    std::shared_ptr<cluster_impl> impl_{};
};
} // namespace couchbase
