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

#include <couchbase/analytics_options.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/query_options.hxx>
#include <couchbase/search_options.hxx>
#include <couchbase/search_query.hxx>

#include <memory>

#ifndef COUCHBASE_CXX_CLIENT_DOXYGEN
namespace couchbase
{
class cluster;
} // namespace couchbase
#endif

namespace couchbase
{
/**
 * The scope identifies a group of collections and allows high application density as a result.
 *
 * @since 1.0.0
 */
class scope
{
  public:
    /**
     * Constant for the name of the default scope in the bucket.
     *
     * @since 1.0.0
     * @committed
     */
    static constexpr auto default_name{ "_default" };

    /**
     * Returns name of the bucket where the scope is defined.
     *
     * @return name of the bucket
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto bucket_name() const -> const std::string&
    {
        return bucket_name_;
    }

    /**
     * Returns name of the scope.
     *
     * @return name of the scope
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto name() const -> const std::string&
    {
        return name_;
    }

    /**
     * Opens a collection for this scope with an explicit name.
     *
     * @param collection_name the collection name.
     * @return the requested collection if successful.
     *
     * @since 1.0.0
     * @committed
     */
    [[nodiscard]] auto collection(std::string_view collection_name) const -> collection
    {
        return { core_, bucket_name_, name_, collection_name };
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
        return core::impl::initiate_query_operation(core_,
                                                    std::move(statement),
                                                    fmt::format("default:`{}`.`{}`", bucket_name_, name_),
                                                    options.build(),
                                                    std::forward<Handler>(handler));
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
    [[nodiscard]] auto query(std::string statement, const query_options& options = {}) const
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

  private:
    friend class bucket;

    scope(std::shared_ptr<couchbase::core::cluster> core, std::string_view bucket_name, std::string_view name)
      : core_(std::move(core))
      , bucket_name_(bucket_name)
      , name_(name)
    {
    }

    std::shared_ptr<couchbase::core::cluster> core_;
    std::string bucket_name_;
    std::string name_;
};
} // namespace couchbase
