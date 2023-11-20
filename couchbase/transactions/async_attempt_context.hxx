/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include <couchbase/transactions/transaction_get_result.hxx>
#include <couchbase/transactions/transaction_query_options.hxx>
#include <couchbase/transactions/transaction_query_result.hxx>

namespace couchbase
{
class collection;
class scope;

namespace transactions
{
using async_result_handler = std::function<void(transaction_op_error_context, transaction_get_result)>;
using async_query_handler = std::function<void(transaction_op_error_context, transaction_query_result)>;
using async_err_handler = std::function<void(transaction_op_error_context)>;

/**
 * The async_attempt_context is used for all asynchronous transaction operations
 *
 * In the example below, we get 3 documents in parallel, and update each when the get returns the document:
 *
 * @snippet test/test_transaction_examples.cxx simple-async-txn
 */
class async_attempt_context
{

  public:
    /**
     * Get document from a collection.
     *
     * Fetch the document contents, in the form of a @ref transaction_get_result.   This can be used in subsequent calls
     * to @ref async_attempt_context::replace or @ref async_attempt_context::remove
     *
     * @param coll The collection which contains the document.
     * @param id The document id which is used to uniquely identify it.
     * @param handler The handler which implements @ref async_result_handler
     */
    virtual void get(const collection& coll, std::string id, async_result_handler&& handler) = 0;
    /**
     * Remove a document from a collection.
     *
     * Removes a document from a collection, where the document was gotten from a previous call to @ref async_attempt_context::get
     *
     * @param doc The document to remove.
     * @param handler The handler which implements @ref async_err_handler
     */
    virtual void remove(transaction_get_result doc, async_err_handler&& handler) = 0;

    /**
     * Insert a document into a collection.
     *
     * Given an id and the content, this inserts a new document into a collection.   Note that currently this content can be either a
     * <std::vector<std::byte>> or an object which can be serialized with the @ref codec::tao_json_serializer.
     *
     * @tparam Content type of the document.
     * @param coll Collection to insert the document into.
     * @param id The document id.
     * @param content The content of the document.
     * @param handler The handler which implements @ref async_result_handler
     */
    template<typename Content>
    void insert(const collection& coll, std::string id, Content&& content, async_result_handler&& handler)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return insert_raw(std::forward<std::vector<std::byte>>(content), std::move(id), content, std::move(handler));
        } else {
            // TODO: transcoder support
            return insert_raw(coll, std::move(id), codec::tao_json_serializer::serialize(content), std::move(handler));
        }
    }
    /**
     * Replace the contents of a document in a collection.
     *
     * Replaces the contents of an existing document. Note that currently this content can be either a
     * <std::vector<std::byte>> or an object which can be serialized with the @ref codec::tao_json_serializer.
     *
     * @tparam Content type of the document
     * @param doc Document whose content will be replaced.  This is gotten from a call to @ref async_attempt_context::get
     * @param content New content of the document
     * @param handler The handler which implements @ref async_result_handler
     */
    template<typename Content>
    void replace(transaction_get_result doc, Content&& content, async_result_handler&& handler)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return replace_raw(std::move(doc), std::forward<std::vector<std::byte>>(content), std::move(handler));
        } else {
            // TODO: transcoder support
            return replace_raw(std::move(doc), codec::tao_json_serializer::serialize(content), std::move(handler));
        }
    }
    /**
     * Perform a query, within a scope.
     *
     * Performs a query given a specific scope.   Note that all subsequent transaction operations will be handled by the query service.
     *
     * @param scope Scope for the query.
     * @param statement The query statement
     * @param opts Options for the query
     * @param handler Handler which implements @ref async_query_handler.
     */
    void query(const scope& scope, std::string statement, transaction_query_options opts, async_query_handler&& handler);

    /**
     * Perform a query.
     *
     * Performs an unscoped query.
     *
     * @param statement The query statement.
     * @param opts Options for the query.
     * @param handler Handler which implements @ref async_query_handler.
     */
    void query(std::string statement, transaction_query_options opts, async_query_handler&& handler)
    {
        return query(statement, opts, {}, std::move(handler));
    };
    /**
     * Perform a query.
     *
     * Performs an unscoped query.
     *
     * @param statement The query statement.
     * @param handler Handler which implements @ref async_query_handler
     */
    void query(std::string statement, async_query_handler&& handler)
    {
        return query(std::move(statement), {}, std::move(handler));
    }

    virtual ~async_attempt_context() = default;

  protected:
    /** @private */
    virtual void insert_raw(const collection& coll, std::string id, std::vector<std::byte> content, async_result_handler&& handler) = 0;
    /** @private */
    virtual void replace_raw(transaction_get_result doc, std::vector<std::byte> content, async_result_handler&& handler) = 0;
    /** @private */
    virtual void query(std::string statement,
                       transaction_query_options opts,
                       std::optional<std::string> query_context,
                       async_query_handler&&) = 0;
};
} // namespace transactions
} // namespace couchbase
