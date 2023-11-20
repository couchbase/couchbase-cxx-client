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
/**
 * The attempt_context is used for all synchronous transaction operations
 *
 * In the example below, we get a document then replace its content:
 *
 * @snippet test/test_transaction_examples.cxx simple-blocking-txn
 */

class attempt_context
{
  public:
    /**
     * Get a document from a collection.
     *
     * Fetch the document contents, in the form of a @ref transaction_get_result.   This can be used in subsequent calls
     * to @ref attempt_context::replace or @ref attempt_context::remove
     *
     * @param coll The collection which contains the document.
     * @param id The unique id of the document.
     * @return The result of the operation, which is an @ref transaction_op_error_context and a @ref transaction_get_result.
     */
    virtual std::pair<transaction_op_error_context, transaction_get_result> get(const couchbase::collection& coll,
                                                                                const std::string& id) = 0;

    /**
     * Insert a document into a collection.
     *
     * Given an id and the content, this inserts a new document into a collection.   Note that currently this content can be either a
     * <std::vector<std::byte>> or an object which can be serialized with the @ref codec::tao_json_serializer.
     *
     * @tparam Content Type of the contents of the document.
     * @param coll Collection in which to insert document.
     * @param id The unique id of the document.
     * @param content The content of the document.
     * @return The result of the operation, which is an @ref transaction_op_error_context and a @ref transaction_get_result.
     */
    template<typename Content>
    std::pair<transaction_op_error_context, transaction_get_result> insert(const couchbase::collection& coll,
                                                                           const std::string& id,
                                                                           const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return insert_raw(coll, id, content);
        } else {
            return insert_raw(coll, id, codec::tao_json_serializer::serialize(content));
        }
    }

    /**
     * Replace the contents of a document in a collection.
     *
     * Replaces the contents of an existing document. Note that currently this content can be either a
     * <std::vector<std::byte>> or an object which can be serialized with the @ref codec::tao_json_serializer.

     * @tparam Content Type of the contents of the document.
     * @param doc Document whose content will be replaced.  This is gotten from a call to @ref attempt_context::get
     * @param content New content of the document.
     * @return The result of the operation, which is an @ref transaction_op_error_context and a @ref transaction_get_result.
     */
    template<typename Content>
    std::pair<transaction_op_error_context, transaction_get_result> replace(const transaction_get_result& doc, const Content& content)
    {
        if constexpr (std::is_same_v<Content, std::vector<std::byte>>) {
            return replace_raw(doc, content);
        } else {
            return replace_raw(doc, codec::tao_json_serializer::serialize(content));
        }
    }

    /**
     * Remove a document.
     *
     * Removes a document from a collection, where the document was gotten from a previous call to @ref attempt_context::get
     *
     * @param doc The document to remove.
     * @return The result of the operation.
     */
    virtual transaction_op_error_context remove(const transaction_get_result& doc) = 0;

    /**
     * Perform an unscoped query.
     *
     * @param statement The query statement.
     * @param options Options for the query.
     * @return The result of the operation, with is an @ref transaction_op_error_context and a @ref transaction_query_result.
     */
    std::pair<transaction_op_error_context, transaction_query_result> query(const std::string& statement,
                                                                            const transaction_query_options& options = {});

    /**
     * Perform a scoped query.
     *
     * @param scope Scope for the query.
     * @param statement The query statement.
     * @param opts Options for the query.
     * @return The result of the operation, with is an @ref transaction_op_error_context and a @ref transaction_query_result.
     */
    std::pair<transaction_op_error_context, transaction_query_result> query(const scope& scope,
                                                                            const std::string& statement,
                                                                            const transaction_query_options& opts = {});

    virtual ~attempt_context() = default;

  protected:
    /** @private */
    virtual std::pair<transaction_op_error_context, transaction_get_result> replace_raw(const transaction_get_result& doc,
                                                                                        std::vector<std::byte> content) = 0;
    /** @private */
    virtual std::pair<transaction_op_error_context, transaction_get_result> insert_raw(const couchbase::collection& coll,
                                                                                       const std::string& id,
                                                                                       std::vector<std::byte> content) = 0;
    /** @private */
    virtual std::pair<transaction_op_error_context, transaction_query_result> do_public_query(const std::string& statement,
                                                                                              const transaction_query_options& options,
                                                                                              std::optional<std::string> query_context) = 0;
};
} // namespace transactions
} // namespace couchbase
