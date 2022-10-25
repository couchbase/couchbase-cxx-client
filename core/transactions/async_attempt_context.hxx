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

#include "couchbase/cluster.hxx"
#include "couchbase/subdocument_error_context.hxx"
#include "couchbase/transactions/transaction_query_options.hxx"
#include "exceptions.hxx"
#include "transaction_get_result.hxx"

#include "core/operations/document_query.hxx"
#include "core/utils/movable_function.hxx"
#include <future>
#include <optional>
#include <string>

namespace couchbase::core::transactions
{
class transaction_operation_failed;
/**
 * @brief Provides methods to perform asynchronous transactional operations.
 *
 * An @ref async_attempt_context object makes all the transactional kv operations
 * available.
 */
class async_attempt_context
{
  public:
    using Callback = utils::movable_function<void(std::exception_ptr, std::optional<transaction_get_result>)>;
    using VoidCallback = utils::movable_function<void(std::exception_ptr)>;
    using QueryCallback = std::function<void(std::exception_ptr, std::optional<core::operations::query_response>)>;
    virtual ~async_attempt_context() = default;
    /**
     * Gets a document from the specified Couchbase collection matching the specified id.
     *
     * @param id the document's ID
     * @param cb callback function with the result when successful, or a @ref transaction_operation_failed.
     */
    virtual void get(const core::document_id& id, Callback&& cb) = 0;

    /**
     * Gets a document from the specified Couchbase collection matching the specified id.
     *
     * @param id the document's ID
     * @param cb callback function with the result when successful, or a @ref transaction_operation_failed.
     */
    virtual void get_optional(const core::document_id& id, Callback&& cb) = 0;

    /**
     * Mutates the specified document with new content, using the document's last TransactionDocument#cas().
     *
     * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase component will see
     * the document's current value, rather than this staged or 'dirty' data.  If the attempt is rolled back, the staged mutation will
     * be removed.
     *
     * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
     * back).
     *
     * If the mutation fails, the transaction will automatically rollback this attempt, then retry.
     * @param document the doc to be updated
     * @param content the content to replace the doc with.
     * @param cb callback function called with the updated @ref transaction_get_result with the new CAS value when
     *           successful, or @ref transaction_operation_failed
     *
     */
    template<typename Content>
    void replace(const transaction_get_result& document, const Content& content, Callback&& cb)
    {
        return replace_raw(document, codec::tao_json_serializer::serialize(content), std::move(cb));
    }
    /**
     * Inserts a new document into the specified Couchbase collection.
     *
     * As with #replace, the insert is staged until the transaction is committed.  Due to technical limitations it is not as possible to
     * completely hide the staged data from the rest of the Couchbase platform, as an empty document must be created.
     *
     * This staged data effectively locks the document from other transactional writes until the attempt completes
     * (commits or rolls back).
     *
     * @param id the document's unique ID
     * @param content the content to insert
     * @param cb callback function called with a @ref transaction_get_result with the new CAS value when
     *           successful, or @ref transaction_operation_failed
     */
    template<typename Content>
    void insert(const core::document_id& id, const Content& content, Callback&& cb)
    {
        return insert_raw(id, codec::tao_json_serializer::serialize(content), std::move(cb));
    }
    /**
     * Removes the specified document, using the document's last TransactionDocument#cas
     *
     * As with {@link #replace}, the remove is staged until the transaction is committed.  That is, the document will continue to exist,
     * and the rest of the Couchbase platform will continue to see it.
     *
     * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
     * back).
     *
     * @param document the document to be removed
     * @param cb callback function called with a @ref transaction_operation_failed when unsuccessful.
     */
    virtual void remove(const transaction_get_result& document, VoidCallback&& cb) = 0;

    /**
     * Performs a Query, within the current transaction.
     *
     * @param statement query statement to execute.
     * @param options options to apply to the query.
     * @param cb callback which is called when the query completes.
     */
    virtual void query(const std::string& statement,
                       const couchbase::transactions::transaction_query_options& options,
                       QueryCallback&& cb) = 0;

    /**
     * Performs a Query, within the current transaction.
     *
     * @param statement query statement to execute.
     * @param cb callback which is called when the query completes.
     */
    void query(const std::string& statement, QueryCallback&& cb)
    {
        couchbase::transactions::transaction_query_options opts;
        return query(statement, opts, std::move(cb));
    }

    /**
     * Commits the transaction.  All staged replaces, inserts and removals will be written.
     *
     * After this, no further operations are permitted on this instance, and they will result in an
     * exception that will, if not caught in the transaction logic, cause the transaction to
     * fail.
     *
     * @param cb callback which is called when the commit succeeds
     */
    virtual void commit(VoidCallback&& cb) = 0;

    /**
     * Rollback the transaction.  All staged mutations will be unstaged.
     *
     * Typically, this is called internally to rollback transaction when errors occur in the lambda.  Though
     * it can be called explicitly from the app logic within the transaction as well, perhaps that is better
     * modeled as a custom exception that you raise instead.
     *
     * @param cb callback which is called when the rollback succeeds
     */
    virtual void rollback(VoidCallback&& cb) = 0;

  protected:
    /** @internal */
    virtual void insert_raw(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb) = 0;

    /** @internal */
    virtual void replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb) = 0;
};

} // namespace couchbase::core::transactions
