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

#include "core/error_context/query.hxx"
#include "error_class.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/transactions/transaction_result.hxx>

#include <stdexcept>
#include <utility>

namespace couchbase::core::transactions
{
/** @internal
 */
class transaction_context;

enum class failure_type { FAIL, EXPIRY, COMMIT_AMBIGUOUS };

enum external_exception {
    UNKNOWN = 0,
    ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND,
    ACTIVE_TRANSACTION_RECORD_FULL,
    ACTIVE_TRANSACTION_RECORD_NOT_FOUND,
    DOCUMENT_ALREADY_IN_TRANSACTION,
    DOCUMENT_EXISTS_EXCEPTION,
    DOCUMENT_NOT_FOUND_EXCEPTION,
    NOT_SET,
    FEATURE_NOT_AVAILABLE_EXCEPTION,
    TRANSACTION_ABORTED_EXTERNALLY,
    PREVIOUS_OPERATION_FAILED,
    FORWARD_COMPATIBILITY_FAILURE,
    PARSING_FAILURE,
    ILLEGAL_STATE_EXCEPTION,
    COUCHBASE_EXCEPTION,
    SERVICE_NOT_AVAILABLE_EXCEPTION,
    REQUEST_CANCELED_EXCEPTION,
    CONCURRENT_OPERATIONS_DETECTED_ON_SAME_DOCUMENT,
    COMMIT_NOT_PERMITTED,
    ROLLBACK_NOT_PERMITTED,
    TRANSACTION_ALREADY_ABORTED,
    TRANSACTION_ALREADY_COMMITTED,
};

couchbase::errc::transaction_op
transaction_op_errc_from_external_exception(external_exception e);

couchbase::core::transactions::error_class
error_class_from_external_exception(external_exception e);

/**
 * @brief Base class for all exceptions expected to be raised from a transaction.
 *
 * Subclasses of this are the only exceptions that are raised out of the transaction lambda.
 */
class transaction_exception : public std::runtime_error
{
  private:
    couchbase::transactions::transaction_result result_;
    external_exception cause_;
    failure_type type_;
    std::string txn_id_;

  public:
    /**
     * @brief Construct from underlying exception.
     *
     * @param cause Underlying cause for this exception.
     * @param context The internal state of the transaction at the time of the exception.
     */
    explicit transaction_exception(const std::runtime_error& cause, const transaction_context& context, failure_type type);

    /**
     * @brief Internal state of transaction at time of exception
     *
     * @returns Internal state of transaction.
     */
    std::pair<couchbase::transaction_error_context, couchbase::transactions::transaction_result> get_transaction_result() const
    {
        return { error_context(), { result_.transaction_id, result_.unstaging_complete } };
    }

    /**
     * @brief The cause of the exception
     *
     * @returns The underlying cause for this exception.
     */
    external_exception cause() const
    {
        return cause_;
    }
    /**
     * @brief The type of the exception - see @ref failure_type
     * @return The failure type.
     */
    failure_type type() const
    {
        return type_;
    }

    [[nodiscard]] transaction_error_context error_context() const
    {
        std::error_code ec{};
        switch (type_) {
            case failure_type::FAIL:
                ec = errc::transaction::failed;
                break;
            case failure_type::EXPIRY:
                ec = errc::transaction::expired;
                break;
            case failure_type::COMMIT_AMBIGUOUS:
                ec = errc::transaction::ambiguous;
                break;
        }
        return { ec, transaction_op_errc_from_external_exception(cause_) };
    }
};
class op_exception : public std::runtime_error
{
  private:
    external_exception cause_;
    transaction_op_error_context ctx_;

  public:
    explicit op_exception(transaction_op_error_context ctx, external_exception cause = COUCHBASE_EXCEPTION)
      : std::runtime_error(ctx.ec().message())
      , cause_(cause)
      , ctx_(std::move(ctx))
    {
    }

    [[nodiscard]] external_exception cause() const
    {
        return cause_;
    }

    [[nodiscard]] const transaction_op_error_context& ctx() const
    {
        return ctx_;
    }
};

class document_not_found : public op_exception
{
  public:
    explicit document_not_found(transaction_op_error_context ctx)
      : op_exception(std::move(ctx), DOCUMENT_NOT_FOUND_EXCEPTION)
    {
    }
};

class document_exists : public op_exception
{
  public:
    explicit document_exists(transaction_op_error_context ctx)
      : op_exception(std::move(ctx), DOCUMENT_EXISTS_EXCEPTION)
    {
    }
};

class query_attempt_not_found : public op_exception
{
  public:
    query_attempt_not_found(transaction_op_error_context ctx)
      : op_exception(std::move(ctx))
    {
    }
};

class query_cas_mismatch : public op_exception
{
  public:
    query_cas_mismatch(transaction_op_error_context ctx)
      : op_exception(std::move(ctx))
    {
    }
};

class query_attempt_expired : public op_exception
{
  public:
    query_attempt_expired(transaction_op_error_context ctx)
      : op_exception(std::move(ctx))
    {
    }
};

class query_parsing_failure : public op_exception
{
  public:
    query_parsing_failure(transaction_op_error_context ctx)
      : op_exception(std::move(ctx), PARSING_FAILURE)
    {
    }
};
} // namespace couchbase::core::transactions
