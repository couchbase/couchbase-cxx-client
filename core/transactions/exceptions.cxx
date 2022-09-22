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
#include "internal/exceptions_internal.hxx"
#include "internal/transaction_context.hxx"

#include "exceptions.hxx"

namespace couchbase::core::transactions
{
external_exception
external_exception_from_error_class(error_class ec)
{
    switch (ec) {
        case FAIL_DOC_NOT_FOUND:
            return DOCUMENT_NOT_FOUND_EXCEPTION;
        case FAIL_DOC_ALREADY_EXISTS:
            return DOCUMENT_EXISTS_EXCEPTION;
        default:
            return UNKNOWN;
    }
}

error_class
error_class_from_result(const result& res)
{
    subdoc_result::status_type subdoc_status = res.subdoc_status();
    assert(res.ec || (!res.ignore_subdoc_errors && subdoc_status != subdoc_result::status_type::success));
    if (res.ec || res.ignore_subdoc_errors) {
        if (res.ec == couchbase::errc::key_value::document_not_found) {
            return FAIL_DOC_NOT_FOUND;
        }
        if (res.ec == couchbase::errc::key_value::document_exists) {
            return FAIL_DOC_ALREADY_EXISTS;
        }
        if (res.ec == couchbase::errc::common::cas_mismatch) {
            return FAIL_CAS_MISMATCH;
        }
        if (res.ec == couchbase::errc::key_value::value_too_large) {
            return FAIL_ATR_FULL;
        }
        if (res.ec == couchbase::errc::common::unambiguous_timeout || res.ec == couchbase::errc::common::temporary_failure ||
            res.ec == couchbase::errc::key_value::durable_write_in_progress) {
            return FAIL_TRANSIENT;
        }
        if (res.ec == couchbase::errc::key_value::durability_ambiguous || res.ec == couchbase::errc::common::ambiguous_timeout ||
            res.ec == couchbase::errc::common::request_canceled) {
            return FAIL_AMBIGUOUS;
        }
        if (res.ec == couchbase::errc::key_value::path_not_found) {
            return FAIL_PATH_NOT_FOUND;
        }
        if (res.ec == couchbase::errc::key_value::path_exists) {
            return FAIL_PATH_ALREADY_EXISTS;
        }
        return FAIL_OTHER;
    } else {
        // TODO this section is likely redundant from TXNCXX-230, but leaving it here to be compatible with older
        // C++ clients.  It can be removed later.
        if (subdoc_status == subdoc_result::status_type::subdoc_path_not_found) {
            return FAIL_PATH_NOT_FOUND;
        } else if (subdoc_status == subdoc_result::status_type::subdoc_path_exists) {
            return FAIL_PATH_ALREADY_EXISTS;
        } else {
            return FAIL_OTHER;
        }
    }
}

transaction_exception::transaction_exception(const std::runtime_error& cause, const transaction_context& context, failure_type type)
  : std::runtime_error(cause)
  , result_(context.get_transaction_result())
  , cause_(UNKNOWN)
  , type_(type)
{
    auto txn_op = dynamic_cast<const transaction_operation_failed*>(&cause);
    if (nullptr != txn_op) {
        cause_ = txn_op->cause();
    }
}
} // namespace couchbase::core::transactions
