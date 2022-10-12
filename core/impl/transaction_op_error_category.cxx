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

#include <couchbase/error_codes.hxx>

#include <string>
namespace couchbase::core::impl
{

struct transaction_op_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.transaction_op";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (static_cast<errc::transaction_op>(ev)) {
            case errc::transaction_op::unknown:
                return "unknown error (1300)";
            case errc::transaction_op::active_transaction_record_entry_not_found:
                return "active transaction record entry not found (1301)";
            case errc::transaction_op::active_transaction_record_full:
                return "active transaction record full (1302)";
            case errc::transaction_op::active_transaction_record_not_found:
                return "active transaction record not found (1303)";
            case errc::transaction_op::document_already_in_transaction:
                return "document already in transaction (1304)";
            case errc::transaction_op::document_exists_exception:
                return "document exists (1305)";
            case errc::transaction_op::document_not_found_exception:
                return "document not found (1306)";
            case errc::transaction_op::not_set:
                return "error not set (1307)";
            case errc::transaction_op::feature_not_available_exception:
                return "feature not available (1308)";
            case errc::transaction_op::transaction_aborted_externally:
                return "transaction aborted externally (1309)";
            case errc::transaction_op::previous_operation_failed:
                return "previous operation failed (1310)";
            case errc::transaction_op::forward_compatibility_failure:
                return "forward compatible failure (1311)";
            case errc::transaction_op::parsing_failure:
                return "parsing failure (1312)";
            case errc::transaction_op::illegal_state_exception:
                return "illegal state (1313)";
            case errc::transaction_op::couchbase_exception:
                return "couchbase exception (1314)";
            case errc::transaction_op::service_not_available_exception:
                return "service not available (1315)";
            case errc::transaction_op::request_canceled_exception:
                return "request canceled (1316)";
            case errc::transaction_op::concurrent_operations_detected_on_same_document:
                return "concurrent operations detected on same document (1317)";
            case errc::transaction_op::commit_not_permitted:
                return "commit not permitted (1318)";
            case errc::transaction_op::rollback_not_permitted:
                return "rollback not permitted (1319)";
            case errc::transaction_op::transaction_already_aborted:
                return "transaction already aborted (1320)";
            case errc::transaction_op::transaction_already_committed:
                return "transaction already committed (1321)";
        }
        return "FIXME: unknown error code (recompile with newer library): couchbase.transaction_op." + std::to_string(ev);
    }
};

const inline static transaction_op_error_category transaction_op_category_instance;

const std::error_category&
transaction_op_category() noexcept
{
    return transaction_op_category_instance;
}

} // namespace couchbase::core::impl
