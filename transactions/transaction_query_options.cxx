/*
 *     Copyright 2022 Couchbase, Inc.
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
#include <couchbase/transactions/internal/transaction_context.hxx>
#include <couchbase/transactions/transaction_query_options.hxx>

namespace couchbase::transactions
{

core::operations::query_request
transaction_query_options::wrap_request(const couchbase::transactions::transaction_context& txn_context) const
{
    // set timeout stuff using the config/context.
    // extra time so we don't timeout right at expiry.
    auto extra =
      txn_context.config().kv_timeout() ? txn_context.config().kv_timeout().value() : core::timeout_defaults::key_value_durable_timeout;
    core::operations::query_request req = query_req_;
    if (!req.scan_consistency) {
        req.scan_consistency = txn_context.config().scan_consistency();
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(txn_context.remaining());
    req.timeout = remaining + extra;
    req.raw["txtimeout"] = fmt::format("\"{}ms\"", remaining.count());
    req.timeout = std::chrono::duration_cast<std::chrono::milliseconds>(txn_context.remaining()) + extra;
    return req;
}
} // namespace couchbase::transactions