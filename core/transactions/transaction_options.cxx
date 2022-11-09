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
#include <core/transactions/attempt_context_testing_hooks.hxx>
#include <core/transactions/cleanup_testing_hooks.hxx>
#include <couchbase/transactions/transaction_options.hxx>

namespace couchbase::transactions
{
transactions_config::built
transaction_options::apply(const transactions_config::built& conf) const
{
    auto query_config = conf.query_config;
    if (scan_consistency_) {
        query_config.scan_consistency = *scan_consistency_;
    }
    return { durability_.value_or(conf.level),
             expiration_time_.value_or(conf.expiration_time),
             kv_timeout_ ? kv_timeout_ : conf.kv_timeout,
             attempt_context_hooks_ ? attempt_context_hooks_ : conf.attempt_context_hooks,
             cleanup_hooks_ ? cleanup_hooks_ : conf.cleanup_hooks,
             metadata_collection_ ? metadata_collection_ : conf.metadata_collection,
             query_config,
             conf.cleanup_config };
}
} // namespace couchbase::transactions