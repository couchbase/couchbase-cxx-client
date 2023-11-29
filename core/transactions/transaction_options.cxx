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

#include <couchbase/collection.hxx>
#include <couchbase/transactions/transaction_options.hxx>

#include <core/transactions/attempt_context_testing_hooks.hxx>
#include <core/transactions/cleanup_testing_hooks.hxx>

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
             timeout_.value_or(conf.timeout),
             attempt_context_hooks_ ? attempt_context_hooks_ : conf.attempt_context_hooks,
             cleanup_hooks_ ? cleanup_hooks_ : conf.cleanup_hooks,
             metadata_collection_ ? metadata_collection_ : conf.metadata_collection,
             query_config,
             conf.cleanup_config };
}

transaction_options&
transaction_options::test_factories(std::shared_ptr<core::transactions::attempt_context_testing_hooks> hooks,
                                    std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks)
{
    attempt_context_hooks_ = hooks;
    cleanup_hooks_ = cleanup_hooks;
    return *this;
}

std::optional<transaction_keyspace>
transaction_options::metadata_collection() const
{
    return metadata_collection_;
}

transaction_options&
transaction_options::durability_level(couchbase::durability_level level)
{
    durability_ = level;
    return *this;
}

std::optional<couchbase::durability_level>
transaction_options::durability_level() const
{
    return durability_;
}

transaction_options&
transaction_options::scan_consistency(query_scan_consistency scan_consistency)
{
    scan_consistency_ = scan_consistency;
    return *this;
}

std::optional<query_scan_consistency>
transaction_options::scan_consistency() const
{
    return scan_consistency_;
}

std::optional<std::chrono::nanoseconds>
transaction_options::timeout()
{
    return timeout_;
}

transaction_options&
transaction_options::metadata_collection(const couchbase::collection& coll)
{
    metadata_collection_.emplace(coll.bucket_name(), coll.scope_name(), coll.name());
    return *this;
}

} // namespace couchbase::transactions
