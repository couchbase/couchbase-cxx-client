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

#pragma once

#include <couchbase/query_scan_consistency.hxx>
#include <couchbase/transactions/transactions_config.hxx>

#include <chrono>
#include <optional>

namespace couchbase::transactions
{
class transaction_options
{
  public:
    transaction_options() = default;

    transaction_options& durability_level(durability_level level)
    {
        durability_ = level;
        return *this;
    }

    std::optional<couchbase::durability_level> durability_level()
    {
        return durability_;
    }

    transaction_options& scan_consistency(query_scan_consistency scan_consistency)
    {
        scan_consistency_ = scan_consistency;
        return *this;
    }

    std::optional<query_scan_consistency> scan_consistency()
    {
        return scan_consistency_;
    }

    transaction_options& kv_timeout(std::chrono::milliseconds kv_timeout)
    {
        kv_timeout_ = kv_timeout;
        return *this;
    }

    std::optional<std::chrono::milliseconds> kv_timeout()
    {
        return kv_timeout_;
    }

    template<typename T>
    transaction_options& expiration_time(T expiration_time)
    {
        expiration_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(expiration_time);
        return *this;
    }

    std::optional<std::chrono::nanoseconds> expiration_time()
    {
        return expiration_time_;
    }

    transaction_options& metadata_collection(const couchbase::collection& coll)
    {
        metadata_collection_.emplace(coll.bucket_name(), coll.scope_name(), coll.name());
        return *this;
    }

    transaction_options& metadata_collection(const couchbase::transactions::transaction_keyspace& keyspace)
    {
        metadata_collection_.emplace(keyspace);
        return *this;
    }

    std::optional<transaction_keyspace> metadata_collection()
    {
        return metadata_collection_;
    }

    /** @internal */
    transaction_options& test_factories(std::shared_ptr<core::transactions::attempt_context_testing_hooks> hooks,
                                        std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks)
    {
        attempt_context_hooks_ = hooks;
        cleanup_hooks_ = cleanup_hooks;
        return *this;
    }

    [[nodiscard]] transactions_config::built apply(const transactions_config::built& conf) const;

  private:
    std::optional<couchbase::durability_level> durability_;
    std::optional<couchbase::query_scan_consistency> scan_consistency_;
    std::optional<std::chrono::milliseconds> kv_timeout_;
    std::optional<std::chrono::nanoseconds> expiration_time_;
    std::optional<transaction_keyspace> metadata_collection_;
    std::shared_ptr<core::transactions::attempt_context_testing_hooks> attempt_context_hooks_;
    std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks_;
};

} // namespace couchbase::transactions
