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

#include "couchbase/transactions/transaction_config.hxx"

#include "core/query_scan_consistency.hxx"

#include <chrono>
#include <optional>

namespace couchbase::transactions
{
class per_transaction_config
{
  public:
    per_transaction_config() = default;

    per_transaction_config& durability_level(durability_level level)
    {
        durability_ = level;
        return *this;
    }

    std::optional<couchbase::durability_level> durability_level()
    {
        return durability_;
    }

    per_transaction_config& scan_consistency(core::query_scan_consistency scan_consistency)
    {
        scan_consistency_ = scan_consistency;
        return *this;
    }

    std::optional<core::query_scan_consistency> scan_consistency()
    {
        return scan_consistency_;
    }

    per_transaction_config& kv_timeout(std::chrono::milliseconds kv_timeout)
    {
        kv_timeout_ = kv_timeout;
        return *this;
    }

    std::optional<std::chrono::milliseconds> kv_timeout()
    {
        return kv_timeout_;
    }

    template<typename T>
    per_transaction_config& expiration_time(T expiration_time)
    {
        expiration_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(expiration_time);
        return *this;
    }

    std::optional<std::chrono::nanoseconds> expiration_time()
    {
        return expiration_time_;
    }

    per_transaction_config& custom_metadata_collection(const transaction_keyspace& keyspace)
    {
        custom_metadata_collection_.emplace(keyspace);
        return *this;
    }

    std::optional<transaction_keyspace> custom_metadata_collection()
    {
        return custom_metadata_collection_;
    }

    transaction_config apply(const transaction_config& conf) const
    {
        transaction_config retval = conf;
        if (durability_) {
            retval.durability_level(*durability_);
        }
        if (scan_consistency_) {
            retval.scan_consistency(*scan_consistency_);
        }
        if (kv_timeout_) {
            retval.kv_timeout(*kv_timeout_);
        }
        if (expiration_time_) {
            retval.expiration_time(*expiration_time_);
        }
        if (custom_metadata_collection_) {
            retval.custom_metadata_collection(*custom_metadata_collection_);
        }
        return retval;
    }

  private:
    std::optional<couchbase::durability_level> durability_;
    std::optional<couchbase::core::query_scan_consistency> scan_consistency_;
    std::optional<std::chrono::milliseconds> kv_timeout_;
    std::optional<std::chrono::nanoseconds> expiration_time_;
    std::optional<transaction_keyspace> custom_metadata_collection_;
};

} // namespace couchbase::transactions
