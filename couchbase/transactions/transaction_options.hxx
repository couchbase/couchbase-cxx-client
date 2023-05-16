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
/**
 * The transaction_options can be passed in to override some elements of the global @ref transactions_config.
 */
class transaction_options
{
  public:
    transaction_options() = default;

    /**
     * Set durability for this transaction.
     *
     * @see couchbase::durability_level for details.
     *
     * @param level Durability level for this transaction.
     * @return reference to this object, convenient for chaining operations.
     */
    transaction_options& durability_level(durability_level level)
    {
        durability_ = level;
        return *this;
    }
    /**
     * Get the durability if it has been set.
     *
     * @see couchbase::durability_level for details.
     *
     * @return durability if set.
     */
    std::optional<couchbase::durability_level> durability_level()
    {
        return durability_;
    }
    /**
     * Set the @ref query_scan_consistency for this transaction.
     *
     * @see query_options::scan_consistency for details.
     *
     * @param scan_consistency The desired @ref query_scan_consistency for this transaction.
     * @return reference to this object, convenient for chaining operations.
     */
    transaction_options& scan_consistency(query_scan_consistency scan_consistency)
    {
        scan_consistency_ = scan_consistency;
        return *this;
    }
    /**
     * Get the scan_consistency if it has been set.
     *
     * @see query_options::scan_consistency for details.
     *
     * @return The scan_consistency, if set.
     */
    std::optional<query_scan_consistency> scan_consistency()
    {
        return scan_consistency_;
    }
    /**
     * Set the timeout for key-value operations for this transaction.
     *
     * @param kv_timeout Desired key-value timeout.
     * @return reference to this object, convenient for chaining operations.
     */
    transaction_options& kv_timeout(std::chrono::milliseconds kv_timeout)
    {
        kv_timeout_ = kv_timeout;
        return *this;
    }

    /**
     * Get the key-value timeout if it has been set.
     *
     * @return The key-value timeout, if set.
     */
    std::optional<std::chrono::milliseconds> kv_timeout()
    {
        return kv_timeout_;
    }

    /**
     * Set the expiration time for this transaction.
     *
     * @tparam T expiration time type, e.g. @ref std::chrono::milliseconds, or similar
     * @param expiration_time Desired expiration time.
     * @return reference to this object, convenient for chaining operations.
     */
    template<typename T>
    transaction_options& expiration_time(T expiration_time)
    {
        expiration_time_ = std::chrono::duration_cast<std::chrono::nanoseconds>(expiration_time);
        return *this;
    }

    /**
     * Get the expiration time, if set.
     *
     * @return the expiration time, if set.
     */
    std::optional<std::chrono::nanoseconds> expiration_time()
    {
        return expiration_time_;
    }

    /**
     * Set the metadata collection to use for this transaction
     *
     * Transactions involve a the creation and use of some metadata documents, which by default are placed in the default collection of
     * scope which the first document in the that has a mutating operation performed on it.   However, you can set this to a specific
     * collection to isolate these documents from your documents, if desired.
     *
     * @param coll The desired collection to use.
     * @return reference to this object, convenient for chaining operations.
     */
    transaction_options& metadata_collection(const couchbase::collection& coll)
    {
        metadata_collection_.emplace(coll.bucket_name(), coll.scope_name(), coll.name());
        return *this;
    }
    /**
     * Set metadata collection to use for this transaction
     *
     * @param keyspace The desired collection to use
     * @return reference to this object, convenient for chaining operations.
     */
    transaction_options& metadata_collection(const couchbase::transactions::transaction_keyspace& keyspace)
    {
        metadata_collection_.emplace(keyspace);
        return *this;
    }
    /**
     * Get the metadata collection, if set.
     *
     * @return the metadata collection, as a @ref transaction_keyspace, if set.
     */
    std::optional<transaction_keyspace> metadata_collection()
    {
        return metadata_collection_;
    }

    /** @private */
    transaction_options& test_factories(std::shared_ptr<core::transactions::attempt_context_testing_hooks> hooks,
                                        std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks)
    {
        attempt_context_hooks_ = hooks;
        cleanup_hooks_ = cleanup_hooks;
        return *this;
    }
    /** @private */
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
