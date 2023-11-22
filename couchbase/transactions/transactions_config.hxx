/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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

#include <couchbase/durability_level.hxx>
#include <couchbase/transactions/transaction_keyspace.hxx>
#include <couchbase/transactions/transactions_cleanup_config.hxx>
#include <couchbase/transactions/transactions_query_config.hxx>

#include <chrono>
#include <memory>
#include <optional>

namespace couchbase::core::transactions
{
/** @internal */
struct attempt_context_testing_hooks;

/** @internal */
struct cleanup_testing_hooks;
} // namespace couchbase::core::transactions

namespace couchbase::transactions
{
/**
 * @brief Configuration parameters for transactions.
 */
class transactions_config
{
  public:
    transactions_config();

    ~transactions_config();

    transactions_config(const transactions_config& c);

    transactions_config(transactions_config&& c) noexcept;

    transactions_config& operator=(const transactions_config& c);

    /**
     * @brief Get the default durability level for all transaction operations
     *
     * @see couchbase::durability_level for details.
     *
     * @return The default durability level used for write operations.
     */
    [[nodiscard]] couchbase::durability_level durability_level() const
    {
        return level_;
    }

    /**
     * @brief Set the default durability level for all transaction operations
     *
     * @see couchbase::durability_level for details.
     *
     * @param level The default durability level desired for write operations.
     * @return reference to this, so calls can be chained.
     */
    transactions_config& durability_level(enum couchbase::durability_level level)
    {
        level_ = level;
        return *this;
    }

    /**
     * @brief Get the timeout for transactions
     *
     * Transactions can conflict (or other operations on those documents), and may retry.
     * This is the maximum time a transaction can take, including any retries.  The transaction will throw
     * an @ref transaction_expired and rollback when this occurs.
     *
     * @return timeout for transactions.
     */
    [[nodiscard]] std::chrono::nanoseconds timeout() const
    {
        return timeout_;
    }
    /**
     * @brief Set the timeout for transactions.
     *
     * @param duration desired expiration for transactions.
     * @return reference to this, so calls can be chained.
     */
    template<typename T>
    transactions_config& timeout(T duration)
    {
        timeout_ = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
        return *this;
    }

    /**
     * Set the transaction's metadata collection.
     *
     * @param keyspace The collection to use for the transaction metadata.
     * @return reference to this, so calls can be chained.
     */
    transactions_config& metadata_collection(const couchbase::transactions::transaction_keyspace& keyspace)
    {
        metadata_collection_ = keyspace;
        return *this;
    }

    [[nodiscard]] std::optional<couchbase::transactions::transaction_keyspace> metadata_collection() const
    {
        return metadata_collection_;
    }

    /**
     * Get the query config values
     *
     * @return The query configuration for transactions.
     */
    [[nodiscard]] const transactions_query_config& query_config() const
    {
        return query_config_;
    }

    /**
     * Get the query config values
     *
     * @return The query configuration for transactions.
     */
    [[nodiscard]] transactions_query_config& query_config()
    {
        return query_config_;
    }

    /**
     * Set the query configuration for transactions.
     *
     * @param config The transactions query configuration to use.
     * @return reference to this, so calls can be chained.
     */
    transactions_config& query_config(const transactions_query_config& config)
    {
        query_config_ = config;
        return *this;
    }

    /**
     * Get the cleanup configuration for transactions.
     *
     * @return The cleanup configuration.
     */
    [[nodiscard]] const transactions_cleanup_config& cleanup_config() const
    {
        return cleanup_config_;
    }

    /**
     * Get the cleanup configuration for transactions.
     *
     * @return The cleanup configuration.
     */
    [[nodiscard]] transactions_cleanup_config& cleanup_config()
    {
        return cleanup_config_;
    }

    /**
     * Set the cleanup configuration.
     *
     * @param cleanup_config The cleanup configuration to use.
     * @return reference to this, so calls can be chained.
     */
    transactions_config& cleanup_config(const transactions_cleanup_config& cleanup_config)
    {
        cleanup_config_ = cleanup_config;
        return *this;
    }

    /** @private */
    transactions_config& test_factories(std::shared_ptr<core::transactions::attempt_context_testing_hooks> hooks,
                                        std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks)
    {
        attempt_context_hooks_ = hooks;
        cleanup_hooks_ = cleanup_hooks;
        return *this;
    }

    /** @private */
    [[nodiscard]] core::transactions::attempt_context_testing_hooks& attempt_context_hooks() const
    {
        return *attempt_context_hooks_;
    }

    /** @private */
    [[nodiscard]] core::transactions::cleanup_testing_hooks& cleanup_hooks() const
    {
        return *cleanup_hooks_;
    }

    /** @private */
    struct built {
        couchbase::durability_level level;
        std::chrono::nanoseconds timeout;
        std::shared_ptr<core::transactions::attempt_context_testing_hooks> attempt_context_hooks;
        std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks;
        std::optional<couchbase::transactions::transaction_keyspace> metadata_collection;
        transactions_query_config::built query_config;
        transactions_cleanup_config::built cleanup_config;
    };

    /** @internal */
    [[nodiscard]] built build() const;

  private:
    couchbase::durability_level level_{ couchbase::durability_level::majority };
    std::chrono::nanoseconds timeout_{ std::chrono::seconds(15) };
    std::shared_ptr<core::transactions::attempt_context_testing_hooks> attempt_context_hooks_;
    std::shared_ptr<core::transactions::cleanup_testing_hooks> cleanup_hooks_;
    std::optional<couchbase::transactions::transaction_keyspace> metadata_collection_;
    transactions_query_config query_config_{};
    transactions_cleanup_config cleanup_config_{};
};
} // namespace couchbase::transactions
