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

#include "core/transactions.hxx"
#include "core/transactions/async_attempt_context.hxx"
#include "couchbase/transactions/transaction_options.hxx"
#include "couchbase/transactions/transaction_result.hxx"
#include "couchbase/transactions/transactions_config.hxx"
#include "transaction_attempt.hxx"
#include "transactions_cleanup.hxx"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace couchbase::core::transactions
{
class attempt_context_impl;

struct exp_delay;

class transaction_context
{
  public:
    transaction_context(transactions& txns,
                        const couchbase::transactions::transaction_options& conf = couchbase::transactions::transaction_options());
    transaction_context(const transaction_context&);

    [[nodiscard]] const std::string& transaction_id() const
    {
        return transaction_id_;
    }

    [[nodiscard]] std::size_t num_attempts() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return attempts_.size();
    }
    [[nodiscard]] const transaction_attempt& current_attempt() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (attempts_.empty()) {
            throw std::runtime_error("transaction context has no attempts yet");
        }
        return attempts_.back();
    }

    void add_attempt();

    void current_attempt_state(attempt_state s)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (attempts_.empty()) {
            throw std::runtime_error("transaction_context has no attempts yet");
        }
        attempts_.back().state = s;
    }

    [[nodiscard]] const core::cluster& cluster_ref() const
    {
        return transactions_.cluster_ref();
    }

    const couchbase::transactions::transactions_config::built& config() const
    {
        return config_;
    }

    transactions_cleanup& cleanup()
    {
        return cleanup_;
    }

    [[nodiscard]] bool has_expired_client_side();

    void after_delay(std::chrono::milliseconds delay, std::function<void()> fn);

    [[nodiscard]] std::chrono::time_point<std::chrono::steady_clock> start_time_client() const
    {
        return start_time_client_;
    }

    [[nodiscard]] const std::string& atr_id() const
    {
        return atr_id_;
    }

    void atr_id(const std::string& id)
    {
        atr_id_ = id;
    }

    [[nodiscard]] std::string atr_collection() const
    {
        return atr_collection_;
    }

    void atr_collection(const std::string& coll)
    {
        atr_collection_ = coll;
    }

    [[nodiscard]] ::couchbase::transactions::transaction_result get_transaction_result() const
    {
        return couchbase::transactions::transaction_result{ transaction_id(), current_attempt().state == attempt_state::COMPLETED };
    }
    void new_attempt_context()
    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        new_attempt_context([barrier](std::exception_ptr err) {
            if (err) {
                return barrier->set_exception(err);
            }
            return barrier->set_value();
        });
        f.get();
    }

    void new_attempt_context(async_attempt_context::VoidCallback&& cb);

    std::shared_ptr<attempt_context_impl> current_attempt_context();

    // These functions just delegate to the current_attempt_context_
    void get(const core::document_id& id, async_attempt_context::Callback&& cb);

    void get_optional(const core::document_id& id, async_attempt_context::Callback&& cb);

    void insert(const core::document_id& id, const std::vector<std::byte>& content, async_attempt_context::Callback&& cb);

    void replace(const transaction_get_result& doc, const std::vector<std::byte>& content, async_attempt_context::Callback&& cb);

    void remove(const transaction_get_result& doc, async_attempt_context::VoidCallback&& cb);

    void query(const std::string& statement,
               const couchbase::transactions::transaction_query_options& opts,
               std::optional<std::string> query_context,
               async_attempt_context::QueryCallback&& cb);

    void query(const std::string& statement,
               const couchbase::transactions::transaction_query_options& opts,
               async_attempt_context::QueryCallback&& cb);

    void commit(async_attempt_context::VoidCallback&& cb);

    void rollback(async_attempt_context::VoidCallback&& cb);

    void finalize(txn_complete_callback&& cb);

    void existing_error(bool previous_op_failed = true);

    void handle_error(std::exception_ptr err, txn_complete_callback&& cb);

    std::chrono::nanoseconds remaining() const;

  private:
    std::string transaction_id_;

    /** The time this overall transaction started */
    const std::chrono::time_point<std::chrono::steady_clock> start_time_client_;

    transactions& transactions_;

    couchbase::transactions::transactions_config::built config_;

    /**
     * Will be non-zero only when resuming a deferred transaction. It records how much time has elapsed in total in the deferred
     * transaction, including the time spent in the original transaction plus any time spent while deferred.
     */
    const std::chrono::nanoseconds deferred_elapsed_;

    std::vector<transaction_attempt> attempts_;
    std::string atr_id_;
    std::string atr_collection_;
    transactions_cleanup& cleanup_;
    std::shared_ptr<attempt_context_impl> current_attempt_context_;
    mutable std::mutex mutex_;

    std::unique_ptr<exp_delay> delay_;
};
} // namespace couchbase::core::transactions
