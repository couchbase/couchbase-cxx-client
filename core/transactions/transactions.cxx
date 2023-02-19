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

#include "attempt_context_impl.hxx"

#include "core/cluster.hxx"

#include "core/transactions.hxx"
#include "internal/exceptions_internal.hxx"
#include "internal/logging.hxx"
#include "internal/transaction_context.hxx"
#include "internal/transactions_cleanup.hxx"
#include "internal/utils.hxx"

namespace couchbase::core::transactions
{
transactions::transactions(std::shared_ptr<core::cluster> cluster, const couchbase::transactions::transactions_config& config)
  : transactions(cluster, config.build())
{
}

transactions::transactions(std::shared_ptr<core::cluster> cluster, const couchbase::transactions::transactions_config::built& config)
  : cluster_(cluster)
  , config_(config)
  , cleanup_(new transactions_cleanup(cluster_, config_))
{
    CB_TXN_LOG_DEBUG(
      "couchbase transactions {} ({}) creating new transaction object", couchbase::core::meta::sdk_id(), couchbase::core::meta::os());
    // if the config specifies custom metadata collection, lets be sure to open that bucket
    // on the cluster before we start.  NOTE: we actually do call get_and_open_buckets which opens all the buckets
    // on the cluster (that we have permissions to open) in the cleanup.   However, that is happening asynchronously
    // so there's a chance we will fail to have opened the custom metadata collection bucket before trying to make a
    // transaction.   We have to open this one _now_.
    if (config_.metadata_collection) {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        std::atomic<bool> callback_called{ false };
        cluster_->open_bucket(config_.metadata_collection->bucket, [&callback_called, barrier](std::error_code ec) {
            if (callback_called.load()) {
                return;
            }
            callback_called = true;
            barrier->set_value(ec);
        });
        auto err = f.get();
        if (err) {
            auto err_msg =
              fmt::format("error opening metadata_collection bucket '{}' specified in the config!", config_.metadata_collection->bucket);
            CB_TXN_LOG_DEBUG(err_msg);
            throw std::runtime_error(err_msg);
        }
    }
}

transactions::~transactions() = default;

template<typename Handler>
::couchbase::transactions::transaction_result
wrap_run(transactions& txns, const couchbase::transactions::transaction_options& config, std::size_t max_attempts, Handler&& fn)
{
    transaction_context overall(txns, config);
    std::size_t attempts{ 0 };
    while (attempts++ < max_attempts) {
        // NOTE: new_attempt_context has the exponential backoff built in.  So, after
        // the first time it is called, it has a 1ms delay, then 2ms, etc... capped at 100ms
        // until (for now) a timeout is reached (2x the expiration_time).   Soon, will build in
        // a max attempts instead.  In any case, the timeout occurs in the logic - adding
        // a max attempts or timeout is just in case a bug prevents timeout, etc...
        overall.new_attempt_context();
        auto barrier = std::make_shared<std::promise<std::optional<couchbase::transactions::transaction_result>>>();
        auto f = barrier->get_future();
        auto finalize_handler = [&, barrier](std::optional<transaction_exception> err,
                                             std::optional<couchbase::transactions::transaction_result> result) {
            if (result) {
                return barrier->set_value(result);
            } else if (err) {
                return barrier->set_exception(std::make_exception_ptr(*err));
            }
            barrier->set_value({});
        };
        try {
            auto ctx = overall.current_attempt_context();
            fn(*ctx);
        } catch (...) {
            overall.handle_error(std::current_exception(), finalize_handler);
            auto retval = f.get();
            if (retval) {
                // no return value, no exception means retry.
                return *retval;
            }
            continue;
        }
        overall.finalize(finalize_handler);
        auto retval = f.get();
        if (retval) {
            return *retval;
        }
        continue;
    }
    // only thing to do here is return, but we really exceeded the max attempts
    assert(true);
    return overall.get_transaction_result();
}

::couchbase::transactions::transaction_result
transactions::run(logic&& code)
{
    couchbase::transactions::transaction_options config;
    return wrap_run(*this, config, max_attempts_, std::move(code));
}

::couchbase::transactions::transaction_result
transactions::run(const couchbase::transactions::transaction_options& config, logic&& code)
{
    return wrap_run(*this, config, max_attempts_, std::move(code));
}

std::pair<couchbase::transaction_error_context, couchbase::transactions::transaction_result>
transactions::run(couchbase::transactions::txn_logic&& code, const couchbase::transactions::transaction_options& config)
{
    try {
        return { {}, wrap_run(*this, config, max_attempts_, std::move(code)) };
    } catch (const transaction_exception& e) {
        // get transaction_error_context from e and return it in the transaction_result
        return e.get_transaction_result();
    }
}

void
transactions::run(const couchbase::transactions::transaction_options& config, async_logic&& code, txn_complete_callback&& cb)
{
    std::thread([this, config, code = std::move(code), cb = std::move(cb)]() {
        try {
            auto result = wrap_run(*this, config, max_attempts_, std::move(code));
            return cb({}, result);
        } catch (const transaction_exception& e) {
            return cb(e, std::nullopt);
        }
    }).detach();
}
void
transactions::run(couchbase::transactions::async_txn_logic&& code,
                  couchbase::transactions::async_txn_complete_logic&& cb,
                  const couchbase::transactions::transaction_options& config)
{
    std::thread([this, config, code = std::move(code), cb = std::move(cb)]() {
        try {
            auto result = wrap_run(*this, config, max_attempts_, std::move(code));
            return cb({}, result);
        } catch (const transaction_exception& e) {
            auto [ctx, res] = e.get_transaction_result();
            return cb(ctx, res);
        }
    }).detach();
}

void
transactions::run(async_logic&& code, txn_complete_callback&& cb)
{
    couchbase::transactions::transaction_options config;
    return run(config, std::move(code), std::move(cb));
}

void
transactions::close()
{
    CB_TXN_LOG_DEBUG("closing transactions");
    cleanup_->close();
    CB_TXN_LOG_DEBUG("transactions closed");
}
} // namespace couchbase::core::transactions
