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
#include "uid_generator.hxx"
#include <asio/post.hpp>
#include <core/cluster.hxx>

#include "internal/logging.hxx"
#include "internal/transaction_context.hxx"
#include "internal/utils.hxx"

namespace couchbase::core::transactions
{
transaction_context::transaction_context(transactions& txns, const couchbase::transactions::transaction_options& config)
  : transaction_id_(uid_generator::next())
  , start_time_client_(std::chrono::steady_clock::now())
  , transactions_(txns)
  , config_(config.apply(txns.config()))
  , deferred_elapsed_(0)
  , cleanup_(txns.cleanup())
  , delay_(new exp_delay(std::chrono::milliseconds(1), std::chrono::milliseconds(100), 2 * config_.expiration_time))
{
}

void
transaction_context::add_attempt()
{
    transaction_attempt attempt{};
    attempts_.push_back(attempt);
}

[[nodiscard]] std::chrono::nanoseconds
transaction_context::remaining() const
{
    const auto& now = std::chrono::steady_clock::now();
    auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
    return config_.expiration_time - expired_nanos;
}

[[nodiscard]] bool
transaction_context::has_expired_client_side()
{
    // repeat code above - nice for logging.  Ponder changing this.
    const auto& now = std::chrono::steady_clock::now();
    auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
    auto expired_millis = std::chrono::duration_cast<std::chrono::milliseconds>(expired_nanos);
    bool is_expired = expired_nanos > config_.expiration_time;
    if (is_expired) {
        txn_log->info("has expired client side (now={}ns, start={}ns, deferred_elapsed={}ns, expired={}ns ({}ms), config={}ms)",
                      now.time_since_epoch().count(),
                      start_time_client_.time_since_epoch().count(),
                      deferred_elapsed_.count(),
                      expired_nanos.count(),
                      expired_millis.count(),
                      std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time).count());
    }
    return is_expired;
}

void
transaction_context::retry_delay()
{
    // when we retry an operation, we typically call that function recursively.  So, we need to
    // limit total number of times we do it.  Later we can be more sophisticated, perhaps.
    auto delay = config_.expiration_time / 100; // the 100 is arbitrary
    txn_log->trace("about to sleep for {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(delay).count());
    std::this_thread::sleep_for(delay);
}

void
transaction_context::new_attempt_context(async_attempt_context::VoidCallback&& cb)
{
    asio::post(transactions_.cluster_ref()->io_context(), [this, cb = std::move(cb)]() {
        // the first time we call the delay, it just records an end time.  After that, it
        // actually delays.
        try {
            (*delay_)();
            current_attempt_context_ = std::make_shared<attempt_context_impl>(*this);
            txn_log->info("starting attempt {}/{}/{}/", num_attempts(), transaction_id(), current_attempt_context_->id());
            cb(nullptr);
        } catch (...) {
            cb(std::current_exception());
        }
    });
}

std::shared_ptr<attempt_context_impl>
transaction_context::current_attempt_context()
{
    return current_attempt_context_;
}

void
transaction_context::get(const core::document_id& id, async_attempt_context::Callback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->get(id, std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
}

void
transaction_context::get_optional(const core::document_id& id, async_attempt_context::Callback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->get_optional(id, std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
}

void
transaction_context::insert(const core::document_id& id, const std::vector<std::byte>& content, async_attempt_context::Callback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->insert_raw(id, content, std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
}

void
transaction_context::replace(const transaction_get_result& doc, const std::vector<std::byte>& content, async_attempt_context::Callback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->replace_raw(doc, content, std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
}

void
transaction_context::remove(const transaction_get_result& doc, async_attempt_context::VoidCallback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->remove(doc, std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
}

void
transaction_context::query(const std::string& statement,
                           const couchbase::transactions::transaction_query_options& opts,
                           async_attempt_context::QueryCallback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->query(statement, opts, std::move(cb));
    }
    throw(transaction_operation_failed(FAIL_OTHER, "no current attempt context"));
}

void
transaction_context::commit(async_attempt_context::VoidCallback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->commit(std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
}

void
transaction_context::rollback(async_attempt_context::VoidCallback&& cb)
{
    if (current_attempt_context_) {
        return current_attempt_context_->rollback(std::move(cb));
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
}

void
transaction_context::existing_error(bool previous_op_failed)
{
    if (current_attempt_context_) {
        return current_attempt_context_->existing_error(previous_op_failed);
    }
    throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
}

void
transaction_context::handle_error(std::exception_ptr err, txn_complete_callback&& callback)
{
    try {
        try {
            std::rethrow_exception(err);
        } catch (const query_exception& e) {
            // turn this into a transaction_operation_failed
            throw transaction_operation_failed(FAIL_OTHER, e.what()).cause(e.cause());
        }
    } catch (const transaction_operation_failed& er) {
        txn_log->error("got transaction_operation_failed {}", er.what());
        if (er.should_rollback()) {
            txn_log->trace("got rollback-able exception, rolling back");
            try {
                current_attempt_context_->rollback();
            } catch (const std::exception& er_rollback) {
                cleanup().add_attempt(*current_attempt_context_);
                txn_log->trace("got error \"{}\" while auto rolling back, throwing original error", er_rollback.what(), er.what());
                auto final = er.get_final_exception(*this);
                // if you get here, we didn't throw, yet we had an error.  Fall through in
                // this case.  Note the current logic is such that rollback will not have a
                // commit ambiguous error, so we should always throw.
                assert(final);
                return callback(final, std::nullopt);
            }
            if (er.should_retry() && has_expired_client_side()) {
                txn_log->trace("auto rollback succeeded, however we are expired so no retry");

                return callback(
                  transaction_operation_failed(FAIL_EXPIRY, "expired in auto rollback").no_rollback().expired().get_final_exception(*this),
                  {});
            }
        }
        if (er.should_retry()) {
            txn_log->trace("got retryable exception, retrying");
            cleanup().add_attempt(*current_attempt_context_);
            return callback(std::nullopt, std::nullopt);
        }

        // throw the expected exception here
        cleanup().add_attempt(*current_attempt_context_);
        auto final = er.get_final_exception(*this);
        std::optional<::couchbase::transactions::transaction_result> res;
        if (!final) {
            res = get_transaction_result();
        }
        return callback(final, res);
    } catch (const std::exception& ex) {
        txn_log->error("got runtime error \"{}\"", ex.what());
        try {
            current_attempt_context_->rollback();
        } catch (...) {
            txn_log->error("got error rolling back \"{}\"", ex.what());
        }
        cleanup().add_attempt(*current_attempt_context_);
        // the assumption here is this must come from the logic, not
        // our operations (which only throw transaction_operation_failed),
        auto op_failed = transaction_operation_failed(FAIL_OTHER, ex.what());
        return callback(op_failed.get_final_exception(*this), std::nullopt);
    } catch (...) {
        txn_log->error("got unexpected error, rolling back");
        try {
            current_attempt_context_->rollback();
        } catch (...) {
            txn_log->error("got error rolling back unexpected error");
        }
        cleanup().add_attempt(*current_attempt_context_);
        // the assumption here is this must come from the logic, not
        // our operations (which only throw transaction_operation_failed),
        auto op_failed = transaction_operation_failed(FAIL_OTHER, "Unexpected error");
        return callback(op_failed.get_final_exception(*this), std::nullopt);
    }
}

void
transaction_context::finalize(txn_complete_callback&& cb)
{

    try {
        existing_error(false);
        if (current_attempt_context_->is_done()) {
            return cb(std::nullopt, get_transaction_result());
        }
        commit([this, cb = std::move(cb)](std::exception_ptr err) mutable {
            if (err) {
                return handle_error(err, std::move(cb));
            }
            cb(std::nullopt, get_transaction_result());
        });
    } catch (...) {
        return handle_error(std::current_exception(), std::move(cb));
    }
}

} // namespace couchbase::core::transactions
