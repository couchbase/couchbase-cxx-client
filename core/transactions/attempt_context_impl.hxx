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

#include <couchbase/transactions/attempt_context.hxx>

#include "attempt_context_testing_hooks.hxx"
#include "error_list.hxx"
#include "waitable_op_list.hxx"

#include "async_attempt_context.hxx"
#include "attempt_context.hxx"
#include "attempt_state.hxx"
#include "internal/atr_cleanup_entry.hxx"
#include "internal/exceptions_internal.hxx"
#include "internal/transaction_context.hxx"
#include "transaction_get_result.hxx"

#include <chrono>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace couchbase::core::transactions
{
/**
 * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well as commit or
 * rollback the transaction.
 */
class transactions;
enum class forward_compat_stage;
class staged_mutation_queue;
class staged_mutation;

class attempt_context_impl
  : public attempt_context
  , public couchbase::transactions::attempt_context
  , public async_attempt_context
{
  private:
    transaction_context& overall_;
    std::optional<core::document_id> atr_id_;
    bool is_done_{ false };
    std::unique_ptr<staged_mutation_queue> staged_mutations_;
    attempt_context_testing_hooks& hooks_;
    error_list errors_;
    std::mutex mutex_;
    waitable_op_list op_list_;

    // commit needs to access the hooks
    friend class staged_mutation_queue;
    // entry needs access to private members
    friend class atr_cleanup_entry;
    // transaction_context needs access to the two functions below
    friend class transaction_context;

    couchbase::transactions::transaction_get_result_ptr insert_raw(std::shared_ptr<couchbase::collection> coll,
                                                                   const std::string& id,
                                                                   std::vector<std::byte> content) override
    {
        try {
            auto res = insert_raw({ coll->bucket_name(), coll->scope_name(), coll->name(), id }, content);
            auto retval = std::make_shared<transaction_get_result>(res);
            return retval;
        } catch (const transaction_operation_failed& e) {
            return {};
        }
    }
    transaction_get_result insert_raw(const core::document_id& id, const std::vector<std::byte>& content) override;

    void insert_raw(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb) override;

    transaction_get_result replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content) override;
    couchbase::transactions::transaction_get_result_ptr replace_raw(const couchbase::transactions::transaction_get_result_ptr doc,
                                                                    std::vector<std::byte> content) override
    {
        try {
            return std::make_shared<transaction_get_result>(replace_raw(dynamic_cast<transaction_get_result&>(*doc), content));
        } catch (const transaction_operation_failed& e) {
            return {};
        }
    }
    void replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb) override;

    void remove_staged_insert(const core::document_id& id, VoidCallback&& cb);

    void get_with_query(const core::document_id& id, bool optional, Callback&& cb);
    void insert_raw_with_query(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb);
    void replace_raw_with_query(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb);
    void remove_with_query(const transaction_get_result& document, VoidCallback&& cb);

    void commit_with_query(VoidCallback&& cb);
    void rollback_with_query(VoidCallback&& cb);

    void query_begin_work(utils::movable_function<void(std::exception_ptr)>&& cb);

    void do_query(const std::string& statement, const transaction_query_options& opts, QueryCallback&& cb);
    std::exception_ptr handle_query_error(const core::operations::query_response& resp);
    void wrap_query(const std::string& statement,
                    const transaction_query_options& opts,
                    const std::vector<core::json_string>& params,
                    const tao::json::value& txdata,
                    const std::string& hook_point,
                    bool check_expiry,
                    utils::movable_function<void(std::exception_ptr, core::operations::query_response)>&& cb);

    void handle_err_from_callback(std::exception_ptr e)
    {
        try {
            throw e;
        } catch (const transaction_operation_failed& ex) {
            txn_log->error("op callback called a txn operation that threw exception {}", ex.what());
            op_list_.decrement_ops();
            // presumably that op called op_completed_with_error already, so
            // don't do anything here but swallow it.
        } catch (const async_operation_conflict& op_ex) {
            // the count isn't changed when this is thrown, so just swallow it and log
            txn_log->error("op callback called a txn operation that threw exception {}", op_ex.what());
        } catch (const query_exception& query_ex) {
            txn_log->warn("op callback called a txn operation that threw (and didn't handle) a query_exception {}", query_ex.what());
            errors_.push_back(transaction_operation_failed(FAIL_OTHER, query_ex.what()).cause(query_ex.cause()));
            op_list_.decrement_ops();
        } catch (const std::exception& std_ex) {
            // if the callback throws something which wasn't handled
            // we just want to handle as a rollback
            txn_log->error("op callback threw exception {}", std_ex.what());
            errors_.push_back(transaction_operation_failed(FAIL_OTHER, std_ex.what()));
            op_list_.decrement_ops();
        } catch (...) {
            // could be something really arbitrary, still...
            txn_log->error("op callback threw unexpected exception");
            errors_.push_back(transaction_operation_failed(FAIL_OTHER, "unexpected error"));
            op_list_.decrement_ops();
        }
    }
    template<typename Cb, typename T>
    void op_completed_with_callback(Cb&& cb, std::optional<T> t)
    {
        try {
            op_list_.decrement_in_flight();
            cb({}, t);
            op_list_.decrement_ops();
        } catch (...) {
            handle_err_from_callback(std::current_exception());
        }
    }

    template<typename Cb>
    void op_completed_with_callback(Cb&& cb)
    {
        try {
            op_list_.decrement_in_flight();
            cb({});
            op_list_.decrement_ops();
        } catch (...) {
            handle_err_from_callback(std::current_exception());
        }
    }

    template<typename E>
    void op_completed_with_error(std::function<void(std::exception_ptr)>&& cb, E err)
    {
        return op_completed_with_error(std::move(cb), std::make_exception_ptr(err));
    }

    void op_completed_with_error(std::function<void(std::exception_ptr)>&& cb, std::exception_ptr err)
    {
        try {
            std::rethrow_exception(err);
        } catch (const transaction_operation_failed& e) {
            // if this is a transaction_operation_failed, we need to cache it before moving on...
            errors_.push_back(e);
            try {
                op_list_.decrement_in_flight();
                cb(std::current_exception());
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        } catch (...) {
            try {
                op_list_.decrement_in_flight();
                cb(std::current_exception());
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        }
    }

    template<typename Ret, typename E>
    void op_completed_with_error(std::function<void(std::exception_ptr, std::optional<Ret>)>&& cb, E err)
    {
        return op_completed_with_error(std::move(cb), std::make_exception_ptr(err));
    }

    template<typename Ret>
    void op_completed_with_error(std::function<void(std::exception_ptr, std::optional<Ret>)>&& cb, std::exception_ptr err)
    {
        try {
            std::rethrow_exception(err);
        } catch (const transaction_operation_failed& e) {
            // if this is a transaction_operation_failed, we need to cache it before moving on...
            errors_.push_back(e);
            try {
                op_list_.decrement_in_flight();
                cb(std::current_exception(), std::optional<Ret>());
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        } catch (...) {
            try {
                op_list_.decrement_in_flight();
                cb(std::current_exception(), std::optional<Ret>());
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        }
    }
    template<typename Ret>
    void op_completed_with_error_no_cache(std::function<void(std::exception_ptr, std::optional<Ret>)>&& cb, std::exception_ptr err)
    {
        try {
            cb(err, std::optional<Ret>());
        } catch (...) {
            // eat it.
        }
    }

    void op_completed_with_error_no_cache(std::function<void(std::exception_ptr)>&& cb, std::exception_ptr err)
    {
        try {
            cb(err);
        } catch (...) {
            // just eat it.
        }
    }

    template<typename Handler>
    void cache_error_async(Handler&& cb, std::function<void()> func)
    {
        try {
            op_list_.increment_ops();
            existing_error();
            return func();
        } catch (const async_operation_conflict& e) {
            // can't do anything here but log and eat it.
            error("Attempted to perform txn operation after commit/rollback started: {}", e.what());
            // you cannot call op_completed_with_error, as it tries to decrement
            // the op count, however it didn't successfully increment it, so...
            op_completed_with_error_no_cache(std::move(cb), std::current_exception());
        } catch (const transaction_operation_failed& e) {
            // thrown only from call_func when previous error exists, so eat it, unless
            // it has PREVIOUS_OP_FAILED cause
            if (e.cause() == PREVIOUS_OPERATION_FAILED) {
                op_completed_with_error(std::move(cb), e);
            }
        } catch (const std::exception& e) {
            op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
        }
    }

    template<typename... Args>
    void trace(const std::string& fmt, Args... args)
    {
        txn_log->trace(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
    }

    template<typename... Args>
    void debug(const std::string& fmt, Args... args)
    {
        txn_log->debug(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
    }

    template<typename... Args>
    void info(const std::string& fmt, Args... args)
    {
        txn_log->info(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
    }

    template<typename... Args>
    void error(const std::string& fmt, Args... args)
    {
        txn_log->error(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
    }

    std::shared_ptr<core::cluster> cluster_ref();

  public:
    explicit attempt_context_impl(transaction_context& transaction_ctx);
    ~attempt_context_impl() override;

    transaction_get_result get(const core::document_id& id) override;
    couchbase::transactions::transaction_get_result_ptr get(std::shared_ptr<couchbase::collection> coll, const std::string& id) override
    {
        return std::make_shared<transaction_get_result>(get({ coll->bucket_name(), coll->scope_name(), coll->name(), id }));
    }
    void get(const core::document_id& id, Callback&& cb) override;

    std::optional<transaction_get_result> get_optional(const core::document_id& id) override;
    void get_optional(const core::document_id& id, Callback&& cb) override;

    void remove(const transaction_get_result& document) override;
    couchbase::transactions::transaction_get_result_ptr remove(couchbase::transactions::transaction_get_result_ptr doc) override
    {
        try {
            remove(dynamic_cast<transaction_get_result&>(*doc));
            return doc;
        } catch (const transaction_operation_failed& e) {
            // TODO: here is where we pop the error in
            return doc;
        } // TODO: handle other exceptions
    }
    void remove(const transaction_get_result& document, VoidCallback&& cb) override;

    core::operations::query_response query(const std::string& statement, const transaction_query_options& options) override;
    void query(const std::string& statement, const transaction_query_options& options, QueryCallback&& cb) override;

    void commit() override;
    void commit(VoidCallback&& cb) override;
    void rollback() override;
    void rollback(VoidCallback&& cb) override;

    void existing_error(bool prev_op_failed = true)
    {
        if (!errors_.empty()) {
            errors_.do_throw((prev_op_failed ? std::make_optional(PREVIOUS_OPERATION_FAILED) : std::nullopt));
        }
    }

    [[nodiscard]] bool is_done()
    {
        return is_done_;
    }

    [[nodiscard]] const std::string& transaction_id()
    {
        return overall_.transaction_id();
    }

    [[nodiscard]] const std::string& id()
    {
        return overall_.current_attempt().id;
    }

    [[nodiscard]] attempt_state state()
    {
        return overall_.current_attempt().state;
    }

    void state(attempt_state s)
    {
        overall_.current_attempt().state = s;
    }

    [[nodiscard]] const std::string atr_id()
    {
        return overall_.atr_id();
    }

    void atr_id(const std::string& atr_id)
    {
        overall_.atr_id(atr_id);
    }

    [[nodiscard]] const std::string atr_collection()
    {
        return overall_.atr_collection();
    }

    void atr_collection_name(const std::string& coll)
    {
        overall_.atr_collection(coll);
    }

    bool has_expired_client_side(std::string place, std::optional<const std::string> doc_id);

  private:
    std::atomic<bool> expiry_overtime_mode_{ false };

    bool check_expiry_pre_commit(std::string stage, std::optional<const std::string> doc_id);

    void check_expiry_during_commit_or_rollback(const std::string& stage, std::optional<const std::string> doc_id);

    template<typename Handler>
    void set_atr_pending_locked(const core::document_id& collection, std::unique_lock<std::mutex>&& lock, Handler&& cb);

    std::optional<error_class> error_if_expired_and_not_in_overtime(const std::string& stage, std::optional<const std::string> doc_id);

    staged_mutation* check_for_own_write(const core::document_id& id);

    template<typename Handler>
    void check_and_handle_blocking_transactions(const transaction_get_result& doc, forward_compat_stage stage, Handler&& cb);

    template<typename Handler, typename Delay>
    void check_atr_entry_for_blocking_document(const transaction_get_result& doc, Delay delay, Handler&& cb);

    template<typename Handler>
    void check_if_done(Handler& cb);

    void atr_commit(bool ambiguity_resolution_mode);

    void atr_commit_ambiguity_resolution();

    void atr_complete();

    void atr_abort();

    void atr_rollback_complete();

    void select_atr_if_needed_unlocked(const core::document_id id,
                                       utils::movable_function<void(std::optional<transaction_operation_failed>)>&& cb);

    template<typename Handler>
    void do_get(const core::document_id& id, const std::optional<std::string> resolving_missing_atr_entry, Handler&& cb);

    void get_doc(
      const core::document_id& id,
      utils::movable_function<void(std::optional<error_class>, std::optional<std::string>, std::optional<transaction_get_result>)>&& cb);

    core::operations::mutate_in_request create_staging_request(const core::document_id& in,
                                                               const transaction_get_result* document,
                                                               const std::string type,
                                                               std::optional<std::vector<std::byte>> content = std::nullopt);

    template<typename Handler, typename Delay>
    void create_staged_insert(const core::document_id& id,
                              const std::vector<std::byte>& content,
                              uint64_t cas,
                              Delay&& delay,
                              Handler&& cb);

    template<typename Handler>
    void create_staged_replace(const transaction_get_result& document, const std::vector<std::byte>& content, Handler&& cb);

    template<typename Handler, typename Delay>
    void create_staged_insert_error_handler(const core::document_id& id,
                                            const std::vector<std::byte>& content,
                                            uint64_t cas,
                                            Delay&& delay,
                                            Handler&& cb,
                                            error_class ec,
                                            const std::string& message);
};
} // namespace couchbase::core::transactions
