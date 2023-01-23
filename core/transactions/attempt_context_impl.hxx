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

#include <core/cluster.hxx>
#include <couchbase/transactions/async_attempt_context.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/transaction_query_options.hxx>

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

// implemented in core::impl::query, to take advantage of the statics over there
namespace couchbase::core::impl
{

couchbase::transactions::transaction_query_result
build_transaction_query_result(operations::query_response resp, std::error_code ec = {});

core::operations::query_request
build_transaction_query_request(couchbase::query_options::built opts);

} // namespace couchbase::core::impl

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
  , public couchbase::transactions::async_attempt_context
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
    std::string query_context_;

    // commit needs to access the hooks
    friend class staged_mutation_queue;
    // entry needs access to private members
    friend class atr_cleanup_entry;
    // transaction_context needs access to the two functions below
    friend class transaction_context;

    couchbase::transactions::transaction_get_result_ptr insert_raw(const couchbase::collection& coll,
                                                                   const std::string& id,
                                                                   std::vector<std::byte> content) override
    {
        return wrap_call_for_public_api([this, coll, &id, &content]() -> transaction_get_result {
            return insert_raw({ coll.bucket_name(), coll.scope_name(), coll.name(), id }, content);
        });
    }
    transaction_get_result insert_raw(const core::document_id& id, const std::vector<std::byte>& content) override;
    void insert_raw(const collection& coll,
                    std::string id,
                    std::vector<std::byte> content,
                    couchbase::transactions::async_result_handler&& handler) override
    {
        insert_raw({ coll.bucket_name(), coll.scope_name(), coll.name(), std::move(id) },
                   content,
                   [this, handler = std::move(handler)](std::exception_ptr err, std::optional<transaction_get_result> res) mutable {
                       wrap_callback_for_async_public_api(err, res, std::move(handler));
                   });
    }
    void insert_raw(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb) override;

    transaction_get_result replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content) override;
    couchbase::transactions::transaction_get_result_ptr replace_raw(const couchbase::transactions::transaction_get_result_ptr doc,
                                                                    std::vector<std::byte> content) override
    {
        return wrap_call_for_public_api(
          [this, doc, &content]() -> transaction_get_result { return replace_raw(dynamic_cast<transaction_get_result&>(*doc), content); });
    }
    void replace_raw(couchbase::transactions::transaction_get_result_ptr doc,
                     std::vector<std::byte> content,
                     couchbase::transactions::async_result_handler&& handler) override
    {
        replace_raw(dynamic_cast<transaction_get_result&>(*doc),
                    content,
                    [this, handler = std::move(handler)](std::exception_ptr err, std::optional<transaction_get_result> res) mutable {
                        wrap_callback_for_async_public_api(err, res, std::move(handler));
                    });
    }
    void replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb) override;

    void remove_staged_insert(const core::document_id& id, VoidCallback&& cb);

    void get_with_query(const core::document_id& id, bool optional, Callback&& cb);
    void insert_raw_with_query(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb);
    void replace_raw_with_query(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb);
    void remove_with_query(const transaction_get_result& document, VoidCallback&& cb);

    void commit_with_query(VoidCallback&& cb);
    void rollback_with_query(VoidCallback&& cb);

    void query_begin_work(std::function<void(std::exception_ptr)>&& cb);

    void do_query(const std::string& statement, const couchbase::transactions::transaction_query_options& opts, QueryCallback&& cb);
    std::exception_ptr handle_query_error(const core::operations::query_response& resp);
    void wrap_query(const std::string& statement,
                    const couchbase::transactions::transaction_query_options& opts,
                    const std::vector<core::json_string>& params,
                    const tao::json::value& txdata,
                    const std::string& hook_point,
                    bool check_expiry,
                    std::function<void(std::exception_ptr, core::operations::query_response)>&& cb);

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
        } catch (const op_exception& op_ex) {
            txn_log->warn("op callback called a txn operation that threw (and didn't handle) a op_exception {}", op_ex.what());
            errors_.push_back(
              transaction_operation_failed(error_class_from_external_exception(op_ex.cause()), op_ex.what()).cause(op_ex.cause()));
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
    void cache_error_async(Handler cb, std::function<void()> func)
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
        } catch (const op_exception& e) {
            op_completed_with_error(std::move(cb), e);
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
    couchbase::transactions::transaction_get_result_ptr get(const couchbase::collection& coll, const std::string& id) override
    {
        return wrap_call_for_public_api([this, coll, id]() mutable -> transaction_get_result {
            auto ret = get_optional({ coll.bucket_name(), coll.scope_name(), coll.name(), id });
            if (ret) {
                return *ret;
            }
            return { transaction_op_error_context{ errc::transaction_op::document_not_found_exception } };
        });
    }
    void get(const couchbase::collection& coll, std::string id, couchbase::transactions::async_result_handler&& handler) override
    {
        get_optional({ coll.bucket_name(), coll.scope_name(), coll.name(), std::move(id) },
                     [this, handler = std::move(handler)](std::exception_ptr err, std::optional<transaction_get_result> res) mutable {
                         if (!res) {
                             return handler(std::make_shared<transaction_get_result>(
                               transaction_op_error_context{ errc::transaction_op::document_not_found_exception }));
                         }
                         return wrap_callback_for_async_public_api(err, res, std::move(handler));
                     });
    }
    void get(const core::document_id& id, Callback&& cb) override;

    std::optional<transaction_get_result> get_optional(const core::document_id& id) override;
    void get_optional(const core::document_id& id, Callback&& cb) override;

    void remove(const transaction_get_result& document) override;
    couchbase::transaction_op_error_context remove(couchbase::transactions::transaction_get_result_ptr doc) override
    {
        return wrap_void_call_for_public_api([this, doc]() { remove(dynamic_cast<transaction_get_result&>(*doc)); });
    }
    void remove(const transaction_get_result& document, VoidCallback&& cb) override;
    void remove(couchbase::transactions::transaction_get_result_ptr doc, couchbase::transactions::async_err_handler&& handler) override
    {
        remove(dynamic_cast<transaction_get_result&>(*doc), [this, handler = std::move(handler)](std::exception_ptr e) mutable {
            wrap_err_callback_for_async_api(e, std::move(handler));
        });
    };

    core::operations::query_response do_core_query(const std::string& statement,
                                                   const couchbase::transactions::transaction_query_options& options) override;

    couchbase::transactions::transaction_query_result_ptr do_public_query(
      const std::string& statement,
      const couchbase::transactions::transaction_query_options& opts) override;

    void query(const std::string& statement,
               const couchbase::transactions::transaction_query_options& options,
               QueryCallback&& cb) override;

    void query(std::string statement,
               couchbase::transactions::transaction_query_options opts,
               couchbase::transactions::async_query_handler&& handler) override
    {
        query(
          statement, opts, [handler = std::move(handler)](std::exception_ptr err, std::optional<core::operations::query_response> resp) {
              if (err) {
                  try {
                      std::rethrow_exception(err);
                  } catch (const transaction_operation_failed& e) {
                      return handler(std::make_shared<couchbase::transactions::transaction_query_result>(e.get_error_ctx()));
                  } catch (const op_exception& ex) {
                      return handler(std::make_shared<couchbase::transactions::transaction_query_result>(ex.ctx()));
                  } catch (...) {
                      // just in case...
                      return handler(std::make_shared<couchbase::transactions::transaction_query_result>(
                        transaction_op_error_context(couchbase::errc::transaction_op::unknown)));
                  }
              }
              handler(
                std::make_shared<couchbase::transactions::transaction_query_result>(core::impl::build_transaction_query_result(*resp)));
          });
    }

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

    void select_atr_if_needed_unlocked(const core::document_id id, std::function<void(std::optional<transaction_operation_failed>)>&& cb);

    template<typename Handler>
    void do_get(const core::document_id& id, const std::optional<std::string> resolving_missing_atr_entry, Handler&& cb);

    void get_doc(const core::document_id& id,
                 std::function<void(std::optional<error_class>, std::optional<std::string>, std::optional<transaction_get_result>)>&& cb);

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

    couchbase::transactions::transaction_get_result_ptr wrap_call_for_public_api(std::function<transaction_get_result()>&& handler)
    {
        try {
            return std::make_shared<transaction_get_result>(handler());
        } catch (const transaction_operation_failed& e) {
            return std::make_shared<transaction_get_result>(e.get_error_ctx());
        } catch (const op_exception& ex) {
            return std::make_shared<transaction_get_result>(ex.ctx());
        } catch (...) {
            // the handler should catch everything else, but just in case...
            return std::make_shared<transaction_get_result>(transaction_op_error_context(errc::transaction_op::unknown));
        }
    }

    couchbase::transaction_op_error_context wrap_void_call_for_public_api(std::function<void()>&& handler)
    {
        try {
            handler();
            return {};
        } catch (const transaction_operation_failed& e) {
            return e.get_error_ctx();
        } catch (...) {
            // the handler should catch everything else, but just in case...
            return transaction_op_error_context(errc::transaction_op::unknown);
        }
    }

    void wrap_callback_for_async_public_api(std::exception_ptr err,
                                            std::optional<transaction_get_result> res,
                                            std::function<void(couchbase::transactions::transaction_get_result_ptr)>&& cb)
    {
        if (res) {
            return cb(std::make_shared<transaction_get_result>(*res));
        }
        if (err) {
            try {
                std::rethrow_exception(err);
            } catch (const op_exception& e) {
                return cb(std::make_shared<transaction_get_result>(e.ctx()));
            } catch (const transaction_operation_failed& e) {
                return cb(std::make_shared<transaction_get_result>(e.get_error_ctx()));
            } catch (...) {
                return cb(std::make_shared<transaction_get_result>(transaction_op_error_context(errc::transaction_op::unknown)));
            }
        }
        return cb(std::make_shared<transaction_get_result>(transaction_op_error_context(errc::transaction_op::unknown)));
    }

    void wrap_err_callback_for_async_api(std::exception_ptr err, std::function<void(couchbase::transaction_op_error_context)>&& cb)
    {
        if (err) {
            try {
                std::rethrow_exception(err);
            } catch (const transaction_operation_failed& e) {
                return cb(e.get_error_ctx());
            } catch (...) {
                return cb({ errc::transaction_op::unknown });
            }
        }
        return cb({});
    }

    void ensure_open_bucket(std::string bucket_name, std::function<void(std::error_code)>&& handler)
    {
        cluster_ref()->open_bucket(bucket_name, [handler = std::move(handler)](std::error_code ec) { handler(ec); });
    }
};

} // namespace couchbase::core::transactions
