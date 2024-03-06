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

#include "staged_mutation.hxx"

#include "attempt_context_impl.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "core/cluster.hxx"

#include "internal/transaction_fields.hxx"
#include "internal/utils.hxx"
#include "result.hxx"
#include "result_fmt.hxx"

#include <asio/bind_executor.hpp>
#include <asio/post.hpp>

namespace couchbase::core::transactions
{

bool
unstaging_state::wait_until_unstage_possible()
{
    std::unique_lock lock(mutex_);
    auto success = cv_.wait_for(lock, ctx_->overall().remaining(), [this] { return (in_flight_count_ < MAX_PARALLELISM) || abort_; });
    if (!abort_) {
        if (success) {
            in_flight_count_++;
        } else {
            abort_ = true;
        }
    }
    lock.unlock();
    return !abort_;
}

void
unstaging_state::notify_unstage_complete()
{
    std::lock_guard lock(mutex_);
    in_flight_count_--;
    cv_.notify_one();
}

void
unstaging_state::notify_unstage_error()
{
    std::lock_guard lock(mutex_);
    abort_ = true;
    in_flight_count_--;
    cv_.notify_all();
}

bool
staged_mutation_queue::empty()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

void
staged_mutation_queue::add(const staged_mutation& mutation)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // Can only have one staged mutation per document.
    queue_.erase(std::remove_if(queue_.begin(),
                                queue_.end(),
                                [&mutation](const staged_mutation& item) { return document_ids_equal(item.id(), mutation.id()); }),
                 queue_.end());
    queue_.push_back(mutation);
}

void
staged_mutation_queue::extract_to(const std::string& prefix, core::operations::mutate_in_request& req)
{
    std::lock_guard<std::mutex> lock(mutex_);
    tao::json::value inserts = tao::json::empty_array;
    tao::json::value replaces = tao::json::empty_array;
    tao::json::value removes = tao::json::empty_array;

    for (const auto& mutation : queue_) {
        tao::json::value doc{ { ATR_FIELD_PER_DOC_ID, mutation.doc().id().key() },
                              { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().id().bucket() },
                              { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().id().scope() },
                              { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().id().collection() } };
        switch (mutation.type()) {
            case staged_mutation_type::INSERT:
                inserts.push_back(doc);
                break;
            case staged_mutation_type::REMOVE:
                removes.push_back(doc);
                break;
            case staged_mutation_type::REPLACE:
                replaces.push_back(doc);
                break;
        }
    }
    auto specs =
      couchbase::mutate_in_specs{
          couchbase::mutate_in_specs::upsert_raw(prefix + ATR_FIELD_DOCS_INSERTED, core::utils::json::generate_binary(inserts))
            .xattr()
            .create_path(),
          couchbase::mutate_in_specs::upsert_raw(prefix + ATR_FIELD_DOCS_REPLACED, core::utils::json::generate_binary(replaces))
            .xattr()
            .create_path(),
          couchbase::mutate_in_specs::upsert_raw(prefix + ATR_FIELD_DOCS_REMOVED, core::utils::json::generate_binary(removes))
            .xattr()
            .create_path(),
      }
        .specs();
    req.specs.insert(req.specs.end(), specs.begin(), specs.end());
}

void
staged_mutation_queue::remove_any(const core::document_id& id)
{
    const std::lock_guard<std::mutex> lock(mutex_);
    auto new_end =
      std::remove_if(queue_.begin(), queue_.end(), [&id](const staged_mutation& item) { return document_ids_equal(item.id(), id); });
    queue_.erase(new_end, queue_.end());
}

staged_mutation*
staged_mutation_queue::find_any(const core::document_id& id)
{
    const std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

staged_mutation*
staged_mutation_queue::find_replace(const core::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

staged_mutation*
staged_mutation_queue::find_insert(const core::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

staged_mutation*
staged_mutation_queue::find_remove(const core::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}
void
staged_mutation_queue::iterate(std::function<void(staged_mutation&)> op)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        op(item);
    }
}

void
staged_mutation_queue::commit(attempt_context_impl* ctx)
{
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "committing staged mutations...");
    std::lock_guard<std::mutex> lock(mutex_);

    unstaging_state state{ ctx };
    std::vector<std::future<void>> futures{};
    futures.reserve(queue_.size());

    bool aborted = false;

    for (auto& item : queue_) {
        aborted = !state.wait_until_unstage_possible();
        if (aborted) {
            // Do not commit any more mutations
            break;
        }

        auto barrier = std::make_shared<std::promise<void>>();
        auto future = barrier->get_future();

        try {
            auto timer = std::make_shared<asio::steady_timer>(ctx->cluster_ref().io_context());
            async_constant_delay delay(timer);

            switch (item.type()) {
                case staged_mutation_type::REMOVE:
                    remove_doc(ctx, item, delay, [&state, barrier](const std::exception_ptr& exc) {
                        if (exc) {
                            state.notify_unstage_error();
                            barrier->set_exception(exc);
                        } else {
                            state.notify_unstage_complete();
                            barrier->set_value();
                        }
                    });
                    break;
                case staged_mutation_type::INSERT:
                case staged_mutation_type::REPLACE:
                    commit_doc(ctx, item, delay, [&state, barrier](const std::exception_ptr& exc) {
                        if (exc) {
                            state.notify_unstage_error();
                            barrier->set_exception(exc);
                        } else {
                            state.notify_unstage_complete();
                            barrier->set_value();
                        }
                    });
                    break;
            }
        } catch (...) {
            // This should not happen, but catching it to ensure that we wait for in-flight operations
            CB_ATTEMPT_CTX_LOG_ERROR(ctx,
                                     "caught exception while trying to initiate commit for {}. Aborting rest of commit and waiting for "
                                     "in-flight rollback operations to finish",
                                     item.doc().id());
            aborted = true;
            break;
        }

        futures.push_back(std::move(future));
    }

    std::exception_ptr exc{};
    for (auto& future : futures) {
        try {
            future.get();
        } catch (...) {
            if (!exc) {
                exc = std::current_exception();
            }
        }
    }
    if (exc) {
        rethrow_exception(exc);
    }
    if (aborted) {
        // Commit was aborted but no exception was raised from the futures (possibly timeout during wait_until_unstage_possible())
        throw transaction_operation_failed(FAIL_OTHER, "commit aborted").no_rollback().failed_post_commit();
    }
}

void
staged_mutation_queue::rollback(attempt_context_impl* ctx)
{
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rolling back staged mutations...");
    std::lock_guard<std::mutex> lock(mutex_);

    unstaging_state state{ ctx };
    std::vector<std::future<void>> futures{};
    futures.reserve(queue_.size());

    bool aborted = false;

    for (auto& item : queue_) {
        aborted = !state.wait_until_unstage_possible();
        if (aborted) {
            // Do not roll back any more mutations
            break;
        }

        auto barrier = std::make_shared<std::promise<void>>();
        auto future = barrier->get_future();

        try {
            auto timer = std::make_shared<asio::steady_timer>(ctx->cluster_ref().io_context());
            async_exp_delay delay(timer);

            switch (item.type()) {
                case staged_mutation_type::INSERT:
                    rollback_insert(ctx, item, delay, [&state, barrier](const std::exception_ptr& exc) {
                        if (exc) {
                            state.notify_unstage_error();
                            barrier->set_exception(exc);
                        } else {
                            state.notify_unstage_complete();
                            barrier->set_value();
                        }
                    });
                    break;
                case staged_mutation_type::REMOVE:
                case staged_mutation_type::REPLACE:
                    rollback_remove_or_replace(ctx, item, delay, [&state, barrier](std::exception_ptr exc) {
                        if (exc) {
                            state.notify_unstage_error();
                            barrier->set_exception(exc);
                        } else {
                            state.notify_unstage_complete();
                            barrier->set_value();
                        }
                    });
                    break;
            }
        } catch (...) {
            // This should not happen, but catching it to ensure that we wait for in-flight operations
            CB_ATTEMPT_CTX_LOG_ERROR(ctx,
                                     "caught exception while trying to initiate rollback for {}. Aborting rollback and waiting for "
                                     "in-flight rollback operations to finish",
                                     item.doc().id());
            aborted = true;
            break;
        }

        futures.push_back(std::move(future));
    }

    std::exception_ptr exc{};
    for (auto& future : futures) {
        try {
            future.get();
        } catch (...) {
            if (!exc) {
                exc = std::current_exception();
            }
        }
    }
    if (exc) {
        rethrow_exception(exc);
    }
    if (aborted) {
        // Rollback was aborted but no exception was raised from the futures (possibly timeout during wait_until_unstage_possible())
        throw transaction_operation_failed(FAIL_OTHER, "rollback aborted").no_rollback();
    }
}

void
staged_mutation_queue::rollback_insert(attempt_context_impl* ctx,
                                       const staged_mutation& item,
                                       async_exp_delay& delay,
                                       utils::movable_function<void(std::exception_ptr)> callback)
{
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rolling back staged insert for {} with cas {}", item.doc().id(), item.doc().cas().value());

    asio::post(asio::bind_executor(ctx->cluster_ref().io_context(), [this, callback = std::move(callback), ctx, &item, delay]() mutable {
        auto handler = [this, callback = std::move(callback), ctx, &item, delay](const std::optional<client_error>& e) mutable {
            if (e) {
                return handle_rollback_insert_error(e.value(), ctx, item, delay, std::move(callback));
            }
            return callback({});
        };

        auto ec = ctx->error_if_expired_and_not_in_overtime(STAGE_DELETE_INSERTED, item.doc().id().key());
        if (ec) {
            return handler(client_error(*ec, "expired in rollback and not in overtime mode"));
        }

        return ctx->hooks_.before_rollback_delete_inserted(
          ctx, item.doc().id().key(), [handler = std::move(handler), ctx, &item, delay](std::optional<error_class> ec) mutable {
              if (ec) {
                  return handler(client_error(*ec, "before_rollback_delete_insert hook threw error"));
              }
              core::operations::mutate_in_request req{ item.doc().id() };
              req.specs =
                couchbase::mutate_in_specs{
                    couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                }
                  .specs();
              req.access_deleted = true;
              req.cas = item.doc().cas();
              wrap_durable_request(req, ctx->overall().config());
              return ctx->cluster_ref().execute(
                req, [handler = std::move(handler), ctx, &item, delay](const core::operations::mutate_in_response& resp) mutable {
                    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "mutate_in for {} with cas {}", item.doc().id(), item.doc().cas().value());

                    auto res = result::create_from_subdoc_response(resp);
                    return validate_rollback_insert_result(ctx, res, item, std::move(handler));
                });
          });
    }));
}

void
staged_mutation_queue::rollback_remove_or_replace(attempt_context_impl* ctx,
                                                  const staged_mutation& item,
                                                  async_exp_delay& delay,
                                                  utils::movable_function<void(std::exception_ptr)> callback)
{
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rolling back staged remove/replace for {} with cas {}", item.doc().id(), item.doc().cas().value());

    asio::post(asio::bind_executor(ctx->cluster_ref().io_context(), [this, callback = std::move(callback), ctx, &item, delay]() mutable {
        auto handler = [this, callback = std::move(callback), ctx, &item, delay](const std::optional<client_error>& e) mutable {
            if (e) {
                return handle_rollback_remove_or_replace_error(e.value(), ctx, item, delay, std::move(callback));
            }
            return callback({});
        };
        auto ec = ctx->error_if_expired_and_not_in_overtime(STAGE_ROLLBACK_DOC, item.doc().id().key());
        if (ec) {
            return handler(client_error(*ec, "expired in rollback_remove_or_replace and not in expiry overtime"));
        }
        ctx->hooks_.before_doc_rolled_back(
          ctx, item.doc().id().key(), [handler = std::move(handler), ctx, &item, delay](std::optional<error_class> ec) mutable {
              if (ec) {
                  return handler(client_error(*ec, "before_doc_rolled_back hook threw error"));
              }
              core::operations::mutate_in_request req{ item.doc().id() };
              req.specs =
                couchbase::mutate_in_specs{
                    couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                }
                  .specs();
              req.cas = item.doc().cas();
              wrap_durable_request(req, ctx->overall().config());
              return ctx->cluster_ref().execute(
                req, [handler = std::move(handler), ctx, &item, delay](const core::operations::mutate_in_response& resp) mutable {
                    auto res = result::create_from_subdoc_response(resp);
                    return validate_rollback_remove_or_replace_result(ctx, res, item, std::move(handler));
                });
          });
    }));
}

void
staged_mutation_queue::commit_doc(attempt_context_impl* ctx,
                                  staged_mutation& item,
                                  async_constant_delay& delay,
                                  utils::movable_function<void(std::exception_ptr)> callback,
                                  bool ambiguity_resolution_mode,
                                  bool cas_zero_mode)
{
    CB_ATTEMPT_CTX_LOG_TRACE(
      ctx, "commit doc {}, cas_zero_mode {}, ambiguity_resolution_mode {}", item.doc().id(), cas_zero_mode, ambiguity_resolution_mode);

    asio::post(asio::bind_executor(
      ctx->cluster_ref().io_context(),
      [this, callback = std::move(callback), ctx, &item, delay, cas_zero_mode, ambiguity_resolution_mode]() mutable {
          ctx->check_expiry_during_commit_or_rollback(STAGE_COMMIT_DOC, std::optional<const std::string>(item.doc().id().key()));

          auto handler = [this, callback = std::move(callback), ctx, &item, delay](
                           const std::optional<client_error>& e, bool ambiguity_resolution_mode, bool cas_zero_mode) mutable {
              if (e) {
                  return handle_commit_doc_error(
                    e.value(), ctx, item, delay, ambiguity_resolution_mode, cas_zero_mode, std::move(callback));
              }
              callback({});
          };

          ctx->hooks_.before_doc_committed(
            ctx,
            item.doc().id().key(),
            [handler = std::move(handler), ctx, &item, delay, ambiguity_resolution_mode, cas_zero_mode](
              std::optional<error_class> ec) mutable {
                if (ec) {
                    return handler(client_error(*ec, "before_doc_committed hook threw error"), ambiguity_resolution_mode, cas_zero_mode);
                }
                // move staged content into doc
                CB_ATTEMPT_CTX_LOG_TRACE(
                  ctx, "commit doc id {}, content {}, cas {}", item.doc().id(), to_string(item.content()), item.doc().cas().value());

                if (item.type() == staged_mutation_type::INSERT && !cas_zero_mode) {
                    core::operations::insert_request req{ item.doc().id(), item.content() };
                    req.flags = couchbase::codec::codec_flags::json_common_flags;
                    wrap_durable_request(req, ctx->overall().config());
                    return ctx->cluster_ref().execute(
                      req,
                      [handler = std::move(handler), ctx, &item, delay, ambiguity_resolution_mode, cas_zero_mode](
                        const core::operations::insert_response& resp) mutable {
                          auto res = result::create_from_mutation_response(resp);
                          return validate_commit_doc_result(
                            ctx, res, item, [ambiguity_resolution_mode, cas_zero_mode, handler = std::move(handler)](auto e) mutable {
                                if (e) {
                                    return handler(e, ambiguity_resolution_mode, cas_zero_mode);
                                }
                                // Commit successful
                                return handler({}, {}, {});
                            });
                      });
                } else {
                    core::operations::mutate_in_request req{ item.doc().id() };
                    req.specs =
                      couchbase::mutate_in_specs{
                          couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                          // subdoc::opcode::set_doc used in replace w/ empty path
                          couchbase::mutate_in_specs::replace_raw("", item.content()),
                      }
                        .specs();
                    req.store_semantics = couchbase::store_semantics::replace;
                    req.cas = couchbase::cas(cas_zero_mode ? 0 : item.doc().cas().value());
                    wrap_durable_request(req, ctx->overall().config());
                    return ctx->cluster_ref().execute(
                      req,
                      [handler = std::move(handler), ctx, &item, delay, ambiguity_resolution_mode, cas_zero_mode](
                        const core::operations::mutate_in_response resp) mutable {
                          auto res = result::create_from_subdoc_response(resp);
                          return validate_commit_doc_result(
                            ctx, res, item, [ambiguity_resolution_mode, cas_zero_mode, handler = std::move(handler)](auto e) mutable {
                                if (e) {
                                    return handler(e, ambiguity_resolution_mode, cas_zero_mode);
                                }
                                // Commit successful
                                return handler({}, {}, {});
                            });
                      });
                }
            });
      }));
}

void
staged_mutation_queue::remove_doc(attempt_context_impl* ctx,
                                  const staged_mutation& item,
                                  async_constant_delay& delay,
                                  utils::movable_function<void(std::exception_ptr)> callback)
{
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "remove doc {}", item.doc().id());

    asio::post(asio::bind_executor(ctx->cluster_ref().io_context(), [this, callback = std::move(callback), ctx, &item, delay]() mutable {
        auto handler = [this, ctx, &item, delay, callback = std::move(callback)](const std::optional<client_error>& e) mutable {
            if (e) {
                return handle_remove_doc_error(e.value(), ctx, item, delay, std::move(callback));
            }
            return callback({});
        };

        ctx->check_expiry_during_commit_or_rollback(STAGE_REMOVE_DOC, std::optional<const std::string>(item.doc().id().key()));
        return ctx->hooks_.before_doc_removed(
          ctx, item.doc().id().key(), [ctx, &item, delay, handler = std::move(handler)](auto ec) mutable {
              if (ec) {
                  return handler(client_error(*ec, "before_doc_removed hook threw error"));
              }
              core::operations::remove_request req{ item.doc().id() };
              wrap_durable_request(req, ctx->overall().config());
              return ctx->cluster_ref().execute(
                req, [handler = std::move(handler), ctx, &item, delay](core::operations::remove_response resp) mutable {
                    auto res = result::create_from_mutation_response(resp);
                    return validate_remove_doc_result(ctx, res, item, std::move(handler));
                });
          });
    }));
}

void
staged_mutation_queue::validate_commit_doc_result(attempt_context_impl* ctx,
                                                  result& res,
                                                  staged_mutation& item,
                                                  client_error_handler&& handler)
{
    try {
        validate_operation_result(res);
    } catch (const client_error& e) {
        return handler(e);
    }
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "commit doc result {}", res);
    // TODO: mutation tokens
    ctx->hooks_.after_doc_committed_before_saving_cas(
      ctx, item.doc().id().key(), [ctx, res, item, handler = std::move(handler)](auto ec) mutable {
          if (ec) {
              return handler(client_error(*ec, "after_doc_committed_before_saving_cas threw error"));
          }
          item.doc().cas(res.cas);
          return ctx->hooks_.after_doc_committed(ctx, item.doc().id().key(), [res, item, handler = std::move(handler)](auto ec) mutable {
              if (ec) {
                  return handler(client_error(*ec, "after_doc_committed threw error"));
              }
              return handler({});
          });
      });
}

void
staged_mutation_queue::validate_remove_doc_result(attempt_context_impl* ctx,
                                                  result& res,
                                                  const staged_mutation& item,
                                                  client_error_handler&& handler)
{
    try {
        validate_operation_result(res);
    } catch (const client_error& e) {
        return handler(e);
    }
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "remove doc result {}", res);
    return ctx->hooks_.after_doc_removed_pre_retry(ctx, item.doc().id().key(), [handler = std::move(handler)](auto ec) {
        if (ec) {
            return handler(client_error(*ec, "after_doc_removed_pre_retry threw error"));
        }
        return handler({});
    });
}

void
staged_mutation_queue::validate_rollback_insert_result(attempt_context_impl* ctx,
                                                       result& res,
                                                       const staged_mutation& item,
                                                       client_error_handler&& handler)
{
    try {
        validate_operation_result(res);
    } catch (const client_error& e) {
        return handler(e);
    }
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback insert result {}", res);
    return ctx->hooks_.after_rollback_delete_inserted(ctx, item.doc().id().key(), [handler = std::move(handler)](auto ec) {
        if (ec) {
            return handler(client_error(*ec, "after_rollback_delete_insert hook threw error"));
        }
        return handler({});
    });
}

void
staged_mutation_queue::validate_rollback_remove_or_replace_result(attempt_context_impl* ctx,
                                                                  result& res,
                                                                  const staged_mutation& item,
                                                                  client_error_handler&& handler)
{
    try {
        validate_operation_result(res);
    } catch (const client_error& e) {
        return handler(e);
    }
    CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback remove or replace result {}", res);
    return ctx->hooks_.after_rollback_replace_or_remove(ctx, item.doc().id().key(), [handler = std::move(handler)](auto ec) {
        if (ec) {
            return handler(client_error(*ec, "after_rollback_replace_or_remove hook threw error"));
        }
        return handler({});
    });
}

void
staged_mutation_queue::handle_commit_doc_error(const client_error& e,
                                               attempt_context_impl* ctx,
                                               staged_mutation& item,
                                               async_constant_delay& delay,
                                               bool ambiguity_resolution_mode,
                                               bool cas_zero_mode,
                                               utils::movable_function<void(std::exception_ptr)> callback)
{
    const error_class ec = e.ec();
    try {
        if (ctx->expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "commit_doc for {} error while in overtime mode {}", item.doc().id(), e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, "expired during commit").no_rollback().failed_post_commit();
        }
        CB_ATTEMPT_CTX_LOG_TRACE(ctx, "commit_doc for {} error {}", item.doc().id(), e.what());
        switch (ec) {
            case FAIL_AMBIGUOUS:
                ambiguity_resolution_mode = true;
                throw retry_operation("FAIL_AMBIGUOUS in commit_doc");
            case FAIL_CAS_MISMATCH:
            case FAIL_DOC_ALREADY_EXISTS:
                if (ambiguity_resolution_mode) {
                    throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
                }
                ambiguity_resolution_mode = true;
                cas_zero_mode = true;
                throw retry_operation("FAIL_DOC_ALREADY_EXISTS in commit_doc");
            default:
                throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
    } catch (const retry_operation&) {
        delay([this, callback = std::move(callback), ctx, &item, delay, ambiguity_resolution_mode, cas_zero_mode](
                const std::exception_ptr& exc) mutable {
            if (exc) {
                callback(exc);
                return;
            }
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "retrying commit_doc");
            commit_doc(ctx, item, delay, std::move(callback), ambiguity_resolution_mode, cas_zero_mode);
        });
    } catch (const transaction_operation_failed&) {
        callback(std::current_exception());
    }
}

void
staged_mutation_queue::handle_remove_doc_error(const client_error& e,
                                               attempt_context_impl* ctx,
                                               const staged_mutation& item,
                                               async_constant_delay& delay,
                                               utils::movable_function<void(std::exception_ptr)> callback)
{
    auto ec = e.ec();
    try {
        if (ctx->expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "remove_doc for {} error while in overtime mode {}", item.doc().id(), e.what());
            throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
        CB_ATTEMPT_CTX_LOG_TRACE(ctx, "remove_doc for {} error {}", item.doc().id(), e.what());
        switch (ec) {
            case FAIL_AMBIGUOUS:
                throw retry_operation("remove_doc got FAIL_AMBIGUOUS");
            default:
                throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
    } catch (const retry_operation&) {
        delay([this, callback = std::move(callback), ctx, &item, delay](const std::exception_ptr& exc) mutable {
            if (exc) {
                callback(exc);
                return;
            }
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "retrying remove_doc");
            remove_doc(ctx, item, delay, std::move(callback));
        });
    } catch (const transaction_operation_failed&) {
        callback(std::current_exception());
    }
}

void
staged_mutation_queue::handle_rollback_insert_error(const client_error& e,
                                                    attempt_context_impl* ctx,
                                                    const staged_mutation& item,
                                                    async_exp_delay& delay,
                                                    utils::movable_function<void(std::exception_ptr)> callback)
{
    auto ec = e.ec();
    try {
        if (ctx->expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback_insert for {} error while in overtime mode {}", item.doc().id(), e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired while rolling back insert with {} ") + e.what())
              .no_rollback()
              .expired();
        }
        CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback_insert for {} error {}", item.doc().id(), e.what());
        switch (ec) {
            case FAIL_HARD:
            case FAIL_CAS_MISMATCH:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                ctx->expiry_overtime_mode_ = true;
                CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback_insert in expiry overtime mode, retrying...");
                throw retry_operation("retry rollback_insert");
            case FAIL_DOC_NOT_FOUND:
            case FAIL_PATH_NOT_FOUND:
                // already cleaned up?
                callback({});
                return;
            default:
                throw retry_operation("retry rollback insert");
        }
    } catch (const retry_operation&) {
        delay([this, callback = std::move(callback), ctx, &item, delay](const std::exception_ptr& exc) mutable {
            if (exc) {
                callback(exc);
                return;
            }
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "retrying rollback_insert");
            rollback_insert(ctx, item, delay, std::move(callback));
        });
    } catch (const transaction_operation_failed&) {
        callback(std::current_exception());
    }
}

void
staged_mutation_queue::handle_rollback_remove_or_replace_error(const client_error& e,
                                                               attempt_context_impl* ctx,
                                                               const staged_mutation& item,
                                                               async_exp_delay& delay,
                                                               utils::movable_function<void(std::exception_ptr)> callback)
{
    auto ec = e.ec();
    try {
        if (ctx->expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_TRACE(
              ctx, "rollback_remove_or_replace_error for {} error while in overtime mode {}", item.doc().id(), e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired while handling ") + e.what()).no_rollback();
        }
        CB_ATTEMPT_CTX_LOG_TRACE(ctx, "rollback_remove_or_replace_error for {} error {}", item.doc().id(), e.what());
        switch (ec) {
            case FAIL_HARD:
            case FAIL_DOC_NOT_FOUND:
            case FAIL_CAS_MISMATCH:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                ctx->expiry_overtime_mode_ = true;
                CB_ATTEMPT_CTX_LOG_TRACE(ctx, "setting expiry overtime mode in {}", STAGE_ROLLBACK_DOC);
                throw retry_operation("retry rollback_remove_or_replace");
            case FAIL_PATH_NOT_FOUND:
                // already cleaned up?
                callback({});
                return;
            default:
                throw retry_operation("retry rollback_remove_or_replace");
        }
    } catch (const retry_operation&) {
        delay([this, callback = std::move(callback), ctx, &item, delay](const std::exception_ptr& exc) mutable {
            if (exc) {
                callback(exc);
                return;
            }
            CB_ATTEMPT_CTX_LOG_TRACE(ctx, "retrying rollback_remove_or_replace");
            rollback_remove_or_replace(ctx, item, delay, std::move(callback));
        });
    } catch (const transaction_operation_failed&) {
        callback(std::current_exception());
    }
}
} // namespace couchbase::core::transactions
