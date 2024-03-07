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

#include "active_transaction_record.hxx"
#include "atr_ids.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "durability_level.hxx"
#include "forward_compat.hxx"
#include "staged_mutation.hxx"

#include "attempt_state.hxx"
#include "internal/exceptions_internal.hxx"
#include "internal/logging.hxx"
#include "internal/utils.hxx"

namespace couchbase::core::transactions
{

// statement constants for queries
static const std::string BEGIN_WORK{ "BEGIN WORK" };
static const std::string COMMIT{ "COMMIT" };
static const std::string ROLLBACK{ "ROLLBACK" };
static const std::string KV_GET{ "EXECUTE __get" };
static const std::string KV_INSERT{ "EXECUTE __insert" };
static const std::string KV_REPLACE{ "EXECUTE __update" };
static const std::string KV_REMOVE{ "EXECUTE __delete" };
static const tao::json::value KV_TXDATA{ { "kv", true } };

// the config may have nullptr for attempt context hooks, so we use the noop here in that case
static auto noop_hooks = attempt_context_testing_hooks{};

const core::cluster&
attempt_context_impl::cluster_ref() const
{
    return overall_.cluster_ref();
}

attempt_context_impl::attempt_context_impl(transaction_context& transaction_ctx)
  : overall_(transaction_ctx)
  , staged_mutations_(std::make_unique<staged_mutation_queue>())
  , hooks_(overall_.config().attempt_context_hooks ? *overall_.config().attempt_context_hooks : noop_hooks)
{
    // put a new transaction_attempt in the context...
    overall_.add_attempt();
    CB_ATTEMPT_CTX_LOG_TRACE(this,
                             "added new attempt, state {}, expiration in {}ms",
                             attempt_state_name(state()),
                             std::chrono::duration_cast<std::chrono::milliseconds>(overall_.remaining()).count());
}

attempt_context_impl::~attempt_context_impl() = default;

template<typename Handler>
void
attempt_context_impl::check_and_handle_blocking_transactions(const transaction_get_result& doc, forward_compat_stage stage, Handler&& cb)
{
    // The main reason to require doc to be fetched inside the transaction is we can detect this on the client side
    if (doc.links().has_staged_write()) {
        // Check not just writing the same doc twice in the same transaction
        // NOTE: we check the transaction rather than attempt id. This is to handle [RETRY-ERR-AMBIG-REPLACE].
        if (doc.links().staged_transaction_id().value() == transaction_id()) {
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "doc {} has been written by this transaction, ok to continue", doc.id());
            return cb(std::nullopt);
        }
        if (doc.links().atr_id() && doc.links().atr_bucket_name() && doc.links().staged_attempt_id()) {
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "doc {} in another txn, checking atr...", doc.id());
            auto err = forward_compat::check(stage, doc.links().forward_compat());
            if (err) {
                return cb(err);
            }
            exp_delay delay(std::chrono::milliseconds(50), std::chrono::milliseconds(500), std::chrono::seconds(1));
            return check_atr_entry_for_blocking_document(doc, delay, std::move(cb));
        }
        CB_ATTEMPT_CTX_LOG_DEBUG(this,
                                 "doc {} is in another transaction {}, but doesn't have enough info to check the atr. "
                                 "probably a bug, proceeding to overwrite",
                                 doc.id(),
                                 *doc.links().staged_attempt_id());
    }
    return cb(std::nullopt);
}

transaction_get_result
attempt_context_impl::get(const core::document_id& id)
{
    auto barrier = std::make_shared<std::promise<transaction_get_result>>();
    auto f = barrier->get_future();
    get(id, [barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value(*res);
        }
    });
    return f.get();
}
void
attempt_context_impl::get(const core::document_id& id, Callback&& cb)
{
    if (op_list_.get_mode().is_query()) {
        return get_with_query(id, false, std::move(cb));
    }
    cache_error_async(cb, [&]() mutable {
        check_if_done(cb);
        do_get(id,
               std::nullopt,
               [this, id, cb = std::move(cb)](
                 std::optional<error_class> ec, std::optional<std::string> err_message, std::optional<transaction_get_result> res) mutable {
                   auto handler = [this, id, err_message, res, cb = std::move(cb)](std::optional<error_class> ec) mutable {
                       if (ec) {
                           switch (*ec) {
                               case FAIL_EXPIRY:
                                   return op_completed_with_error(
                                     std::move(cb), transaction_operation_failed(*ec, "transaction expired during get").expired());
                               case FAIL_DOC_NOT_FOUND:
                                   return op_completed_with_error(
                                     std::move(cb),
                                     transaction_operation_failed(*ec, fmt::format("document not found {}", err_message.value_or("")))
                                       .cause(external_exception::DOCUMENT_NOT_FOUND_EXCEPTION));
                               case FAIL_TRANSIENT:
                                   return op_completed_with_error(
                                     std::move(cb),
                                     transaction_operation_failed(*ec, fmt::format("transient failure in get {}", err_message.value_or("")))
                                       .retry());
                               case FAIL_HARD:
                                   return op_completed_with_error(
                                     std::move(cb),
                                     transaction_operation_failed(*ec, fmt::format("fail hard in get {}", err_message.value_or("")))
                                       .no_rollback());
                               default: {
                                   auto msg = fmt::format("got error \"{}\" while getting doc {}", err_message.value_or(""), id.key());
                                   return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, msg));
                               }
                           }
                       } else {
                           if (!res) {
                               return op_completed_with_error(std::move(cb), transaction_operation_failed(*ec, "document not found"));
                           }
                           auto err = forward_compat::check(forward_compat_stage::GETS, res->links().forward_compat());
                           if (err) {
                               return op_completed_with_error(std::move(cb), *err);
                           }
                           return op_completed_with_callback(std::move(cb), res);
                       }
                   };

                   if (!ec) {
                       return hooks_.after_get_complete(this, id.key(), std::move(handler));
                   }
                   return handler(ec);
               });
    });
}

std::optional<transaction_get_result>
attempt_context_impl::get_optional(const core::document_id& id)
{
    auto barrier = std::make_shared<std::promise<std::optional<transaction_get_result>>>();
    auto f = barrier->get_future();
    get_optional(id, [barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        if (err) {
            return barrier->set_exception(err);
        }
        return barrier->set_value(res);
    });
    return f.get();
}

void
attempt_context_impl::get_optional(const core::document_id& id, Callback&& cb)
{

    if (op_list_.get_mode().is_query()) {
        return get_with_query(id, true, std::move(cb));
    }
    cache_error_async(cb, [&]() {
        ensure_open_bucket(id.bucket(), [this, id, cb = std::move(cb)](std::error_code ec) mutable {
            if (ec) {
                return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, ec.message()));
            }
            check_if_done(cb);
            do_get(
              id,
              std::nullopt,
              [this, id, cb = std::move(cb)](
                std::optional<error_class> ec, std::optional<std::string> err_message, std::optional<transaction_get_result> res) mutable {
                  auto handler = [this, id, err_message, res, cb = std::move(cb)](std::optional<error_class> ec) mutable {
                      if (ec) {
                          switch (*ec) {
                              case FAIL_EXPIRY:
                                  return op_completed_with_error(
                                    std::move(cb),
                                    transaction_operation_failed(*ec,
                                                                 fmt::format("transaction expired during get {}", err_message.value_or("")))
                                      .expired());
                              case FAIL_DOC_NOT_FOUND:
                                  return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>());
                              case FAIL_TRANSIENT:
                                  return op_completed_with_error(
                                    std::move(cb),
                                    transaction_operation_failed(*ec, fmt::format("transient failure in get {}", err_message.value_or("")))
                                      .retry());
                              case FAIL_HARD:
                                  return op_completed_with_error(
                                    std::move(cb),
                                    transaction_operation_failed(*ec, fmt::format("fail hard in get {}", err_message.value_or("")))
                                      .no_rollback());
                              default: {
                                  return op_completed_with_error(
                                    std::move(cb),
                                    transaction_operation_failed(FAIL_OTHER,
                                                                 fmt::format("error getting {} {}", id.key(), err_message.value_or(""))));
                              }
                          }
                      } else {
                          if (res) {
                              auto err = forward_compat::check(forward_compat_stage::GETS, res->links().forward_compat());
                              if (err) {
                                  return op_completed_with_error(std::move(cb), *err);
                              }
                          }
                          return op_completed_with_callback(std::move(cb), res);
                      }
                  };

                  if (!ec) {
                      return hooks_.after_get_complete(this, id.key(), std::move(handler));
                  }
                  return handler(ec);
              });
        });
    });
}

core::operations::mutate_in_request
attempt_context_impl::create_staging_request(const core::document_id& id,
                                             const transaction_get_result* document,
                                             const std::string type,
                                             const std::string op_id,
                                             std::optional<std::vector<std::byte>> content)
{
    core::operations::mutate_in_request req{ id };
    tao::json::value txn;
    txn["id"] = tao::json::empty_object;
    txn["id"]["txn"] = transaction_id();
    txn["id"]["atmpt"] = this->id();
    txn["id"]["op"] = op_id;
    txn["atr"] = tao::json::empty_object;
    txn["atr"]["id"] = atr_id();
    txn["atr"]["bkt"] = atr_id_->bucket();
    txn["atr"]["scp"] = atr_id_->scope();
    txn["atr"]["coll"] = atr_id_->collection();
    txn["op"] = tao::json::empty_object;
    txn["op"]["type"] = type;

    if (document != nullptr && document->metadata()) {
        txn["restore"] = tao::json::empty_object;
        if (document->metadata()->cas()) {
            txn["restore"]["CAS"] = document->metadata()->cas().value();
        }
        if (document->metadata()->revid()) {
            txn["restore"]["revid"] = document->metadata()->revid().value();
        }
        if (document->metadata()->exptime()) {
            txn["restore"]["exptime"] = document->metadata()->exptime().value();
        }
    }

    auto mut_specs =
      couchbase::mutate_in_specs(couchbase::mutate_in_specs::upsert_raw("txn", core::utils::to_binary(jsonify(txn))).xattr().create_path());
    if (type != "remove") {
        mut_specs.push_back(couchbase::mutate_in_specs::upsert_raw("txn.op.stgd", content.value()).xattr());
    }
    mut_specs.push_back(
      couchbase::mutate_in_specs::upsert("txn.op.crc32", couchbase::subdoc::mutate_in_macro::value_crc32c).xattr().create_path());
    req.specs = mut_specs.specs();

    return wrap_durable_request(req, overall_.config());
}

void
attempt_context_impl::replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb)
{

    if (op_list_.get_mode().is_query()) {
        return replace_raw_with_query(document, content, std::move(cb));
    }
    return cache_error_async(cb, [&]() {
        ensure_open_bucket(document.bucket(), [this, cb = std::move(cb), document, content](std::error_code ec) mutable {
            if (ec) {
                return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, ec.message()));
            }
            try {
                auto op_id = uid_generator::next();
                // a get can return a 'empty' doc, so check for that and short-circuit the eventual error that will occur...
                if (document.key().empty() || document.bucket().empty()) {
                    return op_completed_with_error(std::move(cb),
                                                   transaction_operation_failed(FAIL_DOC_NOT_FOUND, "can't replace empty doc")
                                                     .cause(external_exception::DOCUMENT_NOT_FOUND_EXCEPTION));
                }
                CB_ATTEMPT_CTX_LOG_TRACE(this, "replacing {} with {}", document, to_string(content));
                check_if_done(cb);
                staged_mutation* existing_sm = staged_mutations_->find_any(document.id());
                if (existing_sm != nullptr && existing_sm->type() == staged_mutation_type::REMOVE) {
                    CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing REMOVE of {} while replacing", document);
                    return op_completed_with_error(
                      std::move(cb),
                      transaction_operation_failed(FAIL_DOC_NOT_FOUND,
                                                   "cannot replace a document that has been removed in the same transaction")
                        .cause(external_exception::DOCUMENT_NOT_FOUND_EXCEPTION));
                }
                if (check_expiry_pre_commit(STAGE_REPLACE, document.id().key())) {
                    return op_completed_with_error(std::move(cb),
                                                   transaction_operation_failed(FAIL_EXPIRY, "transaction expired").expired());
                }

                check_and_handle_blocking_transactions(
                  document,
                  forward_compat_stage::WWC_REPLACING,
                  [this, existing_sm = std::move(existing_sm), document = std::move(document), cb = std::move(cb), op_id, content](
                    std::optional<transaction_operation_failed> e1) mutable {
                      if (e1) {
                          return op_completed_with_error(std::move(cb), *e1);
                      }
                      auto tmp_doc =
                        document_id{ document.id().bucket(), document.id().scope(), document.id().collection(), document.id().key() };
                      select_atr_if_needed_unlocked(
                        tmp_doc,
                        [this, existing_sm = std::move(existing_sm), document = std::move(document), cb = std::move(cb), op_id, content](
                          std::optional<transaction_operation_failed> e2) mutable {
                            if (e2) {
                                return op_completed_with_error(std::move(cb), *e2);
                            }
                            if (existing_sm != nullptr && existing_sm->type() == staged_mutation_type::INSERT) {
                                CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing INSERT of {} while replacing", document);
                                exp_delay delay(std::chrono::milliseconds(5), std::chrono::milliseconds(300), overall_.config().timeout);
                                create_staged_insert(document.id(), content, existing_sm->doc().cas().value(), delay, op_id, std::move(cb));
                                return;
                            }
                            create_staged_replace(document, content, op_id, std::move(cb));
                        });
                  });
            } catch (const client_error& e) {
                error_class errc = e.ec();
                switch (errc) {
                    case FAIL_EXPIRY:
                        expiry_overtime_mode_ = true;
                        throw transaction_operation_failed(errc, e.what()).expired();
                    default:
                        throw transaction_operation_failed(errc, e.what());
                }
            }
        });
    });
}

template<typename Handler>
void
attempt_context_impl::create_staged_replace(const transaction_get_result& document,
                                            const std::vector<std::byte>& content,
                                            const std::string& op_id,
                                            Handler&& cb)
{
    auto req = create_staging_request(document.id(), &document, "replace", op_id, content);
    req.cas = document.cas();
    req.access_deleted = true;
    auto error_handler = [this](error_class ec, const std::string& msg, Handler&& cb) {
        transaction_operation_failed err(ec, msg);
        switch (ec) {
            case FAIL_DOC_NOT_FOUND:
            case FAIL_DOC_ALREADY_EXISTS:
            case FAIL_CAS_MISMATCH:
            case FAIL_TRANSIENT:
            case FAIL_AMBIGUOUS:
                return op_completed_with_error(std::move(cb), err.retry());
            case FAIL_HARD:
                return op_completed_with_error(std::move(cb), err.no_rollback());
            default:
                return op_completed_with_error(std::move(cb), err);
        }
    };
    auto ec = wait_for_hook(
      [this, key = document.id().key()](auto handler) mutable { return hooks_.before_staged_replace(this, key, std::move(handler)); });
    if (ec) {
        return error_handler(*ec, "before_staged_replace hook raised error", std::move(cb));
    }
    CB_ATTEMPT_CTX_LOG_TRACE(
      this, "about to replace doc {} with cas {} in txn {}", document.id(), document.cas().value(), overall_.transaction_id());
    overall_.cluster_ref().execute(
      req,
      [this, document, content, cb = std::move(cb), error_handler = std::move(error_handler)](
        core::operations::mutate_in_response resp) mutable {
          if (auto ec2 = error_class_from_response(resp); ec2) {
              return error_handler(*ec2, resp.ctx.ec().message(), std::move(cb));
          }
          return hooks_.after_staged_replace_complete(
            this,
            document.id().key(),
            [this, document, content, error_handler = std::move(error_handler), cb = std::move(cb), resp = std::move(resp)](
              auto ec) mutable {
                if (ec) {
                    return error_handler(*ec, "after_staged_replace_commit hook returned error", std::move(cb));
                }

                transaction_get_result out = document;
                out.cas(resp.cas.value());
                out.content(content);
                CB_ATTEMPT_CTX_LOG_TRACE(this, "replace staged content, result {}", out);
                staged_mutations_->add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>(out));
            });
      });
}

transaction_get_result
attempt_context_impl::replace_raw(const transaction_get_result& document, const std::vector<std::byte>& content)
{
    auto barrier = std::make_shared<std::promise<transaction_get_result>>();
    auto f = barrier->get_future();
    replace_raw(document, content, [barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        if (err) {
            return barrier->set_exception(err);
        }
        barrier->set_value(*res);
    });
    return f.get();
}

transaction_get_result
attempt_context_impl::insert_raw(const core::document_id& id, const std::vector<std::byte>& content)
{
    auto barrier = std::make_shared<std::promise<transaction_get_result>>();
    auto f = barrier->get_future();
    insert_raw(id, content, [barrier](std::exception_ptr err, std::optional<transaction_get_result> res) {
        if (err) {
            return barrier->set_exception(err);
        }
        barrier->set_value(*res);
    });
    return f.get();
}

void
attempt_context_impl::insert_raw(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb)
{

    if (op_list_.get_mode().is_query()) {
        return insert_raw_with_query(id, content, std::move(cb));
    }
    return cache_error_async(cb, [&]() mutable {
        ensure_open_bucket(id.bucket(), [this, id, content, cb = std::move(cb)](std::error_code ec) mutable {
            if (ec) {
                return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, ec.message()));
            }
            try {
                check_if_done(cb);
                auto op_id = uid_generator::next();
                staged_mutation* existing_sm = staged_mutations_->find_any(id);
                if ((existing_sm != nullptr) &&
                    (existing_sm->type() == staged_mutation_type::INSERT || existing_sm->type() == staged_mutation_type::REPLACE)) {
                    CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing insert or replace of {} while inserting", id);
                    return op_completed_with_error(
                      std::move(cb),
                      transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, "found existing insert or replace of same document"));
                }
                if (check_expiry_pre_commit(STAGE_INSERT, id.key())) {
                    return op_completed_with_error(std::move(cb),
                                                   transaction_operation_failed(FAIL_EXPIRY, "transaction expired").expired());
                }
                select_atr_if_needed_unlocked(
                  id, [this, existing_sm, cb = std::move(cb), id, op_id, content](std::optional<transaction_operation_failed> err) mutable {
                      if (err) {
                          return op_completed_with_error(std::move(cb), *err);
                      }
                      if (existing_sm != nullptr && existing_sm->type() == staged_mutation_type::REMOVE) {
                          CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing remove of {} while inserting", id);
                          return create_staged_replace(existing_sm->doc(), content, op_id, std::move(cb));
                      }
                      uint64_t cas = 0;
                      exp_delay delay(std::chrono::milliseconds(5), std::chrono::milliseconds(300), overall_.config().timeout);
                      create_staged_insert(id, content, cas, delay, op_id, std::move(cb));
                  });
            } catch (const std::exception& e) {
                return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
            }
        });
    });
}

void
attempt_context_impl::select_atr_if_needed_unlocked(const core::document_id id,
                                                    std::function<void(std::optional<transaction_operation_failed>)>&& cb)
{
    try {
        std::unique_lock<std::mutex> lock(mutex_);
        if (atr_id_) {
            CB_ATTEMPT_CTX_LOG_TRACE(this, "atr exists, moving on");
            return cb(std::nullopt);
        }
        std::size_t vbucket_id = 0;
        std::optional<const std::string> hook_atr = hooks_.random_atr_id_for_vbucket(this);
        if (hook_atr) {
            atr_id_ = atr_id_from_bucket_and_key(overall_.config(), id.bucket(), hook_atr.value());
        } else {
            vbucket_id = atr_ids::vbucket_for_key(id.key());
            atr_id_ = atr_id_from_bucket_and_key(overall_.config(), id.bucket(), atr_ids::atr_id_for_vbucket(vbucket_id));
        }
        // TODO: cleanup the transaction_context - this should be set (threadsafe) from the above calls
        overall_.atr_collection(collection_spec_from_id(id));
        overall_.atr_id(atr_id_->key());
        state(attempt_state::NOT_STARTED);
        CB_ATTEMPT_CTX_LOG_TRACE(
          this, R"(first mutated doc in transaction is "{}" on vbucket {}, so using atr "{}")", id, vbucket_id, atr_id_.value());
        overall_.cleanup().add_collection({ atr_id_->bucket(), atr_id_->scope(), atr_id_->collection() });
        set_atr_pending_locked(id, std::move(lock), std::move(cb));
    } catch (const std::exception& e) {
        CB_ATTEMPT_CTX_LOG_ERROR(this, "unexpected error \"{}\" during select atr if needed", e.what());
    }
}
template<typename Handler, typename Delay>
void
attempt_context_impl::check_atr_entry_for_blocking_document(const transaction_get_result& doc, Delay delay, Handler&& cb)
{
    try {
        delay();
        return hooks_.before_check_atr_entry_for_blocking_doc(
          this, doc.id().key(), [this, delay = std::move(delay), cb = std::move(cb), doc = std::move(doc)](auto ec) mutable {
              if (ec) {
                  return cb(transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction").retry());
              }

              core::document_id atr_id(doc.links().atr_bucket_name().value(),
                                       doc.links().atr_scope_name().value(),
                                       doc.links().atr_collection_name().value(),
                                       doc.links().atr_id().value());
              return active_transaction_record::get_atr(
                cluster_ref(),
                atr_id,
                [this, delay = std::move(delay), cb = std::move(cb), doc = std::move(doc)](
                  std::error_code err, std::optional<active_transaction_record> atr) mutable {
                    if (!err) {
                        if (atr) {
                            auto entries = atr->entries();
                            auto it = std::find_if(entries.begin(), entries.end(), [&doc](const atr_entry& e) {
                                return e.attempt_id() == doc.links().staged_attempt_id();
                            });
                            if (it != entries.end()) {
                                auto fwd_err = forward_compat::check(forward_compat_stage::WWC_READING_ATR, it->forward_compat());
                                if (fwd_err) {
                                    return cb(fwd_err);
                                }
                                switch (it->state()) {
                                    case attempt_state::COMPLETED:
                                    case attempt_state::ROLLED_BACK:
                                        CB_ATTEMPT_CTX_LOG_DEBUG(
                                          this, "existing atr entry can be ignored due to state {}", attempt_state_name(it->state()));
                                        return cb(std::nullopt);
                                    default:
                                        CB_ATTEMPT_CTX_LOG_DEBUG(
                                          this, "existing atr entry found in state {}, retrying", attempt_state_name(it->state()));
                                }
                                return check_atr_entry_for_blocking_document(doc, delay, std::move(cb));
                            }
                        }
                        CB_ATTEMPT_CTX_LOG_DEBUG(this, "no blocking atr entry");
                        return cb(std::nullopt);
                    }
                    // if we are here, there is still a write-write conflict
                    return cb(transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction").retry());
                });
          });
    } catch (const retry_operation_timeout&) {
        return cb(transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction").retry());
    }
}
void
attempt_context_impl::remove(const transaction_get_result& document, VoidCallback&& cb)
{

    if (op_list_.get_mode().is_query()) {
        return remove_with_query(document, std::move(cb));
    }
    return cache_error_async(cb, [&]() mutable {
        check_if_done(cb);
        ensure_open_bucket(document.bucket(), [this, document, cb = std::move(cb)](std::error_code ec) mutable {
            if (ec) {
                return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, ec.message()));
            }
            staged_mutation* existing_sm = staged_mutations_->find_any(document.id());
            auto error_handler = [this](error_class ec, const std::string msg, VoidCallback&& cb) mutable {
                transaction_operation_failed err(ec, msg);
                switch (ec) {
                    case FAIL_EXPIRY:
                        expiry_overtime_mode_ = true;
                        return op_completed_with_error(std::move(cb), err.expired());
                    case FAIL_DOC_NOT_FOUND:
                    case FAIL_DOC_ALREADY_EXISTS:
                    case FAIL_CAS_MISMATCH:
                    case FAIL_TRANSIENT:
                    case FAIL_AMBIGUOUS:
                        return op_completed_with_error(std::move(cb), err.retry());
                    case FAIL_HARD:
                        return op_completed_with_error(std::move(cb), err.no_rollback());
                    default:
                        return op_completed_with_error(std::move(cb), err);
                }
            };
            if (check_expiry_pre_commit(STAGE_REMOVE, document.id().key())) {
                return error_handler(FAIL_EXPIRY, "transaction expired", std::move(cb));
            }
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "removing {}", document);
            auto op_id = uid_generator::next();
            if (existing_sm != nullptr) {
                if (existing_sm->type() == staged_mutation_type::REMOVE) {
                    CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing REMOVE of {} while removing", document);
                    return op_completed_with_error(
                      std::move(cb),
                      transaction_operation_failed(FAIL_DOC_NOT_FOUND,
                                                   "cannot remove a document that has been removed in the same transaction")
                        .cause(external_exception::DOCUMENT_NOT_FOUND_EXCEPTION));
                }
                if (existing_sm->type() == staged_mutation_type::INSERT) {
                    remove_staged_insert(document.id(), std::move(cb));
                    return;
                }
            }
            return check_and_handle_blocking_transactions(
              document,
              forward_compat_stage::WWC_REMOVING,
              [this, document = std::move(document), cb = std::move(cb), op_id, error_handler = std::move(error_handler)](
                std::optional<transaction_operation_failed> err1) mutable {
                  if (err1) {
                      return op_completed_with_error(std::move(cb), *err1);
                  }
                  auto tmp_doc =
                    document_id{ document.id().bucket(), document.id().scope(), document.id().collection(), document.id().key() };
                  return select_atr_if_needed_unlocked(
                    tmp_doc,
                    [document = std::move(document), cb = std::move(cb), this, op_id, error_handler = std::move(error_handler)](
                      std::optional<transaction_operation_failed> err2) mutable {
                        if (err2) {
                            return op_completed_with_error(std::move(cb), *err2);
                        }
                        auto key = document.id().key();
                        return hooks_.before_staged_remove(
                          this,
                          key,
                          [document = std::move(document), cb = std::move(cb), this, op_id, error_handler = std::move(error_handler)](
                            auto ec) mutable {
                              if (ec) {
                                  return error_handler(*ec, "before_staged_remove hook raised error", std::move(cb));
                              }
                              CB_ATTEMPT_CTX_LOG_TRACE(this, "about to remove doc {} with cas {}", document.id(), document.cas().value());
                              auto req = create_staging_request(document.id(), &document, "remove", op_id);
                              req.cas = document.cas();
                              req.access_deleted = document.links().is_deleted();
                              return overall_.cluster_ref().execute(
                                req,
                                [this, document = std::move(document), cb = std::move(cb), error_handler = std::move(error_handler)](
                                  core::operations::mutate_in_response resp) mutable {
                                    auto ec = error_class_from_response(resp);
                                    if (ec) {
                                        return error_handler(*ec, resp.ctx.ec().message(), std::move(cb));
                                    }
                                    auto key = document.id().key();
                                    return hooks_.after_staged_remove_complete(
                                      this,
                                      key,
                                      [this,
                                       document = std::move(document),
                                       cb = std::move(cb),
                                       error_handler = std::move(error_handler),
                                       resp = std::move(resp)](auto ec) mutable {
                                          if (ec) {
                                              return error_handler(*ec, resp.ctx.ec().message(), std::move(cb));
                                          }

                                          CB_ATTEMPT_CTX_LOG_TRACE(
                                            this, "removed doc {} CAS={}, rc={}", document.id(), resp.cas.value(), resp.ctx.ec().message());
                                          // TODO: this copy...  can we do better?
                                          transaction_get_result new_res = document;
                                          new_res.cas(resp.cas.value());
                                          staged_mutations_->add(
                                            staged_mutation(new_res, std::vector<std::byte>{}, staged_mutation_type::REMOVE));
                                          return op_completed_with_callback(cb);
                                      });
                                });
                          });
                    });
              });
        });
    });
}

void
attempt_context_impl::remove_staged_insert(const core::document_id& id, VoidCallback&& cb)
{
    if (auto ec = error_if_expired_and_not_in_overtime(STAGE_REMOVE_STAGED_INSERT, id.key()); ec) {
        return op_completed_with_error(
          std::move(cb), transaction_operation_failed(FAIL_EXPIRY, std::string("expired in remove_staged_insert")).no_rollback().expired());
    }

    auto error_handler = [this](error_class ec, const std::string& msg, VoidCallback&& cb) mutable {
        transaction_operation_failed err(ec, msg);
        switch (ec) {
            case FAIL_HARD:
                return op_completed_with_error(std::move(cb), err.no_rollback());
            default:
                return op_completed_with_error(std::move(cb), err.retry());
        }
    };
    CB_ATTEMPT_CTX_LOG_DEBUG(this, "removing staged insert {}", id);

    return hooks_.before_remove_staged_insert(
      this, id.key(), [this, id = std::move(id), cb = std::move(cb), error_handler = std::move(error_handler)](auto ec) mutable {
          if (ec) {
              return error_handler(*ec, "before_remove_staged_insert hook returned error", std::move(cb));
          }
          core::operations::mutate_in_request req{ id };
          req.specs =
            couchbase::mutate_in_specs{
                couchbase::mutate_in_specs::remove("txn").xattr(),
            }
              .specs();
          wrap_durable_request(req, overall_.config());
          req.access_deleted = true;

          return overall_.cluster_ref().execute(
            req,
            [this, id = std::move(id), cb = std::move(cb), error_handler = std::move(error_handler)](
              core::operations::mutate_in_response resp) mutable {
                auto ec = error_class_from_response(resp);
                if (ec) {
                    CB_ATTEMPT_CTX_LOG_DEBUG(this, "remove_staged_insert got error {}", *ec);
                    return error_handler(*ec, resp.ctx.ec().message(), std::move(cb));
                }
                return hooks_.after_remove_staged_insert(
                  this,
                  id.key(),
                  [this, id = std::move(id), cb = std::move(cb), error_handler = std::move(error_handler)](auto ec) mutable {
                      if (ec) {
                          return error_handler(*ec, "after_remove_staged_insert hook returned error", std::move(cb));
                      }
                      staged_mutations_->remove_any(id);
                      return op_completed_with_callback(std::move(cb));
                  });
            });
      });
}

void
attempt_context_impl::remove(const transaction_get_result& document)
{
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    remove(document, [barrier](std::exception_ptr err) {
        if (err) {
            return barrier->set_exception(err);
        }
        barrier->set_value();
    });
    f.get();
}

static core::operations::query_request
wrap_query_request(const couchbase::transactions::transaction_query_options& opts, const transaction_context& txn_context)
{
    // build what we can directly from the options:
    auto req = core::impl::build_transaction_query_request(opts.get_query_options().build());
    // set timeout to remaining time plus some extra time, so we don't time out right at expiry.
    auto extra = core::timeout_defaults::key_value_durable_timeout;
    if (!req.scan_consistency) {
        req.scan_consistency = txn_context.config().query_config.scan_consistency;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(txn_context.remaining());
    req.timeout = remaining + extra + std::chrono::milliseconds(1000); // match java with 1 second over the kv durable timeout.
    req.raw["txtimeout"] = fmt::format("\"{}ms\"", remaining.count());
    req.timeout = std::chrono::duration_cast<std::chrono::milliseconds>(txn_context.remaining()) + extra;
    return req;
}

void
attempt_context_impl::query_begin_work(std::optional<std::string> query_context, std::function<void(std::exception_ptr)>&& cb)
{
    // construct the txn_data and query options for the existing transaction
    couchbase::transactions::transaction_query_options opts;
    tao::json::value txdata;
    txdata["id"] = tao::json::empty_object;
    txdata["id"]["atmpt"] = id();
    txdata["id"]["txn"] = transaction_id();
    txdata["state"] = tao::json::empty_object;
    txdata["state"]["timeLeftMs"] = overall_.remaining().count() / 1000000;
    txdata["config"] = tao::json::empty_object;
    auto [ec, origin] = overall_.cluster_ref().origin();
    txdata["config"]["kvTimeoutMs"] =
      (ec) ? core::timeout_defaults::key_value_durable_timeout.count() : origin.options().key_value_durable_timeout.count();
    txdata["config"]["numAtrs"] = 1024;
    opts.raw("numatrs", jsonify(1024));
    txdata["config"]["durabilityLevel"] = durability_level_to_string(overall_.config().level);
    opts.raw("durability_level", durability_level_to_string_for_query(overall_.config().level));
    if (atr_id_) {
        txdata["atr"] = tao::json::empty_object;
        txdata["atr"]["scp"] = atr_id_->scope();
        txdata["atr"]["coll"] = atr_id_->collection();
        txdata["atr"]["bkt"] = atr_id_->bucket();
        txdata["atr"]["id"] = atr_id_->key();
    } else if (overall_.config().metadata_collection) {
        auto id = atr_id_from_bucket_and_key(overall_.config(), "", "");
        txdata["atr"] = tao::json::empty_object;
        txdata["atr"]["scp"] = id.scope();
        txdata["atr"]["coll"] = id.collection();
        txdata["atr"]["bkt"] = id.bucket();
        opts.raw("atrcollection", fmt::format("\"`{}`.`{}`.`{}`\"", id.bucket(), id.scope(), id.collection()));
    }
    tao::json::value mutations = tao::json::empty_array;
    if (!staged_mutations_->empty()) {
        staged_mutations_->iterate([&mutations](staged_mutation& mut) {
            mutations.push_back(tao::json::value{
              { "scp", mut.doc().id().scope() },
              { "coll", mut.doc().id().collection() },
              { "bkt", mut.doc().id().bucket() },
              { "id", mut.doc().id().key() },
              { "cas", std::to_string(mut.doc().cas().value()) },
              { "type", mut.type_as_string() },
            });
        });
    }
    txdata["mutations"] = mutations;
    std::vector<core::json_string> params;
    CB_ATTEMPT_CTX_LOG_TRACE(this, "begin_work using txdata: {}", core::utils::json::generate(txdata));
    wrap_query(BEGIN_WORK,
               opts,
               params,
               txdata,
               STAGE_QUERY_BEGIN_WORK,
               false,
               query_context,
               [this, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response resp) mutable {
                   if (resp.served_by_node.empty()) {
                       CB_ATTEMPT_CTX_LOG_TRACE(this, "begin_work didn't reach a query node, resetting mode to kv");
                       op_list_.reset_query_mode();
                   } else {
                       CB_ATTEMPT_CTX_LOG_TRACE(this, "begin_work setting query node to {}", resp.served_by_node);
                       op_list_.set_query_node(resp.served_by_node);
                   }
                   // we check for expiry _after_ this call, so we always set the query node if we can.
                   if (has_expired_client_side(STAGE_QUERY_BEGIN_WORK, {})) {
                       return cb(std::make_exception_ptr(
                         transaction_operation_failed(FAIL_EXPIRY, "expired in BEGIN WORK").no_rollback().expired()));
                   }
                   return cb(err);
               });
}

tao::json::value
choose_error(std::vector<tao::json::value>& errors)
{
    auto chosen_error = errors.front();
    if (errors.size() > 1) {
        // if there's one with a "reason":{"cause", ...} field, choose it
        for (const auto& e : errors) {
            auto reason = e.find("reason");
            auto cause = e.find("cause");
            if (reason && !reason->is_null() && cause && !cause->is_null()) {
                return e;
            }
        }
        // ok, so now lets see if we have one with code in the range 17000-18000 and return that.
        for (const auto& e : errors) {
            auto code = e.at("code").as<uint64_t>();
            if (code >= 17000 && code <= 18000) {
                return e;
            }
        }
    }
    // then, just the first one.
    return chosen_error;
}

std::exception_ptr
attempt_context_impl::handle_query_error(const core::operations::query_response& resp)
{
    if (!resp.ctx.ec && !resp.meta.errors) {
        return {};
    }
    auto [tx_err, query_result] = couchbase::core::impl::build_transaction_query_result(resp);
    // TODO: look at ambiguous and unambiguous timeout errors vs the codes, etc...
    CB_ATTEMPT_CTX_LOG_TRACE(
      this, "handling query error {}, {} errors in meta_data", resp.ctx.ec.message(), resp.meta.errors ? "has" : "no");
    if (resp.ctx.ec == couchbase::errc::common::ambiguous_timeout || resp.ctx.ec == couchbase::errc::common::unambiguous_timeout) {
        return std::make_exception_ptr(query_attempt_expired(tx_err));
    }
    if (resp.ctx.ec == couchbase::errc::common::parsing_failure) {
        return std::make_exception_ptr(query_parsing_failure(tx_err));
    }
    if (!resp.meta.errors) {
        // can't choose an error, map using the ec...
        external_exception cause =
          (resp.ctx.ec == couchbase::errc::common::service_not_available ? SERVICE_NOT_AVAILABLE_EXCEPTION : COUCHBASE_EXCEPTION);
        return std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, resp.ctx.ec.message()).cause(cause));
    }
    // TODO: we should have all the fields in these transactional query errors in the errors object.  For now, lets
    //   parse the body, get the serialized info to decide which error to choose.
    auto errors = core::utils::json::parse(resp.ctx.http_body)["errors"].get_array();

    // just chose first one, to start with...
    auto chosen_error = choose_error(errors);
    CB_ATTEMPT_CTX_LOG_TRACE(this, "chosen query error: {}", jsonify(chosen_error));
    auto code = chosen_error.at("code").as<uint64_t>();

    // we have a fixed strategy for these errors...
    switch (code) {
        case 1065:
            return std::make_exception_ptr(
              transaction_operation_failed(FAIL_OTHER, "N1QL Queries in transactions are supported in couchbase server 7.0 and later")
                .cause(FEATURE_NOT_AVAILABLE_EXCEPTION));
        case 1197:
            return std::make_exception_ptr(
              transaction_operation_failed(FAIL_OTHER, "This couchbase server requires all queries use a scope.")
                .cause(FEATURE_NOT_AVAILABLE_EXCEPTION));
        case 17004:
            return std::make_exception_ptr(query_attempt_not_found(tx_err));
        case 1080:
        case 17010:
            return std::make_exception_ptr(transaction_operation_failed(FAIL_EXPIRY, "transaction expired").expired());
        case 17012:
            return std::make_exception_ptr(document_exists(tx_err));
        case 17014:
            return std::make_exception_ptr(document_not_found(tx_err));
        case 17015:
            return std::make_exception_ptr(query_cas_mismatch(tx_err));
    }

    // For these errors, we will create a transaction_operation_failed from the info in it.
    if (code >= 17000 && code <= 18000) {
        // the assumption below is there's always a top-level msg.
        // TODO: when we parse the errors more thoroughly in the client, we should be able to add a lot of info on the underlying
        //   cause of the error here (in addition to perhaps a more granular message).
        transaction_operation_failed err(FAIL_OTHER, chosen_error.at("msg").as<std::string>());
        // parse the body for now, get the serialized info to create a transaction_operation_failed:
        if (const auto* cause = chosen_error.find("cause"); cause != nullptr) {
            if (cause->find("retry")->get_boolean()) {
                err.retry();
            }
            if (!cause->find("rollback")->get_boolean()) {
                err.no_rollback();
            }
            if (auto raise = cause->find("raise")->get_string(); raise == std::string("expired")) {
                err.expired();
            } else if (raise == std::string("commit_ambiguous")) {
                err.ambiguous();
            } else if (raise == std::string("failed_post_commit")) {
                err.failed_post_commit();
            } else if (raise != std::string("failed")) {
                CB_ATTEMPT_CTX_LOG_TRACE(this, "unknown value in raise field: {}, raising failed", raise);
            }
            return std::make_exception_ptr(err);
        }
    }

    return { std::make_exception_ptr(op_exception(tx_err)) };
}

void
attempt_context_impl::do_query(const std::string& statement,
                               const couchbase::transactions::transaction_query_options& opts,
                               std::optional<std::string> query_context,
                               QueryCallback&& cb)
{
    std::vector<core::json_string> params;
    tao::json::value txdata;
    CB_ATTEMPT_CTX_LOG_TRACE(this, "do_query called with statement {}", statement);
    wrap_query(statement,
               opts,
               params,
               txdata,
               STAGE_QUERY,
               true,
               query_context,
               [this, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response resp) mutable {
                   if (err) {
                       return op_completed_with_error(std::move(cb), err);
                   }
                   op_completed_with_callback(std::move(cb), std::optional<core::operations::query_response>(resp));
               });
}
std::string
dump_request(const core::operations::query_request& req)
{
    std::string raw = "{";
    for (const auto& x : req.raw) {
        raw += x.first;
        raw += ":";
        raw += x.second.str();
        raw += ",";
    }
    raw += "}";
    std::string params;
    for (const auto& x : req.positional_parameters) {
        params.append(x.str());
    }
    return fmt::format("request: {}, {}, {}", req.statement, params, raw);
}
void
attempt_context_impl::wrap_query(const std::string& statement,
                                 const couchbase::transactions::transaction_query_options& opts,
                                 const std::vector<core::json_string>& params,
                                 const tao::json::value& txdata,
                                 const std::string& hook_point,
                                 bool check_expiry,
                                 std::optional<std::string> query_context,
                                 std::function<void(std::exception_ptr, core::operations::query_response)>&& cb)
{
    auto req = wrap_query_request(opts, overall_);
    if (statement != BEGIN_WORK) {
        auto mode = op_list_.get_mode();
        assert(mode.is_query());
        if (!op_list_.get_mode().query_node.empty()) {
            req.send_to_node = op_list_.get_mode().query_node;
        }
    }
    // set the query_context, if one has been set, unless this query already has one
    if (!query_context && !query_context_.empty()) {
        req.query_context = query_context_;
    } else if (query_context) {
        req.query_context = query_context;
    }

    if (check_expiry) {
        if (has_expired_client_side(hook_point, std::nullopt)) {
            auto err = std::make_exception_ptr(
              transaction_operation_failed(FAIL_EXPIRY, fmt::format("{} expired in stage {}", statement, hook_point))
                .no_rollback()
                .expired());
            return cb(err, {});
        }
    }

    if (!params.empty()) {
        req.positional_parameters = params;
    }
    if (statement != BEGIN_WORK) {
        req.raw["txid"] = jsonify(id());
    }
    if (txdata.is_object() && !txdata.get_object().empty()) {
        req.raw["txdata"] = core::utils::json::generate(txdata);
    }
    req.statement = statement;
    return hooks_.before_query(this, statement, [this, statement, req, cb = std::move(cb)](auto ec) mutable {
        if (ec) {
            auto err = std::make_exception_ptr(transaction_operation_failed(*ec, "before_query hook raised error"));
            if (statement == BEGIN_WORK) {
                return cb(std::make_exception_ptr(transaction_operation_failed(*ec, "before_query hook raised error").no_rollback()), {});
            }
            return cb(std::make_exception_ptr(transaction_operation_failed(*ec, "before_query hook raised error")), {});
        }

        CB_ATTEMPT_CTX_LOG_TRACE(this, "http request: {}", dump_request(req));
        return overall_.cluster_ref().execute(req, [this, cb = std::move(cb)](core::operations::query_response resp) mutable {
            CB_ATTEMPT_CTX_LOG_TRACE(this, "response: {} status: {}", resp.ctx.http_body, resp.meta.status);
            return hooks_.after_query(this, resp.ctx.statement, [this, resp = std::move(resp), cb = std::move(cb)](auto ec) mutable {
                if (ec) {
                    auto err = std::make_exception_ptr(transaction_operation_failed(*ec, "after_query hook raised error"));
                    return cb(err, {});
                }
                return cb(handle_query_error(resp), resp);
            });
        });
    });
}

void
attempt_context_impl::query(const std::string& statement,
                            const couchbase::transactions::transaction_query_options& options,
                            std::optional<std::string> query_context,
                            QueryCallback&& cb)
{
    return cache_error_async(cb, [&]() {
        check_if_done(cb);
        // decrement in_flight, as we just incremented it in cache_error_async.
        op_list_.set_query_mode(
          [this, statement, options, query_context, cb]() mutable {
              // set query context if set
              if (query_context) {
                  query_context_ = query_context.value();
              }
              query_begin_work(query_context,
                               [this, statement, query_context, options, cb = std::move(cb)](std::exception_ptr err) mutable {
                                   if (err) {
                                       return op_completed_with_error(std::move(cb), err);
                                   }
                                   return do_query(statement, options, query_context, std::move(cb));
                               });
          },
          [this, statement, options, query_context, cb]() mutable { return do_query(statement, options, query_context, std::move(cb)); });
    });
}

core::operations::query_response
attempt_context_impl::do_core_query(const std::string& statement,
                                    const couchbase::transactions::transaction_query_options& options,
                                    std::optional<std::string> query_context)
{
    auto barrier = std::make_shared<std::promise<core::operations::query_response>>();
    auto f = barrier->get_future();
    query(statement, options, query_context, [barrier](std::exception_ptr err, std::optional<core::operations::query_response> resp) {
        if (err) {
            return barrier->set_exception(err);
        }
        barrier->set_value(*resp);
    });
    return f.get();
}

std::pair<couchbase::transaction_op_error_context, couchbase::transactions::transaction_query_result>
attempt_context_impl::do_public_query(const std::string& statement,
                                      const couchbase::transactions::transaction_query_options& opts,
                                      std::optional<std::string> query_context)
{
    try {
        auto result = do_core_query(statement, opts, query_context);
        return core::impl::build_transaction_query_result(result);
    } catch (const transaction_operation_failed& e) {
        return { e.get_error_ctx(), {} };
    } catch (const op_exception& qe) {
        return { qe.ctx(), {} };
    } catch (...) {
        // should not be necessary, but just in case...
        transaction_op_error_context ctx(couchbase::errc::transaction_op::unknown);
        return { ctx, {} };
    }
}

std::vector<core::json_string>
make_params(const core::document_id& id, std::optional<std::vector<std::byte>> content)
{
    std::vector<core::json_string> retval;
    auto keyspace = fmt::format("default:`{}`.`{}`.`{}`", id.bucket(), id.scope(), id.collection());
    retval.push_back(jsonify(keyspace));
    if (!id.key().empty()) {
        retval.push_back(jsonify(id.key()));
    }
    if (content) {
        retval.push_back(std::string(reinterpret_cast<const char*>(content->data()), content->size()));
        retval.push_back(core::utils::json::generate(tao::json::empty_object));
    }
    return retval;
}

tao::json::value
make_kv_txdata(std::optional<transaction_get_result> doc = std::nullopt)
{
    tao::json::value retval{ { "kv", true } };
    if (doc) {
        retval["scas"] = fmt::format("{}", doc->cas().value());
        doc->links().append_to_json(retval);
    }
    return retval;
}

void
attempt_context_impl::get_with_query(const core::document_id& id, bool optional, Callback&& cb)
{
    cache_error_async(cb, [&]() {
        auto params = make_params(id, {});
        couchbase::transactions::transaction_query_options opts;
        opts.readonly(true);
        return wrap_query(KV_GET,
                          opts,
                          params,
                          make_kv_txdata(),
                          STAGE_QUERY_KV_GET,
                          true,
                          {},
                          [this, id, optional, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response resp) mutable {
                              if (resp.ctx.ec == couchbase::errc::key_value::document_not_found) {
                                  return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>());
                              }
                              if (!err) {
                                  // make a transaction_get_result from the row...
                                  try {
                                      if (resp.rows.empty()) {
                                          if (optional) {
                                              return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>());
                                          }
                                          return op_completed_with_error(
                                            std::move(cb), transaction_operation_failed(FAIL_DOC_NOT_FOUND, "document not found"));
                                      }
                                      CB_ATTEMPT_CTX_LOG_TRACE(this, "get_with_query got: {}", resp.rows.front());
                                      transaction_get_result doc(id, core::utils::json::parse(resp.rows.front()));
                                      return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>(doc));
                                  } catch (const std::exception& e) {
                                      // TODO: unsure what to do here, but this is pretty fatal, so
                                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
                                  }
                              }
                              // for get_optional.   <sigh>
                              if (optional) {
                                  try {
                                      std::rethrow_exception(err);
                                  } catch (const document_not_found&) {
                                      return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>());
                                  } catch (...) {
                                      return op_completed_with_error(std::move(cb), std::current_exception());
                                  }
                              }
                              return op_completed_with_error(std::move(cb), err);
                          });
    });
}

void
attempt_context_impl::insert_raw_with_query(const core::document_id& id, const std::vector<std::byte>& content, Callback&& cb)
{
    cache_error_async(cb, [&]() {
        std::vector<std::byte> content_copy = content;
        auto params = make_params(id, std::move(content_copy));
        couchbase::transactions::transaction_query_options opts;
        return wrap_query(KV_INSERT,
                          opts,
                          params,
                          make_kv_txdata(),
                          STAGE_QUERY_KV_INSERT,
                          true,
                          {},
                          [this, id, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response resp) mutable {
                              if (err) {
                                  try {
                                      std::rethrow_exception(err);
                                  } catch (const transaction_operation_failed&) {
                                      return op_completed_with_error(std::move(cb), err);

                                  } catch (const document_exists& ex) {
                                      return op_completed_with_error(std::move(cb), ex);
                                  } catch (const std::exception& e) {
                                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
                                  } catch (...) {
                                      return op_completed_with_error(std::move(cb),
                                                                     transaction_operation_failed(FAIL_OTHER, "unexpected error"));
                                  }
                              }
                              // make a transaction_get_result from the row...
                              try {
                                  CB_ATTEMPT_CTX_LOG_TRACE(this, "insert_raw_with_query got: {}", resp.rows.front());
                                  transaction_get_result doc(id, core::utils::json::parse(resp.rows.front()));
                                  return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>(doc));
                              } catch (const std::exception& e) {
                                  // TODO: unsure what to do here, but this is pretty fatal, so
                                  return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
                              }
                          });
    });
}

void
attempt_context_impl::replace_raw_with_query(const transaction_get_result& document, const std::vector<std::byte>& content, Callback&& cb)
{
    cache_error_async(cb, [&]() {
        std::vector<std::byte> content_copy = content;
        auto params = make_params(document.id(), std::move(content_copy));
        couchbase::transactions::transaction_query_options opts;
        return wrap_query(
          KV_REPLACE,
          opts,
          params,
          make_kv_txdata(document),
          STAGE_QUERY_KV_REPLACE,
          true,
          {},
          [this, id = document.id(), cb = std::move(cb)](std::exception_ptr err, core::operations::query_response resp) mutable {
              if (err) {
                  try {
                      std::rethrow_exception(err);
                  } catch (const query_cas_mismatch& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_CAS_MISMATCH, e.what()).retry());
                  } catch (const document_not_found& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_DOC_NOT_FOUND, e.what()).retry());
                  } catch (const transaction_operation_failed& e) {
                      return op_completed_with_error(std::move(cb), e);
                  } catch (std::exception& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
                  } catch (...) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, "unexpected exception"));
                  }
              }
              // make a transaction_get_result from the row...
              try {
                  CB_ATTEMPT_CTX_LOG_TRACE(this, "replace_raw_with_query got: {}", resp.rows.front());
                  transaction_get_result doc(id, core::utils::json::parse(resp.rows.front()));
                  return op_completed_with_callback(std::move(cb), std::optional<transaction_get_result>(doc));
              } catch (const std::exception& e) {
                  // TODO: unsure what to do here, but this is pretty fatal, so
                  return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
              }
          });
    });
}

void
attempt_context_impl::remove_with_query(const transaction_get_result& document, VoidCallback&& cb)
{
    cache_error_async(cb, [&]() {
        auto params = make_params(document.id(), {});
        couchbase::transactions::transaction_query_options opts;
        return wrap_query(
          KV_REMOVE,
          opts,
          params,
          make_kv_txdata(document),
          STAGE_QUERY_KV_REMOVE,
          true,
          {},
          [this, id = document.id(), cb = std::move(cb)](std::exception_ptr err, core::operations::query_response /* resp */) mutable {
              if (err) {
                  try {
                      std::rethrow_exception(err);
                  } catch (const transaction_operation_failed& e) {
                      return op_completed_with_error(std::move(cb), e);
                  } catch (const document_not_found& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_DOC_NOT_FOUND, e.what()).retry());
                  } catch (const query_cas_mismatch& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_CAS_MISMATCH, e.what()).retry());
                  } catch (const std::exception& e) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, e.what()));
                  } catch (...) {
                      return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_OTHER, "unexpected exception"));
                  }
              }
              // make a transaction_get_result from the row...
              return op_completed_with_callback(std::move(cb));
          });
    });
}

void
attempt_context_impl::commit_with_query(VoidCallback&& cb)
{
    core::operations::query_request req;
    CB_ATTEMPT_CTX_LOG_TRACE(this, "commit_with_query called");
    couchbase::transactions::transaction_query_options opts;
    std::vector<core::json_string> params;
    wrap_query(
      COMMIT,
      opts,
      params,
      make_kv_txdata(std::nullopt),
      STAGE_QUERY_COMMIT,
      true,
      {},
      [this, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response /* resp */) mutable {
          is_done_ = true;
          if (err) {
              try {
                  std::rethrow_exception(err);
              } catch (const transaction_operation_failed&) {
                  return cb(std::current_exception());
              } catch (const query_attempt_expired& e) {
                  return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_EXPIRY, e.what()).ambiguous().no_rollback()));
              } catch (const document_not_found& e) {
                  return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_DOC_NOT_FOUND, e.what()).no_rollback()));
              } catch (const document_exists& e) {
                  return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, e.what()).no_rollback()));
              } catch (const query_cas_mismatch& e) {
                  return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_CAS_MISMATCH, e.what()).no_rollback()));
              } catch (const std::exception& e) {
                  return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, e.what()).no_rollback()));
              }
          }
          state(attempt_state::COMPLETED);
          return cb({});
      });
}
void
attempt_context_impl::rollback_with_query(VoidCallback&& cb)
{
    core::operations::query_request req;
    CB_ATTEMPT_CTX_LOG_TRACE(this, "rollback_with_query called");
    couchbase::transactions::transaction_query_options opts;
    std::vector<core::json_string> params;
    wrap_query(ROLLBACK,
               opts,
               params,
               make_kv_txdata(std::nullopt),
               STAGE_QUERY_ROLLBACK,
               true,
               {},
               [this, cb = std::move(cb)](std::exception_ptr err, core::operations::query_response /* resp */) mutable {
                   is_done_ = true;
                   if (err) {
                       try {
                           std::rethrow_exception(err);
                       } catch (const transaction_operation_failed&) {
                           return cb(std::current_exception());
                       } catch (const query_attempt_not_found& e) {
                           CB_ATTEMPT_CTX_LOG_DEBUG(
                             this, "got query_attempt_not_found, assuming query was already rolled back successfullly: {}", e.what());
                       } catch (const std::exception& e) {
                           return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, e.what()).no_rollback()));
                       }
                   }
                   state(attempt_state::ROLLED_BACK);
                   CB_ATTEMPT_CTX_LOG_TRACE(this, "rollback successful");
                   return cb({});
               });
}
void
attempt_context_impl::atr_commit(bool ambiguity_resolution_mode)
{
    retry_op<void>([this, &ambiguity_resolution_mode]() {
        try {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            core::operations::mutate_in_request req{ atr_id_.value() };
            req.specs =
              couchbase::mutate_in_specs{
                  couchbase::mutate_in_specs::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMMITTED)).xattr(),
                  couchbase::mutate_in_specs::upsert(prefix + ATR_FIELD_START_COMMIT, subdoc::mutate_in_macro::cas).xattr(),
                  couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_PREVENT_COLLLISION, 0).xattr(),
              }
                .specs();
            wrap_durable_request(req, overall_.config());
            auto ec = error_if_expired_and_not_in_overtime(STAGE_ATR_COMMIT, {});
            if (ec) {
                throw client_error(*ec, "atr_commit check for expiry threw error");
            }
            ec = wait_for_hook([this](auto handler) mutable { return hooks_.before_atr_commit(this, std::move(handler)); });
            if (ec) {
                // for now, throw.  Later, if this is async, we will use error handler no doubt.
                throw client_error(*ec, "before_atr_commit hook raised error");
            }
            staged_mutations_->extract_to(prefix, req);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            CB_ATTEMPT_CTX_LOG_TRACE(this, "updating atr {}, setting to {}", req.id, attempt_state_name(attempt_state::COMMITTED));
            overall_.cluster_ref().execute(
              req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
            auto res = wrap_operation_future(f, false);
            ec = wait_for_hook([this](auto handler) mutable { return hooks_.after_atr_commit(this, std::move(handler)); });
            if (ec) {
                throw client_error(*ec, "after_atr_commit hook raised error");
            }
            state(attempt_state::COMMITTED);
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_EXPIRY: {
                    expiry_overtime_mode_ = true;
                    auto out = transaction_operation_failed(ec, e.what()).no_rollback();
                    if (ambiguity_resolution_mode) {
                        out.ambiguous();
                    } else {
                        out.expired();
                    }
                    throw out;
                }
                case FAIL_AMBIGUOUS:
                    CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_commit got FAIL_AMBIGUOUS, resolving ambiguity...");
                    ambiguity_resolution_mode = true;
                    throw retry_operation(e.what());
                case FAIL_TRANSIENT:
                    if (ambiguity_resolution_mode) {
                        throw retry_operation(e.what());
                    }
                    throw transaction_operation_failed(ec, e.what()).retry();

                case FAIL_PATH_ALREADY_EXISTS:
                    // Need retry_op as atr_commit_ambiguity_resolution can throw retry_operation
                    return retry_op<void>([&]() { return atr_commit_ambiguity_resolution(); });
                case FAIL_HARD: {
                    auto out = transaction_operation_failed(ec, e.what()).no_rollback();
                    if (ambiguity_resolution_mode) {
                        out.ambiguous();
                    }
                    throw out;
                }
                case FAIL_DOC_NOT_FOUND: {
                    auto out = transaction_operation_failed(ec, e.what())
                                 .cause(external_exception::ACTIVE_TRANSACTION_RECORD_NOT_FOUND)
                                 .no_rollback();
                    if (ambiguity_resolution_mode) {
                        out.ambiguous();
                    }
                    throw out;
                }
                case FAIL_PATH_NOT_FOUND: {
                    auto out = transaction_operation_failed(ec, e.what())
                                 .cause(external_exception::ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND)
                                 .no_rollback();
                    if (ambiguity_resolution_mode) {
                        out.ambiguous();
                    }
                    throw out;
                }
                case FAIL_ATR_FULL: {
                    auto out =
                      transaction_operation_failed(ec, e.what()).cause(external_exception::ACTIVE_TRANSACTION_RECORD_FULL).no_rollback();
                    if (ambiguity_resolution_mode) {
                        out.ambiguous();
                    }
                    throw out;
                }
                default: {
                    CB_ATTEMPT_CTX_LOG_ERROR(this,
                                             "failed to commit transaction {}, attempt {}, ambiguity_resolution_mode {}, with error {}",
                                             transaction_id(),
                                             id(),
                                             ambiguity_resolution_mode,
                                             e.what());
                    auto out = transaction_operation_failed(ec, e.what());
                    if (ambiguity_resolution_mode) {
                        out.no_rollback().ambiguous();
                    }
                    throw out;
                }
            }
        }
    });
}

void
attempt_context_impl::atr_commit_ambiguity_resolution()
{
    try {
        auto ec = error_if_expired_and_not_in_overtime(STAGE_ATR_COMMIT_AMBIGUITY_RESOLUTION, {});
        if (ec) {
            throw client_error(*ec, "atr_commit_ambiguity_resolution raised error");
        }
        ec =
          wait_for_hook([this](auto handler) mutable { return hooks_.before_atr_commit_ambiguity_resolution(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "before_atr_commit_ambiguity_resolution hook threw error");
        }
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
        core::operations::lookup_in_request req{ atr_id_.value() };
        req.specs = lookup_in_specs{ lookup_in_specs::get(prefix + ATR_FIELD_STATUS).xattr() }.specs();
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        overall_.cluster_ref().execute(
          req, [barrier](core::operations::lookup_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        auto res = wrap_operation_future(f);
        auto atr_status_raw = res.values[0].content_as<std::string>();
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_commit_ambiguity_resolution read atr state {}", atr_status_raw);
        auto atr_status = attempt_state_value(atr_status_raw);
        switch (atr_status) {
            case attempt_state::COMMITTED:
                return;
            case attempt_state::ABORTED:
                // aborted by another process?
                throw transaction_operation_failed(FAIL_OTHER, "transaction aborted externally").retry();
            default:
                throw transaction_operation_failed(FAIL_OTHER, "unexpected state found on ATR ambiguity resolution")
                  .cause(ILLEGAL_STATE_EXCEPTION)
                  .no_rollback();
        }
    } catch (const client_error& e) {
        error_class ec = e.ec();
        switch (ec) {
            case FAIL_EXPIRY:
                throw transaction_operation_failed(ec, e.what()).no_rollback().ambiguous();
            case FAIL_HARD:
                throw transaction_operation_failed(ec, e.what()).no_rollback().ambiguous();
            case FAIL_TRANSIENT:
            case FAIL_OTHER:
                throw retry_operation(e.what());
            case FAIL_PATH_NOT_FOUND:
                throw transaction_operation_failed(ec, e.what()).cause(ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND).no_rollback().ambiguous();
            case FAIL_DOC_NOT_FOUND:
                throw transaction_operation_failed(ec, e.what()).cause(ACTIVE_TRANSACTION_RECORD_NOT_FOUND).no_rollback().ambiguous();
            default:
                throw transaction_operation_failed(ec, e.what()).no_rollback().ambiguous();
        }
    }
}

void
attempt_context_impl::atr_complete()
{
    try {
        result atr_res;
        auto ec = wait_for_hook([this](auto handler) mutable { return hooks_.before_atr_complete(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "before_atr_complete hook threw error");
        }
        // if we have expired (and not in overtime mode), just raise the final error.
        if (!!(ec = error_if_expired_and_not_in_overtime(STAGE_ATR_COMPLETE, {}))) {
            throw client_error(*ec, "atr_complete threw error");
        }
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "removing attempt {} from atr", atr_id_.value());
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id());
        core::operations::mutate_in_request req{ atr_id_.value() };
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::remove(prefix).xattr(),
          }
            .specs();
        wrap_durable_request(req, overall_.config());
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        overall_.cluster_ref().execute(
          req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        wrap_operation_future(f);
        ec = wait_for_hook([this](auto handler) mutable { return hooks_.after_atr_complete(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "after_atr_complete hook threw error");
        }
        state(attempt_state::COMPLETED);
    } catch (const client_error& er) {
        error_class ec = er.ec();
        switch (ec) {
            case FAIL_HARD:
                throw transaction_operation_failed(ec, er.what()).no_rollback().failed_post_commit();
            default:
                CB_ATTEMPT_CTX_LOG_INFO(this, "ignoring error in atr_complete {}", er.what());
        }
    }
}
void
attempt_context_impl::commit(VoidCallback&& cb)
{
    // for now, lets keep the blocking implementation
    std::thread([cb = std::move(cb), this]() mutable {
        try {
            commit();
            return cb({});
        } catch (const transaction_operation_failed&) {
            return cb(std::current_exception());
        } catch (const std::exception& e) {
            return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, e.what())));
        }
    }).detach();
}

void
attempt_context_impl::commit()
{
    CB_ATTEMPT_CTX_LOG_DEBUG(this, "waiting on ops to finish...");
    op_list_.wait_and_block_ops();
    existing_error(false);
    CB_ATTEMPT_CTX_LOG_DEBUG(this, "commit {}", id());
    if (op_list_.get_mode().is_query()) {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        commit_with_query([barrier](std::exception_ptr err) {
            if (err) {
                barrier->set_exception(err);
            } else {
                barrier->set_value();
            }
        });
        f.get();
    } else {
        if (check_expiry_pre_commit(STAGE_BEFORE_COMMIT, {})) {
            throw transaction_operation_failed(FAIL_EXPIRY, "transaction expired").expired();
        }
        if (atr_id_ && !atr_id_->key().empty() && !is_done_) {
            retry_op_exp<void>([&]() { atr_commit(false); });
            staged_mutations_->commit(this);
            atr_complete();
            is_done_ = true;
        } else {
            // no mutation, no need to commit
            if (!is_done_) {
                CB_ATTEMPT_CTX_LOG_DEBUG(this, "calling commit on attempt that has got no mutations, skipping");
                is_done_ = true;
                return;
            } // do not rollback or retry
            throw transaction_operation_failed(FAIL_OTHER, "calling commit on attempt that is already completed").no_rollback();
        }
    }
}

void
attempt_context_impl::atr_abort()
{
    try {
        auto ec = error_if_expired_and_not_in_overtime(STAGE_ATR_ABORT, {});
        if (ec) {
            throw client_error(*ec, "atr_abort check for expiry threw error");
        }
        ec = wait_for_hook([this](auto handler) mutable { return hooks_.before_atr_aborted(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "before_atr_aborted hook threw error");
        }
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
        core::operations::mutate_in_request req{ atr_id_.value() };
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::ABORTED))
                .xattr()
                .create_path(),
              couchbase::mutate_in_specs::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_START, subdoc::mutate_in_macro::cas)
                .xattr()
                .create_path(),
          }
            .specs();
        staged_mutations_->extract_to(prefix, req);
        wrap_durable_request(req, overall_.config());
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        overall_.cluster_ref().execute(
          req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        wrap_operation_future(f);
        state(attempt_state::ABORTED);

        ec = wait_for_hook([this](auto handler) mutable { return hooks_.after_atr_aborted(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "after_atr_aborted hook threw error");
        }
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "rollback completed atr abort phase");
    } catch (const client_error& e) {
        auto ec = e.ec();
        CB_ATTEMPT_CTX_LOG_TRACE(this, "atr_abort got {} {}", ec, e.what());
        if (expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_abort got error \"{}\" while in overtime mode", e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired in atr_abort with {} ") + e.what())
              .no_rollback()
              .expired();
        }
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_abort got error {}", ec);
        switch (ec) {
            case FAIL_EXPIRY:
                expiry_overtime_mode_ = true;
                throw retry_operation("expired, setting overtime mode and retry atr_abort");
            case FAIL_PATH_NOT_FOUND:
                throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND);
            case FAIL_DOC_NOT_FOUND:
                throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_NOT_FOUND);
            case FAIL_ATR_FULL:
                throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_FULL);
            case FAIL_HARD:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            default:
                throw retry_operation("retry atr_abort");
        }
    }
}

void
attempt_context_impl::atr_rollback_complete()
{
    try {
        auto ec = error_if_expired_and_not_in_overtime(STAGE_ATR_ROLLBACK_COMPLETE, std::nullopt);
        if (ec) {
            throw client_error(*ec, "atr_rollback_complete raised error");
        }

        ec = wait_for_hook([this](auto handler) mutable { return hooks_.before_atr_rolled_back(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "before_atr_rolled_back hook threw error");
        }
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id());
        core::operations::mutate_in_request req{ atr_id_.value() };
        req.specs =
          couchbase::mutate_in_specs{
              couchbase::mutate_in_specs::remove(prefix).xattr(),
          }
            .specs();
        wrap_durable_request(req, overall_.config());
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        overall_.cluster_ref().execute(
          req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        wrap_operation_future(f);
        state(attempt_state::ROLLED_BACK);
        ec = wait_for_hook([this](auto handler) mutable { return hooks_.after_atr_rolled_back(this, std::move(handler)); });
        if (ec) {
            throw client_error(*ec, "after_atr_rolled_back hook threw error");
        }
        is_done_ = true;

    } catch (const client_error& e) {
        auto ec = e.ec();
        if (expiry_overtime_mode_.load()) {
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_rollback_complete error while in overtime mode {}", e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired in atr_rollback_complete with {} ") + e.what())
              .no_rollback()
              .expired();
        }
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr_rollback_complete got error {}", ec);
        switch (ec) {
            case FAIL_DOC_NOT_FOUND:
            case FAIL_PATH_NOT_FOUND:
                CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr {} not found, ignoring", atr_id_->key());
                is_done_ = true;
                break;
            case FAIL_ATR_FULL:
                CB_ATTEMPT_CTX_LOG_DEBUG(this, "atr {} full!", atr_id_->key());
                throw retry_operation(e.what());
            case FAIL_HARD:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                CB_ATTEMPT_CTX_LOG_DEBUG(this, "timed out writing atr {}", atr_id_->key());
                throw transaction_operation_failed(ec, e.what()).no_rollback().expired();
            default:
                CB_ATTEMPT_CTX_LOG_DEBUG(this, "retrying atr_rollback_complete");
                throw retry_operation(e.what());
        }
    }
}
void
attempt_context_impl::rollback(VoidCallback&& cb)
{
    // for now, lets keep the blocking implementation
    std::thread([cb = std::move(cb), this]() mutable {
        if (op_list_.get_mode().is_query()) {
            return rollback_with_query(std::move(cb));
        }
        try {
            rollback();
            return cb({});
        } catch (const transaction_operation_failed&) {
            return cb(std::current_exception());
        } catch (const std::exception& e) {
            return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, e.what()).no_rollback()));
        } catch (...) {
            return cb(std::make_exception_ptr(transaction_operation_failed(FAIL_OTHER, "unexpected exception during rollback")));
        }
    }).detach();
}

void
attempt_context_impl::rollback()
{
    op_list_.wait_and_block_ops();
    CB_ATTEMPT_CTX_LOG_DEBUG(this, "rolling back {}", id());
    if (op_list_.get_mode().is_query()) {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        rollback_with_query([barrier](std::exception_ptr err) {
            if (err) {
                barrier->set_exception(err);
            } else {
                barrier->set_value();
            }
        });
        return f.get();
    }
    // check for expiry
    check_expiry_during_commit_or_rollback(STAGE_ROLLBACK, std::nullopt);
    if (!atr_id_ || atr_id_->key().empty() || state() == attempt_state::NOT_STARTED) {
        // TODO: check this, but if we try to rollback an empty txn, we should prevent a subsequent commit
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "rollback called on txn with no mutations");
        is_done_ = true;
        return;
    }
    if (is_done()) {
        std::string msg("Transaction already done, cannot rollback");
        CB_ATTEMPT_CTX_LOG_ERROR(this, "{}", msg);
        // need to raise a FAIL_OTHER which is not retryable or rollback-able
        throw transaction_operation_failed(FAIL_OTHER, msg).no_rollback();
    }
    try {
        // (1) atr_abort
        retry_op_exp<void>([&]() { atr_abort(); });
        // (2) rollback staged mutations
        staged_mutations_->rollback(this);
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "rollback completed unstaging docs");

        // (3) atr_rollback
        retry_op_exp<void>([&]() { atr_rollback_complete(); });
    } catch (const client_error& e) {
        error_class ec = e.ec();
        CB_ATTEMPT_CTX_LOG_ERROR(this, "rollback transaction {}, attempt {} fail with error {}", transaction_id(), id(), e.what());
        if (ec == FAIL_HARD) {
            throw transaction_operation_failed(ec, e.what()).no_rollback();
        }
    }
}

bool
attempt_context_impl::has_expired_client_side(std::string place, std::optional<const std::string> doc_id)
{
    bool over = overall_.has_expired_client_side();
    bool hook = hooks_.has_expired_client_side(this, place, doc_id);
    if (over) {
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "{} expired in {}", id(), place);
    }
    if (hook) {
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "{} fake expiry in {}", id(), place);
    }
    return over || hook;
}

bool
attempt_context_impl::check_expiry_pre_commit(std::string stage, std::optional<const std::string> doc_id)
{
    if (has_expired_client_side(stage, std::move(doc_id))) {
        CB_ATTEMPT_CTX_LOG_DEBUG(
          this, "{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", id(), stage);

        // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in an attempt to rollback, which will
        // ignore expiry, and bail out if anything fails
        expiry_overtime_mode_ = true;
        return true;
    }
    return false;
}

std::optional<error_class>
attempt_context_impl::error_if_expired_and_not_in_overtime(const std::string& stage, std::optional<const std::string> doc_id)
{
    if (expiry_overtime_mode_.load()) {
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "not doing expired check in {} as already in expiry-overtime", stage);
        return {};
    }
    if (has_expired_client_side(stage, std::move(doc_id))) {
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "expired in {}", stage);
        return FAIL_EXPIRY;
    }
    return {};
}

void
attempt_context_impl::check_expiry_during_commit_or_rollback(const std::string& stage, std::optional<const std::string> doc_id)
{
    // [EXP-COMMIT-OVERTIME]
    if (!expiry_overtime_mode_.load()) {
        if (has_expired_client_side(stage, std::move(doc_id))) {
            CB_ATTEMPT_CTX_LOG_DEBUG(
              this, "{} has expired in stage {}, entering expiry-overtime mode (one attempt to complete commit)", id(), stage);
            expiry_overtime_mode_ = true;
        }
    } else {
        CB_ATTEMPT_CTX_LOG_DEBUG(this, "{} ignoring expiry in stage {}  as in expiry-overtime mode", id(), stage);
    }
}
template<typename Handler>
void
attempt_context_impl::set_atr_pending_locked(const core::document_id& id, std::unique_lock<std::mutex>&& lock, Handler&& fn)
{
    try {
        if (staged_mutations_->empty()) {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + this->id() + ".");
            if (!atr_id_) {
                return fn(transaction_operation_failed(FAIL_OTHER, std::string("ATR ID is not initialized")));
            }
            if (auto ec = error_if_expired_and_not_in_overtime(STAGE_ATR_PENDING, {})) {
                return fn(transaction_operation_failed(*ec, "transaction expired setting ATR").expired());
            }
            auto error_handler =
              [this, &lock](error_class ec, const std::string& message, const core::document_id& doc_id, Handler&& fn) mutable {
                  transaction_operation_failed err(ec, message);
                  CB_ATTEMPT_CTX_LOG_TRACE(this, "got {} trying to set atr to pending", message);
                  if (expiry_overtime_mode_.load()) {
                      return fn(err.no_rollback().expired());
                  }
                  switch (ec) {
                      case FAIL_EXPIRY:
                          expiry_overtime_mode_ = true;
                          // this should trigger rollback (unlike the above when already in overtime mode)
                          return fn(err.expired());
                      case FAIL_ATR_FULL:
                          return fn(err);
                      case FAIL_PATH_ALREADY_EXISTS:
                          // assuming this got resolved, moving on as if ok
                          return fn(std::nullopt);
                      case FAIL_AMBIGUOUS:
                          // Retry just this
                          CB_ATTEMPT_CTX_LOG_DEBUG(this, "got FAIL_AMBIGUOUS, retrying set atr pending", ec);
                          return overall_.after_delay(std::chrono::milliseconds(1), [this, doc_id, &lock, fn = std::move(fn)]() {
                              set_atr_pending_locked(doc_id, std::move(lock), std::move(fn));
                          });
                      case FAIL_TRANSIENT:
                          // Retry txn
                          return fn(err.retry());
                      case FAIL_HARD:
                          return fn(err.no_rollback());
                      default:
                          return fn(err);
                  }
              };
            return hooks_.before_atr_pending(
              this, [this, id, prefix, fn = std::forward<Handler>(fn), error_handler = std::move(error_handler)](auto ec) mutable {
                  if (ec) {
                      return error_handler(*ec, "before_atr_pending hook raised error", id, std::forward<Handler>(fn));
                  }

                  CB_ATTEMPT_CTX_LOG_DEBUG(this, "updating atr {}", atr_id_.value());

                  std::chrono::nanoseconds remaining = overall_.remaining();
                  // This bounds the value to [0-timeout].  It should always be in this range, this is just to protect
                  // against the application clock changing.
                  long remaining_bounded_nanos =
                    std::max(std::min(remaining.count(), overall_.config().timeout.count()), static_cast<std::chrono::nanoseconds::rep>(0));
                  long remaining_bounded_msecs = remaining_bounded_nanos / 1'000'000;

                  core::operations::mutate_in_request req{ atr_id_.value() };

                  req.specs =
                    couchbase::mutate_in_specs{
                        couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_TRANSACTION_ID, overall_.transaction_id())
                          .xattr()
                          .create_path(),
                        couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING))
                          .xattr()
                          .create_path(),
                        couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_START_TIMESTAMP, subdoc::mutate_in_macro::cas)
                          .xattr()
                          .create_path(),
                        couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, remaining_bounded_msecs)
                          .xattr()
                          .create_path(),
                        // ExtStoreDurability
                        couchbase::mutate_in_specs::insert(prefix + ATR_FIELD_DURABILITY_LEVEL,
                                                           store_durability_level_to_string(overall_.config().level))
                          .xattr()
                          .create_path(),
                        // subdoc::opcode::set_doc used in replace w/ empty path
                        // ExtBinaryMetadata
                        couchbase::mutate_in_specs::replace_raw({}, std::vector<std::byte>{ std::byte{ 0x00 } }),
                    }
                      .specs();
                  req.store_semantics = couchbase::store_semantics::upsert;

                  wrap_durable_request(req, overall_.config());
                  return overall_.cluster_ref().execute(
                    req,
                    [this, fn = std::forward<Handler>(fn), error_handler = std::move(error_handler)](
                      core::operations::mutate_in_response resp) mutable {
                        auto ec = error_class_from_response(resp);
                        if (ec) {
                            return error_handler(*ec,
                                                 resp.ctx.ec().message(),
                                                 { resp.ctx.bucket(), resp.ctx.scope(), resp.ctx.collection(), resp.ctx.id() },
                                                 std::forward<Handler>(fn));
                        }
                        return hooks_.after_atr_pending(
                          this,
                          [this, fn = std::forward<Handler>(fn), error_handler = std::move(error_handler), resp = std::move(resp)](
                            auto ec) mutable {
                              if (ec) {
                                  return error_handler(*ec,
                                                       fmt::format("after_atr_pending returned hook raised {}", *ec),
                                                       { resp.ctx.bucket(), resp.ctx.scope(), resp.ctx.collection(), resp.ctx.id() },
                                                       std::forward<Handler>(fn));
                              }

                              state(attempt_state::PENDING);
                              CB_ATTEMPT_CTX_LOG_DEBUG(
                                this, "set ATR {} to Pending, got CAS (start time) {}", atr_id_.value(), resp.cas.value());
                              return fn(std::nullopt);
                          });
                    });
              });
        }
    } catch (const std::exception& e) {
        CB_ATTEMPT_CTX_LOG_ERROR(this, "unexpected error setting atr pending {}", e.what());
        return fn(transaction_operation_failed(FAIL_OTHER, "unexpected error setting atr pending"));
    }
}

staged_mutation*
attempt_context_impl::check_for_own_write(const core::document_id& id)
{
    staged_mutation* own_replace = staged_mutations_->find_replace(id);
    if (own_replace != nullptr) {
        return own_replace;
    }
    staged_mutation* own_insert = staged_mutations_->find_insert(id);
    if (own_insert != nullptr) {
        return own_insert;
    }
    return nullptr;
}

template<typename Handler>
void
attempt_context_impl::check_if_done(Handler& cb)
{
    if (is_done_) {
        return op_completed_with_error(
          std::move(cb),
          transaction_operation_failed(FAIL_OTHER, "Cannot perform operations after transaction has been committed or rolled back")
            .no_rollback());
    }
}
template<typename Handler>
void
attempt_context_impl::do_get(const core::document_id& id, const std::optional<std::string> resolving_missing_atr_entry, Handler&& cb)
{
    try {
        if (check_expiry_pre_commit(STAGE_GET, id.key())) {
            return cb(FAIL_EXPIRY, "expired in do_get", std::nullopt);
        }

        staged_mutation* own_write = check_for_own_write(id);
        if (own_write) {
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "found own-write of mutated doc {}", id);
            return cb(std::nullopt, std::nullopt, transaction_get_result::create_from(own_write->doc(), own_write->content()));
        }
        staged_mutation* own_remove = staged_mutations_->find_remove(id);
        if (own_remove) {
            auto msg = fmt::format("found own-write of removed doc {}", id);
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "{}", msg);
            return cb(FAIL_DOC_NOT_FOUND, msg, std::nullopt);
        }

        return hooks_.before_doc_get(
          this,
          id.key(),
          [this, id, resolving_missing_atr_entry = std::move(resolving_missing_atr_entry), cb = std::move(cb)](auto ec) mutable {
              if (ec) {
                  return cb(ec, "before_doc_get hook raised error", std::nullopt);
              }

              return get_doc(
                id,
                [this, id, resolving_missing_atr_entry = std::move(resolving_missing_atr_entry), cb = std::move(cb)](
                  std::optional<error_class> ec,
                  std::optional<std::string> err_message,
                  std::optional<transaction_get_result> doc) mutable {
                    if (!ec && !doc) {
                        // it just isn't there.
                        return cb(std::nullopt, std::nullopt, std::nullopt);
                    }
                    if (!ec) {
                        if (doc->links().is_document_in_transaction()) {
                            CB_ATTEMPT_CTX_LOG_DEBUG(this,
                                                     "doc {} in transaction, resolving_missing_atr_entry={}",
                                                     *doc,
                                                     resolving_missing_atr_entry.value_or("-"));

                            if (resolving_missing_atr_entry.has_value() &&
                                resolving_missing_atr_entry.value() == doc->links().staged_attempt_id()) {
                                CB_ATTEMPT_CTX_LOG_DEBUG(this, "doc is in lost pending transaction");

                                if (doc->links().is_document_being_inserted()) {
                                    // this document is being inserted, so should not be visible yet
                                    return cb(std::nullopt, std::nullopt, std::nullopt);
                                }

                                return cb(std::nullopt, std::nullopt, doc);
                            }

                            core::document_id doc_atr_id{ doc->links().atr_bucket_name().value(),
                                                          doc->links().atr_scope_name().value(),
                                                          doc->links().atr_collection_name().value(),
                                                          doc->links().atr_id().value() };
                            active_transaction_record::get_atr(
                              cluster_ref(),
                              doc_atr_id,
                              [this, id, doc, cb = std::move(cb)](std::error_code ec2,
                                                                  std::optional<active_transaction_record> atr) mutable {
                                  if (!ec2 && atr) {
                                      active_transaction_record& atr_doc = atr.value();
                                      std::optional<atr_entry> entry;
                                      for (const auto& e : atr_doc.entries()) {
                                          if (doc->links().staged_attempt_id().value() == e.attempt_id()) {
                                              entry.emplace(e);
                                              break;
                                          }
                                      }
                                      bool ignore_doc = false;
                                      auto content = doc->content();
                                      if (entry) {
                                          if (doc->links().staged_attempt_id() && entry->attempt_id() == this->id()) {
                                              // Attempt is reading its own writes
                                              // This is here as backup, it should be returned from the in-memory cache instead
                                              content = doc->links().staged_content();
                                          } else {
                                              auto err =
                                                forward_compat::check(forward_compat_stage::GETS_READING_ATR, entry->forward_compat());
                                              if (err) {
                                                  return cb(FAIL_OTHER, err->what(), std::nullopt);
                                              }
                                              switch (entry->state()) {
                                                  case attempt_state::COMPLETED:
                                                  case attempt_state::COMMITTED:
                                                      if (doc->links().is_document_being_removed()) {
                                                          ignore_doc = true;
                                                      } else {
                                                          content = doc->links().staged_content();
                                                      }
                                                      break;
                                                  default:
                                                      if (doc->links().is_document_being_inserted()) {
                                                          // This document is being inserted, so should not be visible yet
                                                          ignore_doc = true;
                                                      }
                                                      break;
                                              }
                                          }
                                      } else {
                                          // failed to get the ATR entry
                                          CB_ATTEMPT_CTX_LOG_DEBUG(this,
                                                                   "could not get ATR entry, checking again with {}",
                                                                   doc->links().staged_attempt_id().value_or("-"));
                                          return do_get(id, doc->links().staged_attempt_id(), cb);
                                      }
                                      if (ignore_doc) {
                                          return cb(std::nullopt, std::nullopt, std::nullopt);
                                      }
                                      return cb(std::nullopt, std::nullopt, transaction_get_result::create_from(*doc, content));

                                  } else {
                                      // failed to get the ATR
                                      CB_ATTEMPT_CTX_LOG_DEBUG(
                                        this, "could not get ATR, checking again with {}", doc->links().staged_attempt_id().value_or("-"));
                                      return do_get(id, doc->links().staged_attempt_id(), cb);
                                  }
                              });
                        } else {
                            if (doc->links().is_deleted()) {
                                CB_ATTEMPT_CTX_LOG_DEBUG(this, "doc not in txn, and is_deleted, so not returning it.");
                                // doc has been deleted, not in txn, so don't return it
                                return cb(std::nullopt, std::nullopt, std::nullopt);
                            }
                            return cb(std::nullopt, std::nullopt, doc);
                        }
                    } else {
                        return cb(ec, err_message, std::nullopt);
                    }
                });
          });
    } catch (const transaction_operation_failed&) {
        throw;
    } catch (const std::exception& ex) {
        std::ostringstream stream;
        stream << "got error while getting doc " << id.key() << ": " << ex.what();
        throw transaction_operation_failed(FAIL_OTHER, ex.what());
    }
}

void
attempt_context_impl::get_doc(
  const core::document_id& id,
  std::function<void(std::optional<error_class>, std::optional<std::string>, std::optional<transaction_get_result>)>&& cb)
{
    core::operations::lookup_in_request req{ id };
    req.specs =
      lookup_in_specs{
          lookup_in_specs::get(ATR_ID).xattr(),
          lookup_in_specs::get(TRANSACTION_ID).xattr(),
          lookup_in_specs::get(ATTEMPT_ID).xattr(),
          lookup_in_specs::get(OPERATION_ID).xattr(),
          lookup_in_specs::get(STAGED_DATA).xattr(),
          lookup_in_specs::get(ATR_BUCKET_NAME).xattr(),
          lookup_in_specs::get(ATR_SCOPE_NAME).xattr(),
          lookup_in_specs::get(ATR_COLL_NAME).xattr(),
          lookup_in_specs::get(TRANSACTION_RESTORE_PREFIX_ONLY).xattr(),
          lookup_in_specs::get(TYPE).xattr(),
          lookup_in_specs::get(subdoc::lookup_in_macro::document).xattr(),
          lookup_in_specs::get(CRC32_OF_STAGING).xattr(),
          lookup_in_specs::get(FORWARD_COMPAT).xattr(),
          lookup_in_specs::get(""),
      }
        .specs();
    req.access_deleted = true;
    try {
        overall_.cluster_ref().execute(req, [this, id, cb = std::move(cb)](core::operations::lookup_in_response resp) {
            auto ec = error_class_from_response(resp);
            if (ec) {
                CB_ATTEMPT_CTX_LOG_TRACE(this, "get_doc got error {} : {}", resp.ctx.ec().message(), *ec);
                switch (*ec) {
                    case FAIL_PATH_NOT_FOUND:
                        return cb(*ec, resp.ctx.ec().message(), transaction_get_result::create_from(resp));
                    default:
                        return cb(*ec, resp.ctx.ec().message(), std::nullopt);
                }
            } else {
                return cb({}, {}, transaction_get_result::create_from(resp));
            }
        });
    } catch (const std::exception& e) {
        return cb(FAIL_OTHER, e.what(), std::nullopt);
    }
}

template<typename Handler, typename Delay>
void
attempt_context_impl::create_staged_insert_error_handler(const core::document_id& id,
                                                         const std::vector<std::byte>& content,
                                                         uint64_t cas,
                                                         Delay&& delay,
                                                         const std::string& op_id,
                                                         Handler&& cb,
                                                         error_class ec,
                                                         const std::string& message)
{
    CB_ATTEMPT_CTX_LOG_TRACE(this, "create_staged_insert got error class {}: {}", ec, message);
    if (expiry_overtime_mode_.load()) {
        return op_completed_with_error(std::forward<Handler>(cb), transaction_operation_failed(FAIL_EXPIRY, "attempt timed out").expired());
    }
    switch (ec) {
        case FAIL_EXPIRY:
            expiry_overtime_mode_ = true;
            return op_completed_with_error(std::forward<Handler>(cb), transaction_operation_failed(ec, "attempt timed-out").expired());
        case FAIL_TRANSIENT:
            return op_completed_with_error(std::forward<Handler>(cb),
                                           transaction_operation_failed(ec, "transient error in insert").retry());
        case FAIL_AMBIGUOUS:
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "FAIL_AMBIGUOUS in create_staged_insert, retrying");
            delay();
            return create_staged_insert(id, content, cas, delay, op_id, std::forward<Handler>(cb));
        case FAIL_OTHER:
            return op_completed_with_error(std::forward<Handler>(cb), transaction_operation_failed(ec, "error in create_staged_insert"));
        case FAIL_HARD:
            return op_completed_with_error(std::forward<Handler>(cb),
                                           transaction_operation_failed(ec, "error in create_staged_insert").no_rollback());
        case FAIL_DOC_ALREADY_EXISTS:
        case FAIL_CAS_MISMATCH: {
            // special handling for doc already existing
            CB_ATTEMPT_CTX_LOG_DEBUG(this, "found existing doc {}, may still be able to insert", id);
            auto error_handler = [this, id, op_id, content](error_class ec2, const std::string& err_message, Handler&& cb) mutable {
                CB_ATTEMPT_CTX_LOG_TRACE(
                  this, "after a CAS_MISMATCH or DOC_ALREADY_EXISTS, then got error {} in create_staged_insert", ec2);
                if (expiry_overtime_mode_.load()) {
                    return op_completed_with_error(std::move(cb), transaction_operation_failed(FAIL_EXPIRY, "attempt timed out").expired());
                }
                switch (ec2) {
                    case FAIL_DOC_NOT_FOUND:
                    case FAIL_TRANSIENT:
                        return op_completed_with_error(
                          std::move(cb),
                          transaction_operation_failed(ec2, fmt::format("error {} while handling existing doc in insert", err_message))
                            .retry());
                    default:
                        return op_completed_with_error(
                          std::move(cb),
                          transaction_operation_failed(ec2,
                                                       fmt::format("failed getting doc in create_staged_insert with {}", err_message)));
                }
            };
            return hooks_.before_get_doc_in_exists_during_staged_insert(
              this, id.key(), [this, id, content, op_id, cb = std::forward<Handler>(cb), error_handler, delay](auto ec) mutable {
                  if (ec) {
                      return error_handler(
                        *ec, fmt::format("before_get_doc_in_exists_during_staged_insert hook raised {}", *ec), std::forward<Handler>(cb));
                  }
                  return get_doc(
                    id,
                    [this, id, content, op_id, cb = std::forward<Handler>(cb), error_handler, delay](
                      std::optional<error_class> ec3,
                      std::optional<std::string> err_message,
                      std::optional<transaction_get_result> doc) mutable {
                        if (!ec3) {
                            if (doc) {
                                CB_ATTEMPT_CTX_LOG_DEBUG(this,
                                                         "document {} exists, is_in_transaction {}, is_deleted {} ",
                                                         doc->id(),
                                                         doc->links().is_document_in_transaction(),
                                                         doc->links().is_deleted());

                                if (auto err =
                                      forward_compat::check(forward_compat_stage::WWC_INSERTING_GET, doc->links().forward_compat());
                                    err) {
                                    return op_completed_with_error(std::forward<Handler>(cb), *err);
                                }
                                if (!doc->links().is_document_in_transaction() && doc->links().is_deleted()) {
                                    // it is just a deleted doc, so we are ok.  Let's try again, but with the cas
                                    CB_ATTEMPT_CTX_LOG_DEBUG(
                                      this, "create staged insert found existing deleted doc, retrying with cas {}", doc->cas().value());
                                    delay();
                                    return create_staged_insert(id, content, doc->cas().value(), delay, op_id, std::forward<Handler>(cb));
                                }
                                if (!doc->links().is_document_in_transaction()) {
                                    // doc was inserted outside txn elsewhere
                                    CB_ATTEMPT_CTX_LOG_TRACE(this, "doc {} not in txn - was inserted outside txn", id);
                                    return op_completed_with_error(
                                      std::forward<Handler>(cb),
                                      document_exists(
                                        { couchbase::errc::transaction_op::document_exists_exception, key_value_error_context() }));
                                }
                                if (doc->links().staged_attempt_id() == this->id()) {
                                    if (doc->links().staged_operation_id() == op_id) {
                                        // this is us dealing with resolving an ambiguity.  So, lets just update the staged_mutation with
                                        // the correct cas and continue...
                                        staged_mutations_->add(staged_mutation(*doc, content, staged_mutation_type::INSERT));
                                        return op_completed_with_callback(std::forward<Handler>(cb), doc);
                                    }
                                    return op_completed_with_error(
                                      std::forward<Handler>(cb),
                                      transaction_operation_failed(FAIL_OTHER, "concurrent operations on a document are not allowed")
                                        .cause(CONCURRENT_OPERATIONS_DETECTED_ON_SAME_DOCUMENT));
                                }
                                // CBD-3787 - Only a staged insert is ok to overwrite
                                if (doc->links().op() && *doc->links().op() != "insert") {
                                    return op_completed_with_error(
                                      std::forward<Handler>(cb),
                                      transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, "doc exists, not a staged insert")
                                        .cause(DOCUMENT_EXISTS_EXCEPTION));
                                }
                                check_and_handle_blocking_transactions(
                                  *doc,
                                  forward_compat_stage::WWC_INSERTING,
                                  [this, id, op_id, content, doc, cb = std::forward<Handler>(cb), delay](
                                    std::optional<transaction_operation_failed> err) mutable {
                                      if (err) {
                                          return op_completed_with_error(std::move(cb), *err);
                                      }
                                      CB_ATTEMPT_CTX_LOG_DEBUG(
                                        this, "doc ok to overwrite, retrying create_staged_insert with cas {}", doc->cas().value());
                                      delay();
                                      return create_staged_insert(id, content, doc->cas().value(), delay, op_id, std::forward<Handler>(cb));
                                  });
                            } else {
                                // no doc now, just retry entire txn
                                CB_ATTEMPT_CTX_LOG_TRACE(this, "got {} from get_doc in exists during staged insert", *ec3);
                                return op_completed_with_error(
                                  std::move(cb),
                                  transaction_operation_failed(FAIL_DOC_NOT_FOUND,
                                                               "insert failed as the doc existed, but now seems to not exist")
                                    .retry());
                            }
                        } else {
                            return error_handler(*ec3, *err_message, std::forward<Handler>(cb));
                        }
                    });
              });
        }
        default:
            return op_completed_with_error(std::forward<Handler>(cb),
                                           transaction_operation_failed(ec, "failed in create_staged_insert").retry());
    }
}

template<typename Handler, typename Delay>
void
attempt_context_impl::create_staged_insert(const core::document_id& id,
                                           const std::vector<std::byte>& content,
                                           uint64_t cas,
                                           Delay&& delay,
                                           const std::string& op_id,
                                           Handler&& cb)
{

    if (auto ec = error_if_expired_and_not_in_overtime(STAGE_CREATE_STAGED_INSERT, id.key()); ec) {
        return create_staged_insert_error_handler(id,
                                                  content,
                                                  cas,
                                                  std::forward<Delay>(delay),
                                                  op_id,
                                                  std::forward<Handler>(cb),
                                                  *ec,
                                                  "create_staged_insert expired and not in overtime");
    }

    auto ec =
      wait_for_hook([this, key = id.key()](auto handler) mutable { return hooks_.before_staged_insert(this, key, std::move(handler)); });
    if (ec) {
        return create_staged_insert_error_handler(
          id, content, cas, std::forward<Delay>(delay), op_id, std::forward<Handler>(cb), *ec, "before_staged_insert hook threw error");
    }
    CB_ATTEMPT_CTX_LOG_DEBUG(this, "about to insert staged doc {} with cas {}", id, cas);
    auto req = create_staging_request(id, nullptr, "insert", op_id, content);
    req.access_deleted = true;
    req.create_as_deleted = true;
    req.cas = couchbase::cas(cas);
    req.store_semantics = cas == 0 ? couchbase::store_semantics::insert : couchbase::store_semantics::replace;
    wrap_durable_request(req, overall_.config());
    overall_.cluster_ref().execute(
      req,
      [this, id, content, cas, op_id, cb = std::forward<Handler>(cb), delay = std::forward<Delay>(delay)](
        core::operations::mutate_in_response resp) mutable {
          if (auto ec = error_class_from_response(resp); ec) {
              return create_staged_insert_error_handler(
                id, content, cas, std::forward<Delay>(delay), op_id, std::forward<Handler>(cb), *ec, resp.ctx.ec().message());
          }
          return hooks_.after_staged_insert_complete(
            this,
            id.key(),
            [this, id, content, cas, op_id, cb = std::forward<Handler>(cb), delay = std::forward<Delay>(delay), resp = std::move(resp)](
              auto ec) mutable {
                if (ec) {
                    auto msg = (resp.ctx.ec() ? resp.ctx.ec().message() : "after_staged_insert hook threw error");
                    return create_staged_insert_error_handler(
                      id, content, cas, std::forward<Delay>(delay), op_id, std::forward<Handler>(cb), *ec, msg);
                }

                CB_ATTEMPT_CTX_LOG_DEBUG(this, "inserted doc {} CAS={}, {}", id, resp.cas.value(), resp.ctx.ec().message());

                // TODO: clean this up (do most of this in transactions_document(...))
                transaction_links links(atr_id_->key(),
                                        id.bucket(),
                                        id.scope(),
                                        id.collection(),
                                        overall_.transaction_id(),
                                        this->id(),
                                        op_id,
                                        content,
                                        std::nullopt,
                                        std::nullopt,
                                        std::nullopt,
                                        std::nullopt,
                                        std::string("insert"),
                                        std::nullopt,
                                        true);
                transaction_get_result out(id, content, resp.cas.value(), links, std::nullopt);
                staged_mutations_->add(staged_mutation(out, content, staged_mutation_type::INSERT));
                return op_completed_with_callback(cb, std::optional<transaction_get_result>(out));
            });
      });
}

} // namespace couchbase::core::transactions
