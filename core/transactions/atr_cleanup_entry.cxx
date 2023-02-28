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

#include "active_transaction_record.hxx"
#include "attempt_context_impl.hxx"
#include "cleanup_testing_hooks.hxx"
#include "durability_level.hxx"
#include "forward_compat.hxx"

#include "core/transactions.hxx"
#include "internal/atr_cleanup_entry.hxx"
#include "internal/logging.hxx"
#include "internal/transactions_cleanup.hxx"
#include "internal/utils.hxx"
#include "result.hxx"

#include <optional>

namespace couchbase::core::transactions
{

// NOTE: priority queue outputs largest to smallest - since we want the least
// recent statr time first, this returns true if lhs > rhs
bool
compare_atr_entries::operator()(atr_cleanup_entry& lhs, atr_cleanup_entry& rhs)
{
    return lhs.min_start_time_ > rhs.min_start_time_;
}
// wait a bit after an attempt is expired before cleaning it.
const uint32_t atr_cleanup_entry::safety_margin_ms_ = 1500;

atr_cleanup_entry::atr_cleanup_entry(const core::document_id& atr_id, const std::string& attempt_id, const transactions_cleanup& cleanup)
  : atr_id_(atr_id)
  , attempt_id_(attempt_id)
  , check_if_expired_(false)
  , cleanup_(&cleanup)
  , atr_entry_(nullptr)
{
}

atr_cleanup_entry::atr_cleanup_entry(const atr_entry& entry,
                                     const core::document_id& atr_id,
                                     const transactions_cleanup& cleanup,
                                     bool check_if_expired)
  //  : atr_id_(atr_id.bucket(), atr_id.scope(), atr_id.collection(), atr_id.key())
  : atr_id_(atr_id)
  , attempt_id_(entry.attempt_id())
  , check_if_expired_(check_if_expired)
  , cleanup_(&cleanup)
  , atr_entry_(&entry)
{
}

atr_cleanup_entry::atr_cleanup_entry(attempt_context& ctx)
  : min_start_time_(std::chrono::steady_clock::now())
  , check_if_expired_(false)
  , atr_entry_(nullptr)
{
    // NOTE: we create these entries externally, in fit_performer tests, hence the
    // use of attempt_context rather than attempt_context_impl
    auto& ctx_impl = static_cast<attempt_context_impl&>(ctx);
    atr_id_ = { ctx_impl.atr_id_.value().bucket(),
                ctx_impl.atr_id_.value().scope(),
                ctx_impl.atr_id_.value().collection(),
                ctx_impl.atr_id_.value().key() };
    attempt_id_ = ctx_impl.id();
    cleanup_ = &ctx_impl.overall_.cleanup();
}

void
atr_cleanup_entry::clean(transactions_cleanup_attempt* result)
{
    CB_ATTEMPT_CLEANUP_LOG_TRACE("cleaning {}", *this);
    // get atr entry if needed
    atr_entry entry;
    if (nullptr == atr_entry_) {
        auto atr = active_transaction_record::get_atr(cleanup_->cluster_ref(), atr_id_);
        if (atr) {
            // now get the specific attempt
            auto it =
              std::find_if(atr->entries().begin(), atr->entries().end(), [&](const atr_entry& e) { return e.attempt_id() == attempt_id_; });
            if (it != atr->entries().end()) {
                atr_entry_ = &(*it);
                return check_atr_and_cleanup(result);
            } else {
                CB_ATTEMPT_CLEANUP_LOG_TRACE("could not find attempt {}, nothing to clean", attempt_id_);
                return;
            }
        } else {
            CB_ATTEMPT_CLEANUP_LOG_TRACE("could not find atr {}, nothing to clean", atr_id_);
            return;
        }
    }
    check_atr_and_cleanup(result);
}

void
atr_cleanup_entry::check_atr_and_cleanup(transactions_cleanup_attempt* result)
{
    // ExtStoreDurability: this is the first point where we're guaranteed to have the ATR entry
    auto durability_level_raw = atr_entry_->durability_level();
    auto durability_level = cleanup_->config().level;
    if (durability_level_raw.has_value()) {
        durability_level = store_string_to_durability_level(durability_level_raw.value());
    }
    if (check_if_expired_ && !atr_entry_->has_expired(safety_margin_ms_)) {
        CB_ATTEMPT_CLEANUP_LOG_TRACE("not expired, nothing to clean");
        return;
    }
    if (result) {
        result->state(atr_entry_->state());
    }
    auto err = forward_compat::check(forward_compat_stage::CLEANUP_ENTRY, atr_entry_->forward_compat());
    if (err) {
        throw *err;
    }
    cleanup_docs(durability_level);
    auto ec = cleanup_->config().cleanup_hooks->on_cleanup_docs_completed();
    if (ec) {
        throw client_error(*ec, "on_cleanup_docs_completed hook threw error");
    }
    cleanup_entry(durability_level);
    ec = cleanup_->config().cleanup_hooks->on_cleanup_completed();
    if (ec) {
        throw client_error(*ec, "on_cleanup_completed hook threw error");
    }
    return;
}

void
atr_cleanup_entry::cleanup_docs(durability_level dl)
{
    switch (atr_entry_->state()) {
        case attempt_state::COMMITTED:
            commit_docs(atr_entry_->inserted_ids(), dl);
            commit_docs(atr_entry_->replaced_ids(), dl);
            remove_docs_staged_for_removal(atr_entry_->removed_ids(), dl);
            break;
        // half-finished commit
        case attempt_state::ABORTED:
            // half finished rollback
            remove_docs(atr_entry_->inserted_ids(), dl);
            remove_txn_links(atr_entry_->replaced_ids(), dl);
            remove_txn_links(atr_entry_->removed_ids(), dl);
            break;
        default:
            CB_ATTEMPT_CLEANUP_LOG_TRACE("attempt in {}, nothing to do in cleanup_docs", attempt_state_name(atr_entry_->state()));
    }
}

void
atr_cleanup_entry::do_per_doc(std::vector<doc_record> docs,
                              bool require_crc_to_match,
                              const std::function<void(transaction_get_result&, bool)>& call)
{
    for (const auto& dr : docs) {
        try {
            core::operations::lookup_in_request req{ dr.document_id() };
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
            wrap_request(req, cleanup_->config());
            // now a blocking lookup_in...
            auto barrier = std::make_shared<std::promise<result>>();
            cleanup_->cluster_ref()->execute(
              req, [barrier](core::operations::lookup_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
            auto f = barrier->get_future();
            auto res = wrap_operation_future(f);

            if (res.values.empty()) {
                CB_ATTEMPT_CLEANUP_LOG_TRACE("cannot create a transaction document from {}, ignoring", res);
                continue;
            }
            auto doc = transaction_get_result::create_from(dr.document_id(), res);
            // now let's decide if we call the function or not
            if (!(doc.links().has_staged_content() || doc.links().is_document_being_removed()) || !doc.links().has_staged_write()) {
                CB_ATTEMPT_CLEANUP_LOG_TRACE("document {} has no staged content - assuming it was "
                                             "committed and skipping",
                                             dr.id());
                continue;
            } else if (doc.links().staged_attempt_id() != attempt_id_) {
                CB_ATTEMPT_CLEANUP_LOG_TRACE(
                  "document {} staged for different attempt {}, skipping", dr.id(), doc.links().staged_attempt_id().value_or("<none>)"));
                continue;
            }
            if (require_crc_to_match) {
                if (!doc.metadata()->crc32() || !doc.links().crc32_of_staging() ||
                    doc.links().crc32_of_staging() != doc.metadata()->crc32()) {
                    CB_ATTEMPT_CLEANUP_LOG_TRACE("document {} crc32 {} doesn't match staged value {}, skipping",
                                                 dr.id(),
                                                 doc.metadata()->crc32().value_or("<none>"),
                                                 doc.links().crc32_of_staging().value_or("<none>"));
                    continue;
                }
            }
            call(doc, res.is_deleted);
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_DOC_NOT_FOUND:
                    CB_ATTEMPT_CLEANUP_LOG_ERROR("document {} not found - ignoring ", dr);
                    break;
                default:
                    CB_ATTEMPT_CLEANUP_LOG_ERROR("got error \"{}\", not ignoring this", e.what());
                    throw;
            }
        }
    }
}

void
atr_cleanup_entry::commit_docs(std::optional<std::vector<doc_record>> docs, durability_level dl)
{
    if (docs) {
        do_per_doc(*docs, true, [&](transaction_get_result& doc, bool) {
            if (doc.links().has_staged_content()) {
                auto content = doc.links().staged_content();
                auto ec = cleanup_->config().cleanup_hooks->before_commit_doc(doc.id().key());
                if (ec) {
                    throw client_error(*ec, "before_commit_doc hook threw error");
                }
                if (doc.links().is_deleted()) {
                    core::operations::insert_request req{ doc.id(), content };
                    auto barrier = std::make_shared<std::promise<result>>();
                    auto f = barrier->get_future();
                    cleanup_->cluster_ref()->execute(wrap_durable_request(req, cleanup_->config(), dl),
                                                     [barrier](core::operations::insert_response resp) {
                                                         barrier->set_value(result::create_from_mutation_response(resp));
                                                     });
                    wrap_operation_future(f);
                } else {
                    core::operations::mutate_in_request req{ doc.id() };
                    req.specs =
                      couchbase::mutate_in_specs{
                          couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                          couchbase::mutate_in_specs::replace_raw({}, content),
                      }
                        .specs();
                    req.cas = doc.cas();
                    req.store_semantics = couchbase::store_semantics::replace;
                    wrap_durable_request(req, cleanup_->config(), dl);
                    auto barrier = std::make_shared<std::promise<result>>();
                    auto f = barrier->get_future();
                    cleanup_->cluster_ref()->execute(req, [barrier](core::operations::mutate_in_response resp) {
                        barrier->set_value(result::create_from_subdoc_response(resp));
                    });
                    wrap_operation_future(f);
                }
                CB_ATTEMPT_CLEANUP_LOG_TRACE("commit_docs replaced content of doc {} with {}", doc.id(), to_string(content));
            } else {
                CB_ATTEMPT_CLEANUP_LOG_TRACE("commit_docs skipping document {}, no staged content", doc.id());
            }
        });
    }
}
void
atr_cleanup_entry::remove_docs(std::optional<std::vector<doc_record>> docs, durability_level dl)
{
    if (docs) {
        do_per_doc(*docs, true, [&](transaction_get_result& doc, bool is_deleted) {
            auto ec = cleanup_->config().cleanup_hooks->before_remove_doc(doc.id().key());
            if (ec) {
                throw client_error(*ec, "before_remove_doc hook threw error");
            }
            if (is_deleted) {
                core::operations::mutate_in_request req{ doc.id() };
                req.specs =
                  couchbase::mutate_in_specs{
                      couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                  }
                    .specs();
                req.cas = doc.cas();
                req.access_deleted = true;
                wrap_durable_request(req, cleanup_->config(), dl);
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                cleanup_->cluster_ref()->execute(req, [barrier](core::operations::mutate_in_response resp) {
                    barrier->set_value(result::create_from_subdoc_response(resp));
                });
                wrap_operation_future(f);
            } else {
                core::operations::remove_request req{ doc.id() };
                req.cas = doc.cas();
                wrap_durable_request(req, cleanup_->config(), dl);
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                cleanup_->cluster_ref()->execute(req, [barrier](core::operations::remove_response resp) {
                    barrier->set_value(result::create_from_mutation_response(resp));
                });
                wrap_operation_future(f);
            }
            CB_ATTEMPT_CLEANUP_LOG_TRACE("remove_docs removed doc {}", doc.id());
        });
    }
}

void
atr_cleanup_entry::remove_docs_staged_for_removal(std::optional<std::vector<doc_record>> docs, durability_level dl)
{
    if (docs) {
        do_per_doc(*docs, true, [&](transaction_get_result& doc, bool) {
            if (doc.links().is_document_being_removed()) {
                auto ec = cleanup_->config().cleanup_hooks->before_remove_doc_staged_for_removal(doc.id().key());
                if (ec) {
                    throw client_error(*ec, "before_remove_doc_staged_for_removal hook threw error");
                }
                core::operations::remove_request req{ doc.id() };
                req.cas = doc.cas();
                wrap_durable_request(req, cleanup_->config(), dl);
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                cleanup_->cluster_ref()->execute(req, [barrier](core::operations::remove_response resp) {
                    barrier->set_value(result::create_from_mutation_response(resp));
                });
                wrap_operation_future(f);
                CB_ATTEMPT_CLEANUP_LOG_TRACE("remove_docs_staged_for_removal removed doc {}", doc.id());
            } else {
                CB_ATTEMPT_CLEANUP_LOG_TRACE("remove_docs_staged_for_removal found document {} not "
                                             "marked for removal, skipping",
                                             doc.id());
            }
        });
    }
}

void
atr_cleanup_entry::remove_txn_links(std::optional<std::vector<doc_record>> docs, durability_level dl)
{
    if (docs) {
        do_per_doc(*docs, false, [&](transaction_get_result& doc, bool) {
            auto ec = cleanup_->config().cleanup_hooks->before_remove_links(doc.id().key());
            if (ec) {
                throw client_error(*ec, "before_remove_links hook threw error");
            }
            core::operations::mutate_in_request req{ doc.id() };
            req.specs =
              couchbase::mutate_in_specs{
                  couchbase::mutate_in_specs::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
              }
                .specs();
            req.access_deleted = true;
            req.cas = doc.cas();
            wrap_durable_request(req, cleanup_->config(), dl);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            cleanup_->cluster_ref()->execute(
              req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
            wrap_operation_future(f);
            CB_ATTEMPT_CLEANUP_LOG_TRACE("remove_txn_links removed links for doc {}", doc.id());
        });
    }
}

void
atr_cleanup_entry::cleanup_entry(durability_level dl)
{
    try {
        auto ec = cleanup_->config().cleanup_hooks->before_atr_remove();
        if (ec) {
            throw client_error(*ec, "before_atr_remove hook threw error");
        }
        core::operations::mutate_in_request req{ atr_id_ };
        couchbase::mutate_in_specs mut_specs;
        if (atr_entry_->state() == attempt_state::PENDING) {
            mut_specs.push_back(
              couchbase::mutate_in_specs::insert("attempts." + atr_entry_->attempt_id() + ".p", tao::json::empty_object).xattr());
        }
        mut_specs.push_back(couchbase::mutate_in_specs::remove("attempts." + atr_entry_->attempt_id()).xattr());
        req.specs = mut_specs.specs();
        wrap_durable_request(req, cleanup_->config(), dl);
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        cleanup_->cluster_ref()->execute(
          req, [barrier](core::operations::mutate_in_response resp) { barrier->set_value(result::create_from_subdoc_response(resp)); });
        wrap_operation_future(f);
        CB_ATTEMPT_CLEANUP_LOG_TRACE("successfully removed attempt {}", attempt_id_);
    } catch (const client_error& e) {
        error_class ec = e.ec();
        switch (ec) {
            case FAIL_PATH_NOT_FOUND:
                CB_ATTEMPT_CLEANUP_LOG_TRACE("found attempt {} has also inserted 'p' field indicating collision with main algo");
                return;
            default:
                CB_ATTEMPT_CLEANUP_LOG_ERROR("cleanup couldn't remove attempt {} due to {} {}", attempt_id_, ec, e.what());
                throw;
        }
    }
}

bool
atr_cleanup_entry::ready() const
{
    return std::chrono::steady_clock::now() > min_start_time_;
}

std::optional<atr_cleanup_entry>
atr_cleanup_queue::pop(bool check_time)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!queue_.empty()) {
        if (!check_time || (check_time && queue_.top().ready())) {
            // copy it
            atr_cleanup_entry top = queue_.top();
            // pop it
            queue_.pop();
            return { top };
        }
    }
    return {};
}

std::size_t
atr_cleanup_queue::size() const
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.size();
}

void
atr_cleanup_queue::push(attempt_context& ctx)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.emplace(ctx);
}

void
atr_cleanup_queue::push(const atr_cleanup_entry& e)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.push(e);
}
} // namespace couchbase::core::transactions
