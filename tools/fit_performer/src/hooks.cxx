/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "hooks.hxx"
#include "connection.hxx"
#include "exceptions.hxx"

#include <core/document_id_fmt.hxx>
#include <core/transactions/attempt_context_impl.hxx>
#include <core/transactions/attempt_context_testing_hooks.hxx>
#include <core/transactions/cleanup_testing_hooks.hxx>
#include <core/utils/split_string.hxx>

#include <memory>
#include <string>
#include <utility>
#include <vector>

void
fit_cxx::TxnSvcHook::update_conn(std::shared_ptr<Connection> conn)
{
  conn_ = conn;
}

fit_cxx::test_hook_pair
fit_cxx::TxnSvcHook::convert_hooks(
  const google::protobuf::RepeatedPtrField<protocol::hooks::transactions::Hook> hooks,
  HookVector& hook_vector,
  std::shared_ptr<Connection> conn)
{
  auto txn_hooks = std::make_shared<couchbase::core::transactions::attempt_context_testing_hooks>();
  auto cleanup_hooks = std::make_shared<couchbase::core::transactions::cleanup_testing_hooks>();
  for (const auto& h : hooks) {
    spdlog::trace("creating TxnSvcHook from {}", h.DebugString());
    hook_vector.emplace_back(h, txn_hooks, cleanup_hooks, conn);
  }
  return { txn_hooks, cleanup_hooks };
}

fit_cxx::test_hook_pair
fit_cxx::TxnSvcHook::clone_hooks(HookVector& hook_vector,
                                 HookVector& cloned_hook_vector,
                                 std::shared_ptr<Connection> conn)
{
  auto txn_hooks = std::make_shared<couchbase::core::transactions::attempt_context_testing_hooks>();
  auto cleanup_hooks = std::make_shared<couchbase::core::transactions::cleanup_testing_hooks>();
  for (auto& h : hook_vector) {
    spdlog::trace("cloning TxnSvcHook from", h.hook_.DebugString());
    cloned_hook_vector.emplace_back(h.hook_, txn_hooks, cleanup_hooks, conn);
  }
  return { txn_hooks, cleanup_hooks };
}

void
fit_cxx::TxnSvcHook::reset()
{
  std::lock_guard lock(mutex_);
  spdlog::trace("resetting hook");
  calls_ = 0;
  has_expired_ = false;
}

fit_cxx::TxnSvcHook::TxnSvcHook(
  protocol::hooks::transactions::Hook hook,
  std::shared_ptr<couchbase::core::transactions::attempt_context_testing_hooks> txn_hooks,
  std::shared_ptr<couchbase::core::transactions::cleanup_testing_hooks> cleanup_hooks,
  std::shared_ptr<Connection> conn)
  : hook_(std::move(hook))
  , conn_(std::move(conn))
  , txn_hooks_(std::move(txn_hooks))
  , cleanup_hooks_(std::move(cleanup_hooks))
{
  // all we do here is point the right testing hook function to the correct function
  // in this object.  All the work is done there.
  switch (hook_.hook_point()) {
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_COMMIT:
      txn_hooks_->before_atr_commit = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_COMMIT_AMBIGUITY_RESOLUTION:
      txn_hooks_->before_atr_commit_ambiguity_resolution = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ATR_COMMIT:
      txn_hooks_->after_atr_commit = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_DOC_COMMITTED:
      txn_hooks_->before_doc_committed = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_DOC_ROLLED_BACK:
      txn_hooks_->before_doc_rolled_back = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_DOC_COMMITTED_BEFORE_SAVING_CAS:
      txn_hooks_->after_doc_committed_before_saving_cas =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_DOC_COMMITTED:
      txn_hooks_->after_doc_committed = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_DOC_REMOVED:
      txn_hooks_->before_doc_removed = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_DOC_REMOVED_PRE_RETRY:
      txn_hooks_->after_doc_removed_pre_retry = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_DOC_REMOVED_POST_RETRY:
      txn_hooks_->after_doc_removed_post_retry = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_DOCS_REMOVED:
      txn_hooks_->after_docs_removed = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_PENDING:
      txn_hooks_->before_atr_pending = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ATR_COMPLETE:
      txn_hooks_->after_atr_complete = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_ROLLED_BACK:
      txn_hooks_->before_atr_rolled_back = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_GET_COMPLETE:
      txn_hooks_->after_get_complete = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ROLLBACK_DELETE_INSERTED:
      txn_hooks_->before_rollback_delete_inserted =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_STAGED_REPLACE_COMPLETE:
      txn_hooks_->after_staged_replace_complete = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_STAGED_REMOVE_COMPLETE:
      txn_hooks_->after_staged_remove_complete = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_STAGED_INSERT:
      txn_hooks_->before_staged_insert = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_STAGED_REMOVE:
      txn_hooks_->before_staged_remove = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_STAGED_REPLACE:
      txn_hooks_->before_staged_replace = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_STAGED_INSERT_COMPLETE:
      txn_hooks_->after_staged_insert_complete = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_GET_ATR_FOR_ABORT:
      txn_hooks_->before_get_atr_for_abort = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_ABORTED:
      txn_hooks_->before_atr_aborted = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ATR_ABORTED:
      txn_hooks_->after_atr_aborted = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ATR_ROLLED_BACK:
      txn_hooks_->after_atr_rolled_back = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ROLLBACK_REPLACE_OR_REMOVE:
      txn_hooks_->after_rollback_replace_or_remove =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ROLLBACK_DELETE_INSERTED:
      txn_hooks_->after_rollback_delete_inserted =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_REMOVING_DOC_DURING_STAGING_INSERT:
      txn_hooks_->before_removing_doc_during_staged_insert =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_CHECK_ATR_ENTRY_FOR_BLOCKING_DOC:
      txn_hooks_->before_check_atr_entry_for_blocking_doc =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_DOC_GET:
      txn_hooks_->before_doc_get = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_GET_DOC_IN_EXISTS_DURING_STAGED_INSERT:
      txn_hooks_->before_get_doc_in_exists_during_staged_insert =
        [this](auto ctx, const auto& id, auto&& handler) {
          return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
        };
      break;
    case protocol::hooks::transactions::HookPoint::AFTER_ATR_PENDING:
      txn_hooks_->after_atr_pending = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_ATR_COMPLETE:
      txn_hooks_->before_atr_complete = [this](auto ctx, auto&& handler) {
        return hook_fn1(ctx, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::ATR_ID_FOR_VBUCKET:
      txn_hooks_->random_atr_id_for_vbucket = [this](auto ctx) {
        return hook_fn3(ctx);
      };
      break;
    case protocol::hooks::transactions::HookPoint::HAS_EXPIRED:
      txn_hooks_->has_expired_client_side = [this](auto ctx, const auto& stage, auto doc_id) {
        return hook_fn4(ctx, stage, doc_id);
      };
      break;
    case protocol::hooks::transactions::HookPoint::BEFORE_QUERY:
      txn_hooks_->before_query = [this](auto ctx, const auto& id, auto&& handler) {
        return hook_fn2(ctx, id, std::forward<decltype(handler)>(handler));
      };
      break;

    // Now the cleanup hooks
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_COMMIT_DOC:
      cleanup_hooks_->before_commit_doc = [this](const auto& id, auto&& handler) {
        return hook_fn5(id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_REMOVE_DOC_STAGED_FOR_REMOVAL:
      cleanup_hooks_->before_remove_doc_staged_for_removal = [this](const auto& id,
                                                                    auto&& handler) {
        return hook_fn5(id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_DOC_GET:
      cleanup_hooks_->before_doc_get = [this](const auto& id, auto&& handler) {
        return hook_fn5(id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_REMOVE_DOC:
      cleanup_hooks_->before_remove_doc = [this](const auto& id, auto&& handler) {
        return hook_fn5(id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_REMOVE_DOC_LINKS:
      cleanup_hooks_->before_remove_links = [this](const auto& id, auto&& handler) {
        return hook_fn5(id, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLEANUP_BEFORE_ATR_REMOVE:
      cleanup_hooks_->before_atr_remove = [this](auto&& handler) {
        return hook_fn6(std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLIENT_RECORD_BEFORE_CREATE:
      cleanup_hooks_->client_record_before_create = [this](const auto& bucket_name,
                                                           auto&& handler) {
        return hook_fn7(bucket_name, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLIENT_RECORD_BEFORE_GET:
      cleanup_hooks_->client_record_before_get = [this](const auto& bucket_name, auto&& handler) {
        return hook_fn7(bucket_name, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLIENT_RECORD_BEFORE_UPDATE:
      cleanup_hooks_->client_record_before_update = [this](const auto& bucket_name,
                                                           auto&& handler) {
        return hook_fn7(bucket_name, std::forward<decltype(handler)>(handler));
      };
      break;
    case protocol::hooks::transactions::HookPoint::CLIENT_RECORD_BEFORE_REMOVE_CLIENT:
      cleanup_hooks_->client_record_before_remove_client = [this](const auto& bucket_name,
                                                                  auto&& handler) {
        return hook_fn7(bucket_name, std::forward<decltype(handler)>(handler));
      };
      break;
    default:
      spdlog::trace("Unknown hook point {}", HookPoint_Name(hook_.hook_point()));
      break;
  }
}

void
fit_cxx::TxnSvcHook::hook_fn5(
  const std::string& id,
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  return hook_fn2(nullptr, id, std::move(handler));
}

void
fit_cxx::TxnSvcHook::hook_fn6(
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  return hook_fn1(nullptr, std::move(handler));
}

void
fit_cxx::TxnSvcHook::hook_fn7(
  const std::string& bucket_name,
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  if (bucket_name_ != bucket_name) {
    bucket_name_ = bucket_name;
    calls_ = 0;
  }
  return hook_fn1(nullptr, std::move(handler));
}

void
fit_cxx::TxnSvcHook::hook_fn1(
  std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  std::lock_guard lock(mutex_);
  calls_++;
  spdlog::trace("hook_fn1 call #{} for hook {}", calls_, this->hook_.DebugString());
  if (should_fire(ctx)) {
    return fire_action(std::move(handler));
  }
  return handler(std::nullopt);
}

void
fit_cxx::TxnSvcHook::hook_fn2(
  std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
  const std::string& doc_id,
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  std::lock_guard lock(mutex_);
  calls_++;
  spdlog::info(
    "hook_fn2 call #{} for hook {} with doc_id {}", calls_, this->hook_.DebugString(), doc_id);
  if (should_fire(ctx, doc_id)) {
    return fire_action(std::move(handler));
  }
  return handler(std::nullopt);
}

std::optional<const std::string>
fit_cxx::TxnSvcHook::hook_fn3(std::shared_ptr<couchbase::core::transactions::attempt_context> ctx)
{
  std::lock_guard lock(mutex_);
  calls_++;
  spdlog::trace("hook_fn3 call #{} for hook {}", calls_, this->hook_.DebugString());
  if (should_fire(ctx)) {
    return fire_action_return_string();
  }
  return std::nullopt;
}

bool
fit_cxx::TxnSvcHook::hook_fn4(
  std::shared_ptr<couchbase::core::transactions::attempt_context> /* ctx */,
  const std::string& stage,
  std::optional<const std::string> doc_id)
{
  std::lock_guard lock(mutex_);
  calls_++;
  spdlog::trace("hook_fn4 call #{} for hook {}", calls_, hook_.DebugString());
  if (has_expired_) {
    spdlog::trace("has already expired in previous hook, returning true");
    return true;
  }
  switch (hook_.hook_condition()) {
    case protocol::hooks::transactions::HookCondition::ALWAYS:
      spdlog::trace("injecting expiry in stage {}", stage);
      has_expired_ = true;
      break;
    case protocol::hooks::transactions::HookCondition::EQUALS_BOTH:
      if (doc_id) {
        has_expired_ =
          (stage == hook_.hook_condition_param3() && *doc_id == hook_.hook_condition_param2());

        if (has_expired_) {
          spdlog::trace("injecting expiry in stage {}, for doc id {}", stage, *doc_id);
        }
      }
      break;
    case protocol::hooks::transactions::HookCondition::EQUALS:
      has_expired_ = (stage == hook_.hook_condition_param2());
      if (has_expired_) {
        spdlog::trace("injecting expiry in stage {}", stage);
      }
      break;
    default:
      throw performer_exception::internal("Unexpected hook condition");
  }
  return has_expired_;
}

std::optional<const std::string>
fit_cxx::TxnSvcHook::fire_action_return_string()
{
  switch (hook_.hook_action()) {
    case protocol::hooks::transactions::HookAction::RETURN_STRING:
      return std::make_optional<const std::string>(hook_.hook_action_param1());
    default:
      spdlog::error("fire_action_return_string unknown action {}",
                    HookAction_Name(hook_.hook_action()));
  }
  return std::nullopt;
}

void
fit_cxx::TxnSvcHook::fire_action(
  std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler)
{
  spdlog::info("Firing hook action {}", HookAction_Name(hook_.hook_action()));
  switch (hook_.hook_action()) {
    case protocol::hooks::transactions::HookAction::FAIL_HARD:
      return handler(couchbase::core::transactions::FAIL_HARD);

    case protocol::hooks::transactions::HookAction::FAIL_OTHER:
      return handler(couchbase::core::transactions::FAIL_OTHER);

    case protocol::hooks::transactions::HookAction::FAIL_TRANSIENT:
      return handler(couchbase::core::transactions::FAIL_TRANSIENT);

    case protocol::hooks::transactions::HookAction::FAIL_AMBIGUOUS:
      return handler(couchbase::core::transactions::FAIL_AMBIGUOUS);

    case protocol::hooks::transactions::HookAction::FAIL_PATH_NOT_FOUND:
      return handler(couchbase::core::transactions::FAIL_PATH_NOT_FOUND);

    case protocol::hooks::transactions::HookAction::FAIL_DOC_NOT_FOUND:
      return handler(couchbase::core::transactions::FAIL_DOC_NOT_FOUND);

    case protocol::hooks::transactions::HookAction::MUTATE_DOC:
    case protocol::hooks::transactions::HookAction::REMOVE_DOC: {
      // doc location in format: bucket/collection/doc_id
      std::vector<std::string> doc_locations =
        couchbase::core::utils::split_string(hook_.hook_action_param1(), '/');
      const auto& bucket_name = doc_locations[0];
      const auto& id = doc_locations[2];
      auto coll_spec = couchbase::core::utils::split_string(doc_locations[1], ':');
      std::string scope{ couchbase::scope::default_name };
      std::string collection;
      if (coll_spec.size() > 1) {
        scope = coll_spec[0];
        collection = coll_spec[1];
      } else {
        collection = coll_spec[0];
      }
      couchbase::core::document_id doc_id{ bucket_name, scope, collection, id };
      if (hook_.hook_action() == protocol::hooks::transactions::HookAction::REMOVE_DOC) {
        spdlog::critical("hook removing {}", doc_id);
        return conn_->remove_doc(doc_id, [handler = std::move(handler)]() mutable {
          spdlog::critical("hook callback");
          return handler(std::nullopt);
        });
      }
      std::string content = hook_.hook_action_param2();
      spdlog::critical("hook upserting id {} with content {}", doc_id.key(), content);
      return conn_->upsert_doc(doc_id, content, [handler = std::move(handler)]() mutable {
        spdlog::critical("hook callback");
        return handler(std::nullopt);
      });
    }
    case protocol::hooks::transactions::HookAction::FAIL_ATR_FULL:
      return handler(couchbase::core::transactions::FAIL_ATR_FULL);

    case protocol::hooks::transactions::HookAction::BLOCK:
      std::this_thread::sleep_for(std::chrono::milliseconds(std::stoi(hook_.hook_action_param1())));
      return handler(std::nullopt);

    default:
      spdlog::warn("HookAction {} not implemented yet", HookAction_Name(hook_.hook_action()));
  }
  return handler(std::nullopt);
}

bool
fit_cxx::TxnSvcHook::should_fire(
  std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
  std::optional<const std::string> doc_id,
  std::optional<const std::string> stage)
{
  auto ctx_impl =
    std::dynamic_pointer_cast<couchbase::core::transactions::attempt_context_impl>(ctx);
  switch (hook_.hook_condition()) {
    case protocol::hooks::transactions::HookCondition::ALWAYS:
      return true;
    case protocol::hooks::transactions::HookCondition::ON_CALL:
      return hook_.hook_condition_param1() == calls_;
    case protocol::hooks::transactions::HookCondition::WHILE_NOT_EXPIRED:
      if (!ctx) {
        throw performer_exception::internal("WHILE_NOT_EXPIRED condition in cleanup hook!");
      }
      return !ctx_impl->has_expired_client_side("hook-check", doc_id);
    case protocol::hooks::transactions::HookCondition::WHILE_EXPIRED:
      if (!ctx) {
        throw performer_exception::internal("WHILE_EXPIRED condition in cleanup hook!");
      }
      return ctx_impl->has_expired_client_side("hook-check", doc_id);
    case protocol::hooks::transactions::HookCondition::ON_CALL_LE:
      return calls_ <= hook_.hook_condition_param1();
    case protocol::hooks::transactions::HookCondition::ON_CALL_GE:
      return calls_ >= hook_.hook_condition_param1();
    case protocol::hooks::transactions::HookCondition::EQUALS:
      return *doc_id == hook_.hook_condition_param2();
    case protocol::hooks::transactions::HookCondition::ON_CALL_AND_EQUALS:
      if (*doc_id == hook_.hook_condition_param2()) {
        return calls_ == hook_.hook_condition_param1();
      }
      // if docid doesn't match the param, we shouldn't count this as a call
      --calls_;
      return false;
    case protocol::hooks::transactions::HookCondition::EQUALS_BOTH:
      // NOTE: this only applies to HAS_EXPIRED
      if (hook_.hook_point() == protocol::hooks::transactions::HookPoint::HAS_EXPIRED) {
        if (has_expired_) {
          return true;
        }
        if (doc_id) {
          if (stage) {
            has_expired_ =
              (*doc_id == hook_.hook_condition_param2() && *stage == hook_.hook_condition_param3());
          } else {
            spdlog::warn("expected stage, but was empty");
          }
        } else {
          spdlog::warn("expected doc id, but was empty");
        }

      } else {
        spdlog::warn("hook condition EQUALS_BOTH only applies to hook point HAS_EXPIRED, not {}",
                     HookPoint_Name(hook_.hook_point()));
      }
      return has_expired_;
    default:
      spdlog::warn("unexpected hook condition {}", HookCondition_Name(hook_.hook_condition()));
  }
  return false;
}
