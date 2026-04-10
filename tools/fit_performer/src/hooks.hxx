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

#pragma once

#include "hooks.hxx"
#include "performer.pb.h"

#include <core/meta/features.hxx>
#include <core/transactions.hxx>
#include <core/transactions/exceptions.hxx>
#include <core/transactions/internal/exceptions_internal.hxx>
#include <core/utils/movable_function.hxx>

#include <spdlog/spdlog.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace fit_cxx
{
class Connection;

using test_hook_pair =
  std::pair<std::shared_ptr<couchbase::core::transactions::attempt_context_testing_hooks>,
            std::shared_ptr<couchbase::core::transactions::cleanup_testing_hooks>>;

class TxnSvcHook
{
private:
  ::protocol::hooks::transactions::Hook hook_;
  std::shared_ptr<Connection> conn_;
  std::shared_ptr<couchbase::core::transactions::attempt_context_testing_hooks> txn_hooks_;
  std::shared_ptr<couchbase::core::transactions::cleanup_testing_hooks> cleanup_hooks_;
  std::mutex mutex_;
  int calls_{ 0 };
  bool has_expired_{ false };
  std::string bucket_name_{}; // used for client_record hooks

public:
  static test_hook_pair convert_hooks(
    google::protobuf::RepeatedPtrField<::protocol::hooks::transactions::Hook> hooks,
    std::vector<TxnSvcHook>& hook_vector,
    std::shared_ptr<Connection> conn);

  static test_hook_pair clone_hooks(std::vector<TxnSvcHook>& hook_vector,
                                    std::vector<TxnSvcHook>& cloned_hook_vector,
                                    std::shared_ptr<Connection> conn);

  void update_conn(std::shared_ptr<Connection> conn);

  void reset();

  explicit TxnSvcHook(
    ::protocol::hooks::transactions::Hook hook,
    std::shared_ptr<couchbase::core::transactions::attempt_context_testing_hooks> txn_hooks,
    std::shared_ptr<couchbase::core::transactions::cleanup_testing_hooks> cleanup_hooks,
    std::shared_ptr<Connection> conn);

  ~TxnSvcHook()
  {
    spdlog::trace("TxnSvcHook destructor called {}", this->hook_.DebugString());
  }

  // just copy the hook and conn.
  TxnSvcHook(const TxnSvcHook& other)
    : hook_(other.hook_)
    , conn_(other.conn_)
  {
  }

  void hook_fn1(
    std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  void hook_fn2(
    std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
    const std::string& doc_id,
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  void hook_fn5(
    const std::string& id,
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  void hook_fn6(
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  void hook_fn7(
    const std::string& bucket_name,
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  void fire_action(
    std::function<void(std::optional<couchbase::core::transactions::error_class>)>&& handler);

  std::optional<const std::string> hook_fn3(
    std::shared_ptr<couchbase::core::transactions::attempt_context> ctx);

  bool hook_fn4(std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
                const std::string& stage,
                std::optional<const std::string> doc_id);

  std::optional<const std::string> fire_action_return_string();

  bool should_fire(std::shared_ptr<couchbase::core::transactions::attempt_context> ctx,
                   std::optional<const std::string> doc_id = std::nullopt,
                   std::optional<const std::string> stage = std::nullopt);
};

using HookVector = std::vector<TxnSvcHook>;
} // namespace fit_cxx
