/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025 Couchbase, Inc.
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

#include "core/impl/get_replica.hxx"
#include "core/impl/lookup_in_replica.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/operation_traits.hxx"
#include "core/utils/movable_function.hxx"

#include <mutex>

namespace couchbase::core
{
namespace impl
{
class cancellation_token
{
public:
  void setup(utils::movable_function<void()> cancel_fn)
  {
    const std::lock_guard lock(mutex_);
    if (cancelled_) {
      cancel_fn();
      return;
    }
    cancel_fn_ = std::move(cancel_fn);
  }

  void cancel()
  {
    utils::movable_function<void()> fn{};
    {
      const std::lock_guard lock(mutex_);
      cancelled_ = true;
      fn = std::move(cancel_fn_);
    }
    if (fn) {
      fn();
    }
  }

private:
  std::mutex mutex_{};
  utils::movable_function<void()> cancel_fn_{};
  bool cancelled_{ false };
};

template<typename kv_operation>
struct with_cancellation : public kv_operation {
  const std::shared_ptr<cancellation_token> cancel_token{ std::make_shared<cancellation_token>() };
};
} // namespace impl

template<typename kv_operation>
struct operations::is_cancellable_operation<impl::with_cancellation<kv_operation>>
  : public std::true_type {
};
} // namespace couchbase::core
