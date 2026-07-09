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

#include "counters.hxx"
#include "exceptions.hxx"

#include <spdlog/spdlog.h>

#include <chrono>

#include "sdk.workload.pb.h"
#include "transactions.workload.pb.h"

namespace fit_cxx
{
class bounds
{
public:
  virtual ~bounds() = default;
  [[nodiscard]] virtual auto can_execute() -> bool = 0;

  static auto from_sdk_workload(counters& global_counters,
                                const protocol::sdk::Workload& proto_workload)
    -> std::unique_ptr<bounds>;
  static auto from_transactions_workload(counters& global_counters,
                                         const protocol::transactions::Workload& proto_workload)
    -> std::unique_ptr<bounds>;
};

class counter_bounds final : public bounds
{
public:
  /**
   * Creates a counter_bounds instance with a local counter.
   */
  explicit counter_bounds(std::int32_t initial_value);

  /**
   * Creates a counter_bounds instances with the provided global counter, that may be shared by
   * multiple bounds instances.
   */
  explicit counter_bounds(std::shared_ptr<counter> global_counter);

  auto can_execute() -> bool override;

private:
  std::shared_ptr<counter> counter_;
};

class timer_bounds final : public bounds
{
public:
  explicit timer_bounds(std::chrono::seconds duration);

  auto can_execute() -> bool override;

private:
  std::chrono::steady_clock::time_point deadline_;
};

class counter_equality_bounds final : public bounds
{
public:
  explicit counter_equality_bounds(std::shared_ptr<counter> global_counter,
                                   std::int32_t target_value);

  auto can_execute() -> bool override;

private:
  std::shared_ptr<counter> counter_;
  std::int32_t target_value_;
};
} // namespace fit_cxx
