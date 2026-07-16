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

#include "shared.bounds.pb.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace fit_cxx
{
class counter
{
public:
  explicit counter(std::int32_t initial_value);

  /**
   * Increments the counter and returns the resulting value
   */
  [[nodiscard]] auto increment() -> std::int32_t;

  /**
   * Decrements the counter and returns the resulting value
   */
  [[nodiscard]] auto decrement() -> std::int32_t;

  void set(std::int32_t value);

  [[nodiscard]] auto get() const -> std::int32_t;

  [[nodiscard]] static auto create(std::int32_t initial_value) -> std::shared_ptr<counter>;

private:
  std::atomic<std::int32_t> value_;
};

class counters
{
public:
  counters() = default;

  void clear();

  /**
   * Returns the counter with the provided counter_id. If the counter does not exist,
   * the counter is created with the provided initial_value and returned.
   */
  [[nodiscard]] auto get_counter(const std::string& counter_id, std::int32_t initial_value)
    -> std::shared_ptr<counter>;

  [[nodiscard]] auto get_counter(const protocol::shared::Counter& proto_counter)
    -> std::shared_ptr<counter>;

  void set_counter_value(const std::string& counter_id, std::int32_t value);

  void set_counter_value(const protocol::shared::Counter& proto_counter);

private:
  std::shared_mutex mutex_;
  std::map<std::string, std::shared_ptr<counter>> counters_{};
};
} // namespace fit_cxx
