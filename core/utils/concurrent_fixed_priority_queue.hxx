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

#include <mutex>
#include <queue>

namespace couchbase::core::utils
{
template<typename T>
class concurrent_fixed_priority_queue
{
private:
  std::mutex mutex_;
  std::priority_queue<T, std::vector<T>, std::greater<T>> data_;
  std::size_t dropped_count_{ 0 };
  std::size_t capacity_{};

public:
  using size_type = typename std::priority_queue<T, std::vector<T>, std::greater<T>>::size_type;

  explicit concurrent_fixed_priority_queue(std::size_t capacity)
    : capacity_(capacity)
  {
  }

  concurrent_fixed_priority_queue(const concurrent_fixed_priority_queue&) = delete;
  concurrent_fixed_priority_queue(concurrent_fixed_priority_queue&&) = delete;
  auto operator=(const concurrent_fixed_priority_queue&)
    -> concurrent_fixed_priority_queue& = delete;
  auto operator=(concurrent_fixed_priority_queue&&) -> concurrent_fixed_priority_queue& = delete;
  ~concurrent_fixed_priority_queue() = default;

  auto size() -> size_type
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return data_.size();
  }

  auto empty() -> bool
  {
    const std::unique_lock<std::mutex> lock(mutex_);
    return data_.empty();
  }

  void emplace(const T&& item)
  {
    const std::unique_lock<std::mutex> lock(mutex_);

    if (data_.size() < capacity_) {
      data_.emplace(std::forward<const T>(item));
    } else {
      // We need to either drop the new item, or an existing item
      ++dropped_count_;
      if (item > data_.top()) {
        // The new item is greater than the smallest item, so we will replace the smallest with the
        // new item
        data_.pop();
        data_.emplace(std::forward<const T>(item));
      }
    }
  }

  /**
   * Clears the internal queue, and returns the data along with the number of items that have been
   * dropped.
   */
  auto steal_data() -> std::pair<std::priority_queue<T>, std::size_t>
  {
    std::priority_queue<T, std::vector<T>, std::greater<T>> reversed_data;
    std::size_t dropped_count{};
    {
      const std::unique_lock<std::mutex> lock(mutex_);
      std::swap(reversed_data, data_);
      std::swap(dropped_count, dropped_count_);
    }

    std::priority_queue<T> data{};
    while (!reversed_data.empty()) {
      data.emplace(std::move(reversed_data.top()));
      reversed_data.pop();
    }

    return std::make_pair(std::move(data), dropped_count);
  }
};
} // namespace couchbase::core::utils
