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
class concurrent_fixed_queue
{
private:
  std::mutex mutex_;
  std::priority_queue<T> data_;
  std::size_t dropped_count_{ 0 };
  std::size_t capacity_{};

public:
  using size_type = typename std::priority_queue<T>::size_type;

  explicit concurrent_fixed_queue(std::size_t capacity)
    : capacity_(capacity)
  {
  }

  concurrent_fixed_queue(const concurrent_fixed_queue&) = delete;
  concurrent_fixed_queue(concurrent_fixed_queue&&) = delete;
  auto operator=(const concurrent_fixed_queue&) -> concurrent_fixed_queue& = delete;
  auto operator=(concurrent_fixed_queue&&) -> concurrent_fixed_queue& = delete;
  ~concurrent_fixed_queue() = default;

  void pop()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    data_.pop();
  }

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
    // TODO(CXXCBC-732): We have a bug here where the remaining N items in the queue are the last
    // N items to be emplaced, irrespective of how they compare to the items currently in the
    // queue. This means that the oprhan reporter & the threshold logging tracer aren't really
    // reporting the top requests, but the last ones to be added to this queue.
    const std::unique_lock<std::mutex> lock(mutex_);
    data_.emplace(std::forward<const T>(item));
    if (data_.size() > capacity_) {
      data_.pop();
      ++dropped_count_;
    }
  }

  /**
   * Clears the internal queue, and returns the data along with the number of items that have been
   * dropped.
   */
  auto steal_data() -> std::pair<std::priority_queue<T>, std::size_t>
  {
    std::priority_queue<T> data;
    std::size_t dropped_count{};

    const std::unique_lock<std::mutex> lock(mutex_);

    std::swap(data, data_);
    std::swap(dropped_count, dropped_count_);

    return std::make_pair(std::move(data), dropped_count);
  }
};
} // namespace couchbase::core::utils
