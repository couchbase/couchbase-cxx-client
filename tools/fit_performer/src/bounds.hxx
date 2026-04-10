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

#include "exceptions.hxx"

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include <map>
#include <mutex>
#include <optional>

namespace fit_cxx
{
class Bounds
{
public:
  Bounds() = default;

  void add_timer(int seconds)
  {
    std::lock_guard<std::mutex> lock(mut_);
    if (timer_) {
      return;
    }
    timer_ = std::chrono::system_clock::now() + std::chrono::seconds(seconds);
  }
  bool check_timer()
  {
    if (timer_) {
      return std::chrono::system_clock::now() <= timer_.value();
    }
    // if no timer, it can run forever, so return true.
    return true;
  }
  void add_counter(const std::string& key, int value)
  {
    std::lock_guard<std::mutex> lock(mut_);
    if (auto it = counters_.find(key); it == counters_.end()) {
      counters_[key] = value;
      return;
    }
    spdlog::info("add_bound found key {} already present, ignoring...", key);
  }

  bool decrement(const std::string& key)
  {
    if (key.empty()) {
      // happens when there is no bounds (or at least, not a counter)
      return true;
    }
    std::lock_guard<std::mutex> lock(mut_);
    if (auto it = counters_.find(key); it != counters_.end()) {
      if (it->second > 0) {
        it->second--;
        return true;
      }
      return false;
    }
    throw performer_exception::internal(fmt::format("decrement called for unknown key {}", key));
  }

  bool can_run(const std::string& counter_key, int executed_cmd_count, int cmd_count)
  {
    if (timer_) {
      return check_timer();
    } else if (!counter_key.empty()) {
      return decrement(counter_key);
    }
    // No bounds are set - all commands should be executed exactly once
    return executed_cmd_count < cmd_count;
  }

private:
  std::mutex mut_;
  std::map<std::string, int> counters_;
  std::optional<std::chrono::system_clock::time_point> timer_;
};

} // namespace fit_cxx
