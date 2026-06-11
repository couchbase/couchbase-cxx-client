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
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

class Counters
{
private:
  std::mutex mutex_;
  std::map<std::string, std::uint32_t> counters_;

public:
  // Returns the current value of the counter and then increments it, so the
  // first call for a given id returns 0, the next returns 1, and so on.
  std::uint32_t get_and_increment_counter(const std::string& counter_id)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    return counters_[counter_id]++;
  }
};
