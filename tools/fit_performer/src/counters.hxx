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
#include <atomic>
#include <map>
#include <string>

class Counters
{
private:
  std::map<std::string, std::atomic_uint32_t> counters_;

public:
  std::uint32_t get_and_increment_counter(std::string counter_id)
  {
    auto it = counters_.find(counter_id);
    if (it == counters_.end()) {
      auto v = std::atomic_uint(0);
      counters_.emplace(
        std::piecewise_construct, std::forward_as_tuple(counter_id), std::forward_as_tuple(0));
      return 0;
    }
    return it->second;
  }
};
