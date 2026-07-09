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

#include "counters.hxx"

#include <mutex>
#include <shared_mutex>

#include "exceptions.hxx"

namespace fit_cxx
{
counter::counter(const std::int32_t initial_value)
  : value_{ initial_value }
{
}

auto
counter::increment() -> std::int32_t
{
  return ++value_;
}

auto
counter::decrement() -> std::int32_t
{
  return --value_;
}

void
counter::set(const std::int32_t value)
{
  value_ = value;
}

auto
counter::get() const -> std::int32_t
{
  return value_.load();
}

auto
counter::create(const std::int32_t initial_value) -> std::shared_ptr<counter>
{
  return std::make_shared<counter>(initial_value);
}

void
counters::clear()
{
  const std::unique_lock lock{ mutex_ };
  counters_.clear();
}

auto
counters::get_counter(const std::string& counter_id, std::int32_t initial_value)
  -> std::shared_ptr<counter>
{
  {
    const std::shared_lock lock{ mutex_ };
    if (const auto& it = counters_.find(counter_id); it != counters_.end()) {
      return it->second;
    }
  }
  {
    const std::unique_lock lock{ mutex_ };
    const auto& [it, _] =
      counters_.try_emplace(counter_id, std::make_shared<counter>(initial_value));
    return it->second;
  }
}

auto
counters::get_counter(const protocol::shared::Counter& proto_counter) -> std::shared_ptr<counter>
{
  if (!proto_counter.has_global()) {
    throw performer_exception::invalid_argument("counter type not recognized: " +
                                                proto_counter.DebugString());
  }
  return get_counter(proto_counter.counter_id(), proto_counter.global().count());
}

void
counters::set_counter_value(const std::string& counter_id, const std::int32_t value)
{
  const auto counter = get_counter(counter_id, value);
  counter->set(value);
}

void
counters::set_counter_value(const protocol::shared::Counter& proto_counter)
{
  if (!proto_counter.has_global()) {
    throw performer_exception::invalid_argument("counter type not recognized: " +
                                                proto_counter.DebugString());
  }
  set_counter_value(proto_counter.counter_id(), proto_counter.global().count());
}

} // namespace fit_cxx
