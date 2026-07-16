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

#include "bounds.hxx"

#include <spdlog/fmt/fmt.h>

#include <optional>

namespace fit_cxx
{
namespace
{
auto
get_initial_counter_value(const protocol::shared::Counter& proto_counter) -> std::int32_t
{
  if (!proto_counter.has_global()) {
    throw performer_exception::invalid_argument(
      fmt::format("unknown counter type: {}", proto_counter.DebugString()));
  }

  return proto_counter.global().count();
}

auto
create_bounds(counters& global_counters,
              const std::optional<protocol::shared::Bounds>& proto_bounds,
              const std::size_t command_count) -> std::unique_ptr<bounds>
{
  if (!proto_bounds.has_value()) {
    return std::make_unique<counter_bounds>(static_cast<std::int32_t>(command_count));
  }

  switch (proto_bounds->bounds_case()) {
    case protocol::shared::Bounds::kCounter:
      return std::make_unique<counter_bounds>(global_counters.get_counter(proto_bounds->counter()));
    case protocol::shared::Bounds::kForTime:
      return std::make_unique<timer_bounds>(
        std::chrono::seconds(proto_bounds->for_time().seconds()));
    case protocol::shared::Bounds::kCounterEq: {
      const auto initial_value = get_initial_counter_value(proto_bounds->counter_eq());
      return std::make_unique<counter_equality_bounds>(
        global_counters.get_counter(proto_bounds->counter_eq()), initial_value);
    }
    case protocol::shared::Bounds::BOUNDS_NOT_SET:
      // A present-but-empty bounds message means "no bounds": execute each command exactly once.
      return std::make_unique<counter_bounds>(static_cast<std::int32_t>(command_count));

    default:
      throw performer_exception::invalid_argument(
        fmt::format("unknown bounds type: {}", proto_bounds->DebugString()));
  }
}
} // namespace

auto
bounds::from_sdk_workload(counters& global_counters, const protocol::sdk::Workload& proto_workload)
  -> std::unique_ptr<bounds>
{
  return create_bounds(global_counters,
                       proto_workload.has_bounds() ? std::make_optional(proto_workload.bounds())
                                                   : std::nullopt,
                       proto_workload.command_size());
}

auto
bounds::from_transactions_workload(counters& global_counters,
                                   const protocol::transactions::Workload& proto_workload)
  -> std::unique_ptr<bounds>
{
  return create_bounds(global_counters,
                       proto_workload.has_bounds() ? std::make_optional(proto_workload.bounds())
                                                   : std::nullopt,
                       proto_workload.command_size());
}

counter_bounds::counter_bounds(const std::int32_t initial_value)
  : counter_{ counter::create(initial_value) }
{
}

counter_bounds::counter_bounds(std::shared_ptr<counter> global_counter)
  : counter_{ std::move(global_counter) }
{
}

auto
counter_bounds::can_execute() -> bool
{
  return counter_->decrement() >= 0;
}

timer_bounds::timer_bounds(const std::chrono::seconds duration)
  : deadline_{ std::chrono::steady_clock::now() + duration }
{
}

auto
timer_bounds::can_execute() -> bool
{
  return std::chrono::steady_clock::now() <= deadline_;
}

counter_equality_bounds::counter_equality_bounds(std::shared_ptr<counter> global_counter,
                                                 const std::int32_t target_value)
  : counter_{ std::move(global_counter) }
  , target_value_{ target_value }
{
}

auto
counter_equality_bounds::can_execute() -> bool
{
  return counter_->get() == target_value_;
}
} // namespace fit_cxx
