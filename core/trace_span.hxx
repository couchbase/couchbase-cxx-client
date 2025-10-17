/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Current Couchbase, Inc.
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

#include "signal_attribute.hxx"
#include "trace_event.hxx"

#include <chrono>
#include <string>
#include <vector>

namespace couchbase::core
{
struct trace_span {
  std::string name;

  struct {
    std::string trace_id{};
    std::string span_id{};
  } context{};

  std::string parent_id{};

  std::chrono::system_clock::time_point start_time{};
  std::chrono::system_clock::time_point end_time{};

  std::vector<signal_attribute> attributes{};
  std::vector<trace_event> events{};
};

auto
to_string(const trace_span& data) -> std::string;

inline auto
operator==(const trace_span& lhs, const trace_span& rhs) -> bool
{
  return lhs.name == rhs.name &&                         //
         lhs.context.trace_id == rhs.context.trace_id && //
         lhs.context.span_id == rhs.context.span_id &&   //
         lhs.parent_id == rhs.parent_id &&               //
         lhs.start_time == rhs.start_time &&             //
         lhs.end_time == rhs.end_time &&                 //
         lhs.attributes == rhs.attributes &&             //
         lhs.events == rhs.events;
}

inline auto
operator!=(const trace_span& lhs, const trace_span& rhs) -> bool
{
  return !(lhs == rhs);
}

} // namespace couchbase::core
