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

#include <string>
#include <vector>

namespace couchbase::core
{
struct log_entry {
  std::string timestamp;
  std::string severity;
  std::string message;

  struct {
    std::string trace_id{};
    std::string span_id{};
  } context{};

  std::vector<signal_attribute> attributes{};
};

auto
to_string(const log_entry& data) -> std::string;

inline auto
operator==(const log_entry& lhs, const log_entry& rhs) -> bool
{
  return lhs.timestamp == rhs.timestamp &&               //
         lhs.severity == rhs.severity &&                 //
         lhs.message == rhs.message &&                   //
         lhs.context.trace_id == rhs.context.trace_id && //
         lhs.context.span_id == rhs.context.span_id &&   //
         lhs.attributes == rhs.attributes;
}

inline auto
operator!=(const log_entry& lhs, const log_entry& rhs) -> bool
{
  return !(lhs == rhs);
}
} // namespace couchbase::core
