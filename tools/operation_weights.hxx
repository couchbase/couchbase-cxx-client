/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <cstddef>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace cbc
{
struct operation_weights {
  std::size_t gets{ 1 };
  std::size_t replaces{ 1 };
  std::size_t deletes{ 0 };
  std::size_t inserts{ 0 };
  std::size_t queries{ 0 };

  [[nodiscard]] auto to_string() const -> std::string
  {
    return std::to_string(gets) + ":" + std::to_string(replaces) + ":" + std::to_string(deletes) +
           ":" + std::to_string(inserts) + ":" + std::to_string(queries);
  }

  [[nodiscard]] auto to_vector() const -> std::vector<std::size_t>
  {
    return { gets, replaces, deletes, inserts, queries };
  }
};

/// Parses "G:R:D:I:Q". A term that is missing or not a number keeps its default.
inline auto
parse_operation_weights(const std::string& input) -> operation_weights
{
  operation_weights weights;
  std::stringstream is(input);
  std::string token;
  for (auto* term :
       { &weights.gets, &weights.replaces, &weights.deletes, &weights.inserts, &weights.queries }) {
    if (!std::getline(is, token, ':')) {
      break;
    }
    try {
      *term = std::stoull(token);
    } catch (const std::invalid_argument&) {
      /* keep the default */
    } catch (const std::out_of_range&) {
      /* keep the default */
    }
  }
  return weights;
}

/// Whether a workload with these weights keeps every key: it neither deletes nor inserts.
[[nodiscard]] inline auto
keeps_every_key(const operation_weights& weights) -> bool
{
  return weights.deletes == 0 && weights.inserts == 0;
}
} // namespace cbc
