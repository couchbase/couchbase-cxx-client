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

#include <charconv>
#include <cstddef>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

namespace cbc
{
struct operation_weights {
  std::size_t gets{ 1 };
  std::size_t replaces{ 1 };
  std::size_t deletes{ 0 };
  std::size_t inserts{ 0 };
  std::size_t queries{ 0 };
  std::size_t upserts{ 0 };

  [[nodiscard]] auto to_string() const -> std::string
  {
    return std::to_string(gets) + ":" + std::to_string(replaces) + ":" + std::to_string(deletes) +
           ":" + std::to_string(inserts) + ":" + std::to_string(queries) + ":" +
           std::to_string(upserts);
  }

  [[nodiscard]] auto to_vector() const -> std::vector<std::size_t>
  {
    return { gets, replaces, deletes, inserts, queries, upserts };
  }
};

/// Parses "G:R:D:I:Q:U". A term is read up to its first non-digit; one that is missing, empty,
/// does not start with a digit or overflows keeps its default, so a five-term ratio has no
/// upserts and "-1" cannot wrap to a huge weight.
inline auto
parse_operation_weights(const std::string& input) -> operation_weights
{
  operation_weights weights;
  std::stringstream is(input);
  std::string token;
  for (auto* term : { &weights.gets,
                      &weights.replaces,
                      &weights.deletes,
                      &weights.inserts,
                      &weights.queries,
                      &weights.upserts }) {
    if (!std::getline(is, token, ':')) {
      break;
    }
    std::size_t value{};
    if (std::from_chars(token.data(), token.data() + token.size(), value).ec == std::errc{}) {
      *term = value;
    }
  }
  return weights;
}

/// Whether a workload with these weights keeps every key: it neither deletes nor inserts. In
/// keyset mode an upsert, like a replace, writes a key inside the keyset.
[[nodiscard]] inline auto
keeps_every_key(const operation_weights& weights) -> bool
{
  return weights.deletes == 0 && weights.inserts == 0;
}
} // namespace cbc
