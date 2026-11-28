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

#include <array>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
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

/// Parses "G:R:D:I:Q:U". A positional term is read up to its first non-digit; one that is missing,
/// empty, does not start with a digit or overflows leaves its weight unchanged, so a purely
/// positional five-term ratio has no upserts and "-1" cannot wrap to a huge weight. A term may
/// instead name its operation by first letter, case-insensitive ("G=1:U=1"); then every weight not
/// given is 0, and a positional term sets the weight after the previous term. A positional term
/// past the Upsert slot is ignored. Throws std::invalid_argument for a named term with an unknown
/// letter or a value that is not a whole number, wherever it stands.
inline auto
parse_operation_weights(const std::string& input) -> operation_weights
{
  operation_weights weights;
  if (input.find('=') != std::string::npos) {
    weights = { 0, 0, 0, 0, 0, 0 };
  }
  const std::array<std::size_t*, 6> terms{ &weights.gets,    &weights.replaces, &weights.deletes,
                                           &weights.inserts, &weights.queries,  &weights.upserts };
  constexpr std::string_view letters{ "GRDIQU" };
  std::stringstream is(input);
  std::string token;
  std::size_t position{ 0 };
  while (std::getline(is, token, ':')) {
    if (const auto eq = token.find('='); eq != std::string::npos) {
      const auto letter =
        eq == 1
          ? letters.find(static_cast<char>(std::toupper(static_cast<unsigned char>(token[0]))))
          : std::string_view::npos;
      std::size_t value{};
      const auto* first = token.data() + eq + 1;
      const auto* last = token.data() + token.size();
      const auto [end, ec] = std::from_chars(first, last, value);
      if (letter == std::string_view::npos || first == last || ec != std::errc{} || end != last) {
        throw std::invalid_argument("--operation-ratio term \"" + token +
                                    "\" is not one of G, R, D, I, Q or U followed by "
                                    "=<whole number>");
      }
      position = letter;
      *terms[position++] = value;
      continue;
    }
    if (position >= terms.size()) {
      continue;
    }
    std::size_t value{};
    if (std::from_chars(token.data(), token.data() + token.size(), value).ec == std::errc{}) {
      *terms[position] = value;
    }
    ++position;
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
