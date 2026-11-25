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

#include <algorithm>
#include <array>
#include <cassert>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace cbc
{
/**
 * Keys of the form prefix + the index as 20 zero-padded decimal digits, for every index
 * in [start, start + count): the names the C pillowfight gives its documents.
 * The C pillowfight formats the index with %020d; below max_end, %020d and the
 * unsigned digits written here are the same bytes.
 */
class key_range
{
public:
  // At or above 2^31, %020d prints the index as a negative number, which names
  // a different document.
  static constexpr std::uint64_t max_end{ 1ULL << 31 };
  static constexpr std::size_t index_width{ 20 };
  // The server limits a logical key to 250 bytes in the default collection and 246 in any
  // other, and the index takes index_width digits of it.
  static constexpr std::size_t max_key_size{ 250 };
  static constexpr std::size_t max_named_collection_key_size{ 246 };
  static constexpr std::size_t max_prefix_size{ max_key_size - index_width };

  [[nodiscard]] static constexpr auto max_prefix_size_for(bool default_collection) -> std::size_t
  {
    return (default_collection ? max_key_size : max_named_collection_key_size) - index_width;
  }

  // Whether [start, start + count) ends at or below max_end.
  [[nodiscard]] static constexpr auto fits(std::uint32_t start, std::uint32_t count) -> bool
  {
    return std::uint64_t{ start } + count <= max_end;
  }

  // Whether the keys go to _default._default, the only collection allowed the full key length.
  [[nodiscard]] static auto is_default_collection(std::string_view scope,
                                                  std::string_view collection) -> bool
  {
    return scope == "_default" && collection == "_default";
  }

  // Why a keyset given as prefix, start and count is unusable, or nullopt when it is usable.
  // default_collection is whether the keys are stored in _default._default.
  [[nodiscard]] static auto validate(const std::string& prefix,
                                     std::uint32_t start,
                                     std::uint32_t count,
                                     bool default_collection) -> std::optional<std::string>
  {
    if (count == 0) {
      return "--number-of-items must be greater than zero";
    }
    if (!fits(start, count)) {
      return "--start-at + --number-of-items must not exceed " + std::to_string(max_end) +
             ": larger indices are incompatible with the C pillowfight's key format";
    }
    if (const auto limit = max_prefix_size_for(default_collection); prefix.size() > limit) {
      return "--key-prefix must be at most " + std::to_string(limit) +
             " bytes: a key is the prefix and " + std::to_string(index_width) +
             " digits, and the server rejects keys longer than " +
             std::to_string(limit + index_width) + " bytes in " +
             (default_collection ? "the default collection" : "a named collection");
    }
    return std::nullopt;
  }

  key_range(std::string prefix, std::uint32_t start, std::uint32_t count)
    : prefix_{ std::move(prefix) }
    , start_{ start }
    , count_{ count }
  {
    if (!fits(start, count)) {
      throw std::invalid_argument("key_range end exceeds max_end");
    }
    if (prefix_.size() > max_prefix_size) {
      throw std::invalid_argument("key_range prefix exceeds max_prefix_size");
    }
  }

  // index is absolute, in [start, start + count).
  [[nodiscard]] auto key_at(std::uint32_t index) const -> std::string
  {
    assert(index - start_ < count_);
    std::string key(prefix_.size() + index_width, '0');
    std::copy(prefix_.begin(), prefix_.end(), key.begin());
    std::array<char, index_width> digits{};
    auto* end = std::to_chars(digits.data(), digits.data() + index_width, index).ptr;
    std::copy(digits.data(), end, key.end() - (end - digits.data()));
    return key;
  }

  // Contiguous slices that cover the range exactly once; the remainder of
  // count / workers goes to the first workers, and a slice may be empty.
  // workers must be greater than zero.
  [[nodiscard]] auto slice(std::size_t worker, std::size_t workers) const -> key_range
  {
    const auto base = count_ / workers;
    const auto remainder = count_ % workers;
    const auto offset = worker * base + std::min(worker, remainder);
    const auto size = base + (worker < remainder ? 1 : 0);
    return { prefix_,
             static_cast<std::uint32_t>(start_ + offset), // fits: start_ + count_ <= max_end
             static_cast<std::uint32_t>(size) };
  }

  // A key drawn uniformly from the range, which must not be empty.
  template<typename Generator>
  [[nodiscard]] auto pick(Generator& gen) const -> std::string
  {
    return key_at(std::uniform_int_distribution<std::uint32_t>(start_, start_ + count_ - 1)(gen));
  }

  [[nodiscard]] auto prefix() const -> const std::string&
  {
    return prefix_;
  }

  [[nodiscard]] auto start() const -> std::uint32_t
  {
    return start_;
  }

  [[nodiscard]] auto count() const -> std::uint32_t
  {
    return count_;
  }

private:
  std::string prefix_;
  std::uint32_t start_;
  std::uint32_t count_;
};
} // namespace cbc
