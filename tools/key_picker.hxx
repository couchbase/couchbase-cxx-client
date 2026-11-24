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
#include <optional>
#include <random>

namespace cbc
{
/**
 * The index of a key drawn uniformly from a list of `size` keys, or nullopt when the list is empty.
 * Every index is below `size`, so a caller that passes a size no larger than its list never
 * indexes past the end.
 */
template<typename Generator>
auto
pick_known_key(std::size_t size, Generator& gen) -> std::optional<std::size_t>
{
  if (size == 0) {
    return std::nullopt;
  }
  return std::uniform_int_distribution<std::size_t>(0, size - 1)(gen);
}
} // namespace cbc
