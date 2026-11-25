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

#include "token_bucket.hxx"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace cbc
{
/**
 * The lowest rate an overload cut can reach: --operation-batch-size operations per second, or the
 * ceiling when that is lower, since a floor above the ceiling is invalid.
 */
inline auto
throttle_floor(std::size_t batch_size, std::optional<double> ceiling) -> double
{
  return std::min(static_cast<double>(batch_size), ceiling.value_or(token_bucket::unlimited));
}

/**
 * The bucket's capacity, which bounds a burst. A batch worker takes a whole batch at once, and a
 * population batch holds up to the larger of `batch_size` and `minimum_population_batch` keys. A
 * pipeline takes one token per operation, and its window already bounds a worker's burst to one
 * batch; the same capacity keeps the tokens that accrue while a deferred send's timer is late.
 */
inline auto
throttle_capacity(std::size_t batch_size, std::size_t minimum_population_batch) -> std::size_t
{
  return std::max(batch_size, minimum_population_batch);
}

/// `granted` tokens over `elapsed`, per second. Zero when no time elapsed, since no rate was seen.
inline auto
admitted_rate(std::uint64_t granted, std::chrono::duration<double> elapsed) -> double
{
  return elapsed.count() > 0 ? static_cast<double>(granted) / elapsed.count() : 0.0;
}
} // namespace cbc
