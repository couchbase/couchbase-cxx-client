/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/retry_reason.hxx>
#include <couchbase/retry_strategy.hxx>

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>

namespace couchbase::core::io
{

template<bool is_idempotent>
class retry_context : public retry_request
{
public:
  retry_context(std::shared_ptr<retry_strategy> strategy = nullptr, std::string identifier = {})
    : identifier_{ std::move(identifier) }
    , strategy_{ std::move(strategy) }
  {
  }

  [[nodiscard]] auto identifier() const -> std::string override
  {
    return identifier_;
  }

  [[nodiscard]] auto idempotent() const -> bool override
  {
    return is_idempotent;
  }

  [[nodiscard]] auto strategy() const -> std::shared_ptr<retry_strategy>
  {
    return strategy_;
  }

  // Copies take the counts as they are at the time of the copy. Copy assignment stays deleted by
  // the const members.
  retry_context(const retry_context& other)
    : retry_request(other)
    , identifier_{ other.identifier_ }
    , strategy_{ other.strategy_ }
    , retry_attempts_{ other.retry_attempts_.load(std::memory_order_acquire) }
    , reason_mask_{ other.reason_mask_.load(std::memory_order_acquire) }
  {
  }

  [[nodiscard]] auto retry_attempts() const -> std::size_t override
  {
    return retry_attempts_.load(std::memory_order_acquire);
  }

  // A reader that observes an attempt also observes the reason recorded with it: the reason is
  // set before the release increment that the acquire load of the count pairs with.
  [[nodiscard]] auto retry_reasons() const -> std::set<retry_reason> override
  {
    (void)retry_attempts_.load(std::memory_order_acquire);
    std::uint64_t mask = reason_mask_.load(std::memory_order_acquire);
    std::set<retry_reason> reasons{};
    for (unsigned bit = 0; mask != 0; ++bit, mask >>= 1U) {
      if ((mask & 1U) != 0) {
        reasons.insert(static_cast<retry_reason>(bit));
      }
    }
    return reasons;
  }

  void record_retry_attempt(retry_reason reason) override
  {
    reason_mask_.fetch_or(bit_of(reason), std::memory_order_relaxed);
    retry_attempts_.fetch_add(1, std::memory_order_release);
  }

  void add_reason(retry_reason reason)
  {
    reason_mask_.fetch_or(bit_of(reason), std::memory_order_release);
  }

private:
  // views_no_active_partition is the last retry_reason enumerator. A new one must be added to this
  // check and to the round-trip test.
  static_assert(static_cast<std::size_t>(retry_reason::views_no_active_partition) < 64,
                "every retry_reason must fit in reason_mask_");

  static constexpr auto bit_of(retry_reason reason) -> std::uint64_t
  {
    return std::uint64_t{ 1 } << static_cast<unsigned>(reason);
  }

  const std::string identifier_;
  const std::shared_ptr<retry_strategy> strategy_;
  std::atomic<std::size_t> retry_attempts_{ 0 };
  std::atomic<std::uint64_t> reason_mask_{ 0 };
};

} // namespace couchbase::core::io
