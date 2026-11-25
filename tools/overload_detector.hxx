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

#include <couchbase/retry_action.hxx>
#include <couchbase/retry_reason.hxx>
#include <couchbase/retry_request.hxx>
#include <couchbase/retry_strategy.hxx>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace cbc
{
/// Counts observed during one control window.
struct overload_window {
  std::uint64_t completions{};
  std::uint64_t slow{};
  std::uint64_t temporary_failure_retries{};
};

// A completion slower than this counts as slow.
inline constexpr std::chrono::milliseconds default_slow_threshold{ 500 };

/**
 * Collects the two overload signals: server-side temporary failures, seen as retries with reason
 * `key_value_temporary_failure`, and completions slower than a threshold. As a retry strategy it
 * only observes; every decision is the delegate's, returned unchanged.
 *
 * All members are safe to call concurrently. Counters are independent relaxed atomics, so an event
 * racing `take_window()` is counted in this window or the next, never lost.
 */
class overload_detector : public couchbase::retry_strategy
{
public:
  overload_detector(std::shared_ptr<couchbase::retry_strategy> delegate,
                    std::chrono::milliseconds slow_threshold)
    : delegate_{ std::move(delegate) }
    , slow_threshold_{ slow_threshold }
  {
    if (delegate_ == nullptr) {
      throw std::invalid_argument("overload_detector: the delegate must not be null");
    }
  }

  auto retry_after(const couchbase::retry_request& request, couchbase::retry_reason reason)
    -> couchbase::retry_action override
  {
    if (reason == couchbase::retry_reason::key_value_temporary_failure) {
      temporary_failure_retries_.fetch_add(1, std::memory_order_relaxed);
    }
    return delegate_->retry_after(request, reason);
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "overload_detector(" + delegate_->to_string() + ")";
  }

  /// Counts one completion, and a slow one when `latency` is strictly above the threshold.
  void record(std::chrono::nanoseconds latency)
  {
    completions_.fetch_add(1, std::memory_order_relaxed);
    if (latency > slow_threshold_) {
      slow_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  /// Returns the counts since the previous call and starts a new window.
  auto take_window() -> overload_window
  {
    return {
      completions_.exchange(0, std::memory_order_relaxed),
      slow_.exchange(0, std::memory_order_relaxed),
      temporary_failure_retries_.exchange(0, std::memory_order_relaxed),
    };
  }

private:
  std::shared_ptr<couchbase::retry_strategy> delegate_;
  std::chrono::nanoseconds slow_threshold_;
  std::atomic<std::uint64_t> completions_{ 0 };
  std::atomic<std::uint64_t> slow_{ 0 };
  std::atomic<std::uint64_t> temporary_failure_retries_{ 0 };
};
} // namespace cbc
