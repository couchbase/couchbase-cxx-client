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
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace cbc
{
/**
 * Grants tokens at a configurable rate. At a finite rate it never holds more than `capacity` of
 * them, however long it sits idle, so no burst exceeds `capacity`. Thread-safe. Time is a parameter
 * rather than read from a clock, so the arithmetic can be tested without sleeping.
 */
class token_bucket
{
public:
  using clock = std::chrono::steady_clock;

  static constexpr double unlimited{ std::numeric_limits<double>::infinity() };
  static constexpr std::chrono::hours max_wait{ 1 };

  /**
   * The bucket starts full.
   *
   * @throws std::invalid_argument unless `tokens_per_second > 0`; `unlimited` is valid.
   */
  token_bucket(std::size_t capacity, double tokens_per_second, clock::time_point now)
    : capacity_{ static_cast<double>(capacity) }
    , tokens_{ capacity_ }
    , last_refill_{ now }
    , rate_{ checked_rate(tokens_per_second) }
  {
  }

  /**
   * Takes `n` tokens and returns zero, or takes nothing and returns how long until `n` tokens will
   * be available at the current rate. Never waits: at a finite rate it takes one mutex, held only
   * for the refill arithmetic, and at an unlimited rate it takes no lock. It is cheap enough to
   * call once per operation from an SDK I/O thread.
   *
   * The returned wait is at most `max_wait`, so a very small rate cannot overflow the clock's
   * duration; the caller then retries.
   *
   * @throws std::invalid_argument when `n` exceeds the capacity, since such a take never succeeds.
   */
  [[nodiscard]] auto try_take(std::size_t n, clock::time_point now) -> clock::duration
  {
    const auto wanted = static_cast<double>(n);
    if (wanted > capacity_) {
      throw std::invalid_argument("token_bucket: a take larger than the capacity never succeeds");
    }
    if (std::isinf(rate_.load(std::memory_order_relaxed))) {
      granted_.fetch_add(n, std::memory_order_relaxed);
      return clock::duration::zero();
    }
    const std::scoped_lock lock{ mutex_ };
    // set_rate() may have made the rate unlimited since the check above; that take is counted too.
    if (std::isinf(rate_.load(std::memory_order_relaxed))) {
      granted_.fetch_add(n, std::memory_order_relaxed);
      return clock::duration::zero();
    }
    refill(now);
    if (tokens_ < wanted) {
      const auto rate = rate_.load(std::memory_order_relaxed);
      const std::chrono::duration<double> wait{ (wanted - tokens_) / rate };
      return std::chrono::ceil<clock::duration>(
        std::min<std::chrono::duration<double>>(wait, max_wait));
    }
    tokens_ -= wanted;
    granted_.fetch_add(n, std::memory_order_relaxed);
    return clock::duration::zero();
  }

  /**
   * Tokens accrued at the old rate up to `now` are kept; the new rate applies from `now`.
   *
   * @throws std::invalid_argument unless `tokens_per_second > 0`; `unlimited` is valid.
   */
  void set_rate(double tokens_per_second, clock::time_point now)
  {
    const auto rate = checked_rate(tokens_per_second);
    const std::scoped_lock lock{ mutex_ };
    refill(now);
    rate_.store(rate, std::memory_order_relaxed);
  }

  [[nodiscard]] auto rate() const -> double
  {
    return rate_.load(std::memory_order_relaxed);
  }

  [[nodiscard]] auto capacity() const -> std::size_t
  {
    return static_cast<std::size_t>(capacity_);
  }

  /// Tokens granted since the previous call, which makes successive calls a rate meter.
  auto take_granted() -> std::uint64_t
  {
    return granted_.exchange(0, std::memory_order_relaxed);
  }

private:
  static auto checked_rate(double tokens_per_second) -> double
  {
    if (!(tokens_per_second > 0)) {
      throw std::invalid_argument("token_bucket: the rate must be greater than zero");
    }
    return tokens_per_second;
  }

  void refill(clock::time_point now)
  {
    if (now <= last_refill_) {
      return; // an older timestamp neither moves the refill point back nor removes tokens
    }
    const auto rate = rate_.load(std::memory_order_relaxed);
    const std::chrono::duration<double> elapsed{ now - last_refill_ };
    last_refill_ = now;
    if (std::isinf(rate)) {
      tokens_ = capacity_;
      return;
    }
    tokens_ = std::min(capacity_, tokens_ + elapsed.count() * rate);
  }

  const double capacity_;
  std::mutex mutex_;
  double tokens_;                 // guarded by mutex_
  clock::time_point last_refill_; // guarded by mutex_
  std::atomic<double> rate_;      // written under mutex_; read without it
  std::atomic<std::uint64_t> granted_{ 0 };
};

inline constexpr std::chrono::milliseconds acquire_wait_slice{ 200 };

/**
 * Blocks until `n` tokens are taken, or until `should_stop()` returns true. The predicate is
 * checked before every attempt, and each wait is one call to `sleep` of at most
 * `acquire_wait_slice`, so a stop request is seen within one slice.
 *
 * @return true when the tokens were taken, false when stopped first.
 */
template<typename StopPredicate, typename Sleep>
auto
acquire(token_bucket& bucket, std::size_t n, StopPredicate should_stop, Sleep sleep) -> bool
{
  while (!should_stop()) {
    const auto wait = bucket.try_take(n, token_bucket::clock::now());
    if (wait == token_bucket::clock::duration::zero()) {
      return true;
    }
    sleep(std::min<token_bucket::clock::duration>(wait, acquire_wait_slice));
  }
  return false;
}

/// acquire() that sleeps on the calling thread.
template<typename StopPredicate>
auto
acquire(token_bucket& bucket, std::size_t n, StopPredicate should_stop) -> bool
{
  return acquire(bucket, n, std::move(should_stop), [](token_bucket::clock::duration wait) {
    std::this_thread::sleep_for(wait);
  });
}

/**
 * acquire() for any `n`: takes at most the bucket's capacity at a time, so `n` may exceed it.
 * Tokens taken before a stop stay taken. Requires a capacity greater than zero.
 */
template<typename StopPredicate>
auto
acquire_in_chunks(token_bucket& bucket, std::size_t n, StopPredicate should_stop) -> bool
{
  for (std::size_t taken = 0; taken < n; taken += bucket.capacity()) {
    if (!acquire(bucket, std::min(bucket.capacity(), n - taken), should_stop)) {
      return false;
    }
  }
  return true;
}
} // namespace cbc
