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

#include "test/framework/test_framework.hxx"

#include "tools/token_bucket.hxx"

#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;
using clock_type = cbc::token_bucket::clock;
constexpr clock_type::duration zero{ clock_type::duration::zero() };

void
starts_full([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_true(bucket.try_take(100, t0) == zero, "a new bucket holds its full capacity");
}

void
refills_at_rate([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_true(bucket.try_take(100, t0) == zero, "a new bucket holds its full capacity");
  assert_true(bucket.try_take(100, t0) == 100ms, "an empty bucket waits n / rate");
  assert_true(bucket.try_take(100, t0 + 100ms) == zero, "after n / rate the tokens are there");
}

void
does_not_accumulate_past_capacity([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_true(bucket.try_take(100, t0 + 10s) == zero, "a full bucket grants its capacity");
  assert_true(bucket.try_take(1, t0 + 10s) > zero, "idle time must not bank tokens past capacity");
}

void
unlimited_never_waits([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, cbc::token_bucket::unlimited, t0 };
  for (int i = 0; i < 1000; ++i) {
    assert_true(bucket.try_take(100, t0) == zero, "an unlimited bucket never asks to wait");
  }
}

void
set_rate_keeps_accrued_tokens([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 100, t0 };
  assert_true(bucket.try_take(100, t0) == zero, "a new bucket holds its full capacity");
  bucket.set_rate(1000, t0 + 500ms);
  assert_eq(bucket.rate(), 1000.0, "rate() reports the rate just set");
  assert_true(bucket.try_take(50, t0 + 500ms) == zero, "500ms at the old rate accrues 50 tokens");
  assert_true(bucket.try_take(1, t0 + 500ms) > zero,
              "the new rate applies only from the moment it was set");
}

void
an_older_timestamp_removes_no_tokens([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_true(bucket.try_take(50, t0) == zero, "a new bucket holds its full capacity");
  assert_true(bucket.try_take(50, t0 - 1s) == zero, "an older timestamp leaves 50 tokens in place");
  assert_true(bucket.try_take(1, t0) > zero,
              "an older timestamp does not move the refill point back");
}

void
a_tiny_rate_waits_at_most_max_wait([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 1, 1e-12, t0 };
  assert_true(bucket.try_take(1, t0) == zero, "a new bucket holds its full capacity");
  assert_true(bucket.try_take(1, t0) == cbc::token_bucket::max_wait,
              "a wait of 1e12 seconds is clamped instead of overflowing");
}

void
rejects_more_than_capacity([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_throws<std::invalid_argument>(
    [&] {
      [[maybe_unused]] auto wait = bucket.try_take(101, t0);
    },
    "a take larger than the capacity could never be granted");
}

void
rejects_non_positive_rate([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  for (const double rate : { 0.0, -1.0, std::numeric_limits<double>::quiet_NaN() }) {
    assert_throws<std::invalid_argument>(
      [&] {
        const cbc::token_bucket rejected{ 100, rate, t0 };
      },
      "construction refuses a rate that is not positive");
  }
  cbc::token_bucket bucket{ 100, 1000, t0 };
  for (const double rate : { 0.0, -1.0, std::numeric_limits<double>::quiet_NaN() }) {
    assert_throws<std::invalid_argument>(
      [&] {
        bucket.set_rate(rate, t0);
      },
      "set_rate refuses a rate that is not positive");
  }
  assert_eq(bucket.rate(), 1000.0, "a refused set_rate leaves the rate unchanged");
}

void
acquire_takes_an_available_token([[maybe_unused]] context& ctx)
{
  // acquire() reads clock_type::now(), which is before `later`, so the bucket cannot refill.
  const auto later = clock_type::now() + 1h;
  cbc::token_bucket bucket{ 1, 1, later };
  assert_true(cbc::acquire(bucket,
                           1,
                           [] {
                             return false;
                           }),
              "an available token is acquired");
  assert_true(bucket.try_take(1, later) > zero, "the acquired token left the bucket");
}

void
acquire_sleeps_in_slices([[maybe_unused]] context& ctx)
{
  // acquire() reads clock_type::now(), which is before `later`, so the bucket cannot refill.
  const auto later = clock_type::now() + 1h;
  cbc::token_bucket bucket{ 1, 1, later };
  assert_true(bucket.try_take(1, later) == zero, "the only token is taken");
  int checks{ 0 };
  std::vector<clock_type::duration> sleeps;
  const bool acquired = cbc::acquire(
    bucket,
    1,
    [&checks] {
      return ++checks > 2;
    },
    [&sleeps](clock_type::duration wait) {
      sleeps.push_back(wait);
    });
  assert_false(acquired, "a stopped acquire reports that it took nothing");
  assert_true(sleeps == std::vector<clock_type::duration>{ 200ms, 200ms },
              "a one-second wait is slept in 200ms slices, with a stop check between them");
}

void
take_granted_counts_and_resets([[maybe_unused]] context& ctx)
{
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ 100, 1000, t0 };
  assert_true(bucket.try_take(30, t0) == zero, "30 of 100 tokens are available");
  assert_true(bucket.try_take(20, t0) == zero, "20 of the remaining 70 are available");
  assert_true(bucket.try_take(100, t0) > zero, "100 of the remaining 50 are not");
  assert_eq(bucket.take_granted(), std::uint64_t{ 50 }, "a refused take grants nothing");
  assert_eq(bucket.take_granted(), std::uint64_t{ 0 }, "take_granted resets the count");

  cbc::token_bucket open{ 100, cbc::token_bucket::unlimited, t0 };
  assert_true(open.try_take(7, t0) == zero, "an unlimited bucket never asks to wait");
  assert_eq(open.take_granted(), std::uint64_t{ 7 }, "an unlimited bucket counts grants too");
}

void
acquire_sleeps_a_short_wait_as_computed([[maybe_unused]] context& ctx)
{
  // acquire() reads clock_type::now(), which is before `later`, so the bucket cannot refill.
  const auto later = clock_type::now() + 1h;
  cbc::token_bucket bucket{ 1, 10, later };
  assert_true(bucket.try_take(1, later) == zero, "the only token is taken");
  int checks{ 0 };
  std::vector<clock_type::duration> sleeps;
  const bool acquired = cbc::acquire(
    bucket,
    1,
    [&checks] {
      return ++checks > 1;
    },
    [&sleeps](clock_type::duration wait) {
      sleeps.push_back(wait);
    });
  assert_false(acquired, "a stopped acquire reports that it took nothing");
  assert_true(sleeps.size() == 1 && sleeps[0] > zero && sleeps[0] <= 100ms,
              "a wait shorter than a slice is slept as computed, not as a whole slice");
}

void
acquire_checks_the_stop_before_the_first_attempt([[maybe_unused]] context& ctx)
{
  cbc::token_bucket bucket{ 1, 1, clock_type::now() };
  assert_false(cbc::acquire(bucket,
                            1,
                            [] {
                              return true;
                            }),
               "a stop before the first attempt takes nothing");
  assert_true(bucket.try_take(1, clock_type::now()) == zero, "the token is still in the bucket");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_token_bucket",
    {
      { "starts_full", starts_full, {}, timeout::instant },
      { "refills_at_rate", refills_at_rate, {}, timeout::instant },
      { "does_not_accumulate_past_capacity",
        does_not_accumulate_past_capacity,
        {},
        timeout::instant },
      { "unlimited_never_waits", unlimited_never_waits, {}, timeout::instant },
      { "set_rate_keeps_accrued_tokens", set_rate_keeps_accrued_tokens, {}, timeout::instant },
      { "an_older_timestamp_removes_no_tokens",
        an_older_timestamp_removes_no_tokens,
        {},
        timeout::instant },
      { "a_tiny_rate_waits_at_most_max_wait",
        a_tiny_rate_waits_at_most_max_wait,
        {},
        timeout::instant },
      { "rejects_more_than_capacity", rejects_more_than_capacity, {}, timeout::instant },
      { "rejects_non_positive_rate", rejects_non_positive_rate, {}, timeout::instant },
      { "take_granted_counts_and_resets", take_granted_counts_and_resets, {}, timeout::instant },
      { "acquire_takes_an_available_token",
        acquire_takes_an_available_token,
        {},
        timeout::instant },
      { "acquire_sleeps_in_slices", acquire_sleeps_in_slices, {}, timeout::instant },
      { "acquire_sleeps_a_short_wait_as_computed",
        acquire_sleeps_a_short_wait_as_computed,
        {},
        timeout::instant },
      { "acquire_checks_the_stop_before_the_first_attempt",
        acquire_checks_the_stop_before_the_first_attempt,
        {},
        timeout::instant },
    },
  };
}
} // namespace couchbase::test
