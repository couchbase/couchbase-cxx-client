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

#include "tools/throttle_sizing.hxx"

#include <chrono>
#include <cstddef>
#include <optional>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;
using clock_type = cbc::token_bucket::clock;
constexpr clock_type::duration zero{ clock_type::duration::zero() };

void
the_floor_is_the_batch_size_or_the_ceiling([[maybe_unused]] context& ctx)
{
  assert_eq(
    cbc::throttle_floor(100, std::nullopt), 100.0, "without a ceiling, the batch size per second");
  assert_eq(
    cbc::throttle_floor(100, 500.0), 100.0, "a ceiling above the batch size leaves the floor");
  assert_eq(cbc::throttle_floor(100, 50.0), 50.0, "a ceiling below the batch size is the floor");
}

void
the_capacity_fits_every_take([[maybe_unused]] context& ctx)
{
  assert_eq(cbc::throttle_capacity(100, 10), std::size_t{ 100 }, "a batch takes one batch");
  assert_eq(cbc::throttle_capacity(5, 10),
            std::size_t{ 10 },
            "a population batch larger than the workload batch fits as well");
}

void
a_late_token_timer_keeps_the_accrued_tokens([[maybe_unused]] context& ctx)
{
  // A deferred pipeline send wakes every 60us and takes single tokens until the bucket asks to
  // wait. A one-token bucket would discard all but one of the three tokens accrued per wake.
  constexpr double rate{ 50000 };
  const auto t0 = clock_type::now();
  cbc::token_bucket bucket{ cbc::throttle_capacity(100, 10), rate, t0 };
  std::size_t granted{ 0 };
  for (auto t = t0; t < t0 + 1s; t += 60us) {
    while (bucket.try_take(1, t) == zero) {
      ++granted;
    }
  }
  assert_true(granted >= 47500, "one second at 50000 tokens/s grants at least 95% of them");
}

void
the_admitted_rate_is_per_second_of_elapsed_time([[maybe_unused]] context& ctx)
{
  assert_eq(cbc::admitted_rate(1000, 5s), 200.0, "a five-second sample is divided by five");
  assert_eq(cbc::admitted_rate(1000, 1s), 1000.0, "a one-second sample is the count");
  assert_eq(cbc::admitted_rate(1000, 0s), 0.0, "no elapsed time observes no rate");
  assert_eq(cbc::admitted_rate(1000, -1s), 0.0, "a negative interval observes no rate");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_throttle_sizing",
    {
      { "the_floor_is_the_batch_size_or_the_ceiling",
        the_floor_is_the_batch_size_or_the_ceiling,
        {},
        timeout::instant },
      { "the_capacity_fits_every_take", the_capacity_fits_every_take, {}, timeout::instant },
      { "a_late_token_timer_keeps_the_accrued_tokens",
        a_late_token_timer_keeps_the_accrued_tokens,
        {},
        timeout::instant },
      { "the_admitted_rate_is_per_second_of_elapsed_time",
        the_admitted_rate_is_per_second_of_elapsed_time,
        {},
        timeout::instant },
    },
  };
}
} // namespace couchbase::test
