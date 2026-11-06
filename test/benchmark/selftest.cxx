/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "benchmark/statistics.hxx"

#include "framework/test_registry.hxx"

#include <cstdint>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace bench;

void
histogram_percentiles_within_one_percent([[maybe_unused]] context& ctx)
{
  histogram h;
  for (std::uint64_t v = 1; v <= 100'000; ++v) {
    h.record(v);
  }
  assert_eq(h.count(), 100'000U);
  assert_true(h.percentile(50) >= 49'500 && h.percentile(50) <= 50'500, "p50");
  assert_true(h.percentile(99) >= 98'010 && h.percentile(99) <= 99'990, "p99");
  assert_true(h.max() >= 99'000 && h.max() <= 101'000, "max");
  assert_eq(h.min(), 1U);
}

void
histogram_merge_equals_single([[maybe_unused]] context& ctx)
{
  histogram all;
  histogram a;
  histogram b;
  for (std::uint64_t v = 1; v <= 10'000; ++v) {
    all.record(v * 7);
    (v <= 5'000 ? a : b).record(v * 7);
  }
  a.merge(b);
  assert_eq(a.count(), all.count());
  assert_eq(a.percentile(90), all.percentile(90));
  assert_eq(a.max(), all.max());
}

void
histogram_clamps_above_range([[maybe_unused]] context& ctx)
{
  histogram h;
  h.record(120'000'000'000ULL);
  assert_eq(h.count(), 1U);
  assert_true(h.max() >= 60'000'000'000ULL, "max reaches the clamp");
}

void
running_stats_matches_two_pass([[maybe_unused]] context& ctx)
{
  running_stats s;
  for (double x : { 2, 4, 4, 4, 5, 5, 7, 9 }) {
    s.add(x);
  }
  assert_near(s.mean(), 5.0, 1e-12);
  assert_near(s.stddev(), 2.138, 0.001);
}

void
median_ci_order_statistics([[maybe_unused]] context& ctx)
{
  std::vector<double> v;
  for (int i = 1; i <= 100; ++i) {
    v.push_back(i);
  }
  const auto ci = median_with_ci(v);
  assert_near(ci.median, 50.5, 1e-12);
  // Zero-based ranks 39 and 60: 2 * P(Bin(100, 1/2) <= 39) is 0.035, and <= 40 is 0.057.
  assert_near(ci.lower, 40, 1e-12);
  assert_near(ci.upper, 61, 1e-12);
}

void
median_ci_exact_ranks([[maybe_unused]] context& ctx)
{
  const auto ranks = [](std::size_t n) {
    std::vector<double> v;
    for (std::size_t i = 0; i < n; ++i) {
      v.push_back(static_cast<double>(i));
    }
    const auto ci = median_with_ci(v);
    return std::make_pair(ci.lower, ci.upper);
  };
  assert_true(ranks(6) == std::make_pair(0.0, 5.0), "n=6 spans the range");
  assert_true(ranks(7) == std::make_pair(0.0, 6.0), "n=7 spans the range");
  assert_true(ranks(8) == std::make_pair(0.0, 7.0), "n=8 spans the range");
  assert_true(ranks(9) == std::make_pair(1.0, 7.0), "n=9 drops one sample at each end");
  assert_true(ranks(100000) == std::make_pair(49689.0, 50310.0), "n=100000");
}

void
median_ci_single_sample([[maybe_unused]] context& ctx)
{
  const auto ci = median_with_ci(std::vector<double>{ 7 });
  assert_near(ci.median, 7, 1e-12);
  assert_true(std::isinf(ci.lower) && ci.lower < 0, "the lower bound is -infinity");
  assert_true(std::isinf(ci.upper) && ci.upper > 0, "the upper bound is +infinity");
}

void
median_ci_five_samples_have_no_interval([[maybe_unused]] context& ctx)
{
  const auto ci = median_with_ci(std::vector<double>{ 5, 1, 4, 2, 3 });
  assert_near(ci.median, 3, 1e-12);
  assert_true(std::isinf(ci.lower) && std::isinf(ci.upper), "five samples have no 95% interval");
}

void
histogram_median_ci_agrees_with_samples([[maybe_unused]] context& ctx)
{
  histogram h;
  for (std::uint64_t v = 1; v <= 100; ++v) {
    h.record(v);
  }
  const auto ci = median_with_ci(h);
  assert_near(ci.median, 50.5, 1e-12);
  assert_near(ci.lower, 40, 1e-12);
  assert_near(ci.upper, 61, 1e-12);
}

void
histogram_relative_error_holds_across_the_range([[maybe_unused]] context& ctx)
{
  for (std::uint64_t v = 1; v <= histogram::max_trackable; v = v + v / 97 + 1) {
    histogram h;
    h.record(v);
    h.record(v);
    // min/max clamp would hide bucket error, so probe a second, distant value.
    h.record(histogram::max_trackable);
    h.record(1);
    const auto got = static_cast<double>(h.value_at_rank(1));
    assert_true(std::abs(got - static_cast<double>(v)) <= 0.01 * static_cast<double>(v) + 1e-9,
                "bucket error exceeds 1%");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(histogram_percentiles_within_one_percent) },
      { CASE(histogram_merge_equals_single) },
      { CASE(histogram_clamps_above_range) },
      { CASE(running_stats_matches_two_pass) },
      { CASE(median_ci_order_statistics) },
      { CASE(median_ci_single_sample) },
      { CASE(median_ci_five_samples_have_no_interval) },
      { CASE(median_ci_exact_ranks) },
      { CASE(histogram_median_ci_agrees_with_samples) },
      { CASE(histogram_relative_error_holds_across_the_range) },
    },
  };
}
} // namespace couchbase::test
