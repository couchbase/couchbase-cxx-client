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

#include "statistics.hxx"

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <optional>

namespace couchbase::test::bench
{
namespace
{
// The largest zero-based rank k with 2 * P(Bin(n, 1/2) <= k) <= 0.05: [x(k), x(n-1-k)] then covers
// the median with probability at least 95%. Empty when no rank qualifies, which is every n < 6.
auto
lower_rank(std::size_t n) -> std::optional<std::size_t>
{
  const auto count = static_cast<double>(n);
  // Terms more than ten standard deviations below n/2 sum to under 1e-22: start there.
  const auto first =
    static_cast<std::size_t>(std::max(0.0, std::floor((count / 2) - (5 * std::sqrt(count)))));
  auto log_term = std::lgamma(count + 1) - std::lgamma(static_cast<double>(first) + 1) -
                  std::lgamma(count - static_cast<double>(first) + 1) - (count * std::log(2.0));
  double tail = 0;
  std::optional<std::size_t> rank;
  for (auto k = first; k < n; ++k) {
    tail += std::exp(log_term);
    if (2 * tail > 0.05) {
      break;
    }
    rank = k;
    log_term += std::log((count - static_cast<double>(k)) / (static_cast<double>(k) + 1));
  }
  return rank;
}

auto
with_ranks(std::size_t n, double median, const std::function<double(std::size_t)>& at) -> median_ci
{
  const auto k = lower_rank(n);
  if (!k) {
    return { median,
             -std::numeric_limits<double>::infinity(),
             std::numeric_limits<double>::infinity() };
  }
  return { median, at(*k), at(n - 1 - *k) };
}
} // namespace

auto
median_with_ci(std::vector<double> samples) -> median_ci
{
  const auto n = samples.size();
  if (n == 0) {
    return { 0, 0, 0 };
  }
  std::sort(samples.begin(), samples.end());
  return with_ranks(n, (samples[(n - 1) / 2] + samples[n / 2]) / 2.0, [&samples](std::size_t i) {
    return samples[i];
  });
}

auto
median_with_ci(const histogram& h) -> median_ci
{
  const std::size_t n = h.count();
  if (n == 0) {
    return { 0, 0, 0 };
  }
  const auto at = [&h](std::size_t i) {
    return static_cast<double>(h.value_at_rank(i));
  };
  return with_ranks(n, (at((n - 1) / 2) + at(n / 2)) / 2.0, at);
}
} // namespace couchbase::test::bench
