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

#pragma once

#include "histogram.hxx"

#include <cmath>
#include <cstddef>
#include <vector>

namespace couchbase::test::bench
{
// Welford's online mean and variance.
struct running_stats {
  void add(double x)
  {
    ++count_;
    const auto delta = x - mean_;
    mean_ += delta / static_cast<double>(count_);
    m2_ += delta * (x - mean_);
  }

  [[nodiscard]] auto count() const -> std::size_t
  {
    return count_;
  }

  [[nodiscard]] auto mean() const -> double
  {
    return mean_;
  }

  // Sample standard deviation; 0 with fewer than two samples.
  [[nodiscard]] auto stddev() const -> double
  {
    return count_ < 2 ? 0.0 : std::sqrt(m2_ / static_cast<double>(count_ - 1));
  }

private:
  std::size_t count_{ 0 };
  double mean_{ 0.0 };
  double m2_{ 0.0 };
};

struct median_ci {
  double median;
  double lower;
  double upper;
};

// Median with a distribution-free 95% confidence interval: the order statistics at zero-based ranks
// k and n-1-k, k the largest rank with 2 * P(Bin(n, 1/2) <= k) <= 0.05. With fewer than 6 samples
// no rank qualifies, and the bounds are -infinity and +infinity. All zeros for no samples.
auto
median_with_ci(std::vector<double> samples) -> median_ci;

// The same ranks, read from the histogram at its bucket resolution.
auto
median_with_ci(const histogram& h) -> median_ci;
} // namespace couchbase::test::bench
