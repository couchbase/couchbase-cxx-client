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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace couchbase::test::bench
{
// Latency histogram with log-linear buckets. Values below sub_buckets are exact. Each power of two
// above that splits into sub_buckets equal buckets, so a bucket spans at most 1/sub_buckets of its
// lower bound. Values above max_trackable clamp to it. The bucket array is sized by the
// constructor; record() never allocates.
class histogram
{
public:
  static constexpr std::uint64_t max_trackable = 60'000'000'000ULL; // 60 s in ns

  histogram()
    : buckets_(bucket_index(max_trackable) + 1, 0)
  {
  }

  void record(std::uint64_t ns) noexcept
  {
    ns = std::min(ns, max_trackable);
    ++buckets_[bucket_index(ns)];
    ++count_;
    min_ = std::min(min_, ns);
    max_ = std::max(max_, ns);
  }

  void merge(const histogram& other)
  {
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
      buckets_[i] += other.buckets_[i];
    }
    count_ += other.count_;
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);
  }

  [[nodiscard]] auto count() const -> std::uint64_t
  {
    return count_;
  }

  [[nodiscard]] auto min() const -> std::uint64_t
  {
    return count_ == 0 ? 0 : min_;
  }

  [[nodiscard]] auto max() const -> std::uint64_t
  {
    return max_;
  }

  // The value of the sample at zero-based `rank` in sorted order, to bucket resolution; ranks past
  // the last sample read as the last one. 0 when the histogram is empty.
  [[nodiscard]] auto value_at_rank(std::uint64_t rank) const -> std::uint64_t
  {
    if (count_ == 0) {
      return 0;
    }
    rank = std::min(rank, count_ - 1);
    std::uint64_t seen = 0;
    for (std::size_t i = 0; i < buckets_.size(); ++i) {
      seen += buckets_[i];
      if (seen > rank) {
        return std::clamp(bucket_value(i), min_, max_);
      }
    }
    return max_;
  }

  // Nearest-rank percentile, p in [0, 100].
  [[nodiscard]] auto percentile(double p) const -> std::uint64_t
  {
    if (count_ == 0) {
      return 0;
    }
    const auto n = static_cast<double>(count_);
    const auto rank = static_cast<std::uint64_t>(std::max(0.0, std::ceil(p / 100.0 * n) - 1.0));
    return value_at_rank(rank);
  }

private:
  static constexpr unsigned sub_bucket_bits = 7;
  static constexpr std::uint64_t sub_buckets = 1ULL << sub_bucket_bits;

  static auto bucket_index(std::uint64_t v) -> std::uint64_t
  {
    if (v < sub_buckets) {
      return v;
    }
    unsigned msb = 63;
    while ((v >> msb) == 0) {
      --msb;
    }
    const unsigned shift = msb - sub_bucket_bits;
    return (shift + 1) * sub_buckets + ((v >> shift) - sub_buckets);
  }

  // Midpoint of the bucket's range.
  static auto bucket_value(std::size_t index) -> std::uint64_t
  {
    if (index < sub_buckets) {
      return index;
    }
    const auto shift = static_cast<unsigned>(index / sub_buckets) - 1;
    const std::uint64_t lower = (sub_buckets + index % sub_buckets) << shift;
    return lower + ((1ULL << shift) >> 1);
  }

  std::vector<std::uint64_t> buckets_;
  std::uint64_t count_{ 0 };
  std::uint64_t min_{ UINT64_MAX };
  std::uint64_t max_{ 0 };
};
} // namespace couchbase::test::bench
