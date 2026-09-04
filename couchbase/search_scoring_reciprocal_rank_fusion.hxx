/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/search_scoring.hxx>

namespace couchbase
{
/**
 * Merges the FTS and vector result sets by rank rather than by raw score.
 *
 * It works well with the server defaults, and is the recommended strategy.
 *
 * @note available from Couchbase Server 8.1. Setting it makes the SDK check for the score fusion
 * cluster capability, and fail the operation with @ref errc::common::feature_not_available if the
 * cluster does not advertise it.
 *
 * @since 1.5.0
 * @volatile
 */
class search_scoring_reciprocal_rank_fusion : public search_scoring
{
public:
  search_scoring_reciprocal_rank_fusion() = default;

  /**
   * The rank constant of the Reciprocal Rank Fusion formula.
   *
   * @param rank_constant the rank constant
   * @return this scoring mode for chaining purposes.
   *
   * @since 1.5.0
   * @volatile
   */
  auto rank_constant(std::uint32_t rank_constant) -> search_scoring_reciprocal_rank_fusion&
  {
    rank_constant_ = rank_constant;
    return *this;
  }

  /**
   * How many results per list are considered for fusion.
   *
   * @param window_size the window size
   * @return this scoring mode for chaining purposes.
   *
   * @since 1.5.0
   * @volatile
   */
  auto window_size(std::uint32_t window_size) -> search_scoring_reciprocal_rank_fusion&
  {
    window_size_ = window_size;
    return *this;
  }

  /**
   * Returns the scoring mode as an immutable value.
   *
   * @return the scoring mode as an immutable value
   *
   * @since 1.5.0
   * @internal
   */
  [[nodiscard]] auto build() const -> search_scoring::built override
  {
    return { search_scoring::built::reciprocal_rank_fusion{ rank_constant_, window_size_ } };
  }

private:
  std::optional<std::uint32_t> rank_constant_{};
  std::optional<std::uint32_t> window_size_{};
};
} // namespace couchbase
