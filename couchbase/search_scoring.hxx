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

#include <cstdint>
#include <optional>

namespace couchbase
{
/**
 * The strategy a @ref search_scoring selects.
 *
 * @since 1.4.0
 * @volatile
 */
enum class search_scoring_strategy {
  /**
   * Scoring is disabled.
   */
  none,

  /**
   * Reciprocal Rank Fusion: merges the result sets by rank.
   */
  reciprocal_rank_fusion,

  /**
   * Relative Score Fusion: merges the result sets by normalized score.
   */
  relative_score_fusion,
};

/**
 * Base class for the scoring mode of a search query.
 *
 * Score fusion controls how the FTS and vector result sets of a hybrid request are merged into a
 * single ranked list. It is only meaningful for a hybrid request (both an FTS query and a vector
 * search); when applied to a single result set the server re-scores the hits but leaves their
 * ordering unchanged.
 *
 * Each mode is its own type and only exposes its own parameters:
 *
 * @li @ref search_scoring_reciprocal_rank_fusion merges by rank.
 * @li @ref search_scoring_relative_score_fusion merges by normalized score.
 * @li @ref search_scoring_none disables scoring.
 *
 * @see search_options#scoring()
 *
 * @since 1.4.0
 * @volatile
 */
class search_scoring
{
public:
  /**
   * Immutable value object representing a scoring mode.
   *
   * @since 1.4.0
   * @internal
   */
  struct built {
    search_scoring_strategy strategy{ search_scoring_strategy::none };
    std::optional<std::uint32_t> rank_constant{};
    std::optional<std::uint32_t> window_size{};
  };

  virtual ~search_scoring() = default;

  /**
   * Returns the scoring mode as an immutable value.
   *
   * @return the scoring mode as an immutable value
   *
   * @since 1.4.0
   * @internal
   */
  [[nodiscard]] virtual auto build() const -> built = 0;

protected:
  search_scoring() = default;
};
} // namespace couchbase
