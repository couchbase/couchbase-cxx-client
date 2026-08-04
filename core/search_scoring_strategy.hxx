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

namespace couchbase::core
{
/**
 * The value of the top-level "score" field of a search request.
 */
enum class search_scoring_strategy {
  /**
   * Disables scoring.
   */
  none,

  /**
   * Reciprocal Rank Fusion.
   */
  reciprocal_rank_fusion,

  /**
   * Relative Score Fusion.
   */
  relative_score_fusion,
};

/**
 * Whether the strategy is one of the score fusion strategies, and therefore requires the
 * "scoreFusion" cluster capability.
 */
[[nodiscard]] constexpr auto
is_score_fusion(search_scoring_strategy strategy) -> bool
{
  return strategy == search_scoring_strategy::reciprocal_rank_fusion ||
         strategy == search_scoring_strategy::relative_score_fusion;
}
} // namespace couchbase::core
