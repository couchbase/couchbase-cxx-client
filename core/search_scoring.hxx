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
#include <variant>

namespace couchbase::core
{
/**
 * Disables scoring.
 */
struct search_scoring_none {
};

/**
 * Reciprocal Rank Fusion.
 */
struct search_scoring_reciprocal_rank_fusion {
  std::optional<std::uint32_t> rank_constant{};
  std::optional<std::uint32_t> window_size{};
};

/**
 * Relative Score Fusion.
 */
struct search_scoring_relative_score_fusion {
  std::optional<std::uint32_t> window_size{};
};

/**
 * The value of the top-level "score" field of a search request. `std::monostate` means the field
 * was not set.
 */
using search_scoring_mode = std::variant<std::monostate,
                                         search_scoring_none,
                                         search_scoring_reciprocal_rank_fusion,
                                         search_scoring_relative_score_fusion>;

/**
 * Whether the strategy is one of the score fusion strategies, and therefore requires the
 * "scoreFusion" cluster capability.
 */
[[nodiscard]] inline auto
is_score_fusion(const search_scoring_mode& scoring) -> bool
{
  return std::holds_alternative<search_scoring_reciprocal_rank_fusion>(scoring) ||
         std::holds_alternative<search_scoring_relative_score_fusion>(scoring);
}
} // namespace couchbase::core
