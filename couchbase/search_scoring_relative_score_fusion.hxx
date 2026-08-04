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
 * Merges the FTS and vector result sets by normalized score rather than by rank.
 *
 * @note available from Couchbase Server 8.1. Setting it makes the SDK check for the score fusion
 * cluster capability, and fail the operation with @ref errc::common::feature_not_available if the
 * cluster does not advertise it.
 *
 * @since 1.4.0
 * @volatile
 */
class search_scoring_relative_score_fusion : public search_scoring
{
public:
  search_scoring_relative_score_fusion() = default;

  /**
   * How many results per list are considered for fusion.
   *
   * @param window_size the window size
   * @return this scoring mode for chaining purposes.
   *
   * @since 1.4.0
   * @volatile
   */
  auto window_size(std::uint32_t window_size) -> search_scoring_relative_score_fusion&
  {
    window_size_ = window_size;
    return *this;
  }

  /**
   * Returns the scoring mode as an immutable value.
   *
   * @return the scoring mode as an immutable value
   *
   * @since 1.4.0
   * @internal
   */
  [[nodiscard]] auto build() const -> search_scoring::built override
  {
    return {
      search_scoring_strategy::relative_score_fusion,
      {},
      window_size_,
    };
  }

private:
  std::optional<std::uint32_t> window_size_{};
};
} // namespace couchbase
