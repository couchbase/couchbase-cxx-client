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
 * Disables scoring, so that the server does not perform any scoring on the hits.
 *
 * This sends the same `"none"` that the deprecated @ref search_options#disable_scoring() sends. It
 * is not a fusion strategy: `"none"` predates score fusion, so it works on older server versions.
 *
 * @since 1.4.0
 * @volatile
 */
class search_scoring_none : public search_scoring
{
public:
  search_scoring_none() = default;

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
      search_scoring_strategy::none,
    };
  }
};
} // namespace couchbase
