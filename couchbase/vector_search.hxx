/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include <couchbase/vector_query.hxx>
#include <couchbase/vector_search_options.hxx>
#include <utility>

namespace couchbase
{
class vector_search
{
  public:
    explicit vector_search(std::vector<vector_query> vector_queries, vector_search_options options = {})
      : vector_queries_{ std::move(vector_queries) }
      , options_{ options }
    {
    }

    [[nodiscard]] const vector_search_options options() const
    {
        return options_;
    }

    [[nodiscard]] auto encode() const -> encoded_search_query;

  private:
    std::vector<vector_query> vector_queries_;
    vector_search_options options_;
};
} // namespace couchbase
