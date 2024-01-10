#include <utility>

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

namespace couchbase
{
class vector_query
{
  public:
    vector_query(std::string vector_field_name, std::vector<float> vector_query)
      : vector_field_name_{ std::move(vector_field_name) }
      , vector_query_{ std::move(vector_query) }
    {
        if (vector_query_.empty()) {
            throw std::invalid_argument("the vector_query cannot be empty");
        }
    }

    auto num_candidates(std::uint32_t num_candidates) -> vector_query&
    {
        if (num_candidates < 1) {
            throw std::invalid_argument("The num_candidates cannot be less than 1");
        }
        num_candidates_ = num_candidates;
        return *this;
    }

    auto boost(double boost) -> vector_query&
    {
        boost_ = boost;
        return *this;
    }

    [[nodiscard]] auto encode() const -> encoded_search_query;

  private:
    std::string vector_field_name_;
    std::vector<float> vector_query_;
    std::uint32_t num_candidates_{ 3 };
    std::optional<double> boost_{};
};
} // namespace couchbase
