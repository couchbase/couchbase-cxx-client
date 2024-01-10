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

#include <couchbase/search_query.hxx>
#include <couchbase/vector_search.hxx>

namespace couchbase
{
class search_request
{
  public:
    explicit search_request(std::unique_ptr<couchbase::search_query> search_query)
      : search_query_{ std::move(search_query) }
    {
    }

    explicit search_request(couchbase::vector_search vector_search)
      : vector_search_{ vector_search }
    {
    }

    auto search_query(std::unique_ptr<couchbase::search_query> search_query) -> search_request&
    {
        if (search_query_.has_value()) {
            throw std::invalid_argument("There can only be one search_query in a search request");
        }
        search_query_ = std::move(search_query);
        return *this;
    }

    auto vector_search(couchbase::vector_search vector_search) -> search_request&
    {
        if (vector_search_.has_value()) {
            throw std::invalid_argument("There can only be one vector_search in a search request");
        }
        vector_search_ = std::move(vector_search);
        return *this;
    }

    [[nodiscard]] const std::optional<std::unique_ptr<couchbase::search_query>>& search_query() const
    {
        return search_query_;
    }

    [[nodiscard]] const std::optional<couchbase::vector_search>& vector_search() const
    {
        return vector_search_;
    }

  private:
    std::optional<std::unique_ptr<couchbase::search_query>> search_query_;
    std::optional<couchbase::vector_search> vector_search_;
};
} // namespace couchbase
