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

#include "core/impl/encoded_search_query.hxx"
#include <couchbase/search_request.hxx>

namespace couchbase
{
class search_request_impl
{
  public:
    explicit search_request_impl(const search_query& query)
      : search_query_(query.encode())
    {
    }

    explicit search_request_impl(couchbase::vector_search vector_search)
      : vector_search_(vector_search)
    {
    }

    void search_query(const couchbase::search_query& query)
    {
        search_query_ = query.encode();
    }

    void vector_search(const couchbase::vector_search& vector_search)
    {
        vector_search_ = vector_search;
    }

    [[nodiscard]] std::optional<encoded_search_query> search_query() const
    {
        return search_query_;
    }

    [[nodiscard]] std::optional<couchbase::vector_search> vector_search() const
    {
        return vector_search_;
    }

  private:
    std::optional<encoded_search_query> search_query_;
    std::optional<couchbase::vector_search> vector_search_;
};

search_request::search_request(const couchbase::search_query& query)
  : impl_(std::make_shared<search_request_impl>(query))
{
}

search_request::search_request(const couchbase::vector_search& search)
  : impl_(std::make_shared<search_request_impl>(search))
{
}

auto
search_request::search_query(const couchbase::search_query& search_query) -> search_request&
{
    if (impl_->search_query().has_value()) {
        throw std::invalid_argument("There can only be one search_query in a search request");
    }
    impl_->search_query(search_query);
    return *this;
}

auto
search_request::vector_search(const couchbase::vector_search& vector_search) -> search_request&
{
    if (impl_->vector_search().has_value()) {
        throw std::invalid_argument("There can only be one vector_search in a search request");
    }
    impl_->vector_search(vector_search);
    return *this;
}

[[nodiscard]] std::optional<encoded_search_query>
search_request::search_query() const
{
    return impl_->search_query();
}

[[nodiscard]] std::optional<couchbase::vector_search>
search_request::vector_search() const
{
    return impl_->vector_search();
}
} // namespace couchbase
