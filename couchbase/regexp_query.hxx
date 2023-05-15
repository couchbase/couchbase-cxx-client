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

#include <optional>
#include <string>

namespace couchbase
{
/**
 * Finds documents containing terms that match the specified regular expression.
 *
 * Match documents where field `reviews.content` contains words starting with `"inter"`:
 * @snippet test_unit_search.cxx search-regexp
 *
 * @see https://docs.couchbase.com/server/current/fts/fts-supported-queries-regexp.html server documentation
 *
 * @since 1.0.0
 * @committed
 */
class regexp_query : public search_query
{
  public:
    /**
     * Create a new regexp query.
     *
     * @param regexp the input string to be matched against
     *
     * @since 1.0.0
     * @committed
     */
    explicit regexp_query(std::string regexp)
      : regexp_{ std::move(regexp) }
    {
    }

    /**
     * If a field is specified, only terms in that field will be matched.
     *
     * @param field_name name of the field to be matched
     *
     * @return this query for chaining purposes.
     *
     * @since 1.0.0
     * @committed
     */
    auto field(std::string field_name) -> regexp_query&
    {
        field_ = std::move(field_name);
        return *this;
    }

    /**
     * @return encoded representation of the query.
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto encode() const -> encoded_search_query override;

  private:
    std::string regexp_;
    std::optional<std::string> field_{};
};
} // namespace couchbase
