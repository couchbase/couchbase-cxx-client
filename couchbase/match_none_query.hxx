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

namespace couchbase
{
/**
 * A query that matches nothing.
 *
 * @see https://docs.couchbase.com/server/current/fts/fts-supported-queries-match-none.html server documentation
 *
 * @since 1.0.0
 * @committed
 */
class match_none_query : public search_query
{
  public:
    /**
     * @return encoded representation of the query.
     *
     * @since 1.0.0
     * @internal
     */
    [[nodiscard]] auto encode() const -> encoded_search_query override;
};
} // namespace couchbase
