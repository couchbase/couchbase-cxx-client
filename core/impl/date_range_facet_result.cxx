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

#include "internal_date_range_facet_result.hxx"

#include <couchbase/date_range_facet_result.hxx>

namespace couchbase
{
date_range_facet_result::date_range_facet_result(internal_date_range_facet_result internal)
  : internal_{ std::make_unique<internal_date_range_facet_result>(std::move(internal)) }
{
}

auto
date_range_facet_result::name() const -> const std::string&
{
    return internal_->name();
}

auto
date_range_facet_result::field() const -> const std::string&
{
    return internal_->field();
}

auto
date_range_facet_result::total() const -> std::uint64_t
{
    return internal_->total();
}

auto
date_range_facet_result::missing() const -> std::uint64_t
{
    return internal_->missing();
}

auto
date_range_facet_result::other() const -> std::uint64_t
{
    return internal_->other();
}

auto
date_range_facet_result::date_ranges() const -> const std::vector<search_date_range>&
{
    return internal_->date_ranges();
}
} // namespace couchbase
