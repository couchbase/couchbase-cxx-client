/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/document_id.hxx>

#include <fmt/core.h>

#include <algorithm>
#include <stdexcept>

namespace couchbase
{
document_id::document_id()
  : scope_("_default")
  , collection_("_default")
  , collection_path_("_default._default")
{
}

static bool
is_valid_collection_char(char ch)
{
    if (ch >= 'A' && ch <= 'Z') {
        return true;
    }
    if (ch >= 'a' && ch <= 'z') {
        return true;
    }
    if (ch >= '0' && ch <= '9') {
        return true;
    }
    switch (ch) {
        case '_':
        case '-':
        case '%':
            return true;
        default:
            return false;
    }
}

static bool
is_valid_collection_element(const std::string_view element)
{
    if (element.empty() || element.size() > 251) {
        return false;
    }
    return std::all_of(element.begin(), element.end(), is_valid_collection_char);
}

document_id::document_id(std::string bucket, std::string scope, std::string collection, std::string key, bool use_collections)
  : bucket_(std::move(bucket))
  , scope_(std::move(scope))
  , collection_(std::move(collection))
  , key_(std::move(key))
  , use_collections_(use_collections)
{
    if (use_collections_) {
        if (!is_valid_collection_element(scope_)) {
            throw std::invalid_argument("invalid scope name");
        }
        if (!is_valid_collection_element(collection_)) {
            throw std::invalid_argument("invalid collection name");
        }
    }

    collection_path_ = fmt::format("{}.{}", scope_, collection_);
}

bool
document_id::has_default_collection() const
{
    return !use_collections_ || collection_path_ == "_default._default";
}
} // namespace couchbase
