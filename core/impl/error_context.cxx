/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include <couchbase/error_context.hxx>

#include <tao/json/to_string.hpp>

#include <string>
#include <utility>

namespace couchbase
{
error_context::error_context(internal_error_context internal)
  : internal_(std::move(internal))
{
}

error_context::error_context(couchbase::internal_error_context internal, couchbase::internal_error_context internal_metadata)
  : internal_(std::move(internal))
  , internal_metadata_(std::move(internal_metadata))
{
}

error_context::operator bool() const
{
    return internal_.operator bool();
}

auto
error_context::to_json(error_context_json_format format) const -> std::string
{
    switch (format) {
        case error_context_json_format::compact:
            return tao::json::to_string(internal_);
        case error_context_json_format::pretty:
            return tao::json::to_string(internal_, 2);
    }
    return "{}";
}

auto
error_context::internal_metadata(couchbase::error_context_json_format format) const -> std::string
{
    switch (format) {
        case error_context_json_format::compact:
            return tao::json::to_string(internal_metadata_);
        case error_context_json_format::pretty:
            return tao::json::to_string(internal_metadata_, 2);
    }
    return "{}";
}
} // namespace couchbase
