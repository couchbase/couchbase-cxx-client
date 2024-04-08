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

#include <string>
#include <utility>

#include <tao/json/to_string.hpp>

namespace couchbase
{
error_context::error_context(internal_error_context internal)
  : internal_{ std::move(internal) }
{
}

auto
error_context::to_string() const -> std::string
{
    return tao::json::to_string(internal_, 2);
}

auto
error_context::to_json() const -> std::string
{
    return tao::json::to_string(internal_);
}
} // namespace couchbase
