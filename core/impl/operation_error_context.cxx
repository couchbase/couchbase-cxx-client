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

#include "error_context_to_json.hxx"

#include "tao/json/to_string.hpp"

#include <couchbase/operation_error_context.hxx>

#include <utility>

namespace couchbase
{
operation_error_context::operation_error_context(core_json_context ctx)
  : ctx_{std::move( ctx )}
{
}

auto
operation_error_context::to_string() const -> std::string
{
    return tao::json::to_string(ctx_, 2);
}

auto
operation_error_context::to_json() const -> std::string
{
    return tao::json::to_string(ctx_);
}
} // namespace couchbase
