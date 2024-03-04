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

#include "internal_operation_error_context.hxx"

#include <tao/json/to_string.hpp>

#include <utility>


namespace couchbase
{
internal_operation_error_context::internal_operation_error_context(internal_operation_error_context&& other) noexcept = default;

internal_operation_error_context&
internal_operation_error_context::operator=(internal_operation_error_context&& other) noexcept = default;

internal_operation_error_context::internal_operation_error_context(tao::json::value ctx)
  : ctx_ {std::move( ctx )}
{
}

auto
internal_operation_error_context::to_json_pretty() const -> const std::string
{
    return tao::json::to_string(ctx_, 2);
}

auto
internal_operation_error_context::to_json() const -> const std::string
{
    return tao::json::to_string(ctx_);
}
} // namespace couchbase
