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

#include <couchbase/key_value_error_context.hxx>
#include <couchbase/manager_error_context.hxx>
#include <couchbase/query_error_context.hxx>

#include <tao/json/value.hpp>

namespace couchbase::core::impl
{

auto
key_value_error_context_to_json(const key_value_error_context& ctx) -> tao::json::value;

auto
manager_error_context_to_json(const manager_error_context& ctx) -> tao::json::value;

auto
query_error_context_to_json(const query_error_context& ctx) -> tao::json::value;

// TODO other types of error_context_to_json

} // namespace couchbase::core::impl
