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

#pragma once

#include "core/json_string.hxx"

#include <tao/json/value.hpp>

namespace couchbase::core::utils::json
{
tao::json::value
parse(std::string_view input);

tao::json::value
parse(const json_string& input);

tao::json::value
parse(const char* input, std::size_t size);

std::string
generate(const tao::json::value& object);

tao::json::value
parse_binary(const std::vector<std::byte>& input);

std::vector<std::byte>
generate_binary(const tao::json::value& object);
} // namespace couchbase::core::utils::json
