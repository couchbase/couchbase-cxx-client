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

#pragma once

#include <cstdint>
#include <system_error>

namespace couchbase::core::columnar
{
auto
columnar_category() noexcept -> const std::error_category&;

enum class errc : std::uint8_t {
  generic = 1,
  invalid_credential = 2,
  timeout = 3,
  query_error = 4,
};

inline auto
make_error_code(errc e) noexcept -> std::error_code
{
  return { static_cast<int>(e), columnar_category() };
}

auto
maybe_convert_error_code(std::error_code e) -> std::error_code;
} // namespace couchbase::core::columnar

template<>
struct std::is_error_code_enum<couchbase::core::columnar::errc> : std::true_type {
};
