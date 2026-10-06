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

#include "eventing_problem.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/forward.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <system_error>

namespace couchbase::core::operations::management
{

std::error_code
extract_common_error_code(std::uint32_t status_code, const std::string& response_body);

std::optional<std::error_code>
extract_common_query_error_code(std::uint64_t code, const std::string& message);

std::pair<std::error_code, eventing_problem>
extract_eventing_error_code(const tao::json::value& response, std::uint32_t status_code);

std::optional<std::error_code>
translate_query_error_code(std::uint64_t error,
                           const std::string& message,
                           std::uint64_t reason = 0);

/**
 * RFC-58: the first query error code in the body decides, as for a query statement. Only a code
 * without a mapping of its own, or a body without one, falls back on the HTTP status.
 */
[[nodiscard]] std::error_code
extract_query_error_code(std::uint32_t status_code, const std::string& response_body);

std::optional<std::error_code>
translate_analytics_error_code(std::uint64_t error, const std::string& message);

/**
 * RFC-58: the first analytics error code decides, through translate_analytics_error_code(). Only a
 * code without a mapping of its own falls back on the HTTP status.
 */
template<typename Problems>
[[nodiscard]] auto
extract_analytics_error_code(std::uint32_t status_code,
                             const Problems& errors,
                             const std::string& response_body) -> std::error_code
{
  if (!errors.empty()) {
    if (auto ec = translate_analytics_error_code(errors.front().code, errors.front().message); ec) {
      return ec.value();
    }
  }
  return extract_common_error_code(status_code, response_body);
}

std::optional<std::error_code>
translate_search_error_code(std::uint32_t status_code, const std::string& response_body);

} // namespace couchbase::core::operations::management
