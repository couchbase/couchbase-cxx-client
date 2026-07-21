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

#include "core/operations/document_analytics.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/forward.hpp>

#include <system_error>

namespace couchbase::core::operations
{

/**
 * Parse analytics meta-data fields (status, metrics, warnings, errors, signature, requestID,
 * clientContextID) from a top-level response payload.
 *
 * Pure function — no side effects, no access to request state.
 */
auto
parse_analytics_meta(const tao::json::value& payload) -> analytics_response::analytics_meta_data;

/**
 * Map analytics meta-data to an error_code.
 *
 * Pure classifier — no side effects, no exceptions thrown. Returns an empty error_code when the
 * analytics query succeeded (meta.status == success).
 */
[[nodiscard]] auto
map_analytics_error(const analytics_response::analytics_meta_data& meta) -> std::error_code;

} // namespace couchbase::core::operations
