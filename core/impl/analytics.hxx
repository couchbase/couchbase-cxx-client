/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/analytics_options.hxx>
#include <couchbase/analytics_status.hxx>

#include <optional>
#include <string>

namespace couchbase::core
{
class cluster;
} // namespace couchbase::core

namespace couchbase::core::impl
{
/**
 * Maps the core analytics status enum onto the public analytics_status enum. Shared by the buffered
 * result builder and the streaming result handle so there is one source of truth.
 */
auto
map_analytics_status(core::operations::analytics_response::analytics_status status)
  -> analytics_status;

auto
build_result(core::operations::analytics_response& resp) -> analytics_result;

/**
 * Dispatches an analytics query as a streaming request and resolves the handler with an
 * analytics_stream_result. Unlike query there is no prepared-statement fallback; every request
 * takes the lazy streaming path.
 */
void
dispatch_analytics_stream(const core::cluster& core,
                          core::operations::analytics_request request,
                          analytics_stream_handler&& handler);

auto
build_analytics_request(std::string statement,
                        analytics_options::built options,
                        std::optional<std::string> bucket_name,
                        std::optional<std::string> scope_name,
                        std::shared_ptr<couchbase::tracing::request_span> op_span)
  -> core::operations::analytics_request;
} // namespace couchbase::core::impl
