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

#include "core/operations/document_query.hxx"

#include <couchbase/query_options.hxx>
#include <couchbase/query_status.hxx>

#include <memory>
#include <string>

namespace couchbase::core
{
class cluster;
} // namespace couchbase::core

namespace couchbase::core::impl
{
class observability_recorder;

/**
 * Maps the N1QL `status` string from a query response onto the public query_status enum. Shared by
 * the buffered result builder and the streaming result handle so there is one source of truth.
 */
auto
map_query_status(std::string status) -> query_status;

auto
build_query_request(std::string statement,
                    std::optional<std::string> query_context,
                    query_options::built options,
                    std::shared_ptr<couchbase::tracing::request_span> op_span)
  -> core::operations::query_request;

auto
build_result(operations::query_response& resp) -> query_result;

/**
 * Dispatches a query as a streaming request and resolves the handler with a query_stream_result.
 *
 * Adhoc requests take the lazy streaming path. Prepared statements (request.adhoc == false) are
 * not streamed; they fall back to the buffered query() path and their rows are replayed through an
 * in-memory query_stream so callers observe identical semantics.
 *
 * @param obs_rec the operation's observability recorder. Its operation span is already threaded
 * into request.parent_span by the caller; ownership is transferred to the resulting stream handle,
 * which calls finish() once when the stream reaches its terminal (drained, errored, or cancelled),
 * so the streaming path emits the same operation span + latency metric as the buffered query().
 */
void
dispatch_query_stream(const core::cluster& core,
                      core::operations::query_request request,
                      std::unique_ptr<observability_recorder> obs_rec,
                      query_stream_handler&& handler);
} // namespace couchbase::core::impl
