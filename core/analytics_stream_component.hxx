/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "analytics_stream.hxx"
#include "error_context/analytics.hxx"
#include "operations/document_analytics.hxx"
#include "utils/movable_function.hxx"

#include <chrono>
#include <memory>
#include <system_error>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class http_component;

/**
 * Dispatches an analytics query as a streaming HTTP request and resolves to an analytics_stream
 * handle once the response preamble (signature + upfront errors) has been parsed.
 *
 * Unlike the buffered analytics path this never materialises the full response body; it performs no
 * client-side retry.
 */
class analytics_stream_component
{
public:
  // The context carries the error code (error_context::analytics::ec) alongside the diagnostics the
  // buffered path reports — statement, parameters, the service's first error, HTTP status and body,
  // and where the request was dispatched — so a streaming failure is as diagnosable as a buffered
  // one. It is delivered on both outcomes: when the stream fails to start it describes that
  // failure, and when it starts it describes the request and its dispatch, for the consumer to
  // report a terminal error against later. Only `ec` differs between the two: falsy means the
  // stream started.
  using handler_type = utils::movable_function<void(analytics_stream, error_context::analytics)>;

  analytics_stream_component(asio::io_context& io,
                             http_component http,
                             std::chrono::milliseconds default_timeout,
                             row_streamer_options streaming_options = {});

  void execute(operations::analytics_request request, handler_type&& handler) const;

private:
  std::shared_ptr<class analytics_stream_component_impl> impl_;
};
} // namespace couchbase::core
