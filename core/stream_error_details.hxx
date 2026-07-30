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

#include <cstdint>
#include <string>

namespace couchbase::core
{
/**
 * Service-reported diagnostics lifted out of a streaming response, so a streaming failure can be
 * reported with the same context the buffered path provides.
 *
 * A stream parses two JSON sections that can carry errors: the preamble (everything before the row
 * array, which is where a rejected request reports itself) and the trailer (which is where a
 * request that failed part-way through reports itself). Whichever section produced the latest
 * diagnostics is what these fields describe; `raw` is the JSON text they came from, and stands in
 * for the buffered path's whole-body capture.
 */
struct stream_error_details {
  std::uint64_t first_error_code{};
  std::string first_error_message{};
  std::string client_context_id{};
  std::string raw{};
};

/**
 * Stamps stream diagnostics onto a query/analytics error context. Templated on the context because
 * the two services carry structurally identical contexts under different types.
 *
 * Each field is only written when the stream actually reported it, so these diagnostics augment the
 * context rather than erasing it. That matters for the prepared-statement path, which reaches the
 * streaming handle by replaying an already-buffered response: its context arrives fully populated
 * (http_command captures the response body for every request), and there is no raw JSON left for
 * the replay to report, so an unconditional write would clear the body it already had.
 *
 * client_context_id is deliberately left alone: the context already holds the id the request was
 * sent with, which is the one to correlate against server logs, and the echoed value can differ.
 */
template<typename Context>
void
apply_error_details(Context& ctx, const stream_error_details& details)
{
  if (details.first_error_code != 0) {
    ctx.first_error_code = details.first_error_code;
  }
  if (!details.first_error_message.empty()) {
    ctx.first_error_message = details.first_error_message;
  }
  if (!details.raw.empty()) {
    ctx.http_body = details.raw;
  }
}
} // namespace couchbase::core
