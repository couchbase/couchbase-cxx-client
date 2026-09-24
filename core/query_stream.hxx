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

#include "core/io/stream_deadline.hxx"
#include "core/operations/document_query.hxx"
#include "row_streamer.hxx"
#include "stream_error_details.hxx"
#include "utils/movable_function.hxx"

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class query_stream_impl;
class http_response_body;

/**
 * L3 streaming state machine for N1QL query responses.
 *
 * Wraps a row_streamer (pointed at "/results/^") and adds query-specific semantics:
 * preamble parsing (signature + upfront error detection), late trailer metadata, and
 * terminal error classification via map_query_error.
 */
class query_stream
{
public:
  /**
   * Constructs an empty handle (no underlying stream). Used to carry the "no stream" outcome on
   * the error path of the dispatch; none of the streaming methods may be called on it.
   */
  query_stream() = default;

  query_stream(asio::io_context& io, http_response_body body, row_streamer_options options = {});

  /**
   * Constructs a stream that replays an already-buffered response (rows are row-JSON strings,
   * metadata already parsed) without any JSON re-parsing. Used for the prepared-statement path,
   * which must run buffered but presents the uniform streaming handle.
   */
  query_stream(asio::io_context& io,
               std::vector<std::string> rows,
               operations::query_response::query_meta_data meta);

  /**
   * Starts the stream. Resolves once the preamble has been parsed. The early_error is set when
   * the response carried an upfront error (or the preamble failed to parse).
   */
  void start(utils::movable_function<void(std::error_code early_error)>&& on_ready);

  /**
   * Service-reported diagnostics from the most recently parsed JSON section — the preamble after
   * start(), the trailer once the terminal has been reached. The dispatch layer stamps these into
   * the error context so a streaming failure reports the same detail as a buffered one.
   */
  [[nodiscard]] auto error_details() const -> stream_error_details;

  /**
   * Retrieves the next row. A populated row is delivered with a falsy error_code. An empty row
   * (std::nullopt) signals the end: a falsy error_code means a clean success end, a truthy
   * error_code means a trailing query/transport error.
   */
  void next_row(
    utils::movable_function<void(std::optional<std::string> row, std::error_code)>&& handler);

  /**
   * Signature captured from the preamble (available after start resolves).
   */
  [[nodiscard]] auto signature() const -> std::optional<std::string>;

  /**
   * Trailer metadata. Valid only after the stream has reached its end.
   */
  [[nodiscard]] auto meta_data() const
    -> std::optional<operations::query_response::query_meta_data>;

  /**
   * Closes the HTTP body at `deadline_tp`, whether or not a row is being pulled, releasing the
   * connection where one is still held. A response received in full holds none: once its bytes
   * have also been parsed the call arms nothing and reports body_already_ended, and while bytes
   * remain unparsed it ends the body, dropping those and reporting the timeout. Rows already
   * parsed stay consumable either way. The inter-read idle timer is armed only while a socket
   * read is in flight, and a consumer stopped above the high-water mark has none.
   *
   * The terminal reaches the consumer through a pull. A pull outstanding at expiry completes
   * with the timeout. A read that completes the response is the exception: the whole response
   * arrived, so those bytes are delivered as a clean end. Otherwise the next pull reports the
   * terminal, after any rows already buffered.
   *
   * Optional. The terminal is unambiguous_timeout for a read-only request, ambiguous_timeout
   * otherwise. Re-arming before expiry replaces the deadline. Once the stream has ended a
   * further call arms nothing and reports body_already_ended. Callable from any thread.
   *
   * A stream replaying an already-buffered response holds no socket, so a deadline on it is a
   * no-op.
   */
  [[nodiscard]] auto set_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp)
    -> io::deadline_state;

  /**
   * Cancels the stream & closes the HTTP connection.
   */
  void cancel();

private:
  std::shared_ptr<query_stream_impl> impl_;
};
} // namespace couchbase::core
