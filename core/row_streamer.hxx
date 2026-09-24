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

#include "core/io/stream_deadline.hxx"
#include "row_streamer_options.hxx"
#include "utils/movable_function.hxx"

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <system_error>

namespace asio
{
class io_context;
} // namespace asio

namespace couchbase::core
{
class row_streamer_impl;
class http_response_body;

class row_streamer
{
public:
  row_streamer(asio::io_context& io,
               http_response_body body,
               const std::string& pointer_expression,
               row_streamer_options options = {});

  /**
   *  Starts the row stream and returns all the metadata preceding the first row. This typically
   * includes errors, if available.
   */
  void start(utils::movable_function<void(std::string, std::error_code)>&& handler);

  /**
   * Retrieves the next row
   */
  void next_row(utils::movable_function<void(std::string, std::error_code)>&& handler);

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
   */
  [[nodiscard]] auto set_deadline(std::chrono::time_point<std::chrono::steady_clock> deadline_tp)
    -> io::deadline_state;

  /**
   * Cancels the row stream & closes the HTTP connection
   */
  void cancel();

  /**
   * If all rows have been streamed, returns the metadata encoded as JSON.
   */
  auto metadata() -> std::optional<std::string>;

  /**
   * Number of row bytes currently buffered (delivered to the channel but not yet consumed).
   * Exposed for back-pressure observability in tests and diagnostics.
   */
  [[nodiscard]] auto buffered_bytes() const -> std::size_t;

private:
  std::shared_ptr<row_streamer_impl> impl_;
};
} // namespace couchbase::core
