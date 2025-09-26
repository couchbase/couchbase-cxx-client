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
               const std::string& client_context_id);

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
   * Cancels the row stream & closes the HTTP connection
   */
  void cancel();

  /**
   * If all rows have been streamed, returns the metadata encoded as JSON.
   */
  auto metadata() -> std::optional<std::string>;

  /**
   * Enables the streamer to timeout after the specified duration.
   */
  void set_streamer_timeout(std::chrono::milliseconds timeout);

  /**
   * Enables the streamer execute upstream functionality after streaming completes or the request
   * times out (if the streamer timeout has been set).
   */
  void set_streamer_done_callback(utils::movable_function<void()>&& done_callback);

private:
  std::shared_ptr<row_streamer_impl> impl_;
};
} // namespace couchbase::core
