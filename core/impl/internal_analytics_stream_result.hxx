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

#include <couchbase/analytics_meta_data.hxx>
#include <couchbase/analytics_stream_result.hxx>
#include <couchbase/error_codes.hxx>

#include "core/analytics_stream.hxx"

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase
{
class internal_analytics_stream_result
  : public std::enable_shared_from_this<internal_analytics_stream_result>
{
public:
  explicit internal_analytics_stream_result(core::analytics_stream stream);
  // Non-copyable and non-movable: holds a std::mutex and is only ever owned via shared_ptr.
  internal_analytics_stream_result(const internal_analytics_stream_result&) = delete;
  internal_analytics_stream_result(internal_analytics_stream_result&&) = delete;
  auto operator=(const internal_analytics_stream_result&)
    -> internal_analytics_stream_result& = delete;
  auto operator=(internal_analytics_stream_result&&) -> internal_analytics_stream_result& = delete;

  ~internal_analytics_stream_result();

  void next(analytics_row_handler&& handler);
  void cancel();

  /**
   * Returns the analytics signature captured from the response metadata.
   */
  [[nodiscard]] auto signature() const -> std::optional<codec::binary>;

  /**
   * Returns a future that resolves to analytics metadata once the stream reaches its terminal
   * state (all rows consumed or an error). If the terminal has already been reached, returns a
   * ready future built from the stored value.
   */
  auto meta_data() -> std::future<std::pair<error, analytics_meta_data>>;

private:
  /**
   * Called when next_row reports the terminal condition (nullopt row).
   * Resolves the meta_data promise from the core stream.
   *
   * @param ec error code from the terminal next_row call (falsy = clean end)
   */
  void resolve_meta_data(std::error_code ec);

  core::analytics_stream stream_;

  // Guards the "only one outstanding next()" contract. Set while a pull is in flight and cleared
  // when its handler runs; a concurrent/overlapping next() is rejected with invalid_argument
  // rather than racing the single-consumer channel. Shared across handle copies (they alias one
  // stream), so copies cannot smuggle in a second concurrent pull either.
  std::atomic_bool pull_in_flight_{ false };

  mutable std::mutex meta_mutex_{};
  bool terminal_reached_{ false };
  std::pair<error, analytics_meta_data> terminal_value_{};
  // meta_data() may be called any number of times before the stream terminates; each call parks a
  // promise here and they are all satisfied together at the terminal. (A single shared promise
  // would throw future_already_retrieved on the second call.)
  std::vector<std::promise<std::pair<error, analytics_meta_data>>> pending_meta_promises_{};
};
} // namespace couchbase
