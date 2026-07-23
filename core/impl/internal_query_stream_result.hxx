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

#include <couchbase/error_codes.hxx>
#include <couchbase/query_meta_data.hxx>
#include <couchbase/query_stream_result.hxx>

#include "core/query_stream.hxx"

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core::impl
{
class observability_recorder;
} // namespace couchbase::core::impl

namespace couchbase
{
class internal_query_stream_result
  : public std::enable_shared_from_this<internal_query_stream_result>
{
public:
  internal_query_stream_result(core::query_stream stream,
                               std::unique_ptr<core::impl::observability_recorder> obs_rec);
  // Non-copyable and non-movable: holds a std::mutex and is only ever owned via shared_ptr.
  internal_query_stream_result(const internal_query_stream_result&) = delete;
  internal_query_stream_result(internal_query_stream_result&&) = delete;
  auto operator=(const internal_query_stream_result&) -> internal_query_stream_result& = delete;
  auto operator=(internal_query_stream_result&&) -> internal_query_stream_result& = delete;

  ~internal_query_stream_result();

  void next(query_row_handler&& handler);
  void cancel();

  /**
   * Returns the query signature captured from the response metadata.
   */
  [[nodiscard]] auto signature() const -> std::optional<codec::binary>;

  /**
   * Returns a future that resolves to query metadata once the stream reaches its terminal state
   * (all rows consumed or an error). If the terminal has already been reached, returns a
   * ready future built from the stored value.
   */
  auto meta_data() -> std::future<std::pair<error, query_meta_data>>;

private:
  /**
   * Called when next_row reports the terminal condition (nullopt row).
   * Resolves the meta_data promise from the core stream.
   *
   * @param ec error code from the terminal next_row call (falsy = clean end)
   */
  void resolve_meta_data(std::error_code ec);

  core::query_stream stream_;

  // Observability recorder for the whole streaming operation. Its operation span already parents
  // the request; finish() is called exactly once from resolve_meta_data() (the single terminal
  // hook, reached on drain/error/cancel/destruction), recording the operation latency metric and
  // ending the span — the same instrumentation the buffered query() emits via its recorder.
  std::unique_ptr<core::impl::observability_recorder> obs_rec_;

  // Guards the "only one outstanding next()" contract. Set while a pull is in flight and cleared
  // when its handler runs; a concurrent/overlapping next() is rejected with invalid_argument
  // rather than racing the single-consumer channel. Shared across handle copies (they alias one
  // stream), so copies cannot smuggle in a second concurrent pull either.
  std::atomic_bool pull_in_flight_{ false };

  mutable std::mutex meta_mutex_{};
  bool terminal_reached_{ false };
  std::pair<error, query_meta_data> terminal_value_{};
  // meta_data() may be called any number of times before the stream terminates; each call parks a
  // promise here and they are all satisfied together at the terminal. (A single shared promise
  // would throw future_already_retrieved on the second call.)
  std::vector<std::promise<std::pair<error, query_meta_data>>> pending_meta_promises_{};
};
} // namespace couchbase
