/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025-Current Couchbase, Inc.
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

#include "signal_data.hxx"

#include <chrono>
#include <memory>
#include <optional>
#include <queue>

namespace couchbase::core
{
class signal_bridge_impl;

class signal_bridge_options
{
public:
  static constexpr std::size_t default_buffer_limit{ 1'000 };
  static constexpr double default_notification_threshold{ 0.7 };

  signal_bridge_options(std::size_t buffer_limit, double notification_threshold);

  signal_bridge_options() = default;
  ~signal_bridge_options() = default;
  signal_bridge_options(const signal_bridge_options&) = default;
  signal_bridge_options(signal_bridge_options&&) = default;
  auto operator=(const signal_bridge_options&) -> signal_bridge_options& = default;
  auto operator=(signal_bridge_options&&) -> signal_bridge_options& = default;

  auto buffer_limit(std::size_t buffer_limit) -> signal_bridge_options&;
  auto notification_threshold(double notification_threshold) -> signal_bridge_options&;

  [[nodiscard]] auto buffer_limit() const -> std::size_t;
  [[nodiscard]] auto notification_threshold() const -> double;

private:
  std::size_t buffer_limit_{ default_buffer_limit };
  double notification_threshold_{ default_notification_threshold };
};

class signal_bridge
{
public:
  static constexpr std::size_t default_buffer_limit{ 10'000 };
  static constexpr double default_notification_threshold{ 0.7 };

  explicit signal_bridge(const signal_bridge_options& options);
  ~signal_bridge();

  signal_bridge(signal_bridge&&) = default;
  signal_bridge(const signal_bridge&) = delete;

  auto operator=(signal_bridge&&) -> signal_bridge& = default;
  auto operator=(const signal_bridge&) -> signal_bridge& = delete;

  /**
   * Add signal data to the queue.
   *
   * The data will be discarded if the queue size reaches the buffer limit
   * (see signal_bridge_options::buffer_limit()).
   *
   * If the buffer size reaches or exceeds the notification threshold, waiting
   * threads will be notified (see signal_bridge_options::notification_threshold()).
   *
   * @param data The signal_data object to insert into the queue. The data is
   *        moved into the internal buffer.
   */
  void emplace(signal_data&& data);

  /**
   * Block the current thread until the buffer is ready (notified) or the given
   * timeout interval passes.
   *
   * @param interval The maximum duration to wait for the buffer to become ready.
   *
   * @returns An optional containing a queue of signal_data moved from the internal
   *          buffer if notified before timeout and the buffer is not empty.
   *          Returns an empty optional if the wait times out or the buffer is empty.
   */
  [[nodiscard]] auto wait_for_buffer_ready(std::chrono::milliseconds interval)
    -> std::optional<std::queue<signal_data>>;

  /**
   * Move out and return the entire buffer of signal events for consumption.
   *
   * This must be called by the user before the destruction of the signal bridge
   * to avoid losing any pending events.
   *
   * @returns A queue containing all signal_data currently stored in the buffer.
   *          The internal buffer will be left in a clean (empty) state after this call.
   */
  [[nodiscard]] auto take_buffer() -> std::queue<signal_data>;

private:
  std::unique_ptr<signal_bridge_impl> impl_;
};
} // namespace couchbase::core
