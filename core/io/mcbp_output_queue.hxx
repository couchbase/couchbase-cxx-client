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

#include <cstddef>
#include <mutex>
#include <utility>
#include <vector>

namespace couchbase::core::io
{
/**
 * Send-side buffering for an mcbp connection.
 *
 * A KV operation appends its encoded frame from the caller's thread while the connection's write
 * loop (`do_write` / the async_write completion) runs on an IO thread; this queue owns both buffers
 * and the single flag that coordinates them, so all of that state is mutated under one lock.
 *
 * The flag exists to eliminate a per-operation `asio::post`. `enqueue()`/`mark_for_dispatch()` only
 * ask the caller to schedule a write on the idle -> scheduled transition; while a write is already
 * scheduled or in flight they return false, because the in-flight write's completion is what
 * re-drives the loop. Draining the queue to empty returns it to idle so the next enqueue re-arms.
 */
class mcbp_output_queue
{
public:
  using buffer = std::vector<std::byte>;

  /**
   * Append a buffer and stage it for the next write.
   *
   * @return true if the caller must schedule a write (the queue transitioned from idle to
   * scheduled); false if a write is already scheduled or in flight.
   */
  [[nodiscard]] auto enqueue(buffer&& buf) -> bool
  {
    const std::scoped_lock lock(mutex_);
    output_.emplace_back(std::move(buf));
    return arm();
  }

  /**
   * Append a buffer without requesting a write. Used to refill the queue before a single
   * mark_for_dispatch(), e.g. when draining the bootstrap pending buffer.
   */
  void stage(buffer&& buf)
  {
    const std::scoped_lock lock(mutex_);
    output_.emplace_back(std::move(buf));
  }

  /**
   * Request a write for data staged earlier.
   *
   * @return true if the caller must schedule a write; false if nothing is queued or a write is
   * already scheduled or in flight.
   */
  [[nodiscard]] auto mark_for_dispatch() -> bool
  {
    const std::scoped_lock lock(mutex_);
    if (output_.empty()) {
      return false;
    }
    return arm();
  }

  /**
   * Move all queued buffers into the writing batch so it can be handed to async_write.
   *
   * @return true if there is a batch to send (see writing()); false if nothing is queued (the queue
   * is returned to idle) or a write is already in flight (left untouched).
   */
  [[nodiscard]] auto begin_writing() -> bool
  {
    const std::scoped_lock lock(mutex_);
    if (!writing_.empty()) {
      // A batch is still in flight; issuing another async_write concurrently would corrupt the
      // stream. Leave it scheduled — the in-flight completion will re-drive us.
      return false;
    }
    if (output_.empty()) {
      scheduled_ = false;
      return false;
    }
    std::swap(writing_, output_);
    return true;
  }

  /**
   * The batch currently being written. Only valid between a begin_writing() that returned true and
   * the matching finish_writing(); accessed solely on the IO thread during that window.
   */
  [[nodiscard]] auto writing() const -> const std::vector<buffer>&
  {
    return writing_;
  }

  /**
   * Release the batch handed out by begin_writing() once its async_write has completed.
   */
  void finish_writing()
  {
    const std::scoped_lock lock(mutex_);
    writing_.clear();
  }

  /**
   * Discard everything and return to idle (used when the connection is reset or stopped).
   */
  void reset()
  {
    const std::scoped_lock lock(mutex_);
    output_.clear();
    writing_.clear();
    scheduled_ = false;
  }

private:
  // Precondition: mutex_ held. Transition idle -> scheduled, reporting whether the caller owns the
  // resulting write post.
  auto arm() -> bool
  {
    if (scheduled_) {
      return false;
    }
    scheduled_ = true;
    return true;
  }

  std::vector<buffer> output_{};
  std::vector<buffer> writing_{};
  bool scheduled_{ false };
  std::mutex mutex_{};
};
} // namespace couchbase::core::io
