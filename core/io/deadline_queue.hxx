/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <utility>

namespace couchbase::core::io
{
struct deadline_target;

// Linkage of one target in a deadline_queue. Owned by the target, written only under the queue's
// mutex.
struct deadline_hook {
  deadline_target* prev{ nullptr };
  deadline_target* next{ nullptr };
  std::chrono::steady_clock::time_point expiry{};
  std::chrono::steady_clock::duration timeout{};
  // Non-null while linked. The queue's strong reference to the target: it keeps a pending target
  // alive until it expires or is removed.
  std::shared_ptr<deadline_target> self{};
};

struct deadline_target {
  deadline_target() = default;
  deadline_target(const deadline_target&) = delete;
  deadline_target(deadline_target&&) = delete;
  auto operator=(const deadline_target&) -> deadline_target& = delete;
  auto operator=(deadline_target&&) -> deadline_target& = delete;
  virtual ~deadline_target() = default;

  // At most one of on_deadline() and on_stop() is called per add(), outside the queue's mutex.
  // on_deadline() runs on an I/O thread when the timeout elapses.
  virtual void on_deadline() = 0;
  // Runs from stop(), or through the io_context for a target added after stop().
  virtual void on_stop() = 0;

  deadline_hook deadline_hook_{};
};

// One timer for every pending request of a bucket. Requests that share a timeout expire in the
// order they were added, so each timeout keeps a FIFO list and the earliest expiry is the head of
// one of them.
class deadline_queue : public std::enable_shared_from_this<deadline_queue>
{
public:
  explicit deadline_queue(asio::io_context& ctx);
  // Calls stop(). A linked entry holds a strong reference to its own target, so an entry still
  // linked at destruction would never be released. An exception from on_stop() here terminates; a
  // bucket's queue is empty by then, because every linked command holds its bucket.
  ~deadline_queue();
  deadline_queue(const deadline_queue&) = delete;
  deadline_queue(deadline_queue&&) = delete;
  auto operator=(const deadline_queue&) -> deadline_queue& = delete;
  auto operator=(deadline_queue&&) -> deadline_queue& = delete;

  // Returns the expiry, and stores it in *expiry_out before the target can expire, so a target
  // that keeps its own copy never has it written concurrently with on_deadline(). After stop(), the
  // target's on_stop() is posted to the io_context instead.
  auto add(std::shared_ptr<deadline_target> target,
           std::chrono::steady_clock::duration timeout,
           std::chrono::steady_clock::time_point* expiry_out = nullptr)
    -> std::chrono::steady_clock::time_point;

  // No effect on a target that is not linked: already expired, removed or never added.
  void remove(deadline_target& target) noexcept;

  // Calls on_stop() on every pending target, also when an earlier on_stop() throws; the first
  // exception is rethrown afterwards.
  void stop();

  // Number of distinct timeouts with a pending target. For tests.
  [[nodiscard]] auto timeout_lists() const -> std::size_t;

private:
  struct fifo {
    deadline_target* head{ nullptr };
    deadline_target* tail{ nullptr };
  };

  void unlink(deadline_target& target, std::shared_ptr<deadline_target>& released);
  void arm(std::chrono::steady_clock::time_point expiry);
  void on_timer();

  asio::io_context& ctx_;
  mutable std::mutex mutex_{};
  asio::steady_timer timer_;
  std::map<std::chrono::steady_clock::duration, fifo> lists_{};
  // (expiry, timeout) of every list's head, earliest first. A bucket's timeouts are each request's
  // remaining budget in milliseconds, so the lists can number in the thousands; a wake must not
  // visit each of them.
  std::set<std::pair<std::chrono::steady_clock::time_point, std::chrono::steady_clock::duration>>
    heads_{};
  std::chrono::steady_clock::time_point armed_{ std::chrono::steady_clock::time_point::max() };
  bool stopped_{ false };
};
} // namespace couchbase::core::io
