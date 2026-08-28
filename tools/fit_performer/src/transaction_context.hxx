/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "exceptions.hxx"

#include <spdlog/spdlog.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace fit_cxx
{

// A counting latch, mirroring the Java performer's use of CountDownLatch. set()
// counts the latch down by one; wait() blocks until the count reaches zero.
// Safe to set() and wait() concurrently from different threads.
class counting_latch
{
public:
  counting_latch(std::string name, std::uint32_t initial_count)
    : name_(std::move(name))
    , value_(initial_count)
  {
  }

  void set()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (value_ > 0) {
      --value_;
    } else {
      spdlog::error("cannot set latch, {} is already at 0", name_);
    }
    if (value_ == 0) {
      cv_.notify_all();
    }
  }

  // Blocks until the latch reaches zero OR the latch is cancelled. cancel() is used on transaction
  // timeout/teardown so a wait for a latch that will never reach zero (e.g. the driver
  // disconnected, broadcast the wrong latch name, or never broadcasts at all) cannot wedge the
  // worker thread forever and deadlock the stream's worker.join(). A cancelled wait throws so the
  // surrounding transaction unwinds rather than silently proceeding as if the precondition had been
  // met.
  void wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (value_ == 0) {
      spdlog::trace("latch {} already at 0, returning immediately", name_);
      return;
    }
    spdlog::trace("waiting for latch {}", name_);
    cv_.wait(lock, [this]() -> bool {
      return value_ == 0 || cancelled_;
    });
    if (value_ != 0) {
      spdlog::warn("wait for latch {} cancelled before it reached 0", name_);
      throw std::runtime_error("wait for latch " + name_ + " was cancelled");
    }
    spdlog::trace("wait for latch {} done, returning now", name_);
  }

  // Wakes any thread blocked in wait() so it can abort. Idempotent.
  void cancel()
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    cancelled_ = true;
    cv_.notify_all();
  }

  [[nodiscard]] const std::string& name() const
  {
    return name_;
  }

private:
  std::string name_;
  std::uint32_t value_;
  bool cancelled_{ false };
  std::mutex mutex_;
  std::condition_variable cv_;
};

// A thread-safe registry of named latches owned by a single transaction stream.
// The Java performer keeps these in a ConcurrentHashMap inside the per-transaction
// object; here the registry has the same per-transaction lifetime, replacing the
// previous (incorrectly Connection-scoped) latch storage. Latches are handed out
// as shared_ptr so a worker blocked in wait() keeps its latch alive regardless of
// concurrent registry mutation.
class latch_registry
{
public:
  void add(const std::string& name, std::uint32_t initial_count)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    latches_.insert_or_assign(name, std::make_shared<counting_latch>(name, initial_count));
  }

  // Returns the named latch, throwing if it was never registered.
  std::shared_ptr<counting_latch> get(const std::string& name)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (auto it = latches_.find(name); it != latches_.end()) {
      return it->second;
    }
    // performer_exception (not std::runtime_error) so the gRPC handlers that look up a latch can
    // convert this to an INVALID_ARGUMENT status reporting the real cause, instead of it surfacing
    // as a misleading timeout via run_with_timeout's generic std::exception handler.
    throw performer_exception::invalid_argument("unknown latch name " + name);
  }

  // Cancels every registered latch so any worker blocked in wait() unblocks and aborts. Used on
  // transaction timeout and on stream teardown.
  void cancel_all()
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    for (auto& [name, latch] : latches_) {
      latch->cancel();
    }
  }

private:
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<counting_latch>> latches_;
};

// Per-transaction-stream context shared between the stream's read loop and its
// single worker thread. It owns the latch registry and an optional broadcaster
// used to tell the driver (and thereby other concurrent transactions) that a
// latch was set from within this transaction. The broadcaster is empty on the
// unary transactionCreate path, which has no concurrent peers to notify.
class transaction_context
{
public:
  explicit transaction_context(std::function<void(const std::string&)> broadcast_set_latch = {})
    : broadcast_set_latch_(std::move(broadcast_set_latch))
  {
  }

  latch_registry& latches()
  {
    return latches_;
  }

  // Mirrors Java handleSetLatch's "tell other workers via the driver" step.
  void broadcast_set_latch(const std::string& name)
  {
    if (broadcast_set_latch_) {
      broadcast_set_latch_(name);
    }
  }

private:
  latch_registry latches_;
  std::function<void(const std::string&)> broadcast_set_latch_;
};

} // namespace fit_cxx
