/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include "internal/logging.hxx"

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>

namespace couchbase::core::transactions
{

class async_operation_conflict : public std::runtime_error
{
public:
  explicit async_operation_conflict(const std::string& msg)
    : std::runtime_error(msg)
  {
  }
};
struct attempt_mode {
  enum class modes {
    KV,
    QUERY,
  };
  modes mode{ modes::KV };
  std::string query_node;

  attempt_mode() = default;

  [[nodiscard]] auto is_query() const -> bool
  {
    return mode == modes::QUERY;
  }
};

class waitable_op_list
{
public:
  waitable_op_list() = default;

  void increment_ops()
  {
    change_count(1);
  }
  void decrement_ops()
  {
    change_count(-1);
  }
  void wait_and_block_ops()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_ops_.wait(lock, [this]() {
      return (0 == count_);
    });
    // we have the lock.  Block all further ops
    allow_ops_ = false;
  }
  // Non-blocking peek at whether query mode has already been entered.  Unlike
  // get_mode(), this never waits for the query node to be set.  Used by defensive
  // invariant checks: a KV mutation must never be staged once query mode is entered.
  auto query_mode_entered() -> bool
  {
    const std::scoped_lock lock(mutex_);
    return mode_.mode == attempt_mode::modes::QUERY;
  }
  auto get_mode() -> attempt_mode
  {
    // NOLINTNEXTLINE(misc-const-correctness) -- lock is mutated via wait()
    std::unique_lock<std::mutex> lock(mutex_);
    if (mode_.mode == attempt_mode::modes::KV) {
      return {};
    }
    if (!mode_.query_node.empty()) {
      return mode_;
    }
    // Another op has set the query mode, and hasn't set the query node yet.  So we wait
    // until the node is set, or until begin_work fails and resets the mode back to KV
    // (reset_query_mode); without the latter the wait would never be satisfied and we
    // would deadlock, since query_node stays empty on the failure path.
    cv_query_.wait(lock, [this]() {
      return !mode_.query_node.empty() || mode_.mode == attempt_mode::modes::KV;
    });
    if (mode_.mode == attempt_mode::modes::KV) {
      return {};
    }
    return mode_;
  }

  template<typename BeginWorkHandler, typename DoQueryHandler>
  void set_query_mode(BeginWorkHandler&& begin_work_cb, DoQueryHandler&& cb)
  {
    // NOLINTNEXTLINE(misc-const-correctness) -- lock is mutated via wait() and unlock()
    std::unique_lock<std::mutex> lock(mutex_);
    // called within an op, so decrement in_flight from that op, wait for
    // other in_flight to complete.
    in_flight_--;
    while (true) {
      if (mode_.mode == attempt_mode::modes::KV) {
        // wait until all in_flight ops are done
        CB_TXN_LOG_TRACE("set_query_mode: waiting for in_flight ops to go to 0...");
        cv_in_flight_.wait(lock, [this]() {
          return (0 == in_flight_);
        });
        // ok, now no outstanding ops(apart from the query that called this), and I have the lock,
        // so...
        if (mode_.mode == attempt_mode::modes::KV) {
          CB_TXN_LOG_TRACE("set_query_mode: in_flight ops = 0, we were kv, setting mode to query");
          // still kv, so now (while blocking) set the mode
          mode_.mode = attempt_mode::modes::QUERY;
          // ok to unlock now, as any racing set_query_mode will wait for
          // a node to be set.
          in_flight_++;
          lock.unlock();
          // now (outside the lock), call the callback, which does the begin_work
          // when initially setting query mode.
          std::forward<BeginWorkHandler>(begin_work_cb)();
          return;
        }
      }
      // you make it here, and someone else is currently setting the node (a byproduct of
      // calling the callback).  So wait for that, or for begin_work to fail and reset the
      // mode back to KV (reset_query_mode) -- otherwise the node never arrives on the failure
      // path and we would wait forever.
      CB_TXN_LOG_TRACE("set_query_mode: mode already query, waiting for node to be set...");
      cv_query_.wait(lock, [this]() {
        return !mode_.query_node.empty() || mode_.mode == attempt_mode::modes::KV;
      });
      if (mode_.mode == attempt_mode::modes::KV) {
        // begin_work failed and reset the mode; re-evaluate from the top so this op can
        // become the initiator and retry begin_work (bounded by transaction expiry).
        CB_TXN_LOG_TRACE("set_query_mode: mode reset to kv, retrying begin_work");
        continue;
      }
      cv_in_flight_.wait(lock, [this]() {
        return 0 == in_flight_;
      });
      in_flight_++;
      CB_TXN_LOG_TRACE("set_query_mode: node set, continuing...");
      lock.unlock();
      std::forward<DoQueryHandler>(cb)();
      return;
    }
  }
  void reset_query_mode()
  {
    // when begin work errors out, it is fatal, so reset to kv mode here, allowing
    // rollback to function properly.  Hold the mutex while mutating mode_ and
    // notifying: this runs in the begin_work completion callback (the op_list lock
    // is not held there, same as the sibling set_query_node path), and get_mode()/
    // set_query_mode() read mode_ under the same mutex.
    const std::scoped_lock lock(mutex_);
    mode_.mode = attempt_mode::modes::KV;
    cv_query_.notify_all();
  }

  void set_query_node(const std::string& node)
  {
    const std::scoped_lock lock(mutex_);
    assert(mode_.mode == attempt_mode::modes::QUERY);
    mode_.query_node = node;
    // now notify everyone waiting in get_mode()
    cv_query_.notify_all();
  }
  void decrement_in_flight()
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    in_flight_--;
    CB_TXN_LOG_TRACE("in_flight decremented to {}", in_flight_);
    assert(in_flight_ >= 0);
    if (0 == in_flight_) {
      cv_in_flight_.notify_all();
    }
  }

private:
  void change_count(int32_t val)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (allow_ops_) {
      count_ += val;
      if (val > 0) {
        in_flight_ += val;
      }
      CB_TXN_LOG_TRACE("op count changed by {} to {}, {} in_flight", val, count_, in_flight_);
      assert(count_ >= 0);
      assert(in_flight_ >= 0);
      if (0 == count_) {
        cv_ops_.notify_all();
      }
      if (0 == in_flight_) {
        cv_in_flight_.notify_all();
      }
    } else {
      CB_TXN_LOG_ERROR("operation attempted after commit/rollback");
      throw async_operation_conflict("Operation attempted after commit or rollback");
    }
  }

  int32_t count_{ 0 };
  bool allow_ops_{ true };
  attempt_mode mode_{};
  int32_t in_flight_{ 0 };
  std::condition_variable cv_ops_;
  std::condition_variable cv_query_;
  std::condition_variable cv_in_flight_;
  std::mutex mutex_;
};
} // namespace couchbase::core::transactions
