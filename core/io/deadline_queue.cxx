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

#include "deadline_queue.hxx"

#include <asio/post.hpp>

#include <exception>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core::io
{
namespace
{
// The targets are already unlinked, so the queue will not call them again: a throwing callback
// does not end the loop. The first exception is rethrown afterwards; later ones are dropped.
template<typename Callback>
void
answer_all(const std::vector<std::shared_ptr<deadline_target>>& targets, Callback callback)
{
  std::exception_ptr failure{};
  for (const auto& target : targets) {
    try {
      callback(*target);
    } catch (...) {
      if (!failure) {
        failure = std::current_exception();
      }
    }
  }
  if (failure) {
    std::rethrow_exception(failure);
  }
}
} // namespace

deadline_queue::deadline_queue(asio::io_context& ctx)
  : ctx_{ ctx }
  , timer_{ ctx }
{
}

deadline_queue::~deadline_queue()
{
  stop();
}

auto
deadline_queue::add(std::shared_ptr<deadline_target> target,
                    std::chrono::steady_clock::duration timeout,
                    std::chrono::steady_clock::time_point* expiry_out)
  -> std::chrono::steady_clock::time_point
{
  std::unique_lock lock(mutex_);
  // Read under the lock, so that within one list expiry order is insertion order.
  const auto expiry = std::chrono::steady_clock::now() + timeout;
  if (expiry_out != nullptr) {
    *expiry_out = expiry;
  }
  if (stopped_) {
    lock.unlock();
    asio::post(ctx_, [target = std::move(target)]() {
      target->on_stop();
    });
    return expiry;
  }
  auto& hook = target->deadline_hook_;
  hook.expiry = expiry;
  hook.timeout = timeout;
  hook.next = nullptr;
  auto& list = lists_[timeout];
  hook.prev = list.tail;
  if (list.tail != nullptr) {
    list.tail->deadline_hook_.next = target.get();
  } else {
    list.head = target.get();
    heads_.emplace(expiry, timeout);
  }
  list.tail = target.get();
  hook.self = std::move(target);
  if (expiry < armed_) {
    arm(expiry);
  }
  return expiry;
}

void
deadline_queue::remove(deadline_target& target) noexcept
{
  std::shared_ptr<deadline_target> released{};
  {
    const std::scoped_lock lock(mutex_);
    if (!target.deadline_hook_.self) {
      return;
    }
    unlink(target, released);
    if (lists_.empty()) {
      // Nothing left to wait for. Without this, io_context::run() would not return before the
      // timer's expiry.
      timer_.cancel();
      armed_ = std::chrono::steady_clock::time_point::max();
    }
  }
  // `released` may hold the last reference; it is dropped here, outside the lock.
}

void
deadline_queue::stop()
{
  std::vector<std::shared_ptr<deadline_target>> pending{};
  {
    const std::scoped_lock lock(mutex_);
    stopped_ = true;
    timer_.cancel();
    armed_ = std::chrono::steady_clock::time_point::max();
    while (!lists_.empty()) {
      auto& head = *lists_.begin()->second.head;
      pending.emplace_back();
      unlink(head, pending.back());
    }
  }
  answer_all(pending, [](deadline_target& target) {
    target.on_stop();
  });
}

auto
deadline_queue::timeout_lists() const -> std::size_t
{
  const std::scoped_lock lock(mutex_);
  return lists_.size();
}

// Requires mutex_. Moves the queue's reference to the target into `released`, for the caller to
// drop after unlocking.
void
deadline_queue::unlink(deadline_target& target, std::shared_ptr<deadline_target>& released)
{
  auto& hook = target.deadline_hook_;
  auto list = lists_.find(hook.timeout);
  if (hook.prev != nullptr) {
    hook.prev->deadline_hook_.next = hook.next;
  } else {
    auto node = heads_.extract({ hook.expiry, hook.timeout });
    list->second.head = hook.next;
    if (hook.next != nullptr) {
      // Reuses the node: a completion usually removes its list's head, so this runs per request.
      node.value().first = hook.next->deadline_hook_.expiry;
      heads_.insert(std::move(node));
    }
  }
  if (hook.next != nullptr) {
    hook.next->deadline_hook_.prev = hook.prev;
  } else {
    list->second.tail = hook.prev;
  }
  if (list->second.head == nullptr) {
    lists_.erase(list);
  }
  hook.prev = nullptr;
  hook.next = nullptr;
  released = std::move(hook.self);
}

// Requires mutex_.
void
deadline_queue::arm(std::chrono::steady_clock::time_point expiry)
{
  armed_ = expiry;
  // Cancels the pending wait, if any, so at most one wait is outstanding.
  timer_.expires_at(expiry);
  timer_.async_wait([weak_self = weak_from_this()](std::error_code ec) {
    if (ec == asio::error::operation_aborted) {
      return;
    }
    if (auto self = weak_self.lock()) {
      self->on_timer();
    }
  });
}

// A wait that completed before a later arm() or cancel still runs. It expires only entries past
// their expiry, and re-arms only for a remaining head, which cancels the newer wait. The extra wake
// does redundant work only.
void
deadline_queue::on_timer()
{
  std::vector<std::shared_ptr<deadline_target>> expired{};
  {
    const std::scoped_lock lock(mutex_);
    armed_ = std::chrono::steady_clock::time_point::max();
    const auto now = std::chrono::steady_clock::now();
    while (!heads_.empty() && heads_.begin()->first <= now) {
      auto& head = *lists_.find(heads_.begin()->second)->second.head;
      expired.emplace_back();
      unlink(head, expired.back());
    }
    if (!heads_.empty()) {
      arm(heads_.begin()->first);
    }
  }
  answer_all(expired, [](deadline_target& target) {
    target.on_deadline();
  });
}
} // namespace couchbase::core::io
