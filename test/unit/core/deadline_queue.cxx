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

#include "framework/context.hxx"
#include "framework/test_registry.hxx"

#include "core/io/deadline_queue.hxx"

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;
using core::io::deadline_queue;
using core::io::deadline_target;
using clock = std::chrono::steady_clock;

struct recorder {
  std::mutex mutex{};
  std::vector<int> order{};
};

struct target : deadline_target {
  explicit target(int id = 0, recorder* rec = nullptr)
    : id{ id }
    , rec{ rec }
  {
  }

  void on_deadline() override
  {
    fired_at = clock::now();
    calls.fetch_add(1);
    if (rec != nullptr) {
      const std::scoped_lock lock(rec->mutex);
      rec->order.push_back(id);
    }
  }

  void on_stop() override
  {
    stops.fetch_add(1);
  }

  int id;
  recorder* rec;
  std::atomic<int> calls{ 0 };
  std::atomic<int> stops{ 0 };
  clock::time_point fired_at{};
};

void
an_entry_expires_after_its_timeout([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto t = std::make_shared<target>();

  const auto started = clock::now();
  const auto expiry = queue->add(t, 50ms);
  io.run();

  assert_eq(t->calls.load(), 1, "on_deadline calls");
  assert_true(t->fired_at >= expiry, "not before the expiry");
  assert_true(expiry >= started + 50ms, "the expiry is the timeout after add");
  assert_eq(t.use_count(), 1L, "the queue released its reference");
}

void
a_removed_entry_never_expires_and_run_returns([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto t = std::make_shared<target>();

  queue->add(t, 10s);
  queue->remove(*t);
  const auto started = clock::now();
  io.run();

  assert_true(clock::now() - started < scaled_budget(100ms), "run() returns promptly");
  assert_eq(t->calls.load(), 0, "on_deadline calls");
  assert_eq(t.use_count(), 1L, "the queue released its reference");
}

void
entries_with_different_timeouts_expire_in_deadline_order([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  recorder rec;
  auto slow = std::make_shared<target>(300, &rec);
  auto fast = std::make_shared<target>(100, &rec);

  queue->add(slow, 300ms);
  queue->add(fast, 100ms);
  io.run();

  assert_eq(rec.order.size(), std::size_t{ 2 }, "both expired");
  assert_eq(rec.order[0], 100, "the shorter timeout first");
  assert_eq(rec.order[1], 300, "then the longer");
}

void
distinct_timeouts_do_not_accumulate([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 1'000; ++i) {
    targets.push_back(std::make_shared<target>(i));
    queue->add(targets.back(), 10s + std::chrono::milliseconds{ i });
  }
  assert_eq(queue->timeout_lists(), std::size_t{ 1'000 }, "one list per distinct timeout");
  for (auto& t : targets) {
    queue->remove(*t);
  }
  assert_eq(queue->timeout_lists(), std::size_t{ 0 }, "every list erased");
}

void
stop_answers_every_pending_entry([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 3; ++i) {
    targets.push_back(std::make_shared<target>(i));
    queue->add(targets.back(), 10s + std::chrono::seconds{ i });
  }

  queue->stop();

  for (const auto& t : targets) {
    assert_eq(t->stops.load(), 1, "on_stop calls");
    assert_eq(t->calls.load(), 0, "on_deadline calls");
    assert_eq(t.use_count(), 1L, "the queue released its reference");
  }
  assert_eq(queue->timeout_lists(), std::size_t{ 0 }, "nothing pending");
}

void
an_entry_added_after_stop_is_answered([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  queue->stop();
  auto t = std::make_shared<target>();

  queue->add(t, 10s);
  io.run();

  assert_eq(t->stops.load(), 1, "on_stop calls");
  assert_eq(t->calls.load(), 0, "on_deadline calls");
  assert_eq(t.use_count(), 1L, "nothing holds it");
}

// Both entries expire in one wake. The first one's on_deadline() removes the second, which the
// wake has already unlinked: remove() finds it unlinked, and the second still expires once.
struct remover : target {
  std::shared_ptr<deadline_queue> queue{};
  deadline_target* victim{ nullptr };

  void on_deadline() override
  {
    target::on_deadline();
    queue->remove(*victim);
  }
};

void
remove_after_the_expiry_unlinked_an_entry_has_no_effect([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto first = std::make_shared<remover>();
  auto second = std::make_shared<target>();
  first->queue = queue;
  first->victim = second.get();

  queue->add(first, 1ms);
  queue->add(second, 1ms);
  std::this_thread::sleep_for(5ms);
  io.run();

  assert_eq(first->calls.load(), 1, "the first expired once");
  assert_eq(second->calls.load(), 1, "the second expired once");
  assert_eq(second.use_count(), 1L, "the queue released its reference");
}

// A later, earlier-expiring entry re-arms the timer. The superseded wake must not leave a second
// wait outstanding: every entry still expires once.
void
re_arming_does_not_duplicate_expiries([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 20; ++i) {
    targets.push_back(std::make_shared<target>(i));
    queue->add(targets.back(), std::chrono::milliseconds{ 100 - (i * 5) });
  }
  io.run();

  for (const auto& t : targets) {
    assert_eq(t->calls.load(), 1, "on_deadline calls");
  }
}
// add() on caller threads while the I/O thread expires and other entries are removed. Under TSan
// this checks the queue's locking; in every build, each entry is answered at most once and every
// reference is released.
void
concurrent_add_remove_and_expiry_release_every_entry([[maybe_unused]] context& ctx)
{
  constexpr int thread_count = 4;
  constexpr int entries_per_thread = 500;
  asio::io_context io;
  auto guard = asio::make_work_guard(io);
  auto queue = std::make_shared<deadline_queue>(io);
  std::thread io_thread([&io] {
    io.run();
  });

  std::vector<std::vector<std::shared_ptr<target>>> per_thread(thread_count);
  std::vector<std::thread> threads;
  for (int t = 0; t < thread_count; ++t) {
    threads.emplace_back([&queue, &entries = per_thread[static_cast<std::size_t>(t)]] {
      for (int i = 0; i < entries_per_thread; ++i) {
        entries.push_back(std::make_shared<target>(i));
        queue->add(entries.back(), std::chrono::microseconds{ 100 * (i % 7) });
        if (i % 2 == 0) {
          queue->remove(*entries.back());
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  queue->stop();
  guard.reset();
  io_thread.join();

  for (const auto& entries : per_thread) {
    for (const auto& t : entries) {
      assert_true(t->calls.load() + t->stops.load() <= 1, "one answer at most");
      assert_eq(t.use_count(), 1L, "the queue released its reference");
    }
  }
}

void
entries_with_the_same_timeout_expire_in_insertion_order([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  recorder rec;
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 5; ++i) {
    targets.push_back(std::make_shared<target>(i, &rec));
    queue->add(targets.back(), 20ms);
  }
  io.run();

  assert_true(rec.order == std::vector<int>{ 0, 1, 2, 3, 4 }, "the order they were added in");
}

// Removing the head, a middle entry and the tail must leave the list linked: the others still
// expire, in order.
void
removing_entries_from_one_list_keeps_the_rest_in_order([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  recorder rec;
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 5; ++i) {
    targets.push_back(std::make_shared<target>(i, &rec));
    queue->add(targets.back(), 20ms);
  }
  queue->remove(*targets[0]);
  queue->remove(*targets[2]);
  queue->remove(*targets[4]);
  io.run();

  assert_true(rec.order == std::vector<int>{ 1, 3 }, "the remaining entries, in order");
  for (const auto& t : targets) {
    assert_eq(t.use_count(), 1L, "the queue released its reference");
  }
}

// The timer is armed for the earliest entry. Once that entry is removed, the wake finds nothing
// expired and must re-arm for the next one, or the next deadline never fires.
void
a_wake_with_nothing_expired_rearms_for_the_next_entry([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto early = std::make_shared<target>();
  auto late = std::make_shared<target>();

  queue->add(early, 20ms);
  const auto late_expiry = queue->add(late, 80ms);
  queue->remove(*early);
  io.run();

  assert_eq(early->calls.load(), 0, "the removed entry does not expire");
  assert_eq(late->calls.load(), 1, "the next entry expires");
  assert_true(late->fired_at >= late_expiry, "not before its expiry");
}

void
add_stores_the_expiry_it_returns([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto t = std::make_shared<target>();
  clock::time_point stored{};

  const auto expiry = queue->add(t, 10s, &stored);
  queue->remove(*t);

  assert_true(stored == expiry, "the expiry is stored");
}

// Callbacks run outside the queue's lock, so one may add to the same queue.
struct re_adder : target {
  std::shared_ptr<deadline_queue> queue{};
  std::shared_ptr<target> next{};

  void on_deadline() override
  {
    target::on_deadline();
    queue->add(next, 10ms);
  }
};

void
a_callback_may_add_to_the_queue([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto first = std::make_shared<re_adder>();
  auto second = std::make_shared<target>();
  first->queue = queue;
  first->next = second;

  queue->add(first, 10ms);
  io.run();
  first->queue.reset();

  assert_eq(first->calls.load(), 1, "the first expired once");
  assert_eq(second->calls.load(), 1, "the entry it added expired once");
  assert_eq(second.use_count(), 2L, "only the test and the first entry hold it");
}

void
destroying_the_queue_answers_and_releases_linked_entries([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  std::vector<std::shared_ptr<target>> targets;
  for (int i = 0; i < 3; ++i) {
    targets.push_back(std::make_shared<target>(i));
    queue->add(targets.back(), 10s);
  }

  queue.reset();

  for (const auto& t : targets) {
    assert_eq(t->stops.load(), 1, "on_stop calls");
    assert_eq(t.use_count(), 1L, "the entry was released");
  }
}
// The timer is armed for the long entry when the short one arrives; add() must re-arm it earlier.
void
an_earlier_expiry_re_arms_the_timer([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto longer = std::make_shared<target>(1);
  auto shorter = std::make_shared<target>(2);

  queue->add(longer, 10s);
  queue->add(shorter, 50ms);
  const auto limit = clock::now() + scaled_budget(1s);
  while (shorter->calls.load() == 0 && clock::now() < limit) {
    io.run_one_for(10ms);
  }

  assert_eq(shorter->calls.load(), 1, "the short entry expired at its own deadline");
  assert_eq(longer->calls.load(), 0, "the long entry is still pending");
}

struct thrower : target {
  void on_deadline() override
  {
    target::on_deadline();
    throw std::runtime_error("on_deadline");
  }

  void on_stop() override
  {
    target::on_stop();
    throw std::runtime_error("on_stop");
  }
};

void
a_throwing_deadline_callback_does_not_skip_the_rest_of_its_wake([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto first = std::make_shared<thrower>();
  auto second = std::make_shared<target>(2);
  // Zero timeouts: both are overdue before run(), so they expire in one wake.
  queue->add(first, 0ms);
  queue->add(second, 0ms);

  bool thrown = false;
  try {
    io.run();
  } catch (const std::runtime_error&) {
    thrown = true;
  }

  assert_true(thrown, "the exception reaches run()");
  assert_eq(first->calls.load(), 1, "the throwing target expired once");
  assert_eq(second->calls.load(), 1, "the next target in the same wake still expired once");
}

void
a_throwing_stop_callback_does_not_skip_the_rest([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  auto first = std::make_shared<thrower>();
  auto second = std::make_shared<target>(2);
  queue->add(first, 10s);
  queue->add(second, 10s);

  bool thrown = false;
  try {
    queue->stop();
  } catch (const std::runtime_error&) {
    thrown = true;
  }

  assert_true(thrown, "the exception reaches the caller of stop()");
  assert_eq(first->stops.load(), 1, "the throwing target was stopped once");
  assert_eq(second->stops.load(), 1, "the next target was still stopped once");
  assert_eq(second.use_count(), 1L, "the queue released it");
}

// The list with the shorter timeout is first in timeout order, but its head expires later.
void
expiry_order_holds_across_lists_added_at_different_times([[maybe_unused]] context& ctx)
{
  asio::io_context io;
  auto queue = std::make_shared<deadline_queue>(io);
  recorder rec;
  auto longer = std::make_shared<target>(1, &rec);
  auto shorter = std::make_shared<target>(2, &rec);

  queue->add(longer, 100ms);
  std::this_thread::sleep_for(80ms);
  queue->add(shorter, 50ms);
  io.run();

  assert_eq(rec.order.size(), std::size_t{ 2 }, "both expired");
  assert_eq(rec.order[0], 1, "the earlier expiry first, although its timeout is longer");
  assert_eq(rec.order[1], 2, "then the later one");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_entry_expires_after_its_timeout), {}, timeout::fast },
      { CASE(a_removed_entry_never_expires_and_run_returns), {}, timeout::fast },
      { CASE(entries_with_different_timeouts_expire_in_deadline_order), {}, timeout::fast },
      { CASE(distinct_timeouts_do_not_accumulate), {}, timeout::fast },
      { CASE(stop_answers_every_pending_entry), {}, timeout::instant },
      { CASE(an_entry_added_after_stop_is_answered), {}, timeout::fast },
      { CASE(remove_after_the_expiry_unlinked_an_entry_has_no_effect), {}, timeout::fast },
      { CASE(re_arming_does_not_duplicate_expiries), {}, timeout::fast },
      { CASE(concurrent_add_remove_and_expiry_release_every_entry), {}, timeout::slow },
      { CASE(entries_with_the_same_timeout_expire_in_insertion_order), {}, timeout::fast },
      { CASE(removing_entries_from_one_list_keeps_the_rest_in_order), {}, timeout::fast },
      { CASE(a_wake_with_nothing_expired_rearms_for_the_next_entry), {}, timeout::fast },
      { CASE(add_stores_the_expiry_it_returns), {}, timeout::fast },
      { CASE(a_callback_may_add_to_the_queue), {}, timeout::fast },
      { CASE(destroying_the_queue_answers_and_releases_linked_entries), {}, timeout::fast },
      { CASE(an_earlier_expiry_re_arms_the_timer), {}, timeout::fast },
      { CASE(a_throwing_deadline_callback_does_not_skip_the_rest_of_its_wake), {}, timeout::fast },
      { CASE(a_throwing_stop_callback_does_not_skip_the_rest), {}, timeout::fast },
      { CASE(expiry_order_holds_across_lists_added_at_different_times), {}, timeout::fast },
    },
  };
}

} // namespace couchbase::test
