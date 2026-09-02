/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "framework/test_registry.hxx"

// Ahead of the header under test: waitable_op_list.hxx calls assert() without including
// <cassert> itself, so it only compiles where something else has already declared it.
#include <cassert>

#include "core/transactions/waitable_op_list.hxx"

#include <atomic>
#include <chrono>
#include <future>
#include <list>
#include <string>
#include <thread>

namespace couchbase::test
{
namespace
{
namespace transactions = couchbase::core::transactions;

const std::string node_address{ "someipaddress" };

// Spins until a worker thread has reached the call under test. It confirms the flag, not the wait
// itself -- waitable_op_list exposes no hook inside either blocking call -- but without it the
// interval below would pass on a thread that had not yet run.
void
wait_until_started(const std::atomic<bool>& started)
{
  while (!started.load()) {
    std::this_thread::yield();
  }
}

void
defaults_to_kv_mode([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  auto mode = op_list.get_mode();
  assert_true(mode.query_node.empty(), "no query node is chosen before query mode is entered");
  assert_eq(mode.mode, transactions::attempt_mode::modes::KV, "the initial mode");
}

void
set_query_mode_enters_query_mode_and_runs_begin_work([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  std::atomic<bool> begin_work_called{ false };
  std::atomic<bool> do_work_called{ false };
  op_list.increment_ops();
  op_list.set_query_mode(
    [&op_list, &begin_work_called]() {
      op_list.set_query_node(node_address);
      begin_work_called = true;
    },
    [&do_work_called]() {
      do_work_called = true;
    });

  auto mode = op_list.get_mode();
  assert_eq(mode.query_node, node_address, "the node the begin-work callback selected");
  assert_eq(mode.mode, transactions::attempt_mode::modes::QUERY, "the mode after the transition");
  assert_true(begin_work_called.load(), "the first caller runs the begin-work callback");
  assert_false(do_work_called.load(), "the first caller does not run the do-work callback");
}

void
set_query_mode_waits_on_in_flight_ops([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  op_list.increment_ops();
  op_list.increment_ops();
  std::atomic<bool> do_work_called{ false };
  std::atomic<bool> transition_started{ false };
  auto f = std::async(std::launch::async, [&op_list, &do_work_called, &transition_started] {
    transition_started = true;
    op_list.set_query_mode(
      [&op_list]() {
        op_list.set_query_node(node_address);
      },
      [&do_work_called]() {
        do_work_called = true;
      });
  });
  wait_until_started(transition_started);
  // Not completing is an absence, and an interval is the only thing that shows one.
  assert_eq(f.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout,
            "the transition blocks while a second op is in flight");
  op_list.decrement_in_flight();
  assert_eq(f.wait_for(std::chrono::seconds(2)),
            std::future_status::ready,
            "draining the second op releases the transition");
  f.get();
  auto mode = op_list.get_mode();
  assert_eq(mode.mode, transactions::attempt_mode::modes::QUERY, "the mode after the transition");
  assert_false(do_work_called.load(), "the first caller does not run the do-work callback");
}

void
only_the_first_concurrent_caller_runs_begin_work([[maybe_unused]] context& ctx)
{
  constexpr int num_futures{ 10 };
  transactions::waitable_op_list op_list;
  std::atomic<int> do_work_calls{ 0 };
  std::atomic<int> begin_work_calls{ 0 };
  auto call_set_query_mode = [&op_list, &begin_work_calls, &do_work_calls]() {
    op_list.increment_ops();
    op_list.set_query_mode(
      [&op_list, &begin_work_calls] {
        begin_work_calls++;
        op_list.set_query_node(node_address);
        op_list.decrement_in_flight();
        op_list.decrement_ops();
      },
      [&op_list, &do_work_calls]() {
        do_work_calls++;
        op_list.decrement_in_flight();
        op_list.decrement_ops();
      });
  };

  std::list<std::future<void>> futures;
  for (int i = 0; i < num_futures; i++) {
    futures.emplace_back(std::async(std::launch::async, call_set_query_mode));
  }
  for (auto& f : futures) {
    f.get();
  }
  assert_eq(
    do_work_calls.load(), num_futures - 1, "every caller but one runs the do-work callback");
  assert_eq(begin_work_calls.load(), 1, "exactly one caller runs the begin-work callback");
}

void
get_mode_waits_for_the_query_node([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  std::atomic<bool> begin_work_called{ false };
  std::atomic<bool> do_work_called{ false };
  op_list.increment_ops();
  op_list.set_query_mode(
    [&begin_work_called]() {
      begin_work_called = true;
    },
    [&do_work_called]() {
      do_work_called = true;
    });
  std::atomic<bool> read_started{ false };
  auto f = std::async(std::launch::async, [&op_list, &read_started] {
    read_started = true;
    auto mode = op_list.get_mode();
    return mode.query_node == node_address && mode.mode == transactions::attempt_mode::modes::QUERY;
  });
  wait_until_started(read_started);
  // Not completing is an absence, and an interval is the only thing that shows one.
  assert_eq(f.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout,
            "get_mode blocks until the query node is published");
  op_list.set_query_node(node_address);
  assert_true(f.get(), "the blocked reader observes query mode and the published node");
  auto mode = op_list.get_mode();
  assert_eq(mode.query_node, node_address, "the published node");
  assert_eq(mode.mode, transactions::attempt_mode::modes::QUERY, "the mode after the transition");
}

void
reset_query_mode_returns_to_kv_mode([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  op_list.increment_ops();
  op_list.set_query_mode(
    [&op_list]() {
      op_list.set_query_node(node_address);
    },
    []() {
    });
  assert_true(op_list.query_mode_entered(), "query mode is entered");

  op_list.reset_query_mode();
  assert_false(op_list.query_mode_entered(), "resetting returns the list to KV mode");
}

void
reset_query_mode_is_synchronized_against_readers([[maybe_unused]] context& ctx)
{
  // Regression guard for the unsynchronized write in reset_query_mode(): it must hold mutex_ while
  // mutating mode_ and notifying, otherwise it races the locked readers get_mode()/
  // query_mode_entered(). The data race is surfaced by ThreadSanitizer; this test drives both sides
  // concurrently so a TSan build can observe it, and asserts the functional outcome (mode reset to
  // KV) on any build.
  for (int i = 0; i < 100; ++i) {
    transactions::waitable_op_list op_list;
    op_list.increment_ops();
    op_list.set_query_mode(
      [&op_list]() {
        op_list.set_query_node(node_address);
      },
      []() {
      });

    std::thread reader([&op_list]() {
      for (int j = 0; j < 100; ++j) {
        (void)op_list.query_mode_entered();
      }
    });
    op_list.reset_query_mode();
    reader.join();

    assert_false(op_list.query_mode_entered(), "resetting returns the list to KV mode");
  }
}

void
query_mode_entered_reflects_the_current_mode([[maybe_unused]] context& ctx)
{
  transactions::waitable_op_list op_list;
  assert_false(op_list.query_mode_entered(), "query mode is not entered before the transition");

  op_list.increment_ops();
  op_list.set_query_mode(
    [&op_list]() {
      op_list.set_query_node(node_address);
    },
    []() {
    });
  assert_true(op_list.query_mode_entered(), "query mode is entered after the transition");
}

void
query_mode_entered_stays_false_until_in_flight_ops_drain([[maybe_unused]] context& ctx)
{
  // The register-then-check invariant behind the KV-op routing fix: a counted (in-flight) KV op can
  // safely observe KV mode while a concurrent set_query_mode() is blocked waiting for in-flight ops
  // to drain. Query mode only becomes observable once that drain completes -- so a KV op that sees
  // KV mode is guaranteed to be part of BEGIN WORK rather than stranded after it.
  transactions::waitable_op_list op_list;
  op_list.increment_ops(); // the op that calls set_query_mode
  op_list.increment_ops(); // a second in-flight op that must drain before query mode is entered

  auto f = std::async(std::launch::async, [&op_list] {
    op_list.set_query_mode(
      [&op_list]() {
        op_list.set_query_node(node_address);
      },
      []() {
      });
  });

  assert_eq(f.wait_for(std::chrono::milliseconds(100)),
            std::future_status::timeout,
            "the transition blocks while a second op is in flight");
  assert_false(op_list.query_mode_entered(), "the mode is still KV while the drain is pending");

  op_list.decrement_in_flight();
  f.get();
  assert_true(op_list.query_mode_entered(), "the completed drain makes query mode observable");
}

void
operations_after_commit_or_rollback_are_rejected_with_a_conflict([[maybe_unused]] context& ctx)
{
  // Once commit/rollback has blocked operations (wait_and_block_ops), a racing operation must be
  // rejected: increment_ops() throws async_operation_conflict. Mapping that conflict to the
  // CONCURRENT_OPERATIONS_DETECTED_ON_SAME_DOCUMENT cause happens in attempt_context_impl and is
  // covered by the FIT ThreadSafety suite, not here.
  transactions::waitable_op_list op_list;
  op_list.wait_and_block_ops(); // no ops in flight, so this returns immediately and blocks new ops

  assert_throws<transactions::async_operation_conflict>(
    [&op_list]() {
      op_list.increment_ops();
    },
    "an operation started after commit/rollback is rejected");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(defaults_to_kv_mode) },
      { CASE(set_query_mode_enters_query_mode_and_runs_begin_work) },
      { CASE(set_query_mode_waits_on_in_flight_ops) },
      { CASE(only_the_first_concurrent_caller_runs_begin_work) },
      { CASE(get_mode_waits_for_the_query_node) },
      { CASE(reset_query_mode_returns_to_kv_mode) },
      { CASE(reset_query_mode_is_synchronized_against_readers) },
      { CASE(query_mode_entered_reflects_the_current_mode) },
      { CASE(query_mode_entered_stays_false_until_in_flight_ops_drain) },
      { CASE(operations_after_commit_or_rollback_are_rejected_with_a_conflict) },
    },
  };
}

} // namespace couchbase::test
