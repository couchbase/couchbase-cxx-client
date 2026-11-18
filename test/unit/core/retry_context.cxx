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

#include "framework/test_registry.hxx"

#include "core/io/retry_context.hxx"

#include <couchbase/retry_reason.hxx>

#include <atomic>
#include <cstddef>
#include <set>
#include <thread>
#include <vector>

namespace couchbase::test
{
namespace
{
using core::io::retry_context;

void
a_fresh_context_reports_no_attempts_and_no_reasons([[maybe_unused]] context& ctx)
{
  const retry_context<false> retries{};
  assert_eq(retries.retry_attempts(), std::size_t{ 0 }, "attempts");
  assert_true(retries.retry_reasons().empty(), "reasons");
}

void
recorded_attempts_are_counted_and_their_reasons_kept([[maybe_unused]] context& ctx)
{
  retry_context<false> retries{};
  retries.record_retry_attempt(retry_reason::key_value_temporary_failure);
  retries.record_retry_attempt(retry_reason::key_value_temporary_failure);
  retries.record_retry_attempt(retry_reason::key_value_locked);

  assert_eq(retries.retry_attempts(), std::size_t{ 3 }, "attempts");
  const auto reasons = retries.retry_reasons();
  assert_eq(reasons.size(), std::size_t{ 2 }, "distinct reasons");
  assert_eq(reasons.count(retry_reason::key_value_temporary_failure), std::size_t{ 1 }, "tmpfail");
  assert_eq(reasons.count(retry_reason::key_value_locked), std::size_t{ 1 }, "locked");
}

// add_reason records a reason without an attempt, as a collection-outdated resend does.
void
a_reason_added_without_an_attempt_is_reported([[maybe_unused]] context& ctx)
{
  retry_context<false> retries{};
  retries.add_reason(retry_reason::key_value_collection_outdated);

  assert_eq(retries.retry_attempts(), std::size_t{ 0 }, "attempts");
  assert_eq(retries.retry_reasons(),
            std::set<retry_reason>{ retry_reason::key_value_collection_outdated },
            "reasons");
}

void
attempts_recorded_from_many_threads_are_all_counted([[maybe_unused]] context& ctx)
{
  constexpr std::size_t thread_count = 8;
  constexpr std::size_t attempts_per_thread = 1'000;
  retry_context<true> retries{};

  std::atomic<bool> go{ false };
  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  for (std::size_t t = 0; t < thread_count; ++t) {
    threads.emplace_back([&retries, &go, t] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::size_t i = 0; i < attempts_per_thread; ++i) {
        retries.record_retry_attempt(static_cast<retry_reason>(t + 1));
      }
    });
  }
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }

  assert_eq(retries.retry_attempts(), thread_count * attempts_per_thread, "attempts");
  assert_eq(retries.retry_reasons().size(), thread_count, "one reason per thread");
}

void
a_reason_is_visible_once_its_attempt_is([[maybe_unused]] context& ctx)
{
  for (int iteration = 0; iteration < 1'000; ++iteration) {
    retry_context<false> retries{};
    std::atomic<bool> go{ false };

    std::thread writer([&retries, &go] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      retries.record_retry_attempt(retry_reason::key_value_locked);
    });

    go.store(true, std::memory_order_release);
    while (retries.retry_attempts() == 0) {
      std::this_thread::yield();
    }
    const auto reasons = retries.retry_reasons();
    writer.join();

    assert_eq(reasons.count(retry_reason::key_value_locked), std::size_t{ 1 }, "the reason");
  }
}

void
a_copy_keeps_the_counts_it_was_taken_with([[maybe_unused]] context& ctx)
{
  retry_context<false> original{ nullptr, "id" };
  original.record_retry_attempt(retry_reason::key_value_locked);

  const retry_context<false> copy{ original };
  original.record_retry_attempt(retry_reason::key_value_temporary_failure);

  assert_eq(copy.identifier(), std::string{ "id" }, "identifier");
  assert_eq(copy.retry_attempts(), std::size_t{ 1 }, "attempts at the time of the copy");
  assert_eq(copy.retry_reasons(),
            std::set<retry_reason>{ retry_reason::key_value_locked },
            "reasons at the time of the copy");
  assert_eq(original.retry_attempts(), std::size_t{ 2 }, "the original keeps counting");
}

void
every_retry_reason_round_trips_through_the_mask([[maybe_unused]] context& ctx)
{
  const auto last = static_cast<int>(retry_reason::views_no_active_partition);
  for (int value = static_cast<int>(retry_reason::do_not_retry); value <= last; ++value) {
    const auto reason = static_cast<retry_reason>(value);
    retry_context<false> retries{};
    retries.record_retry_attempt(reason);
    assert_eq(retries.retry_reasons(), std::set<retry_reason>{ reason }, "the one reason");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_fresh_context_reports_no_attempts_and_no_reasons), {}, timeout::instant },
      { CASE(recorded_attempts_are_counted_and_their_reasons_kept), {}, timeout::instant },
      { CASE(a_reason_added_without_an_attempt_is_reported), {}, timeout::instant },
      { CASE(attempts_recorded_from_many_threads_are_all_counted), {}, timeout::fast },
      { CASE(a_reason_is_visible_once_its_attempt_is), {}, timeout::slow },
      { CASE(a_copy_keeps_the_counts_it_was_taken_with), {}, timeout::instant },
      { CASE(every_retry_reason_round_trips_through_the_mask), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
