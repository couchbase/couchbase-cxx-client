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

#include "test/framework/test_framework.hxx"

#include "tools/overload_detector.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>

namespace couchbase::test
{
namespace
{
using namespace std::chrono_literals;

class fixed_delay_strategy : public couchbase::retry_strategy
{
public:
  auto retry_after(const couchbase::retry_request& /* request */,
                   couchbase::retry_reason /* reason */) -> couchbase::retry_action override
  {
    return couchbase::retry_action{ 7ms };
  }

  [[nodiscard]] auto to_string() const -> std::string override
  {
    return "fixed_delay_strategy";
  }
};

class stub_request : public couchbase::retry_request
{
public:
  [[nodiscard]] auto retry_attempts() const -> std::size_t override
  {
    return 0;
  }
  [[nodiscard]] auto identifier() const -> std::string override
  {
    return "stub";
  }
  [[nodiscard]] auto idempotent() const -> bool override
  {
    return true;
  }
  [[nodiscard]] auto retry_reasons() const -> std::set<couchbase::retry_reason> override
  {
    return {};
  }
  void record_retry_attempt(couchbase::retry_reason /* reason */) override
  {
  }
};

auto
make_detector() -> cbc::overload_detector
{
  return cbc::overload_detector{ std::make_shared<fixed_delay_strategy>(), 500ms };
}

void
rejects_a_null_delegate([[maybe_unused]] context& ctx)
{
  assert_throws<std::invalid_argument>(
    [] {
      const cbc::overload_detector rejected{ nullptr, 500ms };
    },
    "a detector without a delegate could not answer retry_after");
}

void
counts_only_temporary_failure_retries([[maybe_unused]] context& ctx)
{
  auto detector = make_detector();
  const stub_request request{};
  for (int i = 0; i < 3; ++i) {
    assert_true(detector.retry_after(request, couchbase::retry_reason::key_value_temporary_failure)
                    .duration() == 7ms,
                "the delegate's action is returned unchanged");
  }
  for (int i = 0; i < 2; ++i) {
    assert_true(
      detector.retry_after(request, couchbase::retry_reason::key_value_locked).duration() == 7ms,
      "the delegate's action is returned unchanged");
  }
  assert_eq(detector.take_window().temporary_failure_retries,
            std::uint64_t{ 3 },
            "a locked document is contention, not overload");
}

void
slow_is_strictly_above_threshold([[maybe_unused]] context& ctx)
{
  auto detector = make_detector();
  detector.record(500ms);
  detector.record(501ms);
  const auto window = detector.take_window();
  assert_eq(window.completions, std::uint64_t{ 2 }, "every record is a completion");
  assert_eq(window.slow, std::uint64_t{ 1 }, "a latency equal to the threshold is not slow");
}

void
take_window_resets([[maybe_unused]] context& ctx)
{
  auto detector = make_detector();
  detector.record(1s);
  assert_true(
    detector.retry_after(stub_request{}, couchbase::retry_reason::key_value_temporary_failure)
        .duration() == 7ms,
    "the delegate's action is returned unchanged");
  const auto first = detector.take_window();
  assert_eq(first.completions, std::uint64_t{ 1 }, "the first window holds the completion");
  assert_eq(first.slow, std::uint64_t{ 1 }, "the first window holds the slow completion");
  assert_eq(
    first.temporary_failure_retries, std::uint64_t{ 1 }, "the first window holds the retry");
  const auto second = detector.take_window();
  assert_eq(second.completions, std::uint64_t{ 0 }, "take_window resets completions");
  assert_eq(second.slow, std::uint64_t{ 0 }, "take_window resets slow completions");
  assert_eq(second.temporary_failure_retries, std::uint64_t{ 0 }, "take_window resets retries");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_overload_detector",
    {
      { "rejects_a_null_delegate", rejects_a_null_delegate, {}, timeout::instant },
      { "counts_only_temporary_failure_retries",
        counts_only_temporary_failure_retries,
        {},
        timeout::instant },
      { "slow_is_strictly_above_threshold",
        slow_is_strictly_above_threshold,
        {},
        timeout::instant },
      { "take_window_resets", take_window_resets, {}, timeout::instant },
    },
  };
}
} // namespace couchbase::test
