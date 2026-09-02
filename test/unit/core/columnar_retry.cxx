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

#include "framework/test_registry.hxx"

#include "core/columnar/backoff_calculator.hxx"

#include <chrono>
#include <cmath>
#include <cstddef>

namespace couchbase::test
{
namespace
{
void
backoff_stays_within_the_full_jitter_bounds([[maybe_unused]] context& ctx)
{
  const auto calculator{ couchbase::core::columnar::default_backoff_calculator };
  const auto base = std::chrono::milliseconds(100);
  const auto cap = std::chrono::minutes(1);
  const double factor = 2;

  // Full Jitter draws uniformly below the exponential bound, so a single draw says nothing about
  // the bound. Repeat.
  for (std::size_t i = 0; i < 10; ++i) {
    assert_true(calculator(0) <= base, "the first retry waits at most the base delay");
    assert_true(calculator(2) <= base * std::pow(factor, 2),
                "the third retry waits at most the base delay grown by the factor");
    assert_true(calculator(1000) <= cap, "an unbounded retry count is still capped");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(backoff_stays_within_the_full_jitter_bounds), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
