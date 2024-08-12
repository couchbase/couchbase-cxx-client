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

#include "test_helper.hxx"

#include "core/columnar/backoff_calculator.hxx"

#include <chrono>
#include <cmath>

TEST_CASE("unit: backoff calculator gives backoff values within expected range", "[unit]")
{
  const auto calculator{ couchbase::core::columnar::default_backoff_calculator };
  const auto base = std::chrono::milliseconds(100);
  const auto cap = std::chrono::minutes(1);
  const double factor = 2;

  for (std::size_t i = 0; i < 10; ++i) {
    // Repeat a few times as the backoff is random with Full Jitter
    REQUIRE(calculator(0) <= base);
    REQUIRE(calculator(2) <= base * std::pow(factor, 2));
    REQUIRE(calculator(1000) <= cap);
  }
}
