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

#include "tools/key_picker.hxx"

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

namespace couchbase::test
{
namespace
{
constexpr std::uint64_t seed{ 1234 };

void
an_empty_list_has_no_key([[maybe_unused]] context& ctx)
{
  std::mt19937_64 gen{ seed };
  assert_false(cbc::pick_known_key(0, gen).has_value(), "an empty list yields no index");
}

void
every_index_is_inside_the_list([[maybe_unused]] context& ctx)
{
  std::mt19937_64 gen{ seed };
  const auto only = cbc::pick_known_key(1, gen);
  assert_true(only.has_value() && *only == 0, "a one-key list yields that key");

  constexpr std::size_t size{ 3 };
  std::vector<bool> drawn(size, false);
  for (int i = 0; i < 1000; ++i) {
    const auto index = cbc::pick_known_key(size, gen);
    assert_true(index.has_value() && *index < size, "every index is below the list size");
    drawn[*index] = true;
  }
  assert_true(drawn == std::vector<bool>(size, true), "every key of the list can be drawn");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_key_picker",
    {
      { "an_empty_list_has_no_key", an_empty_list_has_no_key, {}, timeout::instant },
      { "every_index_is_inside_the_list", every_index_is_inside_the_list, {}, timeout::instant },
    },
  };
}
} // namespace couchbase::test
