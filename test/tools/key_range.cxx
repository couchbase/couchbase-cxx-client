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

#include "tools/key_range.hxx"

#include <cstdint>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
void
keys_match_the_c_pillowfight_format([[maybe_unused]] context& ctx)
{
  assert_eq(cbc::key_range{ "user_", 0, 1000 }.key_at(999),
            std::string{ "user_00000000000000000999" });
  assert_eq(cbc::key_range{ "", 0, 100 }.key_at(42), std::string{ "00000000000000000042" });
  assert_eq(cbc::key_range{ "p", 0, 1U << 31 }.key_at((1U << 31) - 1),
            std::string{ "p00000000002147483647" });
}

void
key_at_takes_an_absolute_index([[maybe_unused]] context& ctx)
{
  const cbc::key_range range{ "k", 7, 1001 };
  assert_eq(range.key_at(7), std::string{ "k00000000000000000007" });
  assert_eq(range.slice(1, 4).key_at(258), std::string{ "k00000000000000000258" });
}

void
slices_cover_the_range_exactly_once([[maybe_unused]] context& ctx)
{
  const cbc::key_range range{ "k", 7, 1001 };
  constexpr std::size_t workers{ 4 };

  std::vector<std::uint32_t> counts;
  std::vector<std::uint32_t> starts;
  std::uint32_t next_start{ range.start() };
  for (std::size_t worker = 0; worker < workers; ++worker) {
    const auto slice = range.slice(worker, workers);
    assert_eq(slice.prefix(), range.prefix());
    assert_eq(slice.start(), next_start, "slices are not contiguous");
    next_start = slice.start() + slice.count();
    counts.push_back(slice.count());
    starts.push_back(slice.start());
  }

  assert_true(counts == std::vector<std::uint32_t>{ 251, 250, 250, 250 },
              "the remainder did not go to the first workers");
  assert_true(starts == std::vector<std::uint32_t>{ 7, 258, 508, 758 });
  assert_eq(next_start, std::uint32_t{ 1008 }, "the last slice does not end at the range end");

  const cbc::key_range ten{ "k", 0, 10 };
  std::vector<std::uint32_t> ten_counts;
  std::vector<std::uint32_t> ten_starts;
  for (std::size_t worker = 0; worker < workers; ++worker) {
    ten_counts.push_back(ten.slice(worker, workers).count());
    ten_starts.push_back(ten.slice(worker, workers).start());
  }
  assert_true(ten_counts == std::vector<std::uint32_t>{ 3, 3, 2, 2 },
              "a remainder of 2 goes to the first two workers");
  assert_true(ten_starts == std::vector<std::uint32_t>{ 0, 3, 6, 8 },
              "each slice starts where the previous one ends");
}

void
a_slice_can_be_empty_when_workers_exceed_items([[maybe_unused]] context& ctx)
{
  const cbc::key_range range{ "k", 0, 3 };

  assert_eq(range.slice(3, 4).count(), std::uint32_t{ 0 });
  assert_eq(range.slice(3, 4).start(), std::uint32_t{ 3 }, "an empty slice starts at the end");
  for (std::size_t worker = 0; worker < 3; ++worker) {
    assert_eq(range.slice(worker, 4).count(), std::uint32_t{ 1 });
    assert_eq(range.slice(worker, 4).start(), static_cast<std::uint32_t>(worker));
  }
}

void
the_end_is_capped_for_c_compatibility([[maybe_unused]] context& ctx)
{
  assert_no_throw([] {
    [[maybe_unused]] const cbc::key_range range{ "k", (1U << 31) - 10, 10 };
  });
  assert_throws<std::invalid_argument>([] {
    [[maybe_unused]] const cbc::key_range range{ "k", (1U << 31) - 10, 11 };
  });
  assert_false(cbc::key_range::fits(0xFFFFFFFFU, 1), "the end is summed without wrapping");
  assert_throws<std::invalid_argument>([] {
    [[maybe_unused]] const cbc::key_range range{ "k", 0xFFFFFFFFU, 1 };
  });
}

void
the_prefix_leaves_room_for_the_index([[maybe_unused]] context& ctx)
{
  assert_no_throw([] {
    [[maybe_unused]] const cbc::key_range range{ std::string(230, 'p'), 0, 1 };
  });
  assert_throws<std::invalid_argument>([] {
    [[maybe_unused]] const cbc::key_range range{ std::string(231, 'p'), 0, 1 };
  });
}

void
validate_rejects_an_unusable_keyset([[maybe_unused]] context& ctx)
{
  assert_false(cbc::key_range::validate("k", 0, 1, true).has_value(),
               "one key from index 0 is usable");
  assert_false(cbc::key_range::validate(std::string(230, 'p'), (1U << 31) - 1, 1, true).has_value(),
               "the last index and the longest prefix are usable in the default collection");
  assert_true(cbc::key_range::validate("k", 0, 0, true).has_value(), "an empty keyset is rejected");
  assert_true(cbc::key_range::validate("k", (1U << 31) - 1, 2, true).has_value(),
              "an end past 2^31 is rejected");
  assert_true(cbc::key_range::validate("k", 0xFFFFFFFFU, 1, true).has_value(),
              "an end that wraps 32 bits is rejected");
  assert_true(cbc::key_range::validate(std::string(231, 'p'), 0, 1, true).has_value(),
              "a prefix that leaves no room for the index is rejected in the default collection");
  assert_false(cbc::key_range::validate(std::string(226, 'p'), 0, 1, false).has_value(),
               "a 226-byte prefix is usable in a named collection");
  assert_true(cbc::key_range::validate(std::string(227, 'p'), 0, 1, false).has_value(),
              "a 227-byte prefix makes a 247-byte key, which a named collection rejects");
  assert_true(cbc::key_range::is_default_collection("_default", "_default"),
              "_default._default is the default collection");
  assert_false(cbc::key_range::is_default_collection("s", "_default"),
               "_default in another scope is a named collection");
  assert_false(cbc::key_range::is_default_collection("_default", "c"),
               "a named collection in the default scope is a named collection");
}

void
pick_stays_inside_the_range([[maybe_unused]] context& ctx)
{
  const cbc::key_range range{ "k", 5, 3 };
  const std::set<std::string> keys{ range.key_at(5), range.key_at(6), range.key_at(7) };
  std::mt19937_64 gen{ 1234 };
  std::set<std::string> drawn;
  for (int i = 0; i < 1000; ++i) {
    const auto key = range.pick(gen);
    assert_true(keys.count(key) == 1, "every picked key is inside [start, start + count)");
    drawn.insert(key);
  }
  assert_true(drawn == keys, "every key of the range can be picked");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_key_range",
    {
      { "keys_match_the_c_pillowfight_format",
        keys_match_the_c_pillowfight_format,
        {},
        timeout::instant },
      { "key_at_takes_an_absolute_index", key_at_takes_an_absolute_index, {}, timeout::instant },
      { "slices_cover_the_range_exactly_once",
        slices_cover_the_range_exactly_once,
        {},
        timeout::instant },
      { "a_slice_can_be_empty_when_workers_exceed_items",
        a_slice_can_be_empty_when_workers_exceed_items,
        {},
        timeout::instant },
      { "the_end_is_capped_for_c_compatibility",
        the_end_is_capped_for_c_compatibility,
        {},
        timeout::instant },
      { "the_prefix_leaves_room_for_the_index",
        the_prefix_leaves_room_for_the_index,
        {},
        timeout::instant },
      { "validate_rejects_an_unusable_keyset",
        validate_rejects_an_unusable_keyset,
        {},
        timeout::instant },
      { "pick_stays_inside_the_range", pick_stays_inside_the_range, {}, timeout::instant },
    },
  };
}
} // namespace couchbase::test
