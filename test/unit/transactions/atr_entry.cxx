/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include "core/transactions/internal/atr_entry.hxx"

#include <cstdint>
#include <limits>
#include <optional>

namespace couchbase::test
{
namespace
{
using couchbase::core::transactions::atr_entry;
using couchbase::core::transactions::attempt_state;

/**
 * Build an atr_entry with CAS and optional start/expiry values.
 * The CAS encodes "current time" in nanoseconds; the entry start is in milliseconds.
 * elapsed_ms = (cas / 1_000_000) - start_ms
 */
auto
make_atr_entry(std::uint64_t cas_ns,
               std::optional<std::uint64_t> start_ms,
               std::optional<std::uint32_t> expires_after_ms) -> atr_entry
{
  return atr_entry{
    /*atr_bucket=*/"b",
    /*atr_id=*/"id",
    /*attempt_id=*/"attempt",
    /*state=*/attempt_state::PENDING,
    /*timestamp_start_ms=*/start_ms,
    /*timestamp_commit_ms=*/std::nullopt,
    /*timestamp_complete_ms=*/std::nullopt,
    /*timestamp_rollback_ms=*/std::nullopt,
    /*timestamp_rolled_back_ms=*/std::nullopt,
    /*expires_after_ms=*/expires_after_ms,
    /*inserted_ids=*/std::nullopt,
    /*replaced_ids=*/std::nullopt,
    /*removed_ids=*/std::nullopt,
    /*forward_compat=*/std::nullopt,
    /*cas=*/cas_ns,
    /*durability_level=*/std::nullopt,
  };
}

void
an_entry_past_its_ttl_has_expired([[maybe_unused]] context& ctx)
{
  // start=1000ms, ttl=500ms, cas encodes 2000ms → elapsed=1000ms > 500ms → expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 2000ULL * 1'000'000ULL; // 2000ms as nanoseconds
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_true(entry.has_expired(), "elapsed time beyond the TTL is expiry");
}

void
an_entry_within_its_ttl_has_not_expired([[maybe_unused]] context& ctx)
{
  // start=1000ms, ttl=5000ms, cas encodes 1100ms → elapsed=100ms < 5000ms → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 5000U;
  const std::uint64_t cas_ns = 1100ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_false(entry.has_expired(), "elapsed time inside the TTL is not expiry");
}

// An entry carrying no expiry cannot be judged against one: expiry is reported only where both the
// start timestamp and the TTL are present, so the answer is conservatively false rather than
// derived from an absent value.
void
an_entry_without_an_expiry_has_not_expired([[maybe_unused]] context& ctx)
{
  const std::uint64_t start_ms = 1000ULL;
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL; // far in the future
  const auto entry = make_atr_entry(cas_ns, start_ms, /*expires_after_ms=*/std::nullopt);

  assert_false(entry.has_expired(), "no TTL means no expiry, however old the entry looks");
}

void
an_entry_without_a_start_timestamp_has_not_expired([[maybe_unused]] context& ctx)
{
  const std::uint32_t ttl_ms = 100U;
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, /*start_ms=*/std::nullopt, ttl_ms);

  assert_false(entry.has_expired(), "no start timestamp means no elapsed time to compare");
}

void
an_entry_without_either_timestamp_has_not_expired([[maybe_unused]] context& ctx)
{
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, std::nullopt, std::nullopt);

  assert_false(entry.has_expired(), "neither bound present means no expiry");
}

void
an_entry_whose_cas_precedes_its_start_has_not_expired([[maybe_unused]] context& ctx)
{
  // cas_ms <= start_ms — the guard (cas_ms > start_ms) is false → returns false
  const std::uint64_t start_ms = 2000ULL;
  const std::uint32_t ttl_ms = 100U;
  const std::uint64_t cas_ns = 1500ULL * 1'000'000ULL; // cas_ms=1500 < start_ms=2000
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_false(entry.has_expired(), "a CAS behind the start yields no elapsed time");
}

void
the_safety_margin_extends_the_effective_ttl([[maybe_unused]] context& ctx)
{
  // start=1000ms, ttl=500ms, cas encodes 1600ms → elapsed=600ms
  // Without margin: 600 > 500 → expired
  // With margin 200: 600 > 700? No → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 1600ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_true(entry.has_expired(/*safety_margin=*/0), "no margin leaves the TTL as written");
  assert_false(entry.has_expired(/*safety_margin=*/200),
               "the margin is added to the TTL, not subtracted from the elapsed time");
}

void
an_entry_exactly_at_its_ttl_has_not_expired([[maybe_unused]] context& ctx)
{
  // elapsed == ttl (not strictly greater) → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 1500ULL * 1'000'000ULL; // elapsed == ttl
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_false(entry.has_expired(), "the comparison is strict, so the boundary is not expiry");
}

void
a_large_ttl_plus_the_safety_margin_does_not_overflow([[maybe_unused]] context& ctx)
{
  // expires_after_ms near UINT32_MAX: (expires_after_ms + safety_margin) must be computed in
  // 64-bit.  In 32-bit it wraps (e.g. (UINT32_MAX - 10) + 100 == 89), so the small elapsed time
  // below would be wrongly considered greater than the TTL and the entry reported as expired.
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = std::numeric_limits<std::uint32_t>::max() - 10U;
  const std::uint64_t cas_ns = 3000ULL * 1'000'000ULL; // cas_ms=3000, elapsed=2000ms
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);

  assert_false(entry.has_expired(/*safety_margin=*/100),
               "the effective TTL is computed in 64-bit and does not wrap");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_entry_past_its_ttl_has_expired), {}, timeout::instant },
      { CASE(an_entry_within_its_ttl_has_not_expired), {}, timeout::instant },
      { CASE(an_entry_without_an_expiry_has_not_expired), {}, timeout::instant },
      { CASE(an_entry_without_a_start_timestamp_has_not_expired), {}, timeout::instant },
      { CASE(an_entry_without_either_timestamp_has_not_expired), {}, timeout::instant },
      { CASE(an_entry_whose_cas_precedes_its_start_has_not_expired), {}, timeout::instant },
      { CASE(the_safety_margin_extends_the_effective_ttl), {}, timeout::instant },
      { CASE(an_entry_exactly_at_its_ttl_has_not_expired), {}, timeout::instant },
      { CASE(a_large_ttl_plus_the_safety_margin_does_not_overflow), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
