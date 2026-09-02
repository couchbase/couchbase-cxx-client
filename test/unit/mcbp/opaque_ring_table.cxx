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

#include "core/io/opaque_ring_table.hxx"
#include "core/utils/movable_function.hxx"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <set>

// Sanitizer builds intercept allocation and provide their own operator new/delete, so defining them
// here would be a multiple-definition link error. Compile the allocation counter, the overrides,
// and the allocation-count case out under sanitizers (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED); the
// functional cases still run.
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
namespace
{
std::atomic<long> g_alloc_count{ 0 };
} // namespace

void*
operator new(std::size_t n)
{
  g_alloc_count.fetch_add(1, std::memory_order_relaxed);
  void* p = std::malloc(n != 0 ? n : 1);
  if (p == nullptr) {
    throw std::bad_alloc{};
  }
  return p;
}

void
operator delete(void* p) noexcept
{
  std::free(p);
}

void
operator delete(void* p, std::size_t /* n */) noexcept
{
  std::free(p);
}
#endif

namespace couchbase::test
{
namespace
{
using handler = couchbase::core::utils::movable_function<void(int)>;
using table = couchbase::core::io::opaque_ring_table<handler>;

// Mirrors opaque_ring_table::ring_size; opaques that differ by this value map to the same ring
// slot.
constexpr std::uint32_t ring_size = 512;

void
a_stored_handler_is_taken_once_and_then_gone([[maybe_unused]] context& ctx)
{
  table t;
  int seen = 0;
  t.insert(42, [&seen](int v) {
    seen = v;
  });

  auto h = t.take(42);
  assert_true(static_cast<bool>(h), "the handler registered for the opaque comes back");
  h(7);
  assert_eq(seen, 7, "the handler that came back is the one that was stored");

  assert_false(static_cast<bool>(t.take(42)), "a taken opaque is no longer registered");
}

void
an_absent_opaque_yields_no_handler([[maybe_unused]] context& ctx)
{
  table t;
  assert_false(static_cast<bool>(t.take(99)), "an opaque that was never inserted");
}

void
ring_colliding_opaques_keep_their_own_handlers([[maybe_unused]] context& ctx)
{
  table t;
  int a = 0;
  int b = 0;
  // 5 and 5 + ring_size map to the same ring slot; both must be retrievable.
  t.insert(5, [&a](int v) {
    a = v;
  });
  t.insert(5 + ring_size, [&b](int v) {
    b = v;
  });

  t.take(5 + ring_size)(2);
  t.take(5)(1);
  assert_eq(a, 1, "the handler held in the ring slot");
  assert_eq(b, 2, "the handler displaced into the overflow map");
}

void
a_duplicate_opaque_in_the_same_slot_keeps_the_first_handler([[maybe_unused]] context& ctx)
{
  // Re-registering an in-flight opaque violates the unique-in-flight precondition; this exercises
  // the cheap same-slot guard, which keeps the first handler when the duplicate lands on the ring
  // slot the opaque already occupies. (Dedup across the overflow map is intentionally not
  // provided.)
  table t;
  int seen = 0;
  t.insert(3, [&seen](int) {
    seen = 1;
  });
  t.insert(3, [&seen](int) {
    seen = 2;
  });

  t.take(3)(0);
  assert_eq(seen, 1, "the first handler registered for the opaque survives the duplicate");
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void
insert_and_take_within_the_ring_do_not_allocate([[maybe_unused]] context& ctx)
{
  table t;
  // Handlers capture only a reference (fits the movable_function small buffer), and the ring is
  // preallocated, so a full insert/take cycle within the ring must perform no heap allocation.
  int sink = 0;
  const long before = g_alloc_count.load(std::memory_order_relaxed);
  for (std::uint32_t i = 0; i < ring_size; ++i) {
    t.insert(i, [&sink](int v) {
      sink += v;
    });
  }
  for (std::uint32_t i = 0; i < ring_size; ++i) {
    static_cast<void>(t.take(i));
  }
  const long after = g_alloc_count.load(std::memory_order_relaxed);
  assert_eq(after - before, 0L, "allocations performed by a full cycle within the ring");
}
#endif

void
drain_yields_every_registered_handler_and_empties_the_table([[maybe_unused]] context& ctx)
{
  table t;
  std::set<std::uint32_t> opaques;
  for (std::uint32_t i = 1; i <= 5; ++i) {
    t.insert(i, [](int) {
    });
  }
  // include a ring collision to exercise the overflow path in the drain
  t.insert(1 + ring_size, [](int) {
  });

  auto drained = t.drain();
  for (auto& [opaque, h] : drained) {
    opaques.insert(opaque);
    assert_true(static_cast<bool>(h), "every drained entry carries its handler");
  }
  assert_eq(drained.size(), std::size_t{ 6 }, "the number of entries drained");
  assert_eq(
    opaques, (std::set<std::uint32_t>{ 1, 2, 3, 4, 5, 1 + ring_size }), "the opaques drained");

  for (std::uint32_t i = 1; i <= 5; ++i) {
    assert_false(static_cast<bool>(t.take(i)), "a drained opaque is no longer registered");
  }
  assert_false(static_cast<bool>(t.take(1 + ring_size)),
               "a drained overflow opaque is no longer registered");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_stored_handler_is_taken_once_and_then_gone) },
      { CASE(an_absent_opaque_yields_no_handler) },
      { CASE(ring_colliding_opaques_keep_their_own_handlers) },
      { CASE(a_duplicate_opaque_in_the_same_slot_keeps_the_first_handler) },
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
      { CASE(insert_and_take_within_the_ring_do_not_allocate) },
#endif
      { CASE(drain_yields_every_registered_handler_and_empties_the_table) },
    },
  };
}

} // namespace couchbase::test
