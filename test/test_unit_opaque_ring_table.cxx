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

#include "core/io/opaque_ring_table.hxx"
#include "core/utils/movable_function.hxx"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <set>

namespace
{
using handler = couchbase::core::utils::movable_function<void(int)>;
using table = couchbase::core::io::opaque_ring_table<handler>;

// Mirrors opaque_ring_table::ring_size; opaques that differ by this value map to the same ring
// slot.
constexpr std::uint32_t ring_size = 512;
} // namespace

// Sanitizer builds intercept allocation and provide their own operator new/delete, so defining them
// here would be a multiple-definition link error. Compile the allocation counter, the overrides,
// and the allocation-count test out under sanitizers (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED); the
// functional tests still run.
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

TEST_CASE("unit: opaque_ring_table stores and takes a handler once", "[unit]")
{
  table t;
  int seen = 0;
  t.insert(42, [&seen](int v) {
    seen = v;
  });

  auto h = t.take(42);
  REQUIRE(static_cast<bool>(h));
  h(7);
  REQUIRE(seen == 7);

  // taken entries are gone
  REQUIRE_FALSE(static_cast<bool>(t.take(42)));
}

TEST_CASE("unit: opaque_ring_table returns empty for an absent opaque", "[unit]")
{
  table t;
  REQUIRE_FALSE(static_cast<bool>(t.take(99)));
}

TEST_CASE("unit: opaque_ring_table routes ring-colliding opaques independently", "[unit]")
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
  REQUIRE(a == 1);
  REQUIRE(b == 2);
}

TEST_CASE("unit: opaque_ring_table same-slot guard keeps the first handler for a duplicate opaque",
          "[unit]")
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
  }); // ignored: the first handler for an opaque in the same ring slot is kept

  t.take(3)(0);
  REQUIRE(seen == 1);
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
TEST_CASE("unit: opaque_ring_table insert and take within the ring do not allocate", "[unit]")
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
  REQUIRE(after - before == 0);
}
#endif

TEST_CASE("unit: opaque_ring_table drains every registered handler and clears", "[unit]")
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
    REQUIRE(static_cast<bool>(h));
  }
  REQUIRE(drained.size() == 6);
  REQUIRE(opaques == std::set<std::uint32_t>{ 1, 2, 3, 4, 5, 1 + ring_size });

  // everything is gone after draining
  for (std::uint32_t i = 1; i <= 5; ++i) {
    REQUIRE_FALSE(static_cast<bool>(t.take(i)));
  }
  REQUIRE_FALSE(static_cast<bool>(t.take(1 + ring_size)));
}
