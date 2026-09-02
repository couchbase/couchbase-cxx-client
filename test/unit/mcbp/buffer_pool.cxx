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

#include "core/io/mcbp_buffer_pool.hxx"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <vector>

// Sanitizer builds intercept allocation and provide their own operator new/delete, so this counter,
// the overrides, and the allocation-count case are compiled out under sanitizers
// (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED); the functional cases still run.
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
operator delete(void* p, std::size_t) noexcept
{
  std::free(p);
}
#endif

namespace couchbase::test
{
namespace
{
using couchbase::core::io::mcbp_buffer_pool;

void
an_empty_pool_hands_out_an_empty_buffer([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool;
  auto buf = pool.acquire();
  assert_true(buf.empty(), "a buffer from an empty pool holds nothing");
  assert_eq(buf.capacity(), std::size_t{ 0 }, "a buffer from an empty pool reserves nothing");
}

void
a_released_buffer_is_recycled_with_its_capacity_retained([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool;

  std::vector<std::byte> buf;
  buf.resize(4096);
  const auto* const data_before = buf.data();
  pool.release(std::move(buf));
  assert_eq(pool.size(), std::size_t{ 1 }, "the released buffer is held");

  auto reused = pool.acquire();
  assert_eq(pool.size(), std::size_t{ 0 }, "the held buffer is handed out rather than copied");
  // Same storage came back, cleared but with capacity intact — this is what lets a large response
  // reuse the buffer instead of allocating (and, at large sizes, mmap'ing) a new one.
  assert_true(reused.capacity() >= 4096, "the recycled buffer keeps its capacity");
  assert_true(reused.empty(), "the recycled buffer is handed back cleared");
  // data() yields a raw pointer, not an iterator, so cppcheck reads the two calls as iterators
  // from different containers. The comparison is the assertion, and it is well formed.
  // cppcheck-suppress mismatchingContainers; data() is a pointer, not an iterator
  assert_true(reused.data() == data_before, "the recycled buffer is the same storage");
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
void
reusing_a_recycled_buffer_performs_no_allocation([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool;

  std::vector<std::byte> buf;
  buf.resize(64 * 1024);
  pool.release(std::move(buf));

  auto reused = pool.acquire();
  const long before = g_alloc_count.load(std::memory_order_relaxed);
  // Refilling within the retained capacity must not touch the allocator.
  reused.resize(64 * 1024);
  const long after = g_alloc_count.load(std::memory_order_relaxed);
  assert_eq(after - before, 0L, "allocations performed by refilling a recycled buffer");
}
#endif

void
a_buffer_with_no_capacity_is_not_pooled([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool;
  pool.release(std::vector<std::byte>{});
  assert_eq(pool.size(), std::size_t{ 0 }, "a buffer with nothing to recycle is dropped");
}

void
the_pool_holds_at_most_its_retained_buffer_count([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool(/*retained_buffers=*/2,
                        /*max_buffer_bytes=*/1024 * 1024);
  for (int i = 0; i < 5; ++i) {
    std::vector<std::byte> buf;
    buf.resize(128);
    pool.release(std::move(buf));
  }
  assert_eq(pool.size(), std::size_t{ 2 }, "releases beyond the retained count are dropped");
}

void
an_oversized_buffer_is_not_pooled([[maybe_unused]] context& ctx)
{
  mcbp_buffer_pool pool(/*retained_buffers=*/8,
                        /*max_buffer_bytes=*/1024 * 1024);

  std::vector<std::byte> ok;
  ok.resize(512 * 1024);
  pool.release(std::move(ok));
  assert_eq(pool.size(), std::size_t{ 1 }, "a buffer within the byte limit is held");

  std::vector<std::byte> huge;
  huge.resize(2 * 1024 * 1024);
  pool.release(std::move(huge));
  assert_eq(
    pool.size(), std::size_t{ 1 }, "a buffer past the byte limit is dropped rather than hoarded");
}

void
the_thread_local_pool_recycles_a_buffer_within_the_calling_thread([[maybe_unused]] context& ctx)
{
  auto& pool = couchbase::core::io::tls_response_body_pool();
  const auto baseline = pool.size();

  std::vector<std::byte> buf;
  buf.resize(8192);
  const auto* const data_before = buf.data();
  pool.release(std::move(buf));
  assert_eq(pool.size(), baseline + 1, "the released buffer is held");

  // The same thread that released it gets the storage back on the next acquire.
  auto reused = pool.acquire();
  assert_eq(pool.size(), baseline, "the held buffer is handed out");
  assert_true(reused.capacity() >= 8192, "the recycled buffer keeps its capacity");
  // data() yields a raw pointer, not an iterator, so cppcheck reads the two calls as iterators
  // from different containers. The comparison is the assertion, and it is well formed.
  // cppcheck-suppress mismatchingContainers; data() is a pointer, not an iterator
  assert_true(reused.data() == data_before, "the recycled buffer is the same storage");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_empty_pool_hands_out_an_empty_buffer) },
      { CASE(a_released_buffer_is_recycled_with_its_capacity_retained) },
#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
      { CASE(reusing_a_recycled_buffer_performs_no_allocation) },
#endif
      { CASE(a_buffer_with_no_capacity_is_not_pooled) },
      { CASE(the_pool_holds_at_most_its_retained_buffer_count) },
      { CASE(an_oversized_buffer_is_not_pooled) },
      { CASE(the_thread_local_pool_recycles_a_buffer_within_the_calling_thread) },
    },
  };
}

} // namespace couchbase::test
