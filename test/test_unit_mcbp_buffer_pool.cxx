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

#include "core/io/mcbp_buffer_pool.hxx"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>
#include <vector>

// Sanitizer builds intercept allocation and provide their own operator new/delete, so this counter,
// the overrides, and the allocation-count test are compiled out under sanitizers
// (COUCHBASE_CXX_CLIENT_BUILD_SANITIZED); the functional tests still run.
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

TEST_CASE("unit: an empty pool hands out an empty buffer", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool;
  auto buf = pool.acquire();
  REQUIRE(buf.empty());
  REQUIRE(buf.capacity() == 0);
}

TEST_CASE("unit: a released buffer is recycled with its capacity retained", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool;

  std::vector<std::byte> buf;
  buf.resize(4096);
  const auto* const data_before = buf.data();
  pool.release(std::move(buf));
  REQUIRE(pool.size() == 1);

  auto reused = pool.acquire();
  REQUIRE(pool.size() == 0);
  // Same storage came back, cleared but with capacity intact — this is what lets a large response
  // reuse the buffer instead of allocating (and, at large sizes, mmap'ing) a new one.
  REQUIRE(reused.capacity() >= 4096);
  REQUIRE(reused.empty());
  REQUIRE(reused.data() == data_before);
}

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
TEST_CASE("unit: reusing a recycled buffer performs no allocation", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool;

  std::vector<std::byte> buf;
  buf.resize(64 * 1024);
  pool.release(std::move(buf));

  auto reused = pool.acquire();
  const long before = g_alloc_count.load(std::memory_order_relaxed);
  // Refilling within the retained capacity must not touch the allocator.
  reused.resize(64 * 1024);
  const long after = g_alloc_count.load(std::memory_order_relaxed);
  REQUIRE(after - before == 0);
}
#endif

TEST_CASE("unit: an empty (capacity-less) buffer is not pooled", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool;
  pool.release(std::vector<std::byte>{});
  REQUIRE(pool.size() == 0);
}

TEST_CASE("unit: the pool holds at most max_buffers", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool(/*retained_buffers=*/2,
                                             /*max_buffer_bytes=*/1024 * 1024);
  for (int i = 0; i < 5; ++i) {
    std::vector<std::byte> buf;
    buf.resize(128);
    pool.release(std::move(buf));
  }
  REQUIRE(pool.size() == 2);
}

TEST_CASE("unit: an oversized buffer is not pooled so memory is not hoarded", "[unit]")
{
  couchbase::core::io::mcbp_buffer_pool pool(/*retained_buffers=*/8,
                                             /*max_buffer_bytes=*/1024 * 1024);

  std::vector<std::byte> ok;
  ok.resize(512 * 1024);
  pool.release(std::move(ok));
  REQUIRE(pool.size() == 1);

  std::vector<std::byte> huge;
  huge.resize(2 * 1024 * 1024);
  pool.release(std::move(huge));
  REQUIRE(pool.size() == 1); // huge buffer was dropped, not pooled
}

TEST_CASE("unit: the thread-local pool recycles a buffer within the calling thread", "[unit]")
{
  auto& pool = couchbase::core::io::tls_response_body_pool();
  const auto baseline = pool.size();

  std::vector<std::byte> buf;
  buf.resize(8192);
  const auto* const data_before = buf.data();
  pool.release(std::move(buf));
  REQUIRE(pool.size() == baseline + 1);

  // The same thread that released it gets the storage back on the next acquire.
  auto reused = pool.acquire();
  REQUIRE(pool.size() == baseline);
  REQUIRE(reused.capacity() >= 8192);
  REQUIRE(reused.data() == data_before);
}
