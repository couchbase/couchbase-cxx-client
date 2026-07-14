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

#pragma once

#include <array>
#include <cstddef>
#include <utility>
#include <vector>

namespace couchbase::core::io
{
/**
 * A tiny free-list of byte buffers used to recycle mcbp response body buffers.
 *
 * A KV response's body is allocated when the frame is parsed and freed a moment later when the
 * decoded response is destroyed. Recycling those buffers keeps their capacity warm, so a subsequent
 * response of similar size reuses the storage instead of allocating (and, once the value crosses
 * the allocator's mmap threshold, mmap/munmap-ing) a fresh block every operation.
 *
 * NOT thread-safe by design, and it performs no synchronization. It is intended to be used as a
 * thread-local pool (see tls_response_body_pool()): acquire and release always happen on the
 * calling thread's own pool, so a buffer acquired on one IO thread but released on another (for
 * example when a replica response is handed to a command strand running on a different IO thread)
 * is simply not recycled back to the acquiring thread — which is safe, because each thread only
 * ever touches its own pool. Recycling is therefore best-effort.
 *
 * Two bounds keep memory in check: at most `max_buffers` buffers are retained, and buffers larger
 * than `max_buffer_bytes` are never pooled (they are freed normally), so a single very large value
 * cannot pin megabytes per thread.
 *
 * The free list is a fixed-size array of (initially empty) buffers, so constructing the pool
 * allocates nothing. That matters because the pool is a lazily-initialized thread_local (see
 * tls_response_body_pool()): the first use on a given thread may be a release() from a response
 * destructor, and that first use triggers the thread_local's construction. Backing the free list
 * with a std::vector would make that construction allocate (and, on failure, throw) inside the
 * noexcept destructor path, where a throw calls std::terminate. A fixed array cannot.
 */
class mcbp_buffer_pool
{
public:
  using buffer = std::vector<std::byte>;

  static constexpr std::size_t max_buffers = 16;
  static constexpr std::size_t default_max_buffer_bytes = std::size_t{ 1024 } * 1024;

  explicit mcbp_buffer_pool(std::size_t retained_buffers = max_buffers,
                            std::size_t max_buffer_bytes = default_max_buffer_bytes)
    : retained_buffers_{ retained_buffers < max_buffers ? retained_buffers : max_buffers }
    , max_buffer_bytes_{ max_buffer_bytes }
  {
    // No allocation here by design (see the class comment): the free list is a fixed-size array of
    // empty buffers, so neither this constructor nor the lazy thread_local init it drives can
    // throw.
  }

  /**
   * Hand out a recycled buffer (cleared, capacity retained) or an empty one if the pool is empty.
   */
  auto acquire() -> buffer
  {
    if (count_ == 0) {
      return {};
    }
    // Move the buffer out, leaving an empty slot (the moved-from vector holds no storage).
    return std::move(free_[--count_]);
  }

  /**
   * Return a buffer for reuse. Empty buffers (nothing to recycle) and buffers above the size cap
   * are dropped, as is any buffer beyond the retained-count bound.
   */
  void release(buffer&& buf) noexcept
  {
    if (buf.capacity() == 0 || buf.capacity() > max_buffer_bytes_ || count_ >= retained_buffers_) {
      return;
    }
    buf.clear();
    // Slots at index >= count_ are always empty, so this move-assignment frees nothing and
    // allocates nothing; release() therefore never touches the allocator, as its noexcept contract
    // requires.
    free_[count_++] = std::move(buf);
  }

  [[nodiscard]] auto size() const -> std::size_t
  {
    return count_;
  }

private:
  std::array<buffer, max_buffers> free_{};
  std::size_t count_{ 0 };
  std::size_t retained_buffers_;
  std::size_t max_buffer_bytes_;
};

/**
 * The calling thread's response-body buffer pool. Shared by every mcbp connection served on that IO
 * thread; because it is thread-local, acquire and release are always confined to a single thread
 * and need no locking (see mcbp_buffer_pool).
 */
inline auto
tls_response_body_pool() -> mcbp_buffer_pool&
{
  static thread_local mcbp_buffer_pool pool;
  return pool;
}
} // namespace couchbase::core::io
