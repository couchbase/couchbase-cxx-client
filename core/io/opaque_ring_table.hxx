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
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>

namespace couchbase::core::io
{
/**
 * Maps a monotonically increasing 32-bit opaque to an in-flight handler without a per-operation
 * heap allocation.
 *
 * The common case uses a fixed ring indexed by `opaque & mask`. Because the opaque increases by one
 * per operation, in-flight operations occupy distinct slots as long as the range between the oldest
 * and newest in-flight opaque stays within the ring; when it does not (a slow operation held open
 * across a full lap of the ring, or opaque wraparound), the colliding entries spill to an overflow
 * map, so lookups stay correct for any number of in-flight operations.
 *
 * Precondition: an opaque is unique while it is in flight — a value is never registered again
 * before its handler has been taken. The single caller guarantees this by allocating a fresh opaque
 * per operation, which is what lets insert() store into a free ring slot without also consulting
 * the overflow map.
 *
 * Not thread-safe: callers serialize access.
 */
template<typename Handler>
class opaque_ring_table
{
public:
  // Register a handler for an opaque. Relies on the unique-in-flight precondition: because a fresh
  // opaque is never already registered, it always finds either a free ring slot or (on a ring
  // collision) a free overflow entry. If the target ring slot happens to already hold the same
  // opaque, insert() keeps the existing handler -- but that is a cheap same-slot guard only; the
  // overflow map is deliberately not consulted (that would add a lookup to every insert on the hot
  // path). Re-registering an opaque that is still in flight therefore violates the precondition and
  // is not a supported dedup: if the original spilled to overflow_ and its ring slot has since been
  // reclaimed, a duplicate would land in the ring and strand the overflow copy.
  //
  // The handler is taken by rvalue reference and moved into storage exactly once (handlers are
  // move-only and every call site passes an rvalue), avoiding the extra move a by-value parameter
  // would add on the hot path.
  void insert(std::uint32_t opaque, Handler&& handler);

  // Remove and return the handler for an opaque, or a default-constructed (empty) handler if
  // absent. The returned handler is the sole owner; dropping it strands the operation, hence
  // [[nodiscard]].
  [[nodiscard]] auto take(std::uint32_t opaque) -> Handler;

  // Move out every registered (opaque, handler) pair and clear the table. The returned handlers are
  // the sole remaining owners, hence [[nodiscard]].
  [[nodiscard]] auto drain() -> std::vector<std::pair<std::uint32_t, Handler>>;

private:
  static constexpr std::size_t ring_size = 512;
  static constexpr std::uint32_t ring_mask = ring_size - 1;
  // `opaque & ring_mask` is a valid modulo only when the size is a power of two.
  static_assert((ring_size & (ring_size - 1)) == 0, "ring_size must be a power of two");

  struct slot {
    std::uint32_t opaque{ 0 };
    bool occupied{ false };
    Handler handler{};
  };

  std::array<slot, ring_size> ring_{};
  std::unordered_map<std::uint32_t, Handler> overflow_{};
};

template<typename Handler>
void
opaque_ring_table<Handler>::insert(std::uint32_t opaque, Handler&& handler)
{
  auto& s = ring_[opaque & ring_mask];
  if (!s.occupied) {
    s.opaque = opaque;
    s.occupied = true;
    s.handler = std::move(handler);
    return;
  }
  if (s.opaque == opaque) {
    return; // keep the existing handler (not expected under the unique-opaque precondition)
  }
  // the slot is held by a different in-flight opaque (ring collision) — spill to the overflow map
  overflow_.try_emplace(opaque, std::move(handler));
}

template<typename Handler>
auto
opaque_ring_table<Handler>::take(std::uint32_t opaque) -> Handler
{
  auto& s = ring_[opaque & ring_mask];
  if (s.occupied && s.opaque == opaque) {
    Handler h = std::move(s.handler);
    s.handler = Handler{};
    s.occupied = false;
    return h;
  }
  if (auto it = overflow_.find(opaque); it != overflow_.end()) {
    Handler h = std::move(it->second);
    overflow_.erase(it);
    return h;
  }
  return Handler{};
}

template<typename Handler>
auto
opaque_ring_table<Handler>::drain() -> std::vector<std::pair<std::uint32_t, Handler>>
{
  std::vector<std::pair<std::uint32_t, Handler>> result;
  result.reserve(ring_size + overflow_.size());
  for (auto& s : ring_) {
    if (s.occupied) {
      result.emplace_back(s.opaque, std::move(s.handler));
      s.handler = Handler{};
      s.occupied = false;
    }
  }
  for (auto& [opaque, h] : overflow_) {
    result.emplace_back(opaque, std::move(h));
  }
  overflow_.clear();
  return result;
}
} // namespace couchbase::core::io
