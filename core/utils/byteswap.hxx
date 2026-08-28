/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <cstdint>
#include <type_traits>

namespace couchbase::core::utils
{
namespace detail
{
static constexpr auto
swap_bytes(std::uint16_t value) -> std::uint16_t
{
  auto hi = static_cast<std::uint16_t>(value << 8U);
  auto lo = static_cast<std::uint16_t>(value >> 8U);
  return hi | lo;
}

static constexpr auto
swap_bytes(std::uint32_t value) -> std::uint32_t
{
  std::uint32_t byte0 = value & 0x000000ffU;
  std::uint32_t byte1 = value & 0x0000ff00U;
  std::uint32_t byte2 = value & 0x00ff0000U;
  std::uint32_t byte3 = value & 0xff000000U;
  return (byte0 << 24) | (byte1 << 8) | (byte2 >> 8) | (byte3 >> 24);
}

static constexpr auto
swap_bytes(std::uint64_t value) -> std::uint64_t
{
  std::uint64_t hi = swap_bytes(static_cast<std::uint32_t>(value));
  std::uint32_t lo = swap_bytes(static_cast<std::uint32_t>(value >> 32));
  return (hi << 32) | lo;
}
} // namespace detail

/**
 * Reverse the bytes of a value on every host, whatever this host's byte order is.
 *
 * This is deliberately not a host/network conversion, and the two are not interchangeable. Use
 * it only where the byte order of the input is fixed by something other than this host. The one
 * such case in this codebase is the ${Mutation.CAS} macro, which kvengine writes as
 * 'macroToString(htonll(info.cas))', so the eight bytes inside that string carry the same order
 * no matter which host later reads them.
 */
static constexpr auto
reverse_bytes(std::uint16_t value) -> std::uint16_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
reverse_bytes(std::uint32_t value) -> std::uint32_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
reverse_bytes(std::uint64_t value) -> std::uint64_t
{
  return detail::swap_bytes(value);
}

// when 'unsigned long long' is not the same as 'std::uint64_t'
template<typename Dummy = void>
static constexpr auto
reverse_bytes(
  unsigned long long value,
  std::enable_if_t<!std::is_same_v<unsigned long long, std::uint64_t>, Dummy>* /* dummy */
  = nullptr) -> std::uint64_t
{
  return reverse_bytes(static_cast<std::uint64_t>(value));
}

/**
 * Convert a value from this host's byte order to network (big-endian) order, ready to be copied
 * into a wire buffer.
 */
static constexpr auto
host_to_network(std::uint16_t value) -> std::uint16_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
host_to_network(std::uint32_t value) -> std::uint32_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
host_to_network(std::uint64_t value) -> std::uint64_t
{
  return detail::swap_bytes(value);
}

// when 'unsigned long long' is not the same as 'std::uint64_t'
template<typename Dummy = void>
static constexpr auto
host_to_network(
  unsigned long long value,
  std::enable_if_t<!std::is_same_v<unsigned long long, std::uint64_t>, Dummy>* /* dummy */
  = nullptr) -> std::uint64_t
{
  return host_to_network(static_cast<std::uint64_t>(value));
}

/**
 * Convert a value read out of a wire buffer from network (big-endian) order to this host's byte
 * order. The inverse of host_to_network, and the same operation.
 */
static constexpr auto
network_to_host(std::uint16_t value) -> std::uint16_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
network_to_host(std::uint32_t value) -> std::uint32_t
{
  return detail::swap_bytes(value);
}

static constexpr auto
network_to_host(std::uint64_t value) -> std::uint64_t
{
  return detail::swap_bytes(value);
}

// when 'unsigned long long' is not the same as 'std::uint64_t'
template<typename Dummy = void>
static constexpr auto
network_to_host(
  unsigned long long value,
  std::enable_if_t<!std::is_same_v<unsigned long long, std::uint64_t>, Dummy>* /* dummy */
  = nullptr) -> std::uint64_t
{
  return network_to_host(static_cast<std::uint64_t>(value));
}

} // namespace couchbase::core::utils
