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

#include "test_helper.hxx"

#include "core/utils/byteswap.hxx"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace
{
using couchbase::core::utils::detail::convert;
using couchbase::core::utils::detail::host_order;
using couchbase::core::utils::detail::native_order;

// std::array::operator== is not constexpr before C++20.
template<std::size_t N>
constexpr auto
bytes_equal(const std::array<std::byte, N>& lhs, const std::array<std::byte, N>& rhs) -> bool
{
  for (std::size_t i = 0; i < N; ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}

// Model of what a memcpy of a native integer lays down in memory on a host of the given byte
// order. Every conversion in the codec is paired with such a memcpy, and it is that pairing, not
// the conversion alone, that has to produce network order. Modelling the store is what lets this
// test check the bytes that would actually reach the wire, on either kind of host.
template<host_order Order, typename T>
constexpr auto
store(T value) -> std::array<std::byte, sizeof(T)>
{
  std::array<std::byte, sizeof(T)> bytes{};
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    const std::size_t shift = (Order == host_order::big) ? (sizeof(T) - 1U - i) * 8U : i * 8U;
    bytes[i] = static_cast<std::byte>((value >> shift) & 0xffU);
  }
  return bytes;
}

// The inverse: what a native integer reads back as on such a host, from bytes in memory.
template<host_order Order, typename T>
constexpr auto
load(const std::array<std::byte, sizeof(T)>& bytes) -> T
{
  T value{};
  for (std::size_t i = 0; i < sizeof(T); ++i) {
    const std::size_t shift = (Order == host_order::big) ? (sizeof(T) - 1U - i) * 8U : i * 8U;
    value =
      static_cast<T>(value | (static_cast<T>(std::to_integer<std::uint8_t>(bytes[i])) << shift));
  }
  return value;
}

// The contract, stated once: a host of the given byte order must be able to put a value onto the
// wire in network (big-endian) order by converting it and copying the native integer out, and to
// recover it by copying the bytes in and converting back.
template<host_order Order, typename T>
constexpr auto
matches_wire_contract(T value) -> bool
{
  const auto network = store<host_order::big, T>(value);
  return bytes_equal(store<Order, T>(convert<Order>(value)), network) &&
         convert<Order>(load<Order, T>(network)) == value;
}
} // namespace

TEST_CASE("unit: byte order conversion holds for a big-endian host", "[unit]")
{
  // No CI host is big-endian, so this branch would otherwise never be verified. Because the host
  // order is a template parameter rather than a preprocessor branch, it can be instantiated and
  // proven here, at compile time, on an ordinary little-endian build.
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint16_t>(0x0102U));
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint32_t>(0x01020304U));
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint64_t>(0x0102030405060708U));

  // The values from the reported bootstrap failure: a HELLO whose key and body lengths went onto
  // the wire byte-reversed.
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint16_t>(188U));
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint32_t>(228U));
  STATIC_REQUIRE(matches_wire_contract<host_order::big, std::uint32_t>(1U));
}

TEST_CASE("unit: byte order conversion holds for a little-endian host", "[unit]")
{
  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint16_t>(0x0102U));
  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint32_t>(0x01020304U));
  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint64_t>(0x0102030405060708U));

  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint16_t>(188U));
  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint32_t>(228U));
  STATIC_REQUIRE(matches_wire_contract<host_order::little, std::uint32_t>(1U));
}

TEST_CASE("unit: conversion is the identity on a big-endian host", "[unit]")
{
  // Stated separately from the contract above because it is the part that regressed: the
  // conversion used to be an unconditional reversal, which on a big-endian host is applied on
  // top of an already-correct value.
  STATIC_REQUIRE(convert<host_order::big>(std::uint16_t{ 0x0102U }) == 0x0102U);
  STATIC_REQUIRE(convert<host_order::big>(std::uint32_t{ 0x01020304U }) == 0x01020304U);
  STATIC_REQUIRE(convert<host_order::big>(std::uint64_t{ 0x0102030405060708U }) ==
                 0x0102030405060708U);

  STATIC_REQUIRE(convert<host_order::little>(std::uint16_t{ 0x0102U }) == 0x0201U);
  STATIC_REQUIRE(convert<host_order::little>(std::uint32_t{ 0x01020304U }) == 0x04030201U);
  STATIC_REQUIRE(convert<host_order::little>(std::uint64_t{ 0x0102030405060708U }) ==
                 0x0807060504030201U);
}

TEST_CASE("unit: the detected host order matches this host", "[unit]")
{
  // Guards the preprocessor detection itself. A misspelt macro (__BYTE_ORDER__ against
  // __BYTE_ORDER, say) compiles fine and silently selects the wrong branch; everything above
  // would still pass, because it never consults the real host.
  constexpr std::uint32_t value{ 0x01020304U };
  std::array<std::byte, sizeof(value)> bytes{};
  std::memcpy(bytes.data(), &value, sizeof(value));

  const auto observed =
    (std::to_integer<std::uint8_t>(bytes[0]) == 0x01U) ? host_order::big : host_order::little;
  REQUIRE(observed == native_order);
}

TEST_CASE("unit: the public conversions follow this host's order", "[unit]")
{
  using couchbase::core::utils::host_to_network;
  using couchbase::core::utils::network_to_host;

  STATIC_REQUIRE(host_to_network(std::uint16_t{ 0x0102U }) ==
                 convert<native_order>(std::uint16_t{ 0x0102U }));
  STATIC_REQUIRE(host_to_network(std::uint32_t{ 0x01020304U }) ==
                 convert<native_order>(std::uint32_t{ 0x01020304U }));
  STATIC_REQUIRE(host_to_network(std::uint64_t{ 0x0102030405060708U }) ==
                 convert<native_order>(std::uint64_t{ 0x0102030405060708U }));

  // The two directions are the same operation, and each undoes the other.
  STATIC_REQUIRE(network_to_host(host_to_network(std::uint16_t{ 0xabcdU })) == 0xabcdU);
  STATIC_REQUIRE(network_to_host(host_to_network(std::uint32_t{ 0xdeadbeefU })) == 0xdeadbeefU);
  STATIC_REQUIRE(network_to_host(host_to_network(std::uint64_t{ 0x0123456789abcdefU })) ==
                 0x0123456789abcdefU);
}

TEST_CASE("unit: reverse_bytes reverses on every host", "[unit]")
{
  using couchbase::core::utils::reverse_bytes;

  // Unlike the conversions above, this one must not depend on host order: its callers read byte
  // orders fixed by something other than this host. Swapping it for host_to_network would keep
  // these passing on a little-endian host and break them on a big-endian one.
  STATIC_REQUIRE(reverse_bytes(std::uint16_t{ 0x0102U }) == 0x0201U);
  STATIC_REQUIRE(reverse_bytes(std::uint32_t{ 0x01020304U }) == 0x04030201U);
  STATIC_REQUIRE(reverse_bytes(std::uint64_t{ 0x0102030405060708U }) == 0x0807060504030201U);

  STATIC_REQUIRE(reverse_bytes(reverse_bytes(std::uint16_t{ 0xabcdU })) == 0xabcdU);
  STATIC_REQUIRE(reverse_bytes(reverse_bytes(std::uint32_t{ 0xdeadbeefU })) == 0xdeadbeefU);
  STATIC_REQUIRE(reverse_bytes(reverse_bytes(std::uint64_t{ 0x0123456789abcdefU })) ==
                 0x0123456789abcdefU);

  // The ${Mutation.CAS} value from the kvengine macro, which is what these callers decode.
  STATIC_REQUIRE(reverse_bytes(std::uint64_t{ 0x000058a71dd25c15U }) == 0x155cd21da7580000U);
}
