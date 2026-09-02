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

#include "framework/test_registry.hxx"

#include "core/protocol/client_request.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/cmd_noop.hxx"
#include "core/protocol/magic.hxx"

#include <couchbase/cas.hxx>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
// Read a header field straight out of the encoded payload the way the server does: as
// big-endian bytes, with no reference to this host's byte order. That is the whole point of
// these assertions, so they must not go through the codec's own conversions.
auto
be16(const std::vector<std::byte>& payload, std::size_t offset) -> std::uint16_t
{
  return static_cast<std::uint16_t>((std::to_integer<std::uint32_t>(payload[offset]) << 8U) |
                                    std::to_integer<std::uint32_t>(payload[offset + 1]));
}

auto
be32(const std::vector<std::byte>& payload, std::size_t offset) -> std::uint32_t
{
  std::uint32_t value{};
  for (std::size_t i = 0; i < 4; ++i) {
    value = (value << 8U) | std::to_integer<std::uint32_t>(payload[offset + i]);
  }
  return value;
}

auto
be64(const std::vector<std::byte>& payload, std::size_t offset) -> std::uint64_t
{
  std::uint64_t value{};
  for (std::size_t i = 0; i < 8; ++i) {
    value = (value << 8U) | std::to_integer<std::uint64_t>(payload[offset + i]);
  }
  return value;
}

void
the_request_header_is_encoded_in_network_byte_order([[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::client_request;
  using couchbase::core::protocol::mcbp_noop_request_body;

  // Every multi-byte header field is written by converting a native integer and then copying it
  // into the buffer. Those two steps have to compose to big-endian bytes; an unconditional
  // reversal composes to big-endian only on a little-endian host.
  //
  // Each value below has eight distinct bytes, or as many as it has, so that any reordering is
  // visible rather than being masked by a symmetric value.
  client_request<mcbp_noop_request_body> request;
  request.partition(0x0102);
  request.opaque(0x01020304);
  request.cas(couchbase::cas{ 0x0102030405060708 });

  auto payload = request.data();
  assert_eq(payload.size(), std::size_t{ 24 }, "a body-less request is a bare header");

  assert_eq(
    payload[0], static_cast<std::byte>(couchbase::core::protocol::magic::client_request), "magic");
  assert_eq(payload[1], static_cast<std::byte>(mcbp_noop_request_body::opcode), "opcode");
  assert_eq(be16(payload, 2), std::uint16_t{ 0 }, "key length");
  assert_eq(payload[4], std::byte{ 0 }, "extras length");
  assert_eq(payload[5], std::byte{ 0 }, "data type");
  assert_eq(be16(payload, 6), std::uint16_t{ 0x0102 }, "vbucket");
  assert_eq(be32(payload, 8), std::uint32_t{ 0 }, "total body length");
  assert_eq(be32(payload, 12), std::uint32_t{ 0x01020304 }, "opaque");
  assert_eq(be64(payload, 16), std::uint64_t{ 0x0102030405060708 }, "CAS");
}

void
a_hello_request_encodes_its_key_length_in_network_byte_order([[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::client_request;
  using couchbase::core::protocol::hello_request_body;

  // A 188-byte user agent becomes a 188-byte key. With bytes 2 and 3 written in the wrong order
  // the server reads a key length of 0xbc00, rejects the frame and closes the socket, so
  // bootstrap fails before any operation is attempted.
  client_request<hello_request_body> request;
  request.opaque(1);
  request.body().user_agent(std::string(188, 'a'));

  auto payload = request.data();
  assert_true(payload.size() > 24, "a HELLO request carries a body");

  assert_eq(be16(payload, 2), std::uint16_t{ 188 }, "key length");
  assert_eq(payload[2], std::byte{ 0x00 }, "the high byte of the key length leads");
  assert_eq(payload[3], std::byte{ 0xbc }, "the low byte of the key length follows");

  assert_eq(std::size_t{ be32(payload, 8) }, payload.size() - 24, "total body length");
  assert_eq(be32(payload, 12), std::uint32_t{ 1 }, "opaque");
  assert_eq(payload[12], std::byte{ 0x00 }, "the most significant opaque byte leads");
  assert_eq(payload[15], std::byte{ 0x01 }, "the least significant opaque byte trails");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(the_request_header_is_encoded_in_network_byte_order) },
      { CASE(a_hello_request_encodes_its_key_length_in_network_byte_order) },
    },
  };
}

} // namespace couchbase::test
