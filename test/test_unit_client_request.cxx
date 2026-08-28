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

#include "core/protocol/client_request.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/cmd_noop.hxx"
#include "core/protocol/magic.hxx"

#include <couchbase/cas.hxx>

#include <cstdint>
#include <string>
#include <vector>

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
} // namespace

TEST_CASE("unit: client_request encodes its header in network byte order", "[unit]")
{
  using couchbase::core::protocol::client_request;
  using couchbase::core::protocol::mcbp_noop_request_body;

  // Every multi-byte header field is written by converting a native integer and then copying it
  // into the buffer. Those two steps have to compose to big-endian bytes; when the conversion
  // was an unconditional reversal they only did so on a little-endian host.
  //
  // Each value below has eight distinct bytes, or as many as it has, so that any reordering is
  // visible rather than being masked by a symmetric value.
  client_request<mcbp_noop_request_body> request;
  request.partition(0x0102);
  request.opaque(0x01020304);
  request.cas(couchbase::cas{ 0x0102030405060708 });

  auto payload = request.data();
  REQUIRE(payload.size() == 24);

  CHECK(payload[0] == static_cast<std::byte>(couchbase::core::protocol::magic::client_request));
  CHECK(payload[1] == static_cast<std::byte>(mcbp_noop_request_body::opcode));
  CHECK(be16(payload, 2) == 0);        // key length
  CHECK(payload[4] == std::byte{ 0 }); // extras length
  CHECK(payload[5] == std::byte{ 0 }); // data type
  CHECK(be16(payload, 6) == 0x0102);   // vbucket
  CHECK(be32(payload, 8) == 0);        // total body length
  CHECK(be32(payload, 12) == 0x01020304);
  CHECK(be64(payload, 16) == 0x0102030405060708);
}

TEST_CASE("unit: client_request encodes the reported HELLO header", "[unit]")
{
  using couchbase::core::protocol::client_request;
  using couchbase::core::protocol::hello_request_body;

  // The frame from the reported bootstrap failure. A 188-byte user agent becomes a 188-byte
  // key, and the server rejected the frame and closed the socket because bytes 2 and 3 arrived
  // as bc 00 rather than 00 bc.
  client_request<hello_request_body> request;
  request.opaque(1);
  request.body().user_agent(std::string(188, 'a'));

  auto payload = request.data();
  REQUIRE(payload.size() > 24);

  CHECK(be16(payload, 2) == 188);
  CHECK(payload[2] == std::byte{ 0x00 });
  CHECK(payload[3] == std::byte{ 0xbc });

  CHECK(be32(payload, 8) == payload.size() - 24);
  CHECK(be32(payload, 12) == 1);
  CHECK(payload[12] == std::byte{ 0x00 });
  CHECK(payload[15] == std::byte{ 0x01 });
}
