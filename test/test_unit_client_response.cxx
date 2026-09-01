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

#include "core/io/mcbp_message.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_cluster_map_change_notification.hxx"
#include "core/protocol/cmd_noop.hxx"
#include "core/protocol/magic.hxx"
#include "core/protocol/server_request.hxx"
#include "core/protocol/status.hxx"

#include <catch2/catch_approx.hpp>

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace
{
// A 24-byte response header written the way it arrives on the wire: multi-byte fields
// big-endian, by shifting, with no reference to this host's byte order.
auto
wire_header(std::uint16_t status, std::uint32_t opaque, std::uint64_t cas)
  -> std::array<std::byte, 24>
{
  std::array<std::byte, 24> bytes{};
  bytes[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  bytes[1] = static_cast<std::byte>(couchbase::core::protocol::client_opcode::noop);
  bytes[6] = static_cast<std::byte>(status >> 8U);
  bytes[7] = static_cast<std::byte>(status);
  for (std::size_t i = 0; i < 4; ++i) {
    bytes[12 + i] = static_cast<std::byte>(opaque >> ((3U - i) * 8U));
  }
  for (std::size_t i = 0; i < 8; ++i) {
    bytes[16 + i] = static_cast<std::byte>(cas >> ((7U - i) * 8U));
  }
  return bytes;
}

// A 24-byte server request header, written the same way. The server request and client response
// headers share this layout, so the two decode paths have the same contract to meet.
auto
server_request_wire_header(std::uint32_t opaque, std::uint64_t cas, std::uint16_t key_size)
  -> std::array<std::byte, 24>
{
  std::array<std::byte, 24> bytes{};
  bytes[0] = static_cast<std::byte>(couchbase::core::protocol::magic::server_request);
  bytes[1] = static_cast<std::byte>(
    couchbase::core::protocol::server_opcode::cluster_map_change_notification);
  bytes[2] = static_cast<std::byte>(key_size >> 8U);
  bytes[3] = static_cast<std::byte>(key_size);
  // No extras, and a body that is the key alone: parse() then reads the bucket name and stops
  // short of the config text, so no cluster map has to be built to reach the header assertions.
  for (std::size_t i = 0; i < 4; ++i) {
    bytes[8 + i] = static_cast<std::byte>(std::uint32_t{ key_size } >> ((3U - i) * 8U));
  }
  for (std::size_t i = 0; i < 4; ++i) {
    bytes[12 + i] = static_cast<std::byte>(opaque >> ((3U - i) * 8U));
  }
  for (std::size_t i = 0; i < 8; ++i) {
    bytes[16 + i] = static_cast<std::byte>(cas >> ((7U - i) * 8U));
  }
  return bytes;
}

// An alt ("flexible framing") response header, written the same way. Bytes 2 and 3 are two
// independent 1-byte fields here: the length of the framing extras region and the key length.
auto
alt_wire_header(std::uint8_t framing_extras_size, std::uint8_t key_size)
  -> std::array<std::byte, 24>
{
  std::array<std::byte, 24> bytes{};
  bytes[0] = static_cast<std::byte>(couchbase::core::protocol::magic::alt_client_response);
  bytes[1] = static_cast<std::byte>(couchbase::core::protocol::client_opcode::get);
  bytes[2] = static_cast<std::byte>(framing_extras_size);
  bytes[3] = static_cast<std::byte>(key_size);
  const auto body_size =
    static_cast<std::uint32_t>(framing_extras_size) + static_cast<std::uint32_t>(key_size);
  for (std::size_t i = 0; i < 4; ++i) {
    bytes[8 + i] = static_cast<std::byte>(body_size >> ((3U - i) * 8U));
  }
  return bytes;
}

auto
message_with(const std::array<std::byte, 24>& header, std::vector<std::byte> body)
  -> couchbase::core::io::mcbp_message
{
  couchbase::core::io::mcbp_message msg;
  std::memcpy(&msg.header, header.data(), header.size());
  msg.body = std::move(body);
  return msg;
}

// One framing extra the parser does not recognise: id 1, length 2. It exists to push the server
// duration frame further into the region, and is skipped by its length like any unknown id.
auto
filler_frame() -> std::vector<std::byte>
{
  return { std::byte{ 0x12 }, std::byte{ 0 }, std::byte{ 0 } };
}

// The server duration frame: id 0, length 2, then the encoded value big-endian.
auto
server_duration_frame(std::uint16_t encoded) -> std::vector<std::byte>
{
  return { std::byte{ 0x02 },
           static_cast<std::byte>(encoded >> 8U),
           static_cast<std::byte>(encoded) };
}
} // namespace

TEST_CASE("unit: client_response decodes its header from network byte order", "[unit]")
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::mcbp_noop_response_body;

  // The decode side is the mirror of the encode side: bytes are copied into a native integer and
  // then converted. On a big-endian host that pairing used to reverse an already-correct value,
  // so the application saw every CAS byte-reversed.
  //
  // A CAS matters more than the rest here because the corruption is invisible on the wire: the
  // same double transform on the way back out restores the original bytes, so an optimistic lock
  // keeps working while every CAS the caller observes is wrong.
  constexpr std::uint64_t cas{ 0x0102030405060708 };
  constexpr std::uint32_t opaque{ 0x01020304 };
  const auto header = wire_header(0x0002, opaque, cas);

  couchbase::core::io::mcbp_message msg;
  std::memcpy(&msg.header, header.data(), header.size());

  const client_response<mcbp_noop_response_body> response{ std::move(msg) };

  CHECK(response.cas().value() == cas);
  CHECK(response.opaque() == opaque);
  CHECK(response.status() == static_cast<key_value_status_code>(0x0002));
}

TEST_CASE("unit: server_request decodes its header from network byte order", "[unit]")
{
  using couchbase::core::protocol::cluster_map_change_notification_request_body;
  using couchbase::core::protocol::server_request;

  // server_request converted its body length and left opaque and CAS as raw wire bytes, the one
  // remaining site in core/ of the shape this series removes elsewhere: a memcpy of a native
  // integer out of a wire buffer with no conversion after it.
  //
  // Nothing observes the reversal today, because the only body this class is instantiated with
  // is the one below and no caller reads either accessor. The assertions here are what keeps the
  // next caller from inheriting it.
  constexpr std::uint64_t cas{ 0x0102030405060708 };
  constexpr std::uint32_t opaque{ 0x01020304 };
  const std::string bucket{ "default" };

  auto header = server_request_wire_header(opaque, cas, static_cast<std::uint16_t>(bucket.size()));
  std::vector<std::byte> body{};
  for (const auto c : bucket) {
    body.push_back(static_cast<std::byte>(c));
  }

  auto msg = message_with(header, std::move(body));
  server_request<cluster_map_change_notification_request_body> request{ std::move(msg) };

  CHECK(request.opaque() == opaque);
  CHECK(request.cas().value() == cas);
  CHECK(request.body().bucket() == bucket);
}

TEST_CASE("unit: parse_server_duration_us reads the framing extras length as a byte", "[unit]")
{
  using couchbase::core::protocol::parse_server_duration_us;

  constexpr std::uint16_t encoded{ 0x0100 };
  const auto expected = std::pow(encoded, 1.74) / 2;

  SECTION("a key length whose low nibble is zero does not hide the framing extras")
  {
    // Byte 2 is 3 and byte 3 is 16. Reading the two as one native integer and masking picks byte
    // 2 on a little-endian host and byte 3 on a big-endian one, where 16 & 0xf is 0: the duration
    // was dropped from every alt response carrying a 16-byte key. Little-endian cannot observe
    // that, so this pins the contract rather than reproducing the failure here.
    auto body = server_duration_frame(encoded);
    body.resize(3 + 16);
    // The call is hoisted out of CHECK deliberately. Under clang, Catch2 expands the macro's
    // argument a second time inside CATCH_INTERNAL_IGNORE_BUT_WARN, which feeds it to
    // __builtin_constant_p. That is never evaluated at run time, but the static analyzer reads
    // it as a second move of `body` and reports a use-after-move.
    const auto parsed =
      parse_server_duration_us(message_with(alt_wire_header(3, 16), std::move(body)));
    CHECK(parsed == Catch::Approx(expected));
  }

  SECTION("a region longer than fifteen bytes is scanned to its end")
  {
    // Five frames, then the duration at offset 15. The region is 18 bytes, whose low nibble is 2,
    // so masking the length to four bits stopped the scan inside the first frame.
    std::vector<std::byte> body;
    for (int i = 0; i < 5; ++i) {
      const auto frame = filler_frame();
      body.insert(body.end(), frame.begin(), frame.end());
    }
    const auto duration = server_duration_frame(encoded);
    body.insert(body.end(), duration.begin(), duration.end());
    REQUIRE(body.size() == 18);
    const auto parsed =
      parse_server_duration_us(message_with(alt_wire_header(18, 0), std::move(body)));
    CHECK(parsed == Catch::Approx(expected));
  }

  SECTION("a region the body cannot hold yields no duration")
  {
    // Nothing bounds the scan against the body, so a length the body cannot hold has to be
    // rejected before the loop starts.
    CHECK(parse_server_duration_us(
            message_with(alt_wire_header(200, 0), server_duration_frame(encoded))) == 0);
  }
}
