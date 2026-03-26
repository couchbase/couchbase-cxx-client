/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-Present Couchbase, Inc.
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

#include "core/mcbp/codec.hxx"
#include "core/mcbp/packet.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/protocol/magic.hxx"

#include <couchbase/error_codes.hxx>

namespace
{
using couchbase::core::mcbp::codec;
using couchbase::core::mcbp::packet;
using couchbase::core::protocol::client_opcode;
using couchbase::core::protocol::hello_feature;
using couchbase::core::protocol::magic;

auto
make_request(client_opcode cmd, std::uint32_t collection_id = 0) -> packet
{
  packet pkt{};
  pkt.magic_ = magic::client_request;
  pkt.command_ = cmd;
  pkt.collection_id_ = collection_id;
  return pkt;
}
} // namespace

// ---------------------------------------------------------------------------
// encode_packet – collection-ID branch logic
// ---------------------------------------------------------------------------

TEST_CASE("unit: codec encode collection-ID with collections disabled", "[unit]")
{
  codec c{ {} };

  SECTION("collection-supporting command with non-zero collection ID succeeds")
  {
    // When collections are not negotiated the collection_id field is silently
    // ignored — the codec must not reject the packet.
    auto result = c.encode_packet(make_request(client_opcode::get, 0x1234));
    REQUIRE(result.has_value());
  }

  SECTION("non-collection command with non-zero collection ID succeeds")
  {
    // The guard only fires when collections are enabled.
    auto result = c.encode_packet(make_request(client_opcode::noop, 0x42));
    REQUIRE(result.has_value());
  }
}

TEST_CASE("unit: codec encode collection-ID with collections enabled", "[unit]")
{
  codec c{ { hello_feature::collections } };

  SECTION("collection-supporting command with non-zero collection ID succeeds")
  {
    // Regression test for CXXCBC-789: the standalone 'if (collection_id_ > 0)'
    // fired even after the LEB128 encoding path, returning invalid_argument for
    // perfectly valid packets.  With the 'else if' fix this must succeed.
    auto result = c.encode_packet(make_request(client_opcode::get, 0x08));
    REQUIRE(result.has_value());
  }

  SECTION("collection-supporting command with zero collection ID succeeds")
  {
    auto result = c.encode_packet(make_request(client_opcode::get, 0x00));
    REQUIRE(result.has_value());
  }

  SECTION("non-collection command with zero collection ID succeeds")
  {
    auto result = c.encode_packet(make_request(client_opcode::noop, 0x00));
    REQUIRE(result.has_value());
  }

  SECTION("non-collection command with non-zero collection ID returns invalid_argument")
  {
    auto result = c.encode_packet(make_request(client_opcode::noop, 0x08));
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == couchbase::errc::common::invalid_argument);
  }

  SECTION("observe command returns unsupported_operation regardless of collection ID")
  {
    auto result = c.encode_packet(make_request(client_opcode::observe, 0x08));
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == couchbase::errc::common::unsupported_operation);
  }

  SECTION("get_random_key encodes collection ID in extras, not in key")
  {
    auto result = c.encode_packet(make_request(client_opcode::get_random_key, 0x0000'00ff));
    REQUIRE(result.has_value());
    const auto& wire = result.value();
    // ext_len lives at byte 4 of the 24-byte header
    auto ext_len = static_cast<std::uint8_t>(wire[4]);
    CHECK(ext_len == sizeof(std::uint32_t));
    // key_len at bytes 2-3 must be 0 — no key was set
    auto key_len = (static_cast<std::uint16_t>(wire[2]) << 8) | static_cast<std::uint16_t>(wire[3]);
    CHECK(key_len == 0);
  }
}

// ---------------------------------------------------------------------------
// decode_packet – early-exit error paths
// ---------------------------------------------------------------------------

TEST_CASE("unit: codec decode empty input returns end_of_stream", "[unit]")
{
  codec c{ {} };
  gsl::span<std::byte> empty{};
  auto [pkt, consumed, ec] = c.decode_packet(empty);
  CHECK(ec == couchbase::errc::network::end_of_stream);
}

TEST_CASE("unit: codec decode truncated header returns need_more_data", "[unit]")
{
  codec c{ {} };
  std::vector<std::byte> buf(10, std::byte{ 0 });
  auto [pkt, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ buf });
  CHECK(ec == couchbase::errc::network::need_more_data);
}

TEST_CASE("unit: codec decode header with declared body returns need_more_data when body absent",
          "[unit]")
{
  codec c{ {} };
  // Build a valid 24-byte header that claims a 10-byte body, but supply no body bytes.
  std::vector<std::byte> buf(24, std::byte{ 0 });
  buf[0] = static_cast<std::byte>(magic::client_request);
  buf[1] = static_cast<std::byte>(client_opcode::get);
  // body_len at offset 8 (4 bytes big-endian) = 10
  buf[11] = std::byte{ 10 };
  auto [pkt, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ buf });
  CHECK(ec == couchbase::errc::network::need_more_data);
}

// ---------------------------------------------------------------------------
// encode → decode round-trip
// ---------------------------------------------------------------------------

TEST_CASE("unit: codec encode/decode round-trip for get request with collection ID", "[unit]")
{
  codec c{ { hello_feature::collections } };

  packet orig = make_request(client_opcode::get, 0x08);
  orig.key_ = { std::byte{ 'd' }, std::byte{ 'o' }, std::byte{ 'c' } };
  orig.vbucket_ = 7;

  auto encoded = c.encode_packet(orig);
  REQUIRE(encoded.has_value());

  auto [decoded, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ encoded.value() });
  REQUIRE_SUCCESS(ec);
  CHECK(consumed == encoded.value().size());
  CHECK(decoded.command_ == client_opcode::get);
  CHECK(decoded.collection_id_ == 0x08);
  CHECK(decoded.vbucket_ == 7);
  CHECK(decoded.key_ == orig.key_);
}
