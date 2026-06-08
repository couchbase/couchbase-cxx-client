/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include "core/io/mcbp_parser.hxx"
#include "core/protocol/magic.hxx"

#include <array>
#include <vector>

namespace
{
using couchbase::core::io::mcbp_message;
using couchbase::core::io::mcbp_parser;
using couchbase::core::protocol::magic;

// A 24-byte KV (memcached binary protocol) response header. Multi-byte fields
// are written big-endian (network order), exactly as they arrive on the wire.
struct frame_builder {
  std::array<std::byte, 24> bytes{};

  auto set(std::size_t i, std::uint8_t v) -> frame_builder&
  {
    bytes.at(i) = std::byte{ v };
    return *this;
  }
  auto magic_byte(magic m) -> frame_builder&
  {
    return set(0, static_cast<std::uint8_t>(m));
  }
  auto opcode(std::uint8_t v) -> frame_builder&
  {
    return set(1, v);
  }
  auto keylen(std::uint16_t v) -> frame_builder& // bytes 2-3
  {
    return set(2, static_cast<std::uint8_t>(v >> 8U)).set(3, static_cast<std::uint8_t>(v & 0xffU));
  }
  auto extlen(std::uint8_t v) -> frame_builder& // byte 4
  {
    return set(4, v);
  }
  auto bodylen(std::uint32_t v) -> frame_builder& // bytes 8-11
  {
    return set(8, static_cast<std::uint8_t>(v >> 24U))
      .set(9, static_cast<std::uint8_t>(v >> 16U))
      .set(10, static_cast<std::uint8_t>(v >> 8U))
      .set(11, static_cast<std::uint8_t>(v));
  }
};
} // namespace

TEST_CASE("unit: mcbp_parser rejects frame whose prefix exceeds the body", "[unit]")
{
  // A response whose extras/key lengths claim far more bytes than the body
  // actually contains. extlen and keylen are separate header fields from
  // bodylen, so the parser must not assume the prefix fits inside the body.
  //
  // magic=client_response, keylen=0xFFFF, extlen=0xFF, bodylen=0
  //   => prefix_size = 0xFF + 0xFFFF = 65790, against a 24-byte buffer.
  //
  // Before the fix this drove an out-of-bounds read (and, on the snappy path,
  // an unsigned underflow of "body_size - prefix_size"); under AddressSanitizer
  // it aborts with a heap-buffer-overflow read of 65790 bytes. The parser must
  // instead reject the frame.
  auto frame = frame_builder{}
                 .magic_byte(magic::client_response)
                 .opcode(0x00)
                 .keylen(0xFFFF)
                 .extlen(0xFF)
                 .bodylen(0)
                 .bytes;

  mcbp_parser parser;
  parser.feed(frame.begin(), frame.end());

  mcbp_message msg;
  CHECK(parser.next(msg) == mcbp_parser::result::failure);
}

TEST_CASE("unit: mcbp_parser rejects prefix overflow with a non-empty body", "[unit]")
{
  // body_size > 0 path: bodylen=4 (and the 4 body bytes supplied), but
  // extlen=0xFF claims a 255-byte prefix => prefix_size (255) > body_size (4).
  auto header = frame_builder{}
                  .magic_byte(magic::client_response)
                  .opcode(0x00)
                  .keylen(0x0000)
                  .extlen(0xFF)
                  .bodylen(4)
                  .bytes;

  std::vector<std::byte> wire(header.begin(), header.end());
  wire.insert(wire.end(), 4, std::byte{ 0xAB }); // 4-byte body

  mcbp_parser parser;
  parser.feed(wire.begin(), wire.end());

  mcbp_message msg;
  CHECK(parser.next(msg) == mcbp_parser::result::failure);
}

TEST_CASE("unit: mcbp_parser accepts a well-formed frame (positive control)", "[unit]")
{
  // extras(4) + key(3) = prefix 7, body 7: prefix_size == body_size, valid.
  auto header = frame_builder{}
                  .magic_byte(magic::client_response)
                  .opcode(0x00)
                  .keylen(0x0003)
                  .extlen(0x04)
                  .bodylen(7)
                  .bytes;

  std::vector<std::byte> wire(header.begin(), header.end());
  wire.insert(wire.end(), 7, std::byte{ 0x00 }); // 4-byte extras + 3-byte key

  mcbp_parser parser;
  parser.feed(wire.begin(), wire.end());

  mcbp_message msg;
  CHECK(parser.next(msg) == mcbp_parser::result::ok);
  CHECK(msg.body.size() == 7);
}
