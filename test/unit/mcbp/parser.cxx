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

#include "framework/test_registry.hxx"

#include "core/io/mcbp_parser.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::binary_header;
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
  auto datatype(std::uint8_t v) -> frame_builder& // byte 5
  {
    return set(5, v);
  }
  auto bodylen(std::uint32_t v) -> frame_builder& // bytes 8-11
  {
    return set(8, static_cast<std::uint8_t>(v >> 24U))
      .set(9, static_cast<std::uint8_t>(v >> 16U))
      .set(10, static_cast<std::uint8_t>(v >> 8U))
      .set(11, static_cast<std::uint8_t>(v));
  }
  auto opaque(std::uint32_t v) -> frame_builder& // bytes 12-15 (copied verbatim)
  {
    std::memcpy(bytes.data() + 12, &v, sizeof(v));
    return *this;
  }
};

void
a_frame_whose_prefix_exceeds_the_body_is_rejected([[maybe_unused]] context& ctx)
{
  // A response whose extras/key lengths claim far more bytes than the body
  // actually contains. extlen and keylen are separate header fields from
  // bodylen, so the parser must not assume the prefix fits inside the body.
  //
  // magic=client_response, keylen=0xFFFF, extlen=0xFF, bodylen=0
  //   => prefix_size = 0xFF + 0xFFFF = 65790, against a 24-byte buffer.
  //
  // Accepting it drives an out-of-bounds read (and, on the snappy path, an
  // unsigned underflow of "body_size - prefix_size"); under AddressSanitizer
  // it aborts with a heap-buffer-overflow read of 65790 bytes.
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
  assert_eq(parser.next(msg), mcbp_parser::result::failure, "a prefix larger than the buffer");
}

void
a_prefix_larger_than_a_non_empty_body_is_rejected([[maybe_unused]] context& ctx)
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
  assert_eq(parser.next(msg), mcbp_parser::result::failure, "a prefix larger than the body");
}

void
a_well_formed_frame_is_accepted([[maybe_unused]] context& ctx)
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
  assert_eq(parser.next(msg), mcbp_parser::result::ok, "a prefix that exactly fills the body");
  assert_eq(msg.body.size(), std::size_t{ 7 }, "the body the header declared");
}

void
alt_sizes_splits_the_two_one_byte_header_fields([[maybe_unused]] context& ctx)
{
  // Bytes 2 and 3 of a flexible-framing header are two independent 1-byte fields, not one
  // 16-bit value. The two must be distinct here for the test to mean anything: with equal
  // values every wrong way of splitting them still passes.
  //
  // Both readers of these fields consume them only through a sum, so swapping them is not
  // observable through either one. Asserting on the split directly is what pins it, and what
  // would catch a return to shifting a native integer apart, which lands each byte in the wrong
  // variable on one host order or the other.
  auto frame = frame_builder{}
                 .magic_byte(magic::alt_client_response)
                 .opcode(0x00)
                 .set(2, 3) // framing extras size
                 .set(3, 5) // key size
                 .extlen(2)
                 .bodylen(10)
                 .bytes;

  binary_header header{};
  std::memcpy(&header, frame.data(), frame.size());

  const auto sizes = header.alt_sizes();
  assert_eq(sizes.framing_extras, std::uint8_t{ 3 }, "byte 2 is the framing extras size");
  assert_eq(sizes.key, std::uint8_t{ 5 }, "byte 3 is the key size");
}

void
pipelined_frames_in_a_single_chunk_and_straddling_chunks_are_parsed_in_order(
  [[maybe_unused]] context& ctx)
{
  std::vector<std::byte> wire;
  constexpr std::uint32_t frame_count = 64;
  for (std::uint32_t i = 0; i < frame_count; ++i) {
    const std::uint32_t body_len = 8U + (i % 17U);
    auto header = frame_builder{}
                    .magic_byte(magic::client_response)
                    .opcode(0x00)
                    .keylen(0)
                    .extlen(4)
                    .bodylen(body_len)
                    .opaque(i + 100U)
                    .bytes;
    wire.insert(wire.end(), header.begin(), header.end());
    wire.insert(wire.end(), body_len, static_cast<std::byte>(i & 0xffU));
  }

  // Exercise both compaction branches in mcbp_parser::next():
  // - (wire.size() / 2) + 7 == 1281 lands inside frame 32's 24-byte header [1265, 1289),
  //   triggering partial-header compaction when read_offset_ > 0.
  // - 1290 (1289 + 1) includes frame 32's full 24-byte header plus 1 of its 23 body bytes,
  //   triggering partial-body compaction when read_offset_ > 0.
  for (const std::size_t split : { (wire.size() / 2) + 7, std::size_t{ 1290 } }) {
    mcbp_parser parser;
    parser.feed(wire.begin(), wire.begin() + static_cast<std::ptrdiff_t>(split));

    std::uint32_t parsed = 0;
    mcbp_message msg{};
    while (true) {
      const auto res = parser.next(msg);
      if (res == mcbp_parser::result::need_data) {
        break;
      }
      assert_eq(res, mcbp_parser::result::ok, "complete frame in first chunk parses");
      assert_eq(msg.header.opaque, parsed + 100U, "opaque matches frame index");
      assert_eq(msg.body.size(), std::size_t{ 8U + (parsed % 17U) }, "body length matches");
      assert_eq(msg.body.front(), static_cast<std::byte>(parsed & 0xffU), "body payload matches");
      ++parsed;
    }
    assert_true(parsed > 0 && parsed < frame_count, "the split falls inside the frame stream");

    parser.feed(wire.begin() + static_cast<std::ptrdiff_t>(split), wire.end());
    while (parsed < frame_count) {
      const auto res = parser.next(msg);
      assert_eq(res, mcbp_parser::result::ok, "remaining frames parse after second chunk");
      assert_eq(msg.header.opaque, parsed + 100U, "opaque matches frame index");
      assert_eq(msg.body.size(), std::size_t{ 8U + (parsed % 17U) }, "body length matches");
      assert_eq(msg.body.back(), static_cast<std::byte>(parsed & 0xffU), "body payload matches");
      ++parsed;
    }
    assert_eq(parser.next(msg), mcbp_parser::result::need_data, "buffer is drained");
  }
}

void
snappy_compressed_frame_is_decompressed_in_place_preserving_prefix([[maybe_unused]] context& ctx)
{
  // Raw Snappy stream for 20 bytes ("ABCDABCDABCDABCDABCD"):
  // 0x14 (uncompressed len = 20), 0x0c (literal 4 bytes), 'A','B','C','D',
  // 0x3e (copy-2 length 16: ((16 - 1) << 2) | 0x02), 0x04, 0x00 (LE16 offset 4).
  const std::array<std::uint8_t, 9> snappy_payload{
    0x14, 0x0c, 'A', 'B', 'C', 'D', 0x3e, 0x04, 0x00,
  };
  auto header = frame_builder{}
                  .magic_byte(magic::client_response)
                  .opcode(0x00)
                  .keylen(0)
                  .extlen(4)
                  .datatype(0x03) // JSON (0x01) | Snappy (0x02)
                  .bodylen(4 + static_cast<std::uint32_t>(snappy_payload.size()))
                  .bytes;

  std::vector<std::byte> wire(header.begin(), header.end());
  // 4-byte extras prefix
  wire.insert(wire.end(), 4, std::byte{ 0x7f });
  for (const auto b : snappy_payload) {
    wire.push_back(static_cast<std::byte>(b));
  }

  mcbp_parser parser;
  parser.feed(wire.begin(), wire.end());

  mcbp_message msg{};
  assert_eq(parser.next(msg), mcbp_parser::result::ok, "snappy frame parses and decompresses");
  assert_eq(couchbase::core::utils::network_to_host(msg.header.bodylen),
            std::uint32_t{ 4 + 20 },
            "header bodylen is patched with uncompressed size");
  assert_eq(msg.body.size(), std::size_t{ 4 + 20 }, "4-byte extras + 20-byte uncompressed payload");
  for (std::size_t i = 0; i < 4; ++i) {
    assert_eq(msg.body[i], std::byte{ 0x7f }, "extras prefix is preserved");
  }
  const std::array<char, 4> pattern{ 'A', 'B', 'C', 'D' };
  for (std::size_t i = 0; i < 20; ++i) {
    assert_eq(msg.body[4 + i], static_cast<std::byte>(pattern[i % 4]), "decompressed byte matches");
  }
}

void
corrupted_snappy_frame_falls_back_to_raw_body([[maybe_unused]] context& ctx)
{
  auto header = frame_builder{}
                  .magic_byte(magic::client_response)
                  .opcode(0x00)
                  .keylen(0)
                  .extlen(2)
                  .datatype(0x02) // Snappy
                  .bodylen(6)
                  .bytes;

  std::vector<std::byte> wire(header.begin(), header.end());
  wire.insert(wire.end(), 2, std::byte{ 0xaa }); // 2-byte extras
  // Valid Snappy uncompressed-length varint (0x40 == 64 bytes) so snappy::GetUncompressedLength
  // succeeds and resizes msg.body to 2 + 64 = 66 bytes, followed by a truncated literal tag
  // (0x1c == 8-byte literal, with only 2 bytes remaining) so snappy::RawUncompress fails and
  // exercises msg.body.clear() before copying the 6 raw frame bytes.
  wire.push_back(std::byte{ 0x40 });
  wire.push_back(std::byte{ 0x1c });
  wire.push_back(std::byte{ 0xde });
  wire.push_back(std::byte{ 0xad });

  mcbp_parser parser;
  parser.feed(wire.begin(), wire.end());

  mcbp_message msg{};
  assert_eq(parser.next(msg),
            mcbp_parser::result::ok,
            "corrupted snappy payload falls back to raw frame bytes");
  assert_eq(couchbase::core::utils::network_to_host(msg.header.bodylen),
            std::uint32_t{ 6 },
            "header bodylen remains raw frame length on fallback");
  assert_eq(msg.body.size(),
            std::size_t{ 6 },
            "resized 66-byte buffer is cleared and raw 6-byte body is preserved");
  assert_eq(msg.body[0], std::byte{ 0xaa }, "extras preserved on fallback");
  assert_eq(msg.body[1], std::byte{ 0xaa }, "extras preserved on fallback");
  assert_eq(msg.body[2], std::byte{ 0x40 }, "raw varint byte preserved on fallback");
  assert_eq(msg.body[3], std::byte{ 0x1c }, "raw tag byte preserved on fallback");
  assert_eq(msg.body[4], std::byte{ 0xde }, "raw payload byte preserved on fallback");
  assert_eq(msg.body[5], std::byte{ 0xad }, "raw payload byte preserved on fallback");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_frame_whose_prefix_exceeds_the_body_is_rejected) },
      { CASE(a_prefix_larger_than_a_non_empty_body_is_rejected) },
      { CASE(a_well_formed_frame_is_accepted) },
      { CASE(alt_sizes_splits_the_two_one_byte_header_fields) },
      { CASE(pipelined_frames_in_a_single_chunk_and_straddling_chunks_are_parsed_in_order) },
      { CASE(snappy_compressed_frame_is_decompressed_in_place_preserving_prefix) },
      { CASE(corrupted_snappy_frame_falls_back_to_raw_body) },
    },
  };
}

} // namespace couchbase::test
