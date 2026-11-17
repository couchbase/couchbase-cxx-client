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
  auto bodylen(std::uint32_t v) -> frame_builder& // bytes 8-11
  {
    return set(8, static_cast<std::uint8_t>(v >> 24U))
      .set(9, static_cast<std::uint8_t>(v >> 16U))
      .set(10, static_cast<std::uint8_t>(v >> 8U))
      .set(11, static_cast<std::uint8_t>(v));
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
a_large_frame_fed_in_socket_reads_is_copied_a_bounded_number_of_times([[maybe_unused]] context& ctx)
{
  // A frame larger than one socket read reaches feed() in 16 KiB pieces while
  // next() reports need_data. Growing the buffer to exactly the size needed
  // on every read copies the whole prefix each time: 510 MiB for a 4 MiB
  // frame. Geometric growth copies a small multiple of the frame.
  constexpr std::size_t read_size = 16384;
  constexpr std::size_t frame_size = 4U << 20U;
  constexpr std::size_t header_size = 24;

  auto header = frame_builder{}
                  .magic_byte(magic::client_response)
                  .opcode(0x00)
                  .bodylen(static_cast<std::uint32_t>(frame_size - header_size))
                  .bytes;
  std::vector<std::byte> wire(header.begin(), header.end());
  wire.resize(frame_size, std::byte{ 0xAB });

  mcbp_parser parser;
  mcbp_message msg;
  std::size_t copied = 0;
  for (std::size_t offset = 0; offset < wire.size(); offset += read_size) {
    const auto capacity = parser.buf.capacity();
    const auto buffered = parser.buf.size();
    const auto first = wire.begin() + static_cast<std::ptrdiff_t>(offset);
    parser.feed(first, first + static_cast<std::ptrdiff_t>(read_size));
    if (parser.buf.capacity() != capacity) {
      copied += buffered;
    }
    if (offset + read_size < wire.size()) {
      assert_eq(parser.next(msg), mcbp_parser::result::need_data, "the frame is incomplete");
    }
  }

  assert_true(copied <= 16U << 20U, "reallocations copy at most 16 MiB of buffered bytes");
  assert_eq(parser.next(msg), mcbp_parser::result::ok, "the complete frame parses");
  assert_eq(
    msg.body.size(), frame_size - header_size, "the body is the whole frame after the header");
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
      { CASE(a_large_frame_fed_in_socket_reads_is_copied_a_bounded_number_of_times) },
    },
  };
}

} // namespace couchbase::test
