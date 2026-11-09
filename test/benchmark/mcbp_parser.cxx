/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

#include "bench_registry.hxx"

#include "core/io/mcbp_message.hxx"
#include "core/io/mcbp_parser.hxx"

#include <snappy.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace couchbase::test::bench
{
namespace
{
constexpr std::size_t frame_count = 1000;
constexpr std::uint8_t magic_client_response = 0x81;
constexpr std::uint8_t datatype_snappy = 0x02;

void
put_u16(std::vector<std::byte>& out, std::uint16_t v)
{
  out.push_back(static_cast<std::byte>(v >> 8));
  out.push_back(static_cast<std::byte>(v));
}

void
put_u32(std::vector<std::byte>& out, std::uint32_t v)
{
  put_u16(out, static_cast<std::uint16_t>(v >> 16));
  put_u16(out, static_cast<std::uint16_t>(v));
}

// A GET response: 24-byte header, 4 bytes of flags as extras, then the value.
void
append_get_response(std::vector<std::byte>& out,
                    std::uint32_t opaque,
                    std::uint8_t datatype,
                    const std::string& value)
{
  constexpr std::uint8_t extlen = 4;
  out.push_back(static_cast<std::byte>(magic_client_response));
  out.push_back(std::byte{ 0x00 }); // opcode: GET
  put_u16(out, 0);                  // key length
  out.push_back(static_cast<std::byte>(extlen));
  out.push_back(static_cast<std::byte>(datatype));
  put_u16(out, 0); // status: success
  put_u32(out, static_cast<std::uint32_t>(extlen + value.size()));
  put_u32(out, opaque);
  put_u32(out, 0); // cas, high
  put_u32(out, opaque + 1);
  put_u32(out, 0); // flags
  for (const char c : value) {
    out.push_back(static_cast<std::byte>(c));
  }
}

// Pseudo-random field values: compressible as JSON text is, not as a repeated phrase is.
auto
make_value(std::size_t i, std::mt19937& rng) -> std::string
{
  constexpr std::string_view alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string value = R"({"id":)" + std::to_string(i);
  for (std::size_t field = 0; field < 20; ++field) {
    value += R"(,"f)" + std::to_string(field) + R"(":")";
    for (std::size_t n = 0; n < 40; ++n) {
      value += alphabet[rng() % alphabet.size()];
    }
    value += '"';
  }
  value += '}';
  return value;
}

auto
make_frames(bool compress, std::string& first_value) -> std::vector<std::byte>
{
  // Each call seeds its own engine, so both variants decode the same values.
  std::mt19937 rng{ 20261105 };
  std::vector<std::byte> frames;
  for (std::size_t i = 0; i < frame_count; ++i) {
    const auto value = make_value(i, rng);
    if (i == 0) {
      first_value = value;
    }
    if (compress) {
      std::string compressed;
      snappy::Compress(value.data(), value.size(), &compressed);
      append_get_response(frames, static_cast<std::uint32_t>(i), datatype_snappy, compressed);
    } else {
      append_get_response(frames, static_cast<std::uint32_t>(i), 0, value);
    }
  }
  return frames;
}

// next() falls back to the raw bytes when Snappy fails, so the decoded body is compared with the
// original value before anything is measured.
void
require_decoded_value(const std::vector<std::byte>& frames, const std::string& value)
{
  core::io::mcbp_parser parser;
  core::io::mcbp_message msg;
  parser.feed(frames.begin(), frames.end());
  constexpr std::size_t extras = 4;
  if (parser.next(msg) != core::io::mcbp_parser::result::ok ||
      msg.body.size() != extras + value.size() ||
      std::memcmp(msg.body.data() + extras, value.data(), value.size()) != 0) {
    throw std::runtime_error("the Snappy frame did not decode to the original value");
  }
}

// All frames are in one buffer and next() drains them one per iteration. The buffer is refilled
// when it runs out, so the cost of consuming from a long buffer is part of what is measured. A
// result other than ok, after the refill too, throws and fails the case.
void
drain(state& st, std::string_view variant, const std::vector<std::byte>& frames)
{
  core::io::mcbp_parser parser;
  core::io::mcbp_message msg;
  parser.feed(frames.begin(), frames.end());
  st.batched(variant, 1, [&] {
    auto result = parser.next(msg);
    if (result == core::io::mcbp_parser::result::need_data) {
      parser.feed(frames.begin(), frames.end());
      result = parser.next(msg);
    }
    if (result != core::io::mcbp_parser::result::ok) {
      throw std::runtime_error("the parser did not return a frame");
    }
    do_not_optimize(msg);
  });
}

void
mcbp_parser_next(state& st)
{
  std::string value;
  drain(st, "plain", make_frames(false, value));
  const auto compressed = make_frames(true, value);
  require_decoded_value(compressed, value);
  drain(st, "snappy", compressed);
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name,
           { { CASE(mcbp_parser_next),
               {},
               "Decodes one GET response frame per iteration with mcbp_parser::next, plain and "
               "Snappy-compressed." } } };
}
} // namespace couchbase::test::bench
