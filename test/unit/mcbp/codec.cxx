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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/mcbp/codec.hxx"
#include "core/mcbp/packet.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/protocol/hello_feature.hxx"
#include "core/protocol/magic.hxx"

#include <couchbase/error_codes.hxx>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace couchbase::test
{
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

void
a_collection_command_carrying_a_collection_id_is_encoded_when_collections_are_off(
  [[maybe_unused]] context& ctx)
{
  // When collections are not negotiated the collection_id field is silently ignored -- the codec
  // must not reject the packet.
  codec c{ {} };
  auto result = c.encode_packet(make_request(client_opcode::get, 0x1234));
  assert_true(result.has_value(), "an unnegotiated collection id is ignored, not rejected");
}

void
a_plain_command_carrying_a_collection_id_is_encoded_when_collections_are_off(
  [[maybe_unused]] context& ctx)
{
  // The guard only fires when collections are enabled.
  codec c{ {} };
  auto result = c.encode_packet(make_request(client_opcode::noop, 0x42));
  assert_true(result.has_value(), "an unnegotiated collection id is ignored, not rejected");
}

void
a_collection_command_carrying_a_non_zero_collection_id_is_encoded([[maybe_unused]] context& ctx)
{
  // Regression: CXXCBC-789, where a standalone 'if (collection_id_ > 0)' fired even after the
  // LEB128 encoding path and returned invalid_argument for valid packets.
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::get, 0x08));
  assert_true(result.has_value(), "a collection-aware command may name a collection");
}

void
a_collection_command_carrying_a_zero_collection_id_is_encoded([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::get, 0x00));
  assert_true(result.has_value(), "the default collection is a valid collection");
}

void
a_plain_command_carrying_no_collection_id_is_encoded([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::noop, 0x00));
  assert_true(result.has_value(), "a command outside collections may leave the field unset");
}

void
a_plain_command_carrying_a_collection_id_is_rejected([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::noop, 0x08));
  assert_false(result.has_value(), "a command outside collections may not name one");
  assert_eq(result.error(),
            couchbase::errc::common::invalid_argument,
            "naming a collection on a command that has none is a caller error");
}

void
observe_is_rejected_whatever_collection_id_it_carries([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::observe, 0x08));
  assert_false(result.has_value(), "observe is not encodable");
  assert_eq(result.error(),
            couchbase::errc::common::unsupported_operation,
            "observe is refused as unsupported rather than as a bad collection id");
}

void
get_random_key_carries_its_collection_id_in_extras([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };
  auto result = c.encode_packet(make_request(client_opcode::get_random_key, 0x0000'00ff));
  assert_true(result.has_value(), "get_random_key encodes with a collection id");
  const auto& wire = result.value();
  // ext_len lives at byte 4 of the 24-byte header
  auto ext_len = static_cast<std::uint8_t>(wire[4]);
  assert_eq(
    ext_len, std::uint8_t{ sizeof(std::uint32_t) }, "the collection id occupies the extras");
  // key_len at bytes 2-3 must be 0 — no key was set
  auto key_len = (static_cast<std::uint16_t>(wire[2]) << 8) | static_cast<std::uint16_t>(wire[3]);
  assert_eq(key_len, 0, "the collection id is not also written into the key");
}

void
decoding_an_empty_buffer_reports_end_of_stream([[maybe_unused]] context& ctx)
{
  codec c{ {} };
  gsl::span<std::byte> empty{};
  auto [pkt, consumed, ec] = c.decode_packet(empty);
  assert_eq(ec, couchbase::errc::network::end_of_stream, "nothing left to read");
}

void
decoding_a_truncated_header_reports_need_more_data([[maybe_unused]] context& ctx)
{
  codec c{ {} };
  std::vector<std::byte> buf(10, std::byte{ 0 });
  auto [pkt, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ buf });
  assert_eq(ec, couchbase::errc::network::need_more_data, "fewer bytes than a header");
}

void
decoding_a_header_whose_body_has_not_arrived_reports_need_more_data([[maybe_unused]] context& ctx)
{
  codec c{ {} };
  // A valid 24-byte header claiming a 10-byte body, with no body bytes supplied.
  std::vector<std::byte> buf(24, std::byte{ 0 });
  buf[0] = static_cast<std::byte>(magic::client_request);
  buf[1] = static_cast<std::byte>(client_opcode::get);
  // body_len at offset 8 (4 bytes big-endian) = 10
  buf[11] = std::byte{ 10 };
  auto [pkt, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ buf });
  assert_eq(ec, couchbase::errc::network::need_more_data, "a declared body that has not arrived");
}

void
a_get_request_survives_an_encode_decode_round_trip([[maybe_unused]] context& ctx)
{
  codec c{ { hello_feature::collections } };

  packet orig = make_request(client_opcode::get, 0x08);
  orig.key_ = { std::byte{ 'd' }, std::byte{ 'o' }, std::byte{ 'c' } };
  orig.vbucket_ = 7;
  // Every byte distinct, so that a reordering of either field is visible rather than being
  // masked by a symmetric value. These two are what carry read_uint32 and read_uint64 through
  // the decode side; vbucket_ above does the same for read_uint16.
  orig.opaque_ = 0x01020304;
  orig.cas_ = 0x0102030405060708;

  auto encoded = c.encode_packet(orig);
  assert_true(encoded.has_value(), "the request encodes");

  auto [decoded, consumed, ec] = c.decode_packet(gsl::span<std::byte>{ encoded.value() });
  assert_success(ec, "the encoded request decodes");
  assert_eq(consumed, encoded.value().size(), "the whole frame is consumed");
  assert_eq(decoded.command_, client_opcode::get, "the opcode survives");
  assert_eq(decoded.collection_id_, std::uint32_t{ 0x08 }, "the collection id survives");
  assert_eq(decoded.vbucket_, std::uint16_t{ 7 }, "the vbucket survives");
  assert_eq(decoded.opaque_, std::uint32_t{ 0x01020304 }, "the opaque survives");
  assert_eq(decoded.cas_, std::uint64_t{ 0x0102030405060708 }, "the CAS survives");
  assert_eq(decoded.key_, orig.key_, "the key survives");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_collection_command_carrying_a_collection_id_is_encoded_when_collections_are_off) },
      { CASE(a_plain_command_carrying_a_collection_id_is_encoded_when_collections_are_off) },
      { CASE(a_collection_command_carrying_a_non_zero_collection_id_is_encoded) },
      { CASE(a_collection_command_carrying_a_zero_collection_id_is_encoded) },
      { CASE(a_plain_command_carrying_no_collection_id_is_encoded) },
      { CASE(a_plain_command_carrying_a_collection_id_is_rejected) },
      { CASE(observe_is_rejected_whatever_collection_id_it_carries) },
      { CASE(get_random_key_carries_its_collection_id_in_extras) },
      { CASE(decoding_an_empty_buffer_reports_end_of_stream) },
      { CASE(decoding_a_truncated_header_reports_need_more_data) },
      { CASE(decoding_a_header_whose_body_has_not_arrived_reports_need_more_data) },
      { CASE(a_get_request_survives_an_encode_decode_round_trip) },
    },
  };
}

} // namespace couchbase::test
