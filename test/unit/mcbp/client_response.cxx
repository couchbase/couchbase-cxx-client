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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/io/mcbp_message.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_cluster_map_change_notification.hxx"
#include "core/protocol/cmd_decrement.hxx"
#include "core/protocol/cmd_get.hxx"
#include "core/protocol/cmd_get_cluster_config.hxx"
#include "core/protocol/cmd_hello.hxx"
#include "core/protocol/cmd_increment.hxx"
#include "core/protocol/cmd_lookup_in.hxx"
#include "core/protocol/cmd_lookup_in_replica.hxx"
#include "core/protocol/cmd_mutate_in.hxx"
#include "core/protocol/cmd_noop.hxx"
#include "core/protocol/cmd_observe_seqno.hxx"
#include "core/protocol/cmd_sasl_auth.hxx"
#include "core/protocol/cmd_sasl_step.hxx"
#include "core/protocol/cmd_upsert.hxx"
#include "core/protocol/magic.hxx"
#include "core/protocol/server_request.hxx"
#include "core/protocol/status.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
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

// The wire value the duration frames below carry, and the microseconds it stands for. RFC-0027
// encodes a duration as pow(value, 1.74) / 2, so 0x0100 is 7750.104242 us. Written out rather
// than recomputed here: a case that evaluates the decoder's own formula cannot disagree with it.
constexpr std::uint16_t encoded_duration{ 0x0100 };
constexpr double expected_duration_us{ 7750.104242 };
constexpr double duration_tolerance_us{ 0.001 };

void
the_response_header_is_decoded_from_network_byte_order([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::mcbp_noop_response_body;

  // The decode side is the mirror of the encode side: bytes are copied into a native integer and
  // then converted. On a big-endian host an unconditional conversion reverses an already-correct
  // value, so the application sees every CAS byte-reversed.
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

  assert_eq(response.cas().value(), cas, "CAS");
  assert_eq(response.opaque(), opaque, "opaque");
  assert_eq(response.status(), static_cast<key_value_status_code>(0x0002), "status");
}

void
a_server_request_header_is_decoded_from_network_byte_order([[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::cluster_map_change_notification_request_body;
  using couchbase::core::protocol::server_request;

  // Nothing observes a reversal here today, because the only body this class is instantiated
  // with is the one below and no caller reads either accessor. These assertions are what keeps
  // the next caller from inheriting one.
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

  assert_eq(request.opaque(), opaque, "opaque");
  assert_eq(request.cas().value(), cas, "CAS");
  assert_eq(request.body().bucket(), bucket, "bucket name");
}

void
a_key_length_whose_low_nibble_is_zero_does_not_hide_the_framing_extras(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::parse_server_duration_us;

  // Byte 2 is 3 and byte 3 is 16. Reading the two as one native integer and masking picks byte 2
  // on a little-endian host and byte 3 on a big-endian one, where 16 & 0xf is 0: the duration is
  // dropped from every alt response carrying a 16-byte key. Little-endian cannot observe that, so
  // this pins the contract rather than reproducing the failure here.
  auto body = server_duration_frame(encoded_duration);
  body.resize(3 + 16);
  const auto parsed =
    parse_server_duration_us(message_with(alt_wire_header(3, 16), std::move(body)));
  assert_near(parsed,
              expected_duration_us,
              duration_tolerance_us,
              "the duration of a response whose key length ends in a zero nibble");
}

void
a_framing_extras_region_longer_than_fifteen_bytes_is_scanned_to_its_end(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::parse_server_duration_us;

  // Five frames, then the duration at offset 15. The region is 18 bytes, whose low nibble is 2,
  // so masking the length to four bits stops the scan inside the first frame.
  std::vector<std::byte> body;
  for (int i = 0; i < 5; ++i) {
    const auto frame = filler_frame();
    body.insert(body.end(), frame.begin(), frame.end());
  }
  const auto duration = server_duration_frame(encoded_duration);
  body.insert(body.end(), duration.begin(), duration.end());
  assert_eq(body.size(), std::size_t{ 18 }, "the region spans five fillers and the duration");
  const auto parsed =
    parse_server_duration_us(message_with(alt_wire_header(18, 0), std::move(body)));
  assert_near(parsed,
              expected_duration_us,
              duration_tolerance_us,
              "the duration of a response whose framing extras exceed fifteen bytes");
}

void
a_framing_extras_region_the_body_cannot_hold_yields_no_duration([[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::parse_server_duration_us;

  // Nothing bounds the scan against the body, so a length the body cannot hold has to be
  // rejected before the loop starts.
  assert_eq(parse_server_duration_us(
              message_with(alt_wire_header(200, 0), server_duration_frame(encoded_duration))),
            0.0,
            "a malformed response reports no duration rather than reading past its body");
}

template<typename Body>
auto
subdoc_header() -> couchbase::core::protocol::header_buffer
{
  couchbase::core::protocol::header_buffer hdr{};
  hdr[0] = static_cast<std::byte>(couchbase::core::protocol::magic::client_response);
  hdr[1] = static_cast<std::byte>(Body::opcode);
  return hdr;
}

void
a_lookup_in_response_with_a_truncated_field_header_is_rejected([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::lookup_in_replica_response_body;
  using couchbase::core::protocol::lookup_in_response_body;

  // Each lookup_in entry begins with a 2-byte status and a 4-byte length. A 3-byte remainder
  // cannot hold that header and must be rejected without reading past the body.
  const std::vector<std::byte> truncated{ std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 } };

  lookup_in_response_body lookup{};
  assert_false(lookup.parse(key_value_status_code::success,
                            subdoc_header<lookup_in_response_body>(),
                            0,
                            0,
                            0,
                            truncated,
                            {}),
               "lookup_in rejects a truncated field header");

  lookup_in_replica_response_body replica{};
  assert_false(replica.parse(key_value_status_code::success,
                             subdoc_header<lookup_in_replica_response_body>(),
                             0,
                             0,
                             0,
                             truncated,
                             {}),
               "lookup_in_replica rejects a truncated field header");
}

void
a_lookup_in_response_whose_field_size_exceeds_the_body_is_rejected([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::lookup_in_replica_response_body;
  using couchbase::core::protocol::lookup_in_response_body;

  // Status 0x0000 followed by entry_size = 100, with only 2 payload bytes present in the body.
  const std::vector<std::byte> body{
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 },
    std::byte{ 0x00 }, std::byte{ 0x64 }, std::byte{ 'o' },  std::byte{ 'k' },
  };

  lookup_in_response_body lookup{};
  assert_false(
    lookup.parse(
      key_value_status_code::success, subdoc_header<lookup_in_response_body>(), 0, 0, 0, body, {}),
    "lookup_in rejects a field whose declared size exceeds the remaining body");

  lookup_in_replica_response_body replica{};
  assert_false(replica.parse(key_value_status_code::success,
                             subdoc_header<lookup_in_replica_response_body>(),
                             0,
                             0,
                             0,
                             body,
                             {}),
               "lookup_in_replica rejects a field whose declared size exceeds the remaining body");
}

void
a_lookup_in_response_accepts_an_exact_twenty_mib_value([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::lookup_in_response_body;

  constexpr std::size_t max_value_size = std::size_t{ 20 } * 1024U * 1024U;
  std::vector<std::byte> body(6 + max_value_size, std::byte{ 'x' });
  body[0] = std::byte{ 0x00 };
  body[1] = std::byte{ 0x00 };
  body[2] = std::byte{ 0x01 };
  body[3] = std::byte{ 0x40 };
  body[4] = std::byte{ 0x00 };
  body[5] = std::byte{ 0x00 };

  lookup_in_response_body lookup{};
  assert_true(
    lookup.parse(
      key_value_status_code::success, subdoc_header<lookup_in_response_body>(), 0, 0, 0, body, {}),
    "an exact 20 MiB subdoc value is within the server limit");
  assert_eq(lookup.fields().size(), std::size_t{ 1 }, "one field parsed");
  assert_eq(lookup.fields()[0].value.size(), max_value_size, "full 20 MiB value retained");
}

void
a_subdoc_response_with_an_unknown_entry_status_is_rejected([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::lookup_in_response_body;
  using couchbase::core::protocol::mutate_in_response_body;

  // Unknown status 0x7FFF must return false rather than terminating the process.
  const std::vector<std::byte> lookup_body{
    std::byte{ 0x7f }, std::byte{ 0xff }, std::byte{ 0x00 },
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 },
  };
  lookup_in_response_body lookup{};
  assert_false(lookup.parse(key_value_status_code::success,
                            subdoc_header<lookup_in_response_body>(),
                            0,
                            0,
                            0,
                            lookup_body,
                            {}),
               "lookup_in rejects an unrecognised field status");

  const std::vector<std::byte> mutate_body{
    std::byte{ 0x00 },
    std::byte{ 0x7f },
    std::byte{ 0xff },
  };
  mutate_in_response_body mutate{};
  assert_false(mutate.parse(key_value_status_code::success,
                            subdoc_header<mutate_in_response_body>(),
                            0,
                            0,
                            0,
                            mutate_body,
                            {}),
               "mutate_in rejects an unrecognised field status");
}

void
a_mutate_in_response_with_truncated_extras_or_field_value_is_rejected([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::mutate_in_response_body;

  // Declares 16-byte mutation-token extras against a 4-byte body.
  const std::vector<std::byte> short_extras{
    std::byte{ 0x01 }, std::byte{ 0x02 }, std::byte{ 0x03 }, std::byte{ 0x04 }
  };
  mutate_in_response_body mutate_extras{};
  assert_false(mutate_extras.parse(key_value_status_code::success,
                                   subdoc_header<mutate_in_response_body>(),
                                   0,
                                   0,
                                   16,
                                   short_extras,
                                   {}),
               "mutate_in rejects extras larger than the body");

  // Index 0, status 0x0000 (success), entry_size = 50, with only 1 payload byte present.
  const std::vector<std::byte> truncated_value{
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 },
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x32 }, std::byte{ '1' },
  };
  mutate_in_response_body mutate_val{};
  assert_false(mutate_val.parse(key_value_status_code::success,
                                subdoc_header<mutate_in_response_body>(),
                                0,
                                0,
                                0,
                                truncated_value,
                                {}),
               "mutate_in rejects a field whose declared size exceeds the remaining body");
}

void
a_subdoc_response_clears_partial_fields_on_failure_and_supports_deleted_statuses(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::lookup_in_replica_response_body;
  using couchbase::core::protocol::lookup_in_response_body;
  using couchbase::core::protocol::mutate_in_response_body;

  // First entry valid (status 0x0000, size 2, "ok"), second entry truncated (only 2 bytes).
  const std::vector<std::byte> two_entry_lookup{
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 },
    std::byte{ 0x02 }, std::byte{ 'o' },  std::byte{ 'k' },  std::byte{ 0x00 }, std::byte{ 0x00 },
  };
  lookup_in_response_body lookup{};
  assert_false(lookup.parse(key_value_status_code::success,
                            subdoc_header<lookup_in_response_body>(),
                            0,
                            0,
                            0,
                            two_entry_lookup,
                            {}),
               "lookup_in rejects a truncated second entry");
  assert_true(lookup.fields().empty(), "lookup_in does not retain partial fields on failure");

  lookup_in_replica_response_body replica{};
  assert_false(replica.parse(key_value_status_code::success,
                             subdoc_header<lookup_in_replica_response_body>(),
                             0,
                             0,
                             0,
                             two_entry_lookup,
                             {}),
               "lookup_in_replica rejects a truncated second entry");
  assert_true(replica.fields().empty(),
              "lookup_in_replica does not retain partial fields on failure");

  // Mutate_in with subdoc_success_deleted and subdoc_multi_path_failure_deleted.
  const std::vector<std::byte> deleted_mutate_body{
    std::byte{ 0x00 },
    std::byte{ 0x00 },
    std::byte{ 0xc5 }, // index 0, subdoc_path_not_found
  };
  mutate_in_response_body mutate_deleted{};
  assert_true(mutate_deleted.parse(key_value_status_code::subdoc_multi_path_failure_deleted,
                                   subdoc_header<mutate_in_response_body>(),
                                   0,
                                   0,
                                   0,
                                   deleted_mutate_body,
                                   {}),
              "mutate_in parses subdoc_multi_path_failure_deleted responses");
  assert_eq(mutate_deleted.fields().size(), std::size_t{ 1 }, "one field parsed on deleted doc");

  // Mutate_in rejects field index >= 16.
  const std::vector<std::byte> out_of_range_index{
    std::byte{ 16 },
    std::byte{ 0x00 },
    std::byte{ 0xc5 },
  };
  mutate_in_response_body mutate_bad_idx{};
  assert_false(mutate_bad_idx.parse(key_value_status_code::subdoc_multi_path_failure,
                                    subdoc_header<mutate_in_response_body>(),
                                    0,
                                    0,
                                    0,
                                    out_of_range_index,
                                    {}),
               "mutate_in rejects field index >= 16");
}

void
a_subdoc_operation_reports_protocol_error_on_mismatched_field_counts([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::operations::lookup_in_request;
  using couchbase::core::operations::mutate_in_request;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::lookup_in_response_body;
  using couchbase::core::protocol::mutate_in_response_body;

  // Lookup_in with 1 requested spec but 0 fields in response body.
  lookup_in_request lookup_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  lookup_req.specs = couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs();

  auto lhdr = subdoc_header<lookup_in_response_body>();
  couchbase::core::io::mcbp_message lmsg{};
  std::memcpy(&lmsg.header, lhdr.data(), lhdr.size());
  const client_response<lookup_in_response_body> lresp{ std::move(lmsg) };
  auto lookup_out = lookup_req.make_response({}, lresp);
  assert_error(lookup_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "lookup_in reports protocol_error when response field count mismatches specs");

  // Mutate_in with status subdoc_multi_path_failure but 0 fields in response body.
  mutate_in_request mutate_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  mutate_req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::remove("a") }.specs();

  auto mhdr = subdoc_header<mutate_in_response_body>();
  mhdr[6] = std::byte{ 0x00 };
  mhdr[7] =
    static_cast<std::byte>(couchbase::core::key_value_status_code::subdoc_multi_path_failure);
  couchbase::core::io::mcbp_message mmsg{};
  std::memcpy(&mmsg.header, mhdr.data(), mhdr.size());
  const client_response<mutate_in_response_body> mresp{ std::move(mmsg) };
  auto mutate_out = mutate_req.make_response({}, mresp);
  assert_error(mutate_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in reports protocol_error when multi-path failure carries no error entry");

  // Mutate_in with 1 requested spec (index 0) but response entry has index 5.
  std::vector<std::byte> oob_idx_body{
    std::byte{ 5 },                       // index 5 >= specs.size() (1)
    std::byte{ 0x00 }, std::byte{ 0x00 }, // status = success
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, // entry_size = 0
  };
  auto mhdr2 = subdoc_header<mutate_in_response_body>();
  mhdr2[11] = static_cast<std::byte>(oob_idx_body.size());
  couchbase::core::io::mcbp_message mmsg2{};
  std::memcpy(&mmsg2.header, mhdr2.data(), mhdr2.size());
  mmsg2.body = std::move(oob_idx_body);
  const client_response<mutate_in_response_body> mresp2{ std::move(mmsg2) };
  auto mutate_out2 = mutate_req.make_response({}, mresp2);
  assert_error(mutate_out2.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in reports protocol_error when response field index exceeds specs count");

  // Sparse success response for non-counter spec remains valid.
  auto mhdr_sparse = subdoc_header<mutate_in_response_body>();
  const client_response<mutate_in_response_body> mresp_sparse{ message_with(mhdr_sparse, {}) };
  auto mutate_sparse_out = mutate_req.make_response({}, mresp_sparse);
  assert_false(static_cast<bool>(mutate_sparse_out.ctx.ec()),
               "mutate_in allows sparse success response for non-counter spec");

  // Mutate_in with a counter spec whose response omits the counter result reports protocol_error.
  mutate_in_request counter_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  counter_req.specs =
    couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("cnt", 1) }.specs();
  auto counter_missing_out = counter_req.make_response({}, mresp_sparse);
  assert_error(counter_missing_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in reports protocol_error when counter result is omitted");

  // Mutate_in with duplicate entry indices reports protocol_error.
  mutate_in_request two_counter_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  two_counter_req.specs =
    couchbase::mutate_in_specs{
      couchbase::mutate_in_specs::increment("c1", 1),
      couchbase::mutate_in_specs::increment("c2", 1),
    }
      .specs();
  std::vector<std::byte> dup_idx_body{
    std::byte{ 0 },                       // index 0
    std::byte{ 0x00 }, std::byte{ 0x00 }, // status = success
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x01 }, std::byte{ '1' },
    std::byte{ 0 },                       // duplicate index 0 (leaves index 1 missing)
    std::byte{ 0x00 }, std::byte{ 0x00 }, // status = success
    std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x01 }, std::byte{ '2' },
  };
  auto mhdr_dup = subdoc_header<mutate_in_response_body>();
  mhdr_dup[11] = static_cast<std::byte>(dup_idx_body.size());
  const client_response<mutate_in_response_body> mresp_dup{ message_with(mhdr_dup,
                                                                         std::move(dup_idx_body)) };
  auto dup_out = two_counter_req.make_response({}, mresp_dup);
  assert_error(dup_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in reports protocol_error on duplicate field indices");
  auto dup_non_counter_out = mutate_req.make_response({}, mresp_dup);
  assert_error(dup_non_counter_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in reports protocol_error on duplicate index 0 for non-counter spec");
}

void
sibling_mcbp_responses_validate_body_and_enhanced_error_bounds([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::decrement_response_body;
  using couchbase::core::protocol::hello_response_body;
  using couchbase::core::protocol::increment_response_body;
  using couchbase::core::protocol::mcbp_noop_response_body;
  using couchbase::core::protocol::observe_seqno_response_body;

  const std::vector<std::byte> short_body{ std::byte{ 0x01 },
                                           std::byte{ 0x02 },
                                           std::byte{ 0x03 } };

  increment_response_body inc{};
  assert_false(inc.parse(key_value_status_code::success,
                         subdoc_header<increment_response_body>(),
                         0,
                         0,
                         0,
                         short_body,
                         {}),
               "increment rejects a body shorter than 8 bytes");

  decrement_response_body dec{};
  assert_false(dec.parse(key_value_status_code::success,
                         subdoc_header<decrement_response_body>(),
                         0,
                         0,
                         0,
                         short_body,
                         {}),
               "decrement rejects a body shorter than 8 bytes");

  observe_seqno_response_body obs{};
  assert_false(obs.parse(key_value_status_code::success,
                         subdoc_header<observe_seqno_response_body>(),
                         0,
                         0,
                         0,
                         short_body,
                         {}),
               "observe_seqno rejects a body shorter than 27 bytes");

  hello_response_body hello{};
  assert_false(hello.parse(key_value_status_code::success,
                           subdoc_header<hello_response_body>(),
                           0,
                           0,
                           0,
                           short_body,
                           {}),
               "hello rejects an odd-length feature list without aborting");

  // Non-success client_response with JSON datatype (0x01), keylen > body.size(), and non-JSON body.
  auto err_hdr = wire_header(0x0001, 1, 0);
  err_hdr[2] = std::byte{ 0x00 };
  err_hdr[3] = std::byte{ 0x20 }; // keylen = 32
  err_hdr[5] = std::byte{ 0x01 }; // JSON datatype
  err_hdr[11] = std::byte{ 0x03 };
  auto msg = message_with(err_hdr, short_body);
  const client_response<mcbp_noop_response_body> resp{ std::move(msg) };
  assert_false(resp.error_info().has_value(),
               "malformed error response with oversized keylen does not underflow or throw");
  assert_error(resp.error_code(),
               couchbase::errc::key_value::document_not_found,
               "non-success error status preserves its mapped error code");
}

void
client_response_propagates_parse_failures_and_preserves_error_statuses(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::operations::get_projected_request;
  using couchbase::core::operations::get_request;
  using couchbase::core::operations::increment_request;
  using couchbase::core::operations::lookup_in_request;
  using couchbase::core::operations::mutate_in_request;
  using couchbase::core::operations::upsert_request;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::get_cluster_config_response_body;
  using couchbase::core::protocol::get_response_body;
  using couchbase::core::protocol::increment_response_body;
  using couchbase::core::protocol::lookup_in_response_body;
  using couchbase::core::protocol::mutate_in_response_body;
  using couchbase::core::protocol::sasl_auth_response_body;
  using couchbase::core::protocol::sasl_step_response_body;
  using couchbase::core::protocol::upsert_response_body;

  const couchbase::core::document_id doc_id{ "bkt", "_default", "_default", "k1" };

  // 1. Truncated success-status mutate_in response propagates protocol_error through
  // client_response and make_response().
  mutate_in_request mutate_req{ doc_id };
  mutate_req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::remove("a") }.specs();
  auto mhdr = subdoc_header<mutate_in_response_body>();
  mhdr[4] = std::byte{ 16 }; // 16-byte mutation token extras declared
  mhdr[11] = std::byte{ 4 }; // only 4 body bytes present
  auto mresp = client_response<mutate_in_response_body>{ message_with(
    mhdr, { std::byte{ 1 }, std::byte{ 2 }, std::byte{ 3 }, std::byte{ 4 } }) };
  assert_error(mresp.error_code(),
               couchbase::errc::network::protocol_error,
               "client_response records protocol_error when mutate_in body parsing fails");
  auto mutate_out = mutate_req.make_response({}, mresp);
  assert_error(mutate_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in make_response propagates protocol_error on rejected body");

  // 2. Truncated subdoc_multi_path_failure lookup_in response propagates protocol_error.
  lookup_in_request lookup_req{ doc_id };
  lookup_req.specs = couchbase::lookup_in_specs{ couchbase::lookup_in_specs::get("a") }.specs();
  auto lhdr = subdoc_header<lookup_in_response_body>();
  lhdr[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  lhdr[11] = std::byte{ 3 }; // 3-byte truncated entry header
  auto lresp = client_response<lookup_in_response_body>{ message_with(
    lhdr, { std::byte{ 0 }, std::byte{ 0 }, std::byte{ 0 } }) };
  assert_error(lresp.error_code(),
               couchbase::errc::network::protocol_error,
               "client_response records protocol_error on truncated subdoc_multi_path_failure");
  auto lookup_out = lookup_req.make_response({}, lresp);
  assert_error(lookup_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "lookup_in make_response propagates protocol_error on truncated body");

  // 3. Projected get with truncated lookup_in body (and empty lookup_in body) reports
  // protocol_error without indexing an empty fields() vector.
  get_projected_request proj_req{ doc_id };
  proj_req.projections = { "a" };
  auto phdr = subdoc_header<lookup_in_response_body>();
  phdr[11] = std::byte{ 3 };
  auto presp_truncated = client_response<lookup_in_response_body>{ message_with(
    phdr, { std::byte{ 0 }, std::byte{ 0 }, std::byte{ 0 } }) };
  assert_error(presp_truncated.error_code(),
               couchbase::errc::network::protocol_error,
               "projected get client_response records protocol_error on truncated body");
  auto proj_out_truncated = proj_req.make_response({}, presp_truncated);
  assert_error(proj_out_truncated.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "get_projected make_response reports protocol_error on truncated body");

  auto phdr_empty = subdoc_header<lookup_in_response_body>();
  auto presp_empty = client_response<lookup_in_response_body>{ message_with(phdr_empty, {}) };
  auto proj_out_empty = proj_req.make_response({}, presp_empty);
  assert_error(proj_out_empty.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "get_projected make_response reports protocol_error when fields() is empty");

  // 4. Truncated increment and upsert responses propagate protocol_error.
  increment_request inc_req{ doc_id };
  auto ihdr = subdoc_header<increment_response_body>();
  ihdr[11] = std::byte{ 3 };
  auto iresp = client_response<increment_response_body>{ message_with(
    ihdr, { std::byte{ 1 }, std::byte{ 2 }, std::byte{ 3 } }) };
  assert_error(iresp.error_code(),
               couchbase::errc::network::protocol_error,
               "increment client_response records protocol_error on short body");
  auto inc_out = inc_req.make_response({}, iresp);
  assert_error(inc_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "increment make_response reports protocol_error on short body");

  upsert_request ups_req{ doc_id, {} };
  auto uhdr = subdoc_header<upsert_response_body>();
  uhdr[4] = std::byte{ 16 }; // 16-byte extras declared
  uhdr[11] = std::byte{ 4 }; // only 4 body bytes present
  auto uresp = client_response<upsert_response_body>{ message_with(
    uhdr, { std::byte{ 1 }, std::byte{ 2 }, std::byte{ 3 }, std::byte{ 4 } }) };
  assert_error(uresp.error_code(),
               couchbase::errc::network::protocol_error,
               "upsert client_response records protocol_error on truncated extras");
  auto ups_out = ups_req.make_response({}, uresp);
  assert_error(ups_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "upsert make_response reports protocol_error on truncated extras");

  auto uhdr_ok = subdoc_header<upsert_response_body>();
  auto uresp_ok = client_response<upsert_response_body>{ message_with(uhdr_ok, {}) };
  assert_false(static_cast<bool>(uresp_ok.error_code()),
               "valid empty upsert response succeeds without mutation token");
  auto ups_out_ok = ups_req.make_response({}, uresp_ok);
  assert_false(static_cast<bool>(ups_out_ok.ctx.ec()),
               "upsert make_response succeeds on valid empty body");

  auto cfghdr = subdoc_header<get_cluster_config_response_body>();
  cfghdr[4] = std::byte{ 8 };  // 8-byte extras declared
  cfghdr[11] = std::byte{ 2 }; // only 2 body bytes declared/present
  auto cfgresp = client_response<get_cluster_config_response_body>{ message_with(
    cfghdr, { std::byte{ '{' }, std::byte{ '}' } }) };
  assert_error(cfgresp.error_code(),
               couchbase::errc::network::protocol_error,
               "get_cluster_config rejects oversized prefix with protocol_error");

  auto sasl_hdr = subdoc_header<sasl_auth_response_body>();
  sasl_hdr[3] = std::byte{ 10 }; // keylen = 10
  sasl_hdr[7] = static_cast<std::byte>(key_value_status_code::auth_continue);
  sasl_hdr[11] = std::byte{ 2 };
  auto sasl_resp = client_response<sasl_auth_response_body>{ message_with(
    sasl_hdr, { std::byte{ 'a' }, std::byte{ 'b' } }) };
  assert_error(sasl_resp.error_code(),
               couchbase::errc::network::protocol_error,
               "sasl_auth rejects oversized prefix on auth_continue with protocol_error");

  auto step_hdr = subdoc_header<sasl_step_response_body>();
  step_hdr[11] = std::byte{ 8 }; // bodylen = 8, but only 2 bytes supplied
  auto step_resp = client_response<sasl_step_response_body>{ message_with(
    step_hdr, { std::byte{ 'o' }, std::byte{ 'k' } }) };
  assert_error(step_resp.error_code(),
               couchbase::errc::network::protocol_error,
               "sasl_step rejects body shorter than advertised bodylen with protocol_error");

  // 5. Normal error-status fallback preserves the mapped status error and enhanced error info.
  get_request get_req{ doc_id };
  const std::string err_json = R"({"error":{"ref":"ref-42","context":"missing key"}})";
  std::vector<std::byte> err_body(err_json.size());
  std::memcpy(err_body.data(), err_json.data(), err_json.size());
  auto ghdr = subdoc_header<get_response_body>();
  ghdr[5] = std::byte{ 0x01 }; // JSON datatype
  ghdr[7] = static_cast<std::byte>(key_value_status_code::not_found);
  ghdr[11] = static_cast<std::byte>(err_body.size());
  auto gresp = client_response<get_response_body>{ message_with(ghdr, std::move(err_body)) };
  assert_eq(gresp.status(), key_value_status_code::not_found, "error status preserved");
  assert_error(gresp.error_code(),
               couchbase::errc::key_value::document_not_found,
               "normal error status maps to document_not_found rather than protocol_error");
  const auto err_info = gresp.error_info();
  assert_true(err_info.has_value(), "enhanced error info decoded on error status");
  if (err_info.has_value()) {
    assert_eq(err_info->reference(), std::string{ "ref-42" }, "enhanced error ref");
    assert_eq(err_info->context(), std::string{ "missing key" }, "enhanced error context");
  }
  auto get_out = get_req.make_response({}, gresp);
  assert_error(get_out.ctx.ec(),
               couchbase::errc::key_value::document_not_found,
               "get make_response preserves document_not_found on error status");
}

void
a_server_request_with_truncated_body_or_oversized_prefix_skips_decoding(
  [[maybe_unused]] context& ctx)
{
  using couchbase::core::protocol::cluster_map_change_notification_request_body;
  using couchbase::core::protocol::server_request;

  // 1. Advertises bodylen = 4 and extlen = 4 with an empty body buffer.
  auto hdr_empty = server_request_wire_header(0x11, 0x22, 0);
  hdr_empty[4] = std::byte{ 4 };  // extlen = 4
  hdr_empty[11] = std::byte{ 4 }; // bodylen = 4
  server_request<cluster_map_change_notification_request_body> req_empty{ message_with(hdr_empty,
                                                                                       {}) };
  assert_eq(req_empty.body().protocol_revision(),
            std::uint32_t{ 0 },
            "truncated server_request does not read protocol_revision from an empty body");
  assert_true(req_empty.body().bucket().empty(), "bucket remains empty");
  assert_false(req_empty.body().config().has_value(), "config remains unset");

  // 2. Advertises extlen = 4 and keylen = 10 (prefix = 14) with a 4-byte body.
  auto hdr_prefix = server_request_wire_header(0x11, 0x22, 10);
  hdr_prefix[4] = std::byte{ 4 };  // extlen = 4
  hdr_prefix[11] = std::byte{ 4 }; // bodylen = 4
  server_request<cluster_map_change_notification_request_body> req_prefix{ message_with(
    hdr_prefix, { std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x07 } }) };
  assert_eq(req_prefix.body().protocol_revision(),
            std::uint32_t{ 0 },
            "oversized extras+key prefix skips decoding");
  assert_true(req_prefix.body().bucket().empty(), "bucket remains empty when prefix exceeds body");
  assert_false(req_prefix.body().config().has_value(), "config remains unset");
}

void
a_mutate_in_multi_path_failure_rejects_trailing_or_malformed_entries([[maybe_unused]] context& ctx)
{
  using couchbase::core::key_value_status_code;
  using couchbase::core::operations::mutate_in_request;
  using couchbase::core::protocol::client_response;
  using couchbase::core::protocol::mutate_in_response_body;

  const std::optional<couchbase::core::topology::configuration> no_cfg{};
  const std::vector<couchbase::core::protocol::hello_feature> no_features{};

  // 1. Valid single-entry subdoc_multi_path_failure preserves per-path error and original_index
  // ordering (XATTR spec at original_index 1 is moved to wire index 0; body spec at
  // original_index 0 moves to wire index 1).
  mutate_in_request two_spec_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  two_spec_req.specs =
    couchbase::mutate_in_specs{
      couchbase::mutate_in_specs::remove("doc_path"),
      couchbase::mutate_in_specs::remove("$xattr.meta").xattr(),
    }
      .specs();
  mutate_in_request::encoded_request_type encoded_two_spec{};
  assert_false(static_cast<bool>(two_spec_req.encode_to(encoded_two_spec, { no_cfg, no_features })),
               "encode_to succeeds for valid two-spec mutate_in request");

  std::vector<std::byte> valid_fail_body{
    std::byte{ 1 }, // wire index 1 -> original_index 0 ("doc_path")
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
  };
  auto hdr_valid = subdoc_header<mutate_in_response_body>();
  hdr_valid[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  hdr_valid[11] = static_cast<std::byte>(valid_fail_body.size());
  const client_response<mutate_in_response_body> resp_valid{ message_with(
    hdr_valid, std::move(valid_fail_body)) };
  auto valid_out = two_spec_req.make_response({}, resp_valid);
  assert_error(valid_out.ctx.ec(),
               couchbase::errc::key_value::path_not_found,
               "valid single-entry multi-path failure reports the per-path error");
  assert_eq(valid_out.fields.size(),
            std::size_t{ 2 },
            "valid multi-path failure keeps fields indexed by original_index for callers");
  const auto first_err_idx = valid_out.ctx.first_error_index();
  assert_true(first_err_idx.has_value(), "valid multi-path failure sets first_error_index");
  if (first_err_idx.has_value()) {
    assert_eq(*first_err_idx, std::size_t{ 0 }, "first_error_index maps back to original_index");
    assert_eq(valid_out.fields.at(*first_err_idx).status,
              key_value_status_code::subdoc_path_not_found,
              "fields[first_error_index] carries the failing status");
  }

  // 2. One requested spec, entry 0 is subdoc_path_not_found followed by out-of-range index 5.
  mutate_in_request one_spec_req{ couchbase::core::document_id{
    "bkt", "_default", "_default", "k1" } };
  one_spec_req.specs =
    couchbase::mutate_in_specs{ couchbase::mutate_in_specs::remove("a") }.specs();
  mutate_in_request::encoded_request_type encoded_one_spec{};
  assert_false(static_cast<bool>(one_spec_req.encode_to(encoded_one_spec, { no_cfg, no_features })),
               "encode_to succeeds for valid one-spec mutate_in request");

  std::vector<std::byte> err_then_oob_body{
    std::byte{ 0 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
    std::byte{ 5 }, // trailing out-of-range index 5 >= specs.size() (1)
    std::byte{ 0x00 },
    std::byte{ 0x00 }, // status = success
    std::byte{ 0x00 },
    std::byte{ 0x00 },
    std::byte{ 0x00 },
    std::byte{ 0x00 }, // entry_size = 0
  };
  auto hdr_oob = subdoc_header<mutate_in_response_body>();
  hdr_oob[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  hdr_oob[11] = static_cast<std::byte>(err_then_oob_body.size());
  const client_response<mutate_in_response_body> resp_oob{ message_with(
    hdr_oob, std::move(err_then_oob_body)) };
  auto oob_out = one_spec_req.make_response({}, resp_oob);
  assert_error(oob_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "trailing out-of-range index after a path error reports protocol_error");
  assert_true(oob_out.fields.empty(), "protocol_error clears fields");
  assert_false(oob_out.ctx.first_error_index().has_value(),
               "protocol_error leaves first_error_index unset");

  // Single-entry multi-path failure with wire index 5 (< 16, valid wire ResultSpec) against a
  // 1-spec request is rejected by make_response() bounds validation.
  std::vector<std::byte> single_oob_fail_body{
    std::byte{ 5 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
  };
  auto hdr_single_oob = subdoc_header<mutate_in_response_body>();
  hdr_single_oob[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  hdr_single_oob[11] = static_cast<std::byte>(single_oob_fail_body.size());
  const client_response<mutate_in_response_body> resp_single_oob{ message_with(
    hdr_single_oob, std::move(single_oob_fail_body)) };
  assert_false(static_cast<bool>(resp_single_oob.error_code()),
               "wire parser accepts single failure entry with index < 16");
  auto single_oob_out = one_spec_req.make_response({}, resp_single_oob);
  assert_error(single_oob_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "make_response rejects failure entry index >= specs.size()");
  assert_true(single_oob_out.fields.empty(), "protocol_error clears fields");
  assert_false(single_oob_out.ctx.first_error_index().has_value(),
               "protocol_error leaves first_error_index unset");

  // 3. Two requested specs, entry 0 is subdoc_path_not_found followed by duplicate index 0 (and a
  // second distinct failure entry, violating the single-ResultSpec failure format).
  std::vector<std::byte> err_then_dup_body{
    std::byte{ 0 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
    std::byte{ 0 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
  };
  auto hdr_dup = subdoc_header<mutate_in_response_body>();
  hdr_dup[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  hdr_dup[11] = static_cast<std::byte>(err_then_dup_body.size());
  const client_response<mutate_in_response_body> resp_dup{ message_with(
    hdr_dup, std::move(err_then_dup_body)) };
  auto dup_out = two_spec_req.make_response({}, resp_dup);
  assert_error(dup_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "duplicate index after a path error reports protocol_error");
  assert_true(dup_out.fields.empty(), "protocol_error clears fields");
  assert_false(dup_out.ctx.first_error_index().has_value(),
               "protocol_error leaves first_error_index unset");

  std::vector<std::byte> two_fail_body{
    std::byte{ 0 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
    std::byte{ 1 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_exists),
  };
  auto hdr_two_fail = subdoc_header<mutate_in_response_body>();
  hdr_two_fail[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
  hdr_two_fail[11] = static_cast<std::byte>(two_fail_body.size());
  const client_response<mutate_in_response_body> resp_two_fail{ message_with(
    hdr_two_fail, std::move(two_fail_body)) };
  auto two_fail_out = two_spec_req.make_response({}, resp_two_fail);
  assert_error(two_fail_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "multi-path failure with more than one ResultSpec reports protocol_error");
  assert_true(two_fail_out.fields.empty(), "protocol_error clears fields");
  assert_false(two_fail_out.ctx.first_error_index().has_value(),
               "protocol_error leaves first_error_index unset");

  // 4. Overall success status carrying a failing ResultSpec contradicts the header status.
  std::vector<std::byte> success_with_err_body{
    std::byte{ 0 },
    std::byte{ 0x00 },
    static_cast<std::byte>(key_value_status_code::subdoc_path_not_found),
  };
  auto hdr_contradict = subdoc_header<mutate_in_response_body>();
  hdr_contradict[11] = static_cast<std::byte>(success_with_err_body.size());
  const client_response<mutate_in_response_body> resp_contradict{ message_with(
    hdr_contradict, std::move(success_with_err_body)) };
  auto contradict_out = one_spec_req.make_response({}, resp_contradict);
  assert_error(contradict_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "success-status mutate_in with a failing entry reports protocol_error");
  assert_true(contradict_out.fields.empty(), "protocol_error clears fields");
  assert_false(contradict_out.ctx.first_error_index().has_value(),
               "protocol_error leaves first_error_index unset");

  // 5. Multi-path failure whose entry status has no valid per-path mapping (e.g.
  // subdoc_multi_path_failure or config_only) reports protocol_error without leaving
  // first_error_index populated.
  for (const auto invalid_entry_status :
       { key_value_status_code::subdoc_multi_path_failure, key_value_status_code::config_only }) {
    std::vector<std::byte> unmapped_err_body{
      std::byte{ 0 },
      static_cast<std::byte>((static_cast<std::uint16_t>(invalid_entry_status) >> 8U) & 0xffU),
      static_cast<std::byte>(static_cast<std::uint16_t>(invalid_entry_status) & 0xffU),
    };
    auto hdr_unmapped = subdoc_header<mutate_in_response_body>();
    hdr_unmapped[7] = static_cast<std::byte>(key_value_status_code::subdoc_multi_path_failure);
    hdr_unmapped[11] = static_cast<std::byte>(unmapped_err_body.size());
    const client_response<mutate_in_response_body> resp_unmapped{ message_with(
      hdr_unmapped, std::move(unmapped_err_body)) };
    auto unmapped_out = one_spec_req.make_response({}, resp_unmapped);
    assert_error(unmapped_out.ctx.ec(),
                 couchbase::errc::network::protocol_error,
                 "multi-path failure with non-per-path entry status reports protocol_error");
    assert_true(unmapped_out.fields.empty(),
                "protocol_error clears fields when entry status is not a per-path error");
    assert_false(unmapped_out.ctx.first_error_index().has_value(),
                 "protocol_error leaves first_error_index unset");
  }

  // 6. Header status unrelated to mutate_in that maps to empty error_code (e.g.
  // range_scan_complete) is rejected as protocol_error rather than treated as success.
  auto hdr_scan = subdoc_header<mutate_in_response_body>();
  hdr_scan[7] = static_cast<std::byte>(key_value_status_code::range_scan_complete);
  const client_response<mutate_in_response_body> resp_scan{ message_with(hdr_scan, {}) };
  assert_error(resp_scan.error_code(),
               couchbase::errc::network::protocol_error,
               "client_response rejects range_scan_complete status on mutate_in");
  auto scan_out = one_spec_req.make_response({}, resp_scan);
  assert_error(scan_out.ctx.ec(),
               couchbase::errc::network::protocol_error,
               "mutate_in make_response propagates protocol_error on range_scan_complete status");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(the_response_header_is_decoded_from_network_byte_order) },
      { CASE(a_server_request_header_is_decoded_from_network_byte_order) },
      { CASE(a_key_length_whose_low_nibble_is_zero_does_not_hide_the_framing_extras) },
      { CASE(a_framing_extras_region_longer_than_fifteen_bytes_is_scanned_to_its_end) },
      { CASE(a_framing_extras_region_the_body_cannot_hold_yields_no_duration) },
      { CASE(a_lookup_in_response_with_a_truncated_field_header_is_rejected) },
      { CASE(a_lookup_in_response_whose_field_size_exceeds_the_body_is_rejected) },
      { CASE(a_lookup_in_response_accepts_an_exact_twenty_mib_value) },
      { CASE(a_subdoc_response_with_an_unknown_entry_status_is_rejected) },
      { CASE(a_mutate_in_response_with_truncated_extras_or_field_value_is_rejected) },
      { CASE(a_subdoc_response_clears_partial_fields_on_failure_and_supports_deleted_statuses) },
      { CASE(a_subdoc_operation_reports_protocol_error_on_mismatched_field_counts) },
      { CASE(sibling_mcbp_responses_validate_body_and_enhanced_error_bounds) },
      { CASE(client_response_propagates_parse_failures_and_preserves_error_statuses) },
      { CASE(a_server_request_with_truncated_body_or_oversized_prefix_skips_decoding) },
      { CASE(a_mutate_in_multi_path_failure_rejects_trailing_or_malformed_entries) },
    },
  };
}

} // namespace couchbase::test
