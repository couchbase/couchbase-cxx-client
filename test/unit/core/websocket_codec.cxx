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

#include "core/crypto/cbcrypto.h"
#include "core/platform/base64.h"
#include "core/websocket_codec.hxx"

#include <spdlog/fmt/bundled/core.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace couchbase::test
{
namespace
{
class recording_callbacks : public couchbase::core::websocket_callbacks
{
public:
  void on_text(const couchbase::core::websocket_codec& /* ws */,
               gsl::span<std::byte> payload) override
  {
    texts.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
  }

  void on_binary(const couchbase::core::websocket_codec& /* ws */,
                 gsl::span<std::byte> payload) override
  {
    binaries.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
  }

  void on_ping(const couchbase::core::websocket_codec& /* ws */,
               gsl::span<std::byte> payload) override
  {
    pings.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
  }

  void on_pong(const couchbase::core::websocket_codec& /* ws */,
               gsl::span<std::byte> payload) override
  {
    pongs.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
  }

  void on_close(const couchbase::core::websocket_codec& /* ws */,
                gsl::span<std::byte> payload) override
  {
    closes.emplace_back(reinterpret_cast<const char*>(payload.data()), payload.size());
  }

  void on_ready(const couchbase::core::websocket_codec& /* ws */) override
  {
    ready = true;
  }

  void on_error(const couchbase::core::websocket_codec& /* ws */,
                const std::string& message) override
  {
    errors.push_back(message);
  }

  bool ready{ false };
  std::vector<std::string> texts{};
  std::vector<std::string> binaries{};
  std::vector<std::string> pings{};
  std::vector<std::string> pongs{};
  std::vector<std::string> closes{};
  std::vector<std::string> errors{};
};

auto
compute_accept_key(std::string_view session_key) -> std::string
{
  const auto salted = fmt::format("{}258EAFA5-E914-47DA-95CA-C5AB0DC85B11", session_key);
  const auto hash =
    couchbase::core::crypto::digest(couchbase::core::crypto::Algorithm::ALG_SHA1, salted);
  return couchbase::core::base64::encode(hash, false);
}

void
complete_handshake(couchbase::core::websocket_codec& ws)
{
  const auto response = fmt::format("HTTP/1.1 101 Switching Protocols\r\n"
                                    "Upgrade: websocket\r\n"
                                    "Connection: Upgrade\r\n"
                                    "Sec-WebSocket-Accept: {}\r\n\r\n",
                                    compute_accept_key(ws.session_key()));
  ws.feed(response);
}

auto
make_unmasked_frame(std::uint8_t first_byte, std::string_view payload) -> std::vector<std::byte>
{
  std::vector<std::byte> frame{
    static_cast<std::byte>(first_byte),
    static_cast<std::byte>(payload.size()),
  };
  for (const char ch : payload) {
    frame.push_back(static_cast<std::byte>(ch));
  }
  return frame;
}

auto
make_unmasked_64bit_frame(std::uint8_t first_byte,
                          std::size_t payload_size,
                          std::byte fill = std::byte{ 'x' }) -> std::vector<std::byte>
{
  std::vector<std::byte> frame(10 + payload_size, fill);
  frame[0] = static_cast<std::byte>(first_byte);
  frame[1] = std::byte{ 127 };
  const auto len = static_cast<std::uint64_t>(payload_size);
  for (std::size_t i = 0; i < 8; ++i) {
    frame[2 + i] = static_cast<std::byte>((len >> ((7U - i) * 8U)) & 0xffU);
  }
  return frame;
}

void
coalesced_frames_in_a_single_chunk_are_delivered_separately([[maybe_unused]] context& ctx)
{
  recording_callbacks cb;
  couchbase::core::websocket_codec ws(&cb);
  complete_handshake(ws);
  assert_true(cb.ready, "handshake completes");

  auto combined = make_unmasked_frame(0x81, "hello");
  const auto second = make_unmasked_frame(0x81, "world");
  combined.insert(combined.end(), second.begin(), second.end());

  ws.feed(combined);

  assert_true(cb.errors.empty(), "no decoding error");
  assert_eq(cb.texts.size(), std::size_t{ 2 }, "two text frames decoded from the coalesced chunk");
  assert_eq(cb.texts[0], "hello", "first frame payload");
  assert_eq(cb.texts[1], "world", "second frame payload");

  // Masked frames coalesced into one buffer must also be bounded by their declared payload length.
  auto masked_combined = ws.text("alpha");
  const auto masked_second = ws.text("beta");
  masked_combined.insert(masked_combined.end(), masked_second.begin(), masked_second.end());

  ws.feed(masked_combined);

  assert_true(cb.errors.empty(), "no decoding error for masked frames");
  assert_eq(cb.texts.size(), std::size_t{ 4 }, "all four frames delivered");
  assert_eq(cb.texts[2], "alpha", "first masked frame payload");
  assert_eq(cb.texts[3], "beta", "second masked frame payload");
}

void
a_fragmented_message_retains_the_final_continuation_and_resets_state([[maybe_unused]] context& ctx)
{
  recording_callbacks cb;
  couchbase::core::websocket_codec ws(&cb);
  complete_handshake(ws);

  // FIN=0 text ("part1-"), FIN=0 continuation ("part2-"), FIN=1 continuation ("part3").
  auto frag1 = make_unmasked_frame(0x01, "part1-");
  auto frag2 = make_unmasked_frame(0x00, "part2-");
  auto frag3 = make_unmasked_frame(0x80, "part3");
  ws.feed(frag1);
  ws.feed(frag2);
  ws.feed(frag3);

  assert_true(cb.errors.empty(), "no decoding error during fragmentation");
  assert_eq(cb.texts.size(), std::size_t{ 1 }, "completed fragmented message delivered");
  assert_eq(cb.texts[0], "part1-part2-part3", "final continuation payload is included");

  // A subsequent standalone data frame must succeed after the fragmented message completes.
  auto next_frame = make_unmasked_frame(0x81, "next-message");
  ws.feed(next_frame);
  assert_true(cb.errors.empty(), "subsequent frame is accepted after continuation completes");
  assert_eq(cb.texts.size(), std::size_t{ 2 }, "second message delivered");
  assert_eq(cb.texts[1], "next-message", "subsequent standalone message payload");
}

void
a_single_byte_frame_header_waits_for_the_second_byte([[maybe_unused]] context& ctx)
{
  recording_callbacks cb;
  couchbase::core::websocket_codec ws(&cb);
  complete_handshake(ws);

  std::vector<std::byte> first_byte{ std::byte{ 0x81 } };
  ws.feed(first_byte);
  assert_true(cb.errors.empty(), "single-byte chunk waits for more data");
  assert_true(cb.texts.empty(), "no message delivered yet");

  std::vector<std::byte> remainder{
    std::byte{ 0x02 },
    std::byte{ 'o' },
    std::byte{ 'k' },
  };
  ws.feed(remainder);
  assert_true(cb.errors.empty(), "frame completes cleanly");
  assert_eq(cb.texts.size(), std::size_t{ 1 }, "one text frame delivered");
  assert_eq(cb.texts[0], "ok", "reassembled payload");
}

void
an_oversized_64_bit_payload_length_reports_a_decoding_error([[maybe_unused]] context& ctx)
{
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    // length_7 = 127 with 64-bit MSB set (0xFFFFFFFFFFFFFFF6).
    std::vector<std::byte> frame{
      std::byte{ 0x82 }, std::byte{ 0x7f }, std::byte{ 0xff }, std::byte{ 0xff }, std::byte{ 0xff },
      std::byte{ 0xff }, std::byte{ 0xff }, std::byte{ 0xff }, std::byte{ 0xff }, std::byte{ 0xf6 },
      std::byte{ 'a' },  std::byte{ 'b' },  std::byte{ 'c' },  std::byte{ 'd' },
    };
    ws.feed(frame);

    assert_false(cb.errors.empty(), "64-bit frame length with MSB set is rejected");
    assert_true(cb.binaries.empty(), "no binary payload delivered");
  }

  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    // 10-byte header declaring 64 MiB + 1 (0x0000000004000001, high bit clear) without body.
    std::vector<std::byte> header_only{
      std::byte{ 0x82 }, std::byte{ 0x7f }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x00 },
      std::byte{ 0x00 }, std::byte{ 0x04 }, std::byte{ 0x00 }, std::byte{ 0x00 }, std::byte{ 0x01 },
    };
    ws.feed(header_only);

    assert_false(cb.errors.empty(),
                 "frame declaring 64 MiB + 1 with high bit clear fails immediately without body");
    assert_true(cb.binaries.empty(), "no binary payload delivered");
  }

  constexpr std::size_t half_limit = std::size_t{ 32 } * 1024U * 1024U;
  constexpr std::size_t exact_limit = std::size_t{ 64 } * 1024U * 1024U;
  auto first_half = make_unmasked_64bit_frame(0x01, half_limit, std::byte{ 'a' });
  auto second_half_final = make_unmasked_64bit_frame(0x80, half_limit, std::byte{ 'b' });
  auto second_half_cont = make_unmasked_64bit_frame(0x00, half_limit, std::byte{ 'b' });

  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    ws.feed(first_half);
    ws.feed(second_half_final);

    assert_true(cb.errors.empty(), "fragmented message of exactly 64 MiB is accepted");
    assert_eq(cb.texts.size(), std::size_t{ 1 }, "exact 64 MiB fragmented message delivered");
    assert_eq(cb.texts[0].size(), exact_limit, "delivered message has exact 64 MiB size");
  }

  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    auto extra_byte = make_unmasked_frame(0x80, "z");
    ws.feed(first_half);
    ws.feed(second_half_cont);
    ws.feed(extra_byte);

    assert_eq(cb.errors.size(),
              std::size_t{ 1 },
              "fragmented message exceeding 64 MiB reports a decoding error");
    assert_true(cb.texts.empty(), "oversized fragmented message is not delivered");
  }
}

void
coalesced_upgrade_response_and_split_handshake_headers_are_processed([[maybe_unused]] context& ctx)
{
  // 1. Coalesced 101 response + WebSocket text frame in a single feed() call.
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    const auto handshake = fmt::format("HTTP/1.1 101 Switching Protocols\r\n"
                                       "Upgrade: websocket\r\n"
                                       "Connection: Upgrade\r\n"
                                       "Sec-WebSocket-Accept: {}\r\n\r\n",
                                       compute_accept_key(ws.session_key()));
    std::vector<std::byte> coalesced{
      reinterpret_cast<const std::byte*>(handshake.data()),
      reinterpret_cast<const std::byte*>(handshake.data()) + handshake.size(),
    };
    const auto frame = make_unmasked_frame(0x81, "coalesced-after-upgrade");
    coalesced.insert(coalesced.end(), frame.begin(), frame.end());

    ws.feed(coalesced);

    assert_true(cb.ready, "handshake completes");
    assert_true(cb.errors.empty(), "no error on coalesced upgrade and frame");
    assert_eq(cb.texts.size(),
              std::size_t{ 1 },
              "frame coalesced with HTTP 101 upgrade is decoded immediately");
    assert_eq(cb.texts[0], "coalesced-after-upgrade", "coalesced frame payload");
  }

  // 2. HTTP 101 upgrade headers split across multiple feed() calls.
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    const auto accept = compute_accept_key(ws.session_key());
    ws.feed("HTTP/1.1 101 Switch");
    ws.feed("ing Protocols\r\nUpgr");
    ws.feed("ade: webso");
    ws.feed("cket\r\nConnection: Upgrade\r\nSec-WebSocket-Acc");
    ws.feed(fmt::format("ept: {}\r\n\r\n", accept.substr(0, 10)));
    assert_false(cb.ready, "mismatched truncated accept fails");
    assert_eq(cb.errors.size(), std::size_t{ 1 }, "mismatched accept triggers error callback");
  }
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    const auto accept = compute_accept_key(ws.session_key());
    ws.feed("HTTP/1.1 101 Switch");
    ws.feed("ing Protocols\r\nUpgr");
    ws.feed("ade: webso");
    ws.feed("cket\r\nConnection: Upgrade\r\nSec-WebSocket-Acc");
    ws.feed(fmt::format("ept: {}", accept.substr(0, 10)));
    ws.feed(fmt::format("{}\r\n\r\n", accept.substr(10)));
    assert_true(cb.ready, "split header field and value handshake succeeds");
    assert_true(cb.errors.empty(), "no handshake error when headers are split across chunks");
  }
}

void
interleaved_control_frames_and_invalid_framing_sequences([[maybe_unused]] context& ctx)
{
  // 1. Control frame (ping) interleaved in the middle of a fragmented text message.
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    auto f1 = make_unmasked_frame(0x01, "hel");
    auto ping = make_unmasked_frame(0x89, "ping-payload");
    auto f2 = make_unmasked_frame(0x80, "lo");
    ws.feed(f1);
    ws.feed(ping);
    ws.feed(f2);

    assert_true(cb.errors.empty(), "interleaved ping does not fail fragmented message");
    assert_eq(cb.pings.size(), std::size_t{ 1 }, "ping delivered mid-fragmentation");
    assert_eq(cb.pings[0], "ping-payload", "ping payload");
    assert_eq(cb.texts.size(), std::size_t{ 1 }, "reassembled text delivered");
    assert_eq(cb.texts[0], "hello", "reassembled text payload");
  }

  // 2. Unexpected continuation frame when not in a fragmented message fails immediately.
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    std::vector<std::byte> lone_continuation{ std::byte{ 0x80 } };
    ws.feed(lone_continuation);
    assert_false(cb.errors.empty(), "unexpected continuation opcode rejected on first byte");
  }

  // 3. 1-byte close frame payload is rejected (RFC 6455 Section 5.5.1 requires 0 or >= 2 bytes).
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    std::vector<std::byte> one_byte_close{
      std::byte{ 0x88 },
      std::byte{ 0x01 },
      std::byte{ 0x03 },
    };
    ws.feed(one_byte_close);
    assert_false(cb.errors.empty(), "1-byte close frame is rejected");
    assert_true(cb.closes.empty(), "invalid close frame is not delivered");
  }

  // 4. Non-minimal 16-bit extended payload length (< 126) is rejected.
  {
    recording_callbacks cb;
    couchbase::core::websocket_codec ws(&cb);
    complete_handshake(ws);

    std::vector<std::byte> non_minimal_16{
      std::byte{ 0x81 }, std::byte{ 126 }, std::byte{ 0x00 }, std::byte{ 0x05 }, std::byte{ 'h' },
      std::byte{ 'e' },  std::byte{ 'l' }, std::byte{ 'l' },  std::byte{ 'o' },
    };
    ws.feed(non_minimal_16);
    assert_false(cb.errors.empty(), "non-minimal 16-bit payload length is rejected");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(coalesced_frames_in_a_single_chunk_are_delivered_separately) },
      { CASE(a_fragmented_message_retains_the_final_continuation_and_resets_state) },
      { CASE(a_single_byte_frame_header_waits_for_the_second_byte) },
      { CASE(an_oversized_64_bit_payload_length_reports_a_decoding_error) },
      { CASE(coalesced_upgrade_response_and_split_handshake_headers_are_processed) },
      { CASE(interleaved_control_frames_and_invalid_framing_sequences) },
    },
  };
}

} // namespace couchbase::test
