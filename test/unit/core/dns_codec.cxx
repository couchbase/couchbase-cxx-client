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

#include "core/io/dns_codec.hxx"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::dns::dns_codec;

void
append_uint16(std::vector<std::uint8_t>& buf, std::uint16_t value)
{
  buf.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
  buf.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void
append_uint32(std::vector<std::uint8_t>& buf, std::uint32_t value)
{
  buf.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xffU));
  buf.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xffU));
  buf.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xffU));
  buf.push_back(static_cast<std::uint8_t>(value & 0xffU));
}

void
append_label(std::vector<std::uint8_t>& buf, std::string_view label)
{
  buf.push_back(static_cast<std::uint8_t>(label.size()));
  for (const char ch : label) {
    buf.push_back(static_cast<std::uint8_t>(ch));
  }
}

void
append_pointer(std::vector<std::uint8_t>& buf, std::uint16_t target_offset)
{
  append_uint16(buf, static_cast<std::uint16_t>(0xc000U | (target_offset & 0x3fffU)));
}

void
multi_hop_compression_pointers_preserve_the_outer_record_offset([[maybe_unused]] context& ctx)
{
  std::vector<std::uint8_t> packet{};
  // 12-byte DNS header: id=0x1234, flags=0x8180, qdcount=1, ancount=2, nscount=0, arcount=0
  append_uint16(packet, 0x1234);
  append_uint16(packet, 0x8180);
  append_uint16(packet, 1);
  append_uint16(packet, 2);
  append_uint16(packet, 0);
  append_uint16(packet, 0);

  // Question at offset 12: "_couchbase._tcp.example.com"
  const auto question_offset = static_cast<std::uint16_t>(packet.size());
  append_label(packet, "_couchbase");
  append_label(packet, "_tcp");
  const auto domain_offset = static_cast<std::uint16_t>(packet.size());
  append_label(packet, "example");
  append_label(packet, "com");
  packet.push_back(0x00);
  append_uint16(packet, 33); // QTYPE = SRV
  append_uint16(packet, 1);  // QCLASS = IN

  // Answer 1: name -> pointer to question_offset
  append_pointer(packet, question_offset);
  append_uint16(packet, 33); // TYPE = SRV
  append_uint16(packet, 1);  // CLASS = IN
  append_uint32(packet, 60); // TTL
  // Target is "node1.svc" + pointer to domain_offset ("example.com"):
  // 1+5 ("node1") + 1+3 ("svc") + 2 (ptr) = 12 bytes; plus 6 bytes priority/weight/port = 18 bytes.
  append_uint16(packet, 18);
  append_uint16(packet, 0);     // priority
  append_uint16(packet, 5);     // weight
  append_uint16(packet, 11210); // port
  append_label(packet, "node1");
  const auto svc_offset = static_cast<std::uint16_t>(packet.size());
  append_label(packet, "svc");
  append_pointer(packet, domain_offset);

  // Answer 2: target uses a two-hop pointer chain: "node2" -> pointer to svc_offset -> pointer to
  // domain_offset. Followed by a third check on the decoded answers.
  append_pointer(packet, question_offset);
  append_uint16(packet, 33); // TYPE = SRV
  append_uint16(packet, 1);  // CLASS = IN
  append_uint32(packet, 60); // TTL
  // Target is 1+5 ("node2") + 2 (ptr to svc_offset) = 8 bytes; plus 6 bytes = 14 bytes.
  append_uint16(packet, 14);
  append_uint16(packet, 0);     // priority
  append_uint16(packet, 10);    // weight
  append_uint16(packet, 11211); // port
  append_label(packet, "node2");
  append_pointer(packet, svc_offset);

  // Add a third SRV answer right after Answer 2 so that any corruption of `offset` during Answer
  // 2's multi-hop target decoding causes Answer 3 to misparse.
  packet[7] = 3; // ancount = 3
  append_pointer(packet, question_offset);
  append_uint16(packet, 33); // TYPE = SRV
  append_uint16(packet, 1);  // CLASS = IN
  append_uint32(packet, 60); // TTL
  append_uint16(packet, 14);
  append_uint16(packet, 0);     // priority
  append_uint16(packet, 15);    // weight
  append_uint16(packet, 11212); // port
  append_label(packet, "node3");
  append_pointer(packet, svc_offset);

  const auto msg = dns_codec::decode(packet);
  assert_eq(msg.answers.size(), std::size_t{ 3 }, "all three SRV answers are decoded");
  assert_eq(msg.answers[0].port, std::uint16_t{ 11210 }, "first SRV port");
  assert_eq(msg.answers[0].target.labels,
            (std::vector<std::string>{ "node1", "svc", "example", "com" }),
            "first SRV target");
  assert_eq(msg.answers[1].port, std::uint16_t{ 11211 }, "second SRV port");
  assert_eq(msg.answers[1].target.labels,
            (std::vector<std::string>{ "node2", "svc", "example", "com" }),
            "second SRV target resolved across two compression hops");
  assert_eq(msg.answers[2].port, std::uint16_t{ 11212 }, "third SRV port after multi-hop target");
  assert_eq(msg.answers[2].target.labels,
            (std::vector<std::string>{ "node3", "svc", "example", "com" }),
            "third SRV target");
}

void
cyclic_compression_pointers_terminate_without_hanging([[maybe_unused]] context& ctx)
{
  std::vector<std::uint8_t> packet{};
  append_uint16(packet, 0x0001);
  append_uint16(packet, 0x8180);
  append_uint16(packet, 1); // 1 question
  append_uint16(packet, 0);
  append_uint16(packet, 0);
  append_uint16(packet, 0);
  // Two-node pointer cycle between offset 12 and offset 14
  append_pointer(packet, 14);
  append_pointer(packet, 12);
  append_uint16(packet, 33);
  append_uint16(packet, 1);

  const auto msg = dns_codec::decode(packet);
  assert_true(msg.questions.empty(), "cyclic question name is rejected");
  assert_true(msg.answers.empty(), "no answers decoded");
}

void
truncated_dns_packets_are_rejected_without_reading_past_the_buffer([[maybe_unused]] context& ctx)
{
  // Empty and sub-header packets
  assert_true(dns_codec::decode({}).answers.empty(), "empty buffer returns empty message");
  assert_true(dns_codec::decode({ 0x01, 0x02, 0x03 }).answers.empty(),
              "truncated header returns empty message");

  // Header claiming 2 questions and 2 answers with no body bytes
  std::vector<std::uint8_t> header_only{};
  append_uint16(header_only, 0x1111);
  append_uint16(header_only, 0x8180);
  append_uint16(header_only, 2);
  append_uint16(header_only, 2);
  append_uint16(header_only, 0);
  append_uint16(header_only, 0);
  const auto msg1 = dns_codec::decode(header_only);
  assert_true(msg1.questions.empty(), "missing question records are not fabricated");
  assert_true(msg1.answers.empty(), "missing answer records are not fabricated");

  // Answer with label length exceeding the buffer
  std::vector<std::uint8_t> truncated_label{};
  append_uint16(truncated_label, 0x2222);
  append_uint16(truncated_label, 0x8180);
  append_uint16(truncated_label, 0);
  append_uint16(truncated_label, 1);
  append_uint16(truncated_label, 0);
  append_uint16(truncated_label, 0);
  truncated_label.push_back(50); // claims 50-byte label, only 2 bytes follow
  truncated_label.push_back('a');
  truncated_label.push_back('b');
  const auto msg2 = dns_codec::decode(truncated_label);
  assert_true(msg2.answers.empty(), "truncated label in answer is rejected");

  // 1. Valid question QNAME (terminating in 0x00) followed by 0..3 footer bytes instead of the
  // 4-byte question footer (QTYPE + QCLASS), exercising dns_codec.hxx line 69.
  for (std::size_t footer_bytes = 0; footer_bytes < 4; ++footer_bytes) {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x3333);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1); // qdcount = 1
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_label(pkt, "example");
    append_label(pkt, "com");
    pkt.push_back(0x00); // complete QNAME
    for (std::size_t b = 0; b < footer_bytes; ++b) {
      pkt.push_back(0x01);
    }
    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.questions.empty(), "question with truncated 0..3 byte footer is rejected");
  }

  // 2. Valid question section + valid answer RR NAME followed by 0..9 bytes instead of the 10-byte
  // RR fixed header (TYPE, CLASS, TTL, RDLENGTH), exercising dns_codec.hxx line 92.
  const std::uint8_t sample_rr_header[9] = { 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00 };
  for (std::size_t hdr_bytes = 0; hdr_bytes < 10; ++hdr_bytes) {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x4444);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1); // qdcount = 1
    append_uint16(pkt, 2); // ancount = 2 (Answer 1 valid SRV, Answer 2 truncated RR header)
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);

    // Valid question at offset 12
    const auto q_off = static_cast<std::uint16_t>(pkt.size());
    append_label(pkt, "svc");
    pkt.push_back(0x00);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);

    // Answer 1: complete valid SRV record
    append_pointer(pkt, q_off);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);
    append_uint32(pkt, 60);
    append_uint16(pkt, 12);
    append_uint16(pkt, 0);
    append_uint16(pkt, 5);
    append_uint16(pkt, 11210);
    append_label(pkt, "node");
    pkt.push_back(0x00);

    // Answer 2: complete RR NAME (compression pointer) followed by 0..9 RR-header bytes
    append_pointer(pkt, q_off);
    for (std::size_t b = 0; b < hdr_bytes; ++b) {
      pkt.push_back(sample_rr_header[b]);
    }

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.answers.empty(), "answer with truncated 0..9 byte RR header is rejected");
  }

  // 3. Valid answer RR header declaring rdlength = 20 (for A, AAAA, and SRV) while only 4 bytes
  // remain in the packet, exercising dns_codec.hxx line 116.
  for (const std::uint16_t rr_type :
       { std::uint16_t{ 1 }, std::uint16_t{ 28 }, std::uint16_t{ 33 } }) {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x5555);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1); // qdcount = 1
    append_uint16(pkt, 2); // ancount = 2 (Answer 1 valid SRV, Answer 2 truncated RDATA)
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);

    const auto q_off = static_cast<std::uint16_t>(pkt.size());
    append_label(pkt, "svc");
    pkt.push_back(0x00);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);

    // Answer 1: complete valid SRV record
    append_pointer(pkt, q_off);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);
    append_uint32(pkt, 60);
    append_uint16(pkt, 12);
    append_uint16(pkt, 0);
    append_uint16(pkt, 5);
    append_uint16(pkt, 11210);
    append_label(pkt, "node");
    pkt.push_back(0x00);

    // Answer 2: valid RR header claiming rdlength = 20, but only 4 bytes remain in packet
    append_pointer(pkt, q_off);
    append_uint16(pkt, rr_type);
    append_uint16(pkt, 1);
    append_uint32(pkt, 60);
    append_uint16(pkt, 20);
    pkt.insert(pkt.end(), { 0x01, 0x02, 0x03, 0x04 });

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.answers.empty(), "answer with RDATA shorter than rdlength is rejected");
  }

  // 4. Valid SRV answer RR header declaring rdlength < 7 (0..6 bytes, with rdlength bytes present
  // in the packet, which is shorter than priority + weight + port + 1-byte minimum target),
  // exercising dns_codec.hxx line 128.
  for (std::uint16_t srv_rdlen = 0; srv_rdlen <= 6; ++srv_rdlen) {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x6666);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1); // qdcount = 1
    append_uint16(pkt, 1); // ancount = 1
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);

    const auto q_off = static_cast<std::uint16_t>(pkt.size());
    append_label(pkt, "svc");
    pkt.push_back(0x00);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);

    append_pointer(pkt, q_off);
    append_uint16(pkt, 33); // TYPE = SRV
    append_uint16(pkt, 1);  // CLASS = IN
    append_uint32(pkt, 60); // TTL
    append_uint16(pkt, srv_rdlen);
    for (std::uint16_t b = 0; b < srv_rdlen; ++b) {
      pkt.push_back(0x00);
    }

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.answers.empty(), "SRV answer with rdlength < 7 is rejected");
  }
}

void
pointers_into_header_forward_pointers_and_overlong_names_are_rejected([[maybe_unused]] context& ctx)
{
  // 1. Pointer targeting offset 0 (inside the 12-byte DNS header, where id=0x0161, flags=0x0000
  // matches a 1-byte label "a" followed by a root 0x00 if read as a name).
  {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x0161); // byte 0 = 1, byte 1 = 'a'
    append_uint16(pkt, 0x0000); // byte 2 = 0 (root terminator if read as name)
    append_uint16(pkt, 1);      // 1 question
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_pointer(pkt, 0); // invalid pointer into DNS header
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.questions.empty(), "compression pointer into 12-byte DNS header is rejected");
  }

  // 2. Forward compression pointer (pointing to offset 18 from offset 12).
  {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x1234);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_pointer(pkt, 18); // offset 12..13 jumps forward to offset 18
    append_uint16(pkt, 33);  // offset 14..15
    append_uint16(pkt, 1);   // offset 16..17
    append_label(pkt, "ok"); // offset 18..20
    pkt.push_back(0x00);     // offset 21

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.questions.empty(), "forward compression pointer is rejected");
  }

  // 3. Domain name exceeding 255 octets (5 labels of 52 bytes = 266 wire octets).
  {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x1234);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 1);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    const std::string label52(52, 'a');
    for (int i = 0; i < 5; ++i) {
      append_label(pkt, label52);
    }
    pkt.push_back(0x00);
    append_uint16(pkt, 33);
    append_uint16(pkt, 1);

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.questions.empty(), "domain name exceeding 255 wire octets is rejected");
  }
}

void
srv_records_with_trailing_rdata_bytes_are_rejected_and_non_srv_records_are_skipped(
  [[maybe_unused]] context& ctx)
{
  // 1. SRV record with trailing stray bytes inside RDATA after target root label.
  {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x1234);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 0);
    append_uint16(pkt, 1);
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);
    append_label(pkt, "svc");
    pkt.push_back(0x00);
    append_uint16(pkt, 33); // SRV
    append_uint16(pkt, 1);  // IN
    append_uint32(pkt, 60); // TTL
    append_uint16(pkt,
                  14); // RDLENGTH = 14, but priority(2)+weight(2)+port(2)+"node"(5)+root(1) = 12
    append_uint16(pkt, 0);     // priority
    append_uint16(pkt, 5);     // weight
    append_uint16(pkt, 11210); // port
    append_label(pkt, "node");
    pkt.push_back(0x00);
    pkt.push_back(0xff); // 2 stray trailing bytes inside RDATA
    pkt.push_back(0xff);

    const auto msg = dns_codec::decode(pkt);
    assert_true(msg.answers.empty(), "SRV record with trailing bytes in RDATA is rejected");
  }

  // 2. Non-SRV answer (A record) followed by a valid SRV answer.
  {
    std::vector<std::uint8_t> pkt{};
    append_uint16(pkt, 0x1234);
    append_uint16(pkt, 0x8180);
    append_uint16(pkt, 0);
    append_uint16(pkt, 2); // 2 answers
    append_uint16(pkt, 0);
    append_uint16(pkt, 0);

    // Answer 1: A record (type 1, class 1, rdlength 4)
    append_label(pkt, "host");
    pkt.push_back(0x00);
    append_uint16(pkt, 1); // TYPE = A
    append_uint16(pkt, 1); // CLASS = IN
    append_uint32(pkt, 60);
    append_uint16(pkt, 4);
    pkt.insert(pkt.end(), { 127, 0, 0, 1 });

    // Answer 2: SRV record (type 33, class 1)
    append_label(pkt, "svc");
    pkt.push_back(0x00);
    append_uint16(pkt, 33); // TYPE = SRV
    append_uint16(pkt, 1);  // CLASS = IN
    append_uint32(pkt, 60);
    append_uint16(pkt, 12);
    append_uint16(pkt, 1);
    append_uint16(pkt, 2);
    append_uint16(pkt, 11210);
    append_label(pkt, "node");
    pkt.push_back(0x00);

    const auto msg = dns_codec::decode(pkt);
    assert_eq(msg.answers.size(), std::size_t{ 1 }, "non-SRV answer skipped and SRV answer kept");
    assert_eq(msg.answers[0].port, std::uint16_t{ 11210 }, "SRV port");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(multi_hop_compression_pointers_preserve_the_outer_record_offset) },
      { CASE(cyclic_compression_pointers_terminate_without_hanging) },
      { CASE(truncated_dns_packets_are_rejected_without_reading_past_the_buffer) },
      { CASE(pointers_into_header_forward_pointers_and_overlong_names_are_rejected) },
      { CASE(srv_records_with_trailing_rdata_bytes_are_rejected_and_non_srv_records_are_skipped) },
    },
  };
}

} // namespace couchbase::test
