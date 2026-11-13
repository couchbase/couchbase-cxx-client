/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#pragma once

#include "core/utils/byteswap.hxx"
#include "dns_message.hxx"

#include <asio/ip/udp.hpp>
#include <cstring>
#include <optional>

namespace couchbase::core::io::dns
{
class dns_codec
{
public:
  static auto decode(const std::vector<std::uint8_t>& payload) -> dns_message
  {
    constexpr std::size_t header_size = 6 * sizeof(std::uint16_t);
    dns_message message{};
    if (payload.size() < header_size) {
      return message;
    }
    std::size_t offset = 0;

    std::memcpy(&message.header.id, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.id = utils::network_to_host(message.header.id);

    std::uint16_t flags = 0;
    std::memcpy(&flags, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.flags.decode(utils::network_to_host(flags));

    std::memcpy(&message.header.question_records, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.question_records = utils::network_to_host(message.header.question_records);

    std::memcpy(&message.header.answer_records, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.answer_records = utils::network_to_host(message.header.answer_records);

    std::memcpy(&message.header.authority_records, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.authority_records = utils::network_to_host(message.header.authority_records);

    std::memcpy(&message.header.additional_records, payload.data() + offset, sizeof(std::uint16_t));
    offset += sizeof(std::uint16_t);
    message.header.additional_records = utils::network_to_host(message.header.additional_records);

    for (std::uint16_t idx = 0; idx < message.header.question_records; ++idx) {
      question_record qr;
      if (!read_name(payload, offset, qr.name) ||
          payload.size() - offset < 2 * sizeof(std::uint16_t)) {
        message.questions.clear();
        return message;
      }

      std::uint16_t val = 0;
      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      val = utils::network_to_host(val);
      qr.type = resource_type(val);

      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      val = utils::network_to_host(val);
      qr.klass = static_cast<resource_class>(val);

      message.questions.emplace_back(std::move(qr));
    }

    constexpr std::size_t rr_fixed_header_size =
      2 * sizeof(std::uint16_t) + sizeof(std::uint32_t) + sizeof(std::uint16_t);
    for (std::uint16_t idx = 0; idx < message.header.answer_records; ++idx) {
      srv_record ar;
      if (!read_name(payload, offset, ar.name) || payload.size() - offset < rr_fixed_header_size) {
        message.answers.clear();
        return message;
      }

      std::uint16_t val = 0;
      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      val = utils::network_to_host(val);
      ar.type = resource_type(val);

      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      val = utils::network_to_host(val);
      ar.klass = resource_class(val);

      std::memcpy(&ar.ttl, payload.data() + offset, sizeof(std::uint32_t));
      offset += sizeof(std::uint32_t);
      ar.ttl = utils::network_to_host(ar.ttl);

      std::uint16_t size = 0;
      std::memcpy(&size, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      size = utils::network_to_host(size);
      if (payload.size() - offset < size) {
        message.answers.clear();
        return message;
      }
      const std::size_t rdata_end = offset + size;

      if (ar.klass != resource_class::in || ar.type != resource_type::srv) {
        // ignore everything except SRV answers
        offset = rdata_end;
        continue;
      }

      if (size < 3 * sizeof(std::uint16_t) + 1) {
        message.answers.clear();
        return message;
      }

      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      ar.priority = utils::network_to_host(val);

      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      ar.weight = utils::network_to_host(val);

      std::memcpy(&val, payload.data() + offset, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
      ar.port = utils::network_to_host(val);

      if (!read_name(payload, offset, ar.target) || offset != rdata_end) {
        message.answers.clear();
        return message;
      }

      message.answers.emplace_back(std::move(ar));
    }
    return message;
  }

  static auto encode(const dns_message& message) -> std::vector<std::uint8_t>
  {
    std::vector<std::uint8_t> payload;
    payload.resize(message.request_size(), 0);
    std::size_t offset = 0;

    // write header
    {
      std::uint16_t val;

      val = utils::host_to_network(message.header.id);
      std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);

      val = utils::host_to_network(message.header.flags.encode());
      std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);

      val = utils::host_to_network(static_cast<std::uint16_t>(message.questions.size()));
      std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t) +
                3 * sizeof(std::uint16_t); // answer, authority, additional are all zeros
    }

    // write body
    for (const auto& question : message.questions) {
      for (const auto& label : question.name.labels) {
        payload[offset] = static_cast<std::uint8_t>(label.size());
        ++offset;
        std::memcpy(payload.data() + offset, label.data(), label.size());
        offset += label.size();
      }
      payload[offset] = '\0';
      ++offset;

      std::uint16_t val;

      val = utils::host_to_network(static_cast<std::uint16_t>(question.type));
      std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);

      val = utils::host_to_network(static_cast<std::uint16_t>(question.klass));
      std::memcpy(payload.data() + offset, &val, sizeof(std::uint16_t));
      offset += sizeof(std::uint16_t);
    }
    return payload;
  }

private:
  static auto read_name(const std::vector<std::uint8_t>& payload,
                        std::size_t& offset,
                        resource_name& name) -> bool
  {
    constexpr std::size_t header_size = 6 * sizeof(std::uint16_t);
    constexpr std::size_t max_domain_name_wire_size = 255;
    name = {};
    std::optional<std::size_t> save_offset{};
    std::size_t max_jump_target = offset;
    std::size_t total_wire_len = 0;
    std::size_t steps = 0;
    while (offset < payload.size() && steps <= payload.size()) {
      ++steps;
      const std::uint8_t len = payload[offset];
      if (len == 0) {
        if (total_wire_len + 1 > max_domain_name_wire_size) {
          return false;
        }
        offset += 1;
        if (save_offset) {
          // restore offset after the first pointer jump
          offset = *save_offset;
        }
        return true;
      }
      const std::uint8_t label_type = len & 0b1100'0000U;
      if (label_type == 0b1100'0000U) {
        if (payload.size() - offset < sizeof(std::uint16_t)) {
          return false;
        }
        std::uint16_t ptr = 0;
        std::memcpy(&ptr, payload.data() + offset, sizeof(std::uint16_t));
        ptr = utils::network_to_host(ptr);
        ptr &= 0b0011'1111'1111'1111U;
        if (ptr < header_size || ptr >= max_jump_target || ptr >= payload.size()) {
          return false;
        }
        if (!save_offset) {
          save_offset = offset + sizeof(std::uint16_t);
        }
        max_jump_target = ptr;
        offset = ptr;
      } else if (label_type == 0) {
        if (payload.size() - offset - 1 < len ||
            total_wire_len + 1U + len >= max_domain_name_wire_size) {
          return false;
        }
        name.labels.emplace_back(payload.data() + offset + 1, payload.data() + offset + 1 + len);
        total_wire_len += 1U + len;
        offset += 1U + len;
      } else {
        return false;
      }
    }
    return false;
  }
};
} // namespace couchbase::core::io::dns
