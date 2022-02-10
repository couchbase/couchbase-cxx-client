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

#include <couchbase/protocol/cmd_mutate_in.hxx>

#include <couchbase/protocol/frame_info_id.hxx>
#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
mutate_in_response_body::parse(protocol::status status,
                               const header_buffer& header,
                               std::uint8_t framing_extras_size,
                               std::uint16_t key_size,
                               std::uint8_t extras_size,
                               const std::vector<uint8_t>& body,
                               const cmd_info&)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success || status == protocol::status::subdoc_multi_path_failure) {
        using offset_type = std::vector<uint8_t>::difference_type;
        offset_type offset = framing_extras_size;
        if (extras_size == 16) {
            memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
            token_.partition_uuid = utils::byte_swap(token_.partition_uuid);
            offset += 8;

            memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
            token_.sequence_number = utils::byte_swap(token_.sequence_number);
            offset += 8;
        } else {
            offset += extras_size;
        }
        offset += key_size;
        fields_.reserve(16); /* we won't have more than 16 entries anyway */
        while (static_cast<std::size_t>(offset) < body.size()) {
            mutate_in_field field;

            field.index = body[static_cast<std::size_t>(offset)];
            offset++;

            std::uint16_t entry_status = 0;
            memcpy(&entry_status, body.data() + offset, sizeof(entry_status));
            entry_status = utils::byte_swap(entry_status);
            Expects(is_valid_status(entry_status));
            field.status = protocol::status(entry_status);
            offset += static_cast<offset_type>(sizeof(entry_status));

            if (field.status == protocol::status::success) {
                std::uint32_t entry_size = 0;
                memcpy(&entry_size, body.data() + offset, sizeof(entry_size));
                entry_size = utils::byte_swap(entry_size);
                Expects(entry_size < 20 * 1024 * 1024);
                offset += static_cast<offset_type>(sizeof(entry_size));

                field.value.resize(entry_size);
                memcpy(field.value.data(), body.data() + offset, entry_size);
                offset += static_cast<offset_type>(entry_size);
            }

            fields_.emplace_back(field);
        }
        return true;
    }
    return false;
}

void
mutate_in_request_body::id(const document_id& id)
{
    key_ = id.key();
    if (id.is_collection_resolved()) {
        utils::unsigned_leb128<uint32_t> encoded(id.collection_uid());
        key_.insert(0, encoded.get());
    }
}

void
mutate_in_request_body::durability(protocol::durability_level level, std::optional<std::uint16_t> timeout)
{
    if (level == protocol::durability_level::none) {
        return;
    }
    auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::durability_requirement);
    auto extras_size = framing_extras_.size();
    if (timeout) {
        framing_extras_.resize(extras_size + 4);
        framing_extras_[extras_size + 0] = static_cast<std::uint8_t>((static_cast<std::uint32_t>(frame_id) << 4U) | 3U);
        framing_extras_[extras_size + 1] = static_cast<std::uint8_t>(level);
        uint16_t val = utils::byte_swap(*timeout);
        memcpy(framing_extras_.data() + extras_size + 2, &val, sizeof(val));
    } else {
        framing_extras_.resize(extras_size + 2);
        framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 1U);
        framing_extras_[extras_size + 1] = static_cast<std::uint8_t>(level);
    }
}

void
mutate_in_request_body::preserve_expiry()
{
    auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::preserve_ttl);
    auto extras_size = framing_extras_.size();
    framing_extras_.resize(extras_size + 1);
    framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 0U);
}

void
mutate_in_request_body::fill_extras()
{
    if (expiry_ != 0) {
        extras_.resize(sizeof(expiry_));
        std::uint32_t field = utils::byte_swap(expiry_);
        memcpy(extras_.data(), &field, sizeof(field));
    }
    if (flags_ != 0) {
        std::size_t offset = extras_.size();
        extras_.resize(offset + sizeof(flags_));
        extras_[offset] = flags_;
    }
}

void
mutate_in_request_body::fill_value()
{
    size_t value_size = 0;
    for (const auto& spec : specs_.entries) {
        value_size +=
          sizeof(spec.opcode) + sizeof(spec.flags) + sizeof(std::uint16_t) + spec.path.size() + sizeof(std::uint32_t) + spec.param.size();
    }
    Expects(value_size > 0);
    value_.resize(value_size);
    std::vector<std::uint8_t>::size_type offset = 0;
    for (auto& spec : specs_.entries) {
        value_[offset++] = spec.opcode;
        value_[offset++] = spec.flags;

        std::uint16_t path_size = utils::byte_swap(gsl::narrow_cast<std::uint16_t>(spec.path.size()));
        std::memcpy(value_.data() + offset, &path_size, sizeof(path_size));
        offset += sizeof(path_size);

        std::uint32_t param_size = utils::byte_swap(gsl::narrow_cast<std::uint32_t>(spec.param.size()));
        std::memcpy(value_.data() + offset, &param_size, sizeof(param_size));
        offset += sizeof(param_size);

        std::memcpy(value_.data() + offset, spec.path.data(), spec.path.size());
        offset += spec.path.size();

        if (param_size != 0u) {
            std::memcpy(value_.data() + offset, spec.param.data(), spec.param.size());
            offset += spec.param.size();
        }
    }
}
} // namespace couchbase::protocol
