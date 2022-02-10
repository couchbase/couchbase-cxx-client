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

#include <couchbase/protocol/cmd_lookup_in.hxx>

#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
lookup_in_response_body::parse(protocol::status status,
                               const header_buffer& header,
                               std::uint8_t framing_extras_size,
                               std::uint16_t key_size,
                               std::uint8_t extras_size,
                               const std::vector<uint8_t>& body,
                               const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success || status == protocol::status::subdoc_multi_path_failure ||
        status == protocol::status::subdoc_success_deleted || status == protocol::status::subdoc_multi_path_failure_deleted) {
        using offset_type = std::vector<uint8_t>::difference_type;
        offset_type offset = framing_extras_size + key_size + extras_size;
        fields_.reserve(16); /* we won't have more than 16 entries anyway */
        while (static_cast<std::size_t>(offset) < body.size()) {
            lookup_in_field field;

            std::uint16_t entry_status = 0;
            memcpy(&entry_status, body.data() + offset, sizeof(entry_status));
            entry_status = utils::byte_swap(entry_status);
            Expects(is_valid_status(entry_status));
            field.status = protocol::status(entry_status);
            offset += static_cast<offset_type>(sizeof(entry_status));

            std::uint32_t entry_size = 0;
            memcpy(&entry_size, body.data() + offset, sizeof(entry_size));
            entry_size = utils::byte_swap(entry_size);
            Expects(entry_size < 20 * 1024 * 1024);
            offset += static_cast<offset_type>(sizeof(entry_size));

            field.value.resize(entry_size);
            memcpy(field.value.data(), body.data() + offset, entry_size);
            offset += static_cast<offset_type>(entry_size);

            fields_.emplace_back(field);
        }
        return true;
    }
    return false;
}

void
lookup_in_request_body::id(const document_id& id)
{
    key_ = id.key();
    if (id.is_collection_resolved()) {
        utils::unsigned_leb128<uint32_t> encoded(id.collection_uid());
        key_.insert(0, encoded.get());
    }
}

void
lookup_in_request_body::fill_extras()
{
    if (flags_ != 0) {
        extras_.resize(sizeof(flags_));
        extras_[0] = flags_;
    }
}

void
lookup_in_request_body::fill_value()
{
    size_t value_size = 0;
    for (const auto& spec : specs_.entries) {
        value_size += sizeof(spec.opcode) + sizeof(spec.flags) + sizeof(std::uint16_t) + spec.path.size();
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
        std::memcpy(value_.data() + offset, spec.path.data(), spec.path.size());
        offset += spec.path.size();
    }
}
} // namespace couchbase::protocol
