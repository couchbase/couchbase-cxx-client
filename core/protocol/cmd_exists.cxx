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

#include "cmd_exists.hxx"

#include "core/utils/byteswap.hxx"
#include "core/utils/unsigned_leb128.hxx"

#include <cstring>
#include <gsl/assert>

namespace couchbase::core::protocol
{
bool
exists_response_body::parse(key_value_status_code status,
                            const header_buffer& header,
                            std::uint8_t framing_extras_size,
                            std::uint16_t key_size,
                            std::uint8_t extras_size,
                            const std::vector<std::byte>& body,
                            const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<std::byte>(opcode));
    if (status == key_value_status_code::success) {
        using offset_type = std::vector<std::byte>::difference_type;
        offset_type offset = framing_extras_size + extras_size + key_size;

        memcpy(&partition_id_, body.data() + offset, sizeof(partition_id_));
        partition_id_ = utils::byte_swap(partition_id_);
        offset += static_cast<offset_type>(sizeof(partition_id_));

        std::uint16_t key_len{};
        memcpy(&key_len, body.data() + offset, sizeof(key_len));
        key_len = utils::byte_swap(key_len);
        offset += static_cast<offset_type>(sizeof(key_len));

        key_.resize(key_len);
        memcpy(key_.data(), body.data() + offset, key_len);
        offset += key_len;

        status_ = std::to_integer<std::uint8_t>(body[static_cast<std::size_t>(offset)]);
        offset++;

        memcpy(&cas_, body.data() + offset, sizeof(cas_));
        cas_ = utils::byte_swap(cas_);
    }
    return false;
}

void
exists_request_body::id(std::uint16_t partition_id, const document_id& id)
{
    partition_id_ = partition_id;
    key_ = make_protocol_key(id);
}

void
exists_request_body::fill_body()
{
    std::vector<std::byte>::size_type offset = 0;

    value_.resize(2 * sizeof(std::uint16_t) + key_.size());

    std::uint16_t field = utils::byte_swap(partition_id_);
    memcpy(value_.data() + offset, &field, sizeof(field));
    offset += sizeof(field);

    field = utils::byte_swap(static_cast<std::uint16_t>(key_.size()));
    memcpy(value_.data() + offset, &field, sizeof(field));
    offset += sizeof(field);

    std::memcpy(value_.data() + offset, key_.data(), key_.size());
}
} // namespace couchbase::core::protocol
