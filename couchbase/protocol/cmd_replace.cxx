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

#include <gsl/assert>
#include <cstring>

#include <couchbase/protocol/cmd_replace.hxx>

#include <couchbase/protocol/frame_info_id.hxx>
#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

namespace couchbase::protocol
{
bool
replace_response_body::parse(protocol::status status,
                             const header_buffer& header,
                             std::uint8_t framing_extras_size,
                             std::uint16_t /* key_size */,
                             std::uint8_t extras_size,
                             const std::vector<uint8_t>& body,
                             const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success) {
        std::vector<uint8_t>::difference_type offset = framing_extras_size;
        if (extras_size == 16) {
            memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
            token_.partition_uuid = utils::byte_swap_64(token_.partition_uuid);
            offset += 8;

            memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
            token_.sequence_number = utils::byte_swap_64(token_.sequence_number);
            return true;
        }
    }
    return false;
}

void
replace_request_body::id(const document_id& id)
{
    key_ = id.key();
    if (id.is_collection_resolved()) {
        utils::unsigned_leb128<uint32_t> encoded(id.collection_uid());
        key_.insert(0, encoded.get());
    }
}

void
replace_request_body::durability(protocol::durability_level level, std::optional<std::uint16_t> timeout)
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
        uint16_t val = htons(*timeout);
        memcpy(framing_extras_.data() + extras_size + 2, &val, sizeof(val));
    } else {
        framing_extras_.resize(extras_size + 2);
        framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 1U);
        framing_extras_[extras_size + 1] = static_cast<std::uint8_t>(level);
    }
}

void
replace_request_body::preserve_expiry()
{
    auto frame_id = static_cast<uint8_t>(protocol::request_frame_info_id::preserve_ttl);
    auto extras_size = framing_extras_.size();
    framing_extras_.resize(extras_size + 1);
    framing_extras_[extras_size + 0] = static_cast<std::uint8_t>(static_cast<std::uint32_t>(frame_id) << 4U | 0U);
}

void
replace_request_body::fill_extras()
{
    extras_.resize(sizeof(flags_) + sizeof(expiry_));

    uint32_t field = htonl(flags_);
    memcpy(extras_.data(), &field, sizeof(field));

    field = htonl(expiry_);
    memcpy(extras_.data() + sizeof(flags_), &field, sizeof(field));
}
} // namespace couchbase::protocol
