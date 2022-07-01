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

#include <couchbase/protocol/cmd_remove.hxx>

#include <couchbase/protocol/frame_info_utils.hxx>
#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

#include <cstring>

namespace couchbase::protocol
{
bool
remove_response_body::parse(protocol::status status,
                            const header_buffer& header,
                            std::uint8_t framing_extras_size,
                            std::uint16_t /* key_size */,
                            std::uint8_t extras_size,
                            const std::vector<std::byte>& body,
                            const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<std::byte>(opcode));
    if (status == protocol::status::success) {
        using offset_type = std::vector<std::byte>::difference_type;
        offset_type offset = framing_extras_size;
        if (extras_size == 16) {
            memcpy(&token_.partition_uuid, body.data() + offset, sizeof(token_.partition_uuid));
            token_.partition_uuid = utils::byte_swap(token_.partition_uuid);
            offset += 8;

            memcpy(&token_.sequence_number, body.data() + offset, sizeof(token_.sequence_number));
            token_.sequence_number = utils::byte_swap(token_.sequence_number);
        }
        return true;
    }
    return false;
}

void
remove_request_body::id(const document_id& id)
{
    key_ = make_protocol_key(id);
}

void
remove_request_body::durability(protocol::durability_level level, std::optional<std::uint16_t> timeout)
{
    if (level == protocol::durability_level::none) {
        return;
    }

    add_durability_frame_info(framing_extras_, level, timeout);
}
} // namespace couchbase::protocol
