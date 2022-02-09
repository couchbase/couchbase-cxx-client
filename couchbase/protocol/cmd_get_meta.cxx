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

#include <couchbase/protocol/cmd_get_meta.hxx>

#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/unsigned_leb128.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
get_meta_response_body::parse(protocol::status status,
                              const header_buffer& header,
                              std::uint8_t framing_extras_size,
                              std::uint16_t /* key_size */,
                              std::uint8_t extras_size,
                              const std::vector<std::uint8_t>& body,
                              const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<std::uint8_t>(opcode));
    if (status == protocol::status::success) {
        std::vector<std::uint8_t>::difference_type offset = framing_extras_size;
        if (extras_size == sizeof(deleted_) + sizeof(flags_) + sizeof(expiry_) + sizeof(sequence_number_) + sizeof(datatype_)) {
            memcpy(&deleted_, body.data() + offset, sizeof(deleted_));
            deleted_ = utils::byte_swap(deleted_);
            offset += 4;

            memcpy(&flags_, body.data() + offset, sizeof(flags_));
            flags_ = utils::byte_swap(flags_);
            offset += 4;

            memcpy(&expiry_, body.data() + offset, sizeof(expiry_));
            expiry_ = utils::byte_swap(expiry_);
            offset += 4;

            memcpy(&sequence_number_, body.data() + offset, sizeof(sequence_number_));
            sequence_number_ = utils::byte_swap(sequence_number_);
            offset += 8;

            datatype_ = body[static_cast<std::size_t>(offset)];
        }
        return true;
    }
    return false;
}

void
get_meta_request_body::id(const document_id& id)
{
    key_ = id.key();
    if (id.is_collection_resolved()) {
        utils::unsigned_leb128<uint32_t> encoded(id.collection_uid());
        key_.insert(0, encoded.get());
    }
}

} // namespace couchbase::protocol
