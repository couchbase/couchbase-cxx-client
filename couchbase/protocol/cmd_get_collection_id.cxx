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

#include <couchbase/protocol/cmd_get_collection_id.hxx>

#include <couchbase/utils/byteswap.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
get_collection_id_response_body::parse(protocol::status status,
                                       const header_buffer& header,
                                       std::uint8_t framing_extras_size,
                                       std::uint16_t key_size,
                                       std::uint8_t extras_size,
                                       const std::vector<uint8_t>& body,
                                       const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success && extras_size == 12) {
        std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size;

        std::memcpy(&manifest_uid_, body.data() + offset, sizeof(manifest_uid_));
        manifest_uid_ = utils::byte_swap(manifest_uid_);
        offset += 8;

        std::memcpy(&collection_uid_, body.data() + offset, sizeof(collection_uid_));
        collection_uid_ = utils::byte_swap(collection_uid_);
        return true;
    }
    return false;
}
} // namespace couchbase::protocol
