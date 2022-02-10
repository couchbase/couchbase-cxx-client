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

#include <couchbase/protocol/cmd_cluster_map_change_notification.hxx>
#include <couchbase/protocol/cmd_get_cluster_config.hxx>
#include <couchbase/topology/configuration_json.hxx>
#include <couchbase/utils/byteswap.hxx>
#include <couchbase/utils/json.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
cluster_map_change_notification_request_body::parse(const header_buffer& header, const std::vector<uint8_t>& body, const cmd_info& info)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    using offset_type = std::vector<uint8_t>::difference_type;

    uint8_t ext_size = header[4];
    offset_type offset = ext_size;
    if (ext_size == 4) {
        memcpy(&protocol_revision_, body.data(), sizeof(protocol_revision_));
        protocol_revision_ = utils::byte_swap(protocol_revision_);
    }
    uint16_t key_size = 0;
    memcpy(&key_size, header.data() + 2, sizeof(key_size));
    key_size = utils::byte_swap(key_size);
    bucket_.assign(body.begin() + offset, body.begin() + offset + key_size);
    offset += key_size;
    if (body.size() > static_cast<std::size_t>(offset)) {
        config_ = parse_config(std::string(body.begin() + offset, body.end()), info.endpoint_address, info.endpoint_port);
    }
    return true;
}
} // namespace couchbase::protocol
