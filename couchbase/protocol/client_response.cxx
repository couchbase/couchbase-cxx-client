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

#include <couchbase/protocol/client_response.hxx>

namespace couchbase::protocol
{
double
parse_server_duration_us(const io::mcbp_message& msg)
{
    if (msg.header.magic != static_cast<std::uint8_t>(magic::alt_client_response)) {
        return 0;
    }
    std::uint8_t framing_extras_size = static_cast<std::uint8_t>(msg.header.keylen & 0xfU);
    if (framing_extras_size == 0) {
        return 0;
    }
    std::size_t offset = 0;
    while (offset < framing_extras_size) {
        std::uint8_t frame_size = static_cast<std::uint8_t>(msg.body[offset] & 0xfU);
        std::uint8_t frame_id = static_cast<std::uint8_t>((static_cast<std::uint32_t>(msg.body[offset]) >> 4U) & 0xfU);
        offset++;
        if (frame_id == static_cast<std::uint8_t>(response_frame_info_id::server_duration)) {
            if (frame_size == 2 && framing_extras_size - offset >= frame_size) {
                std::uint16_t encoded_duration{};
                std::memcpy(&encoded_duration, msg.body.data() + offset, sizeof(encoded_duration));
                encoded_duration = ntohs(encoded_duration);
                return std::pow(encoded_duration, 1.74) / 2;
            }
        }
        offset += frame_size;
    }
    return 0;
}
} // namespace couchbase::protocol
