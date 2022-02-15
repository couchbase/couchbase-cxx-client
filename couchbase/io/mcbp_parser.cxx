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

#include <couchbase/io/mcbp_parser.hxx>

#include <couchbase/logger/logger.hxx>
#include <couchbase/protocol/datatype.hxx>
#include <couchbase/protocol/magic.hxx>
#include <couchbase/utils/byteswap.hxx>

#include <cstring>
#include <gsl/assert>
#include <snappy.h>
#include <spdlog/fmt/bin_to_hex.h>

namespace couchbase::io
{
mcbp_parser::result
mcbp_parser::next(mcbp_message& msg)
{
    static const size_t header_size = 24;
    if (buf.size() < header_size) {
        return result::need_data;
    }
    std::memcpy(&msg.header, buf.data(), header_size);
    std::uint32_t body_size = utils::byte_swap(msg.header.bodylen);
    if (body_size > 0 && buf.size() - header_size < body_size) {
        return result::need_data;
    }
    msg.body.clear();
    msg.body.reserve(body_size);
    std::uint32_t key_size = utils::byte_swap(msg.header.keylen);
    std::uint32_t prefix_size = std::uint32_t(msg.header.extlen) + key_size;
    if (msg.header.magic == static_cast<uint8_t>(protocol::magic::alt_client_response)) {
        std::uint8_t framing_extras_size = msg.header.keylen & 0xfU;
        key_size = (msg.header.keylen & 0xf0U) >> 8U;
        prefix_size = std::uint32_t(framing_extras_size) + std::uint32_t(msg.header.extlen) + key_size;
    }
    std::copy(buf.begin() + header_size, buf.begin() + header_size + prefix_size, std::back_insert_iterator(msg.body));

    bool is_compressed = (msg.header.datatype & static_cast<uint8_t>(protocol::datatype::snappy)) != 0;
    bool use_raw_value = true;
    if (is_compressed) {
        std::string uncompressed;
        size_t offset = header_size + prefix_size;
        bool success = snappy::Uncompress(reinterpret_cast<const char*>(buf.data() + offset), body_size - prefix_size, &uncompressed);
        if (success) {
            std::copy(uncompressed.begin(), uncompressed.end(), std::back_insert_iterator(msg.body));
            use_raw_value = false;
            // patch header with new body size
            msg.header.bodylen = utils::byte_swap(static_cast<std::uint32_t>(prefix_size + uncompressed.size()));
        }
    }
    if (use_raw_value) {
        std::copy(buf.begin() + header_size + prefix_size, buf.begin() + header_size + body_size, std::back_insert_iterator(msg.body));
    }
    buf.erase(buf.begin(), buf.begin() + header_size + body_size);
    if (!buf.empty() && !protocol::is_valid_magic(buf[0])) {
        LOG_WARNING("parsed frame for magic={:x}, opcode={:x}, opaque={}, body_len={}. Invalid magic of the next frame: {:x}, {} "
                    "bytes to parse{}",
                    msg.header.magic,
                    msg.header.opcode,
                    msg.header.opaque,
                    body_size,
                    buf[0],
                    buf.size(),
                    spdlog::to_hex(buf));
        reset();
    }
    return result::ok;
}
} // namespace couchbase::io
