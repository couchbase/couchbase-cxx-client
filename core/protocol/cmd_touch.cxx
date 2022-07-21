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

#include "cmd_touch.hxx"

#include "core/utils/byteswap.hxx"
#include "core/utils/unsigned_leb128.hxx"

#include <cstring>
#include <gsl/assert>

namespace couchbase::core::protocol
{
bool
touch_response_body::parse(key_value_status_code /* status */,
                           const header_buffer& header,
                           std::uint8_t /* framing_extras_size */,
                           std::uint16_t /* key_size */,
                           std::uint8_t /* extras_size */,
                           const std::vector<std::byte>& /* body */,
                           const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<std::byte>(opcode));
    return false;
}

void
touch_request_body::id(const document_id& id)
{
    key_ = make_protocol_key(id);
}
void
touch_request_body::expiry(std::uint32_t seconds)
{
    extras_.resize(sizeof(seconds));
    seconds = utils::byte_swap(seconds);
    memcpy(extras_.data(), &seconds, sizeof(seconds));
}
} // namespace couchbase::core::protocol
