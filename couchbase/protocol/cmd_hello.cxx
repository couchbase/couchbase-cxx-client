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

#include <couchbase/protocol/cmd_hello.hxx>

#include <couchbase/utils/byteswap.hxx>

#include <cstring>
#include <gsl/assert>

namespace couchbase::protocol
{
bool
hello_response_body::parse(protocol::status status,
                           const header_buffer& header,
                           std::uint8_t framing_extras_size,
                           std::uint16_t key_size,
                           std::uint8_t extras_size,
                           const std::vector<uint8_t>& body,
                           const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success) {
        std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
        size_t value_size = body.size() - static_cast<std::size_t>(offset);
        Expects(value_size % 2 == 0);
        size_t num_features = value_size / 2;
        supported_features_.reserve(num_features);
        const auto* value = body.data() + offset;
        for (size_t i = 0; i < num_features; i++) {
            std::uint16_t field = 0;
            std::memcpy(&field, value + i * 2, sizeof(std::uint16_t));
            field = utils::byte_swap(field);
            if (is_valid_hello_feature(field)) {
                supported_features_.push_back(static_cast<hello_feature>(field));
            }
        }
        return true;
    }
    return false;
}

void
hello_request_body::fill_body()
{
    value_.resize(2 * features_.size());
    for (std::size_t idx = 0; idx < features_.size(); idx++) {
        value_[idx * 2] = 0; // we don't need this byte while feature codes fit the 8-bit
        value_[idx * 2 + 1] = static_cast<uint8_t>(features_[idx]);
    }
}
} // namespace couchbase::protocol
