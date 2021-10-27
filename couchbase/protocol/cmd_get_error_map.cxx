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

#include <spdlog/spdlog.h>

#include <couchbase/protocol/cmd_get_error_map.hxx>

#include <couchbase/utils/byteswap.hxx>

#include <couchbase/utils/json.hxx>
#include <couchbase/topology/error_map_json.hxx>

namespace couchbase::protocol
{
bool
get_error_map_response_body::parse(protocol::status status,
                                   const header_buffer& header,
                                   std::uint8_t framing_extras_size,
                                   std::uint16_t key_size,
                                   std::uint8_t extras_size,
                                   const std::vector<uint8_t>& body,
                                   const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success) {
        try {
            std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
            errmap_ = utils::json::parse(std::string(body.begin() + offset, body.end())).as<error_map>();
        } catch (const tao::pegtl::parse_error& e) {
            spdlog::debug("unable to parse error map as JSON: {}, {}", e.message(), std::string(body.begin(), body.end()));
        }
        return true;
    }
    return false;
}

void
get_error_map_request_body::fill_body()
{
    std::uint16_t version = htons(version_);
    value_.resize(sizeof(version));
    std::memcpy(value_.data(), &version, sizeof(version));
}
} // namespace couchbase::protocol
