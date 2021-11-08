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

#include <couchbase/protocol/cmd_unlock.hxx>

#include <couchbase/utils/unsigned_leb128.hxx>

namespace couchbase::protocol
{
bool
unlock_response_body::parse(protocol::status,
                            const header_buffer& header,
                            std::uint8_t,
                            std::uint16_t,
                            std::uint8_t,
                            const std::vector<uint8_t>&,
                            const cmd_info&)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    return false;
}

void
unlock_request_body::id(const document_id& id)
{
    key_ = id.key();
    if (id.is_collection_resolved()) {
        utils::unsigned_leb128<uint32_t> encoded(id.collection_uid());
        key_.insert(0, encoded.get());
    }
}
} // namespace couchbase::protocol
