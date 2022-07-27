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

#include "cmd_sasl_step.hxx"

#include "core/utils/binary.hxx"

#include <gsl/assert>

#include <algorithm>

namespace couchbase::core::protocol
{
bool
sasl_step_response_body::parse(key_value_status_code status,
                               const header_buffer& header,
                               std::uint8_t framing_extras_size,
                               std::uint16_t key_size,
                               std::uint8_t extras_size,
                               const std::vector<std::byte>& body,
                               const cmd_info& /* info */)
{
    Expects(header[1] == static_cast<std::byte>(opcode));
    if (status == key_value_status_code::success) {
        std::transform(body.begin() + framing_extras_size + extras_size + key_size,
                       body.end(),
                       std::back_insert_iterator(value_),
                       [](auto b) { return static_cast<char>(b); });
        return true;
    }
    return false;
}

void
sasl_step_request_body::mechanism(std::string_view mech)
{
    key_.reserve(mech.size());
    utils::to_binary(mech, std::back_insert_iterator(key_));
}

void
sasl_step_request_body::sasl_data(std::string_view data)
{
    value_.reserve(data.size());
    utils::to_binary(data, std::back_insert_iterator(value_));
}
} // namespace couchbase::core::protocol
