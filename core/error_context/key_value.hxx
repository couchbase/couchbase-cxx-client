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

#pragma once

#include <couchbase/key_value_error_context.hxx>
#include <couchbase/subdocument_error_context.hxx>

#include "core/document_id.hxx"

#include <optional>
#include <set>
#include <string>
#include <system_error>

namespace couchbase::core
{

key_value_error_context
make_key_value_error_context(std::error_code ec, const document_id& id);

template<typename Command, typename Response>
key_value_error_context
make_key_value_error_context(std::error_code ec, std::uint16_t status_code, const Command& command, const Response& response)
{

    const auto& key = command->request.id.key();
    const auto& collection = command->request.id.collection();
    const auto& scope = command->request.id.scope();
    const auto& bucket = command->request.id.bucket();
    std::uint32_t opaque = (ec && response.opaque() == 0) ? command->request.opaque : response.opaque();
    auto status = response.status();
    auto retry_attempts = command->request.retries.retry_attempts();
    auto retry_reasons = command->request.retries.retry_reasons();
    std::optional<std::string> last_dispatched_from{};
    std::optional<std::string> last_dispatched_to{};
    std::optional<key_value_error_map_info> error_map_info{};
    if (command->session_) {
        last_dispatched_from = command->session_->local_address();
        last_dispatched_to = command->session_->remote_address();
        if (status_code) {
            error_map_info = command->session_->decode_error_code(status_code);
        }
    }

    return { ec,
             std::move(last_dispatched_from),
             std::move(last_dispatched_to),
             retry_attempts,
             std::move(retry_reasons),
             key,
             bucket,
             scope,
             collection,
             opaque,
             status,
             response.cas(),
             std::move(error_map_info),
             response.error_info() };
}

subdocument_error_context
make_subdocument_error_context(const key_value_error_context& ctx,
                               std::error_code ec,
                               std::optional<std::string> first_error_path,
                               std::optional<std::uint64_t> first_error_index,
                               bool deleted);

} // namespace couchbase::core
