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

#include "document_lookup_in.hxx"

#include <couchbase/error_codes.hxx>

namespace couchbase::core::operations
{
std::error_code
lookup_in_request::encode_to(lookup_in_request::encoded_request_type& encoded, mcbp_context&& /* context */)
{
    for (std::size_t i = 0; i < specs.entries.size(); ++i) {
        specs.entries[i].original_index = i;
    }
    std::stable_sort(specs.entries.begin(), specs.entries.end(), [](const auto& lhs, const auto& rhs) {
        return (lhs.flags & protocol::lookup_in_request_body::lookup_in_specs::path_flag_xattr) >
               (rhs.flags & protocol::lookup_in_request_body::lookup_in_specs::path_flag_xattr);
    });

    encoded.opaque(opaque);
    encoded.partition(partition);
    encoded.body().id(id);
    encoded.body().access_deleted(access_deleted);
    encoded.body().specs(specs);
    return {};
}

lookup_in_response
lookup_in_request::make_response(key_value_error_context&& ctx, const encoded_response_type& encoded) const
{
    lookup_in_response response{ std::move(ctx) };
    if (encoded.status() == key_value_status_code::subdoc_success_deleted ||
        encoded.status() == key_value_status_code::subdoc_multi_path_failure_deleted) {
        response.deleted = true;
    }
    if (!response.ctx.ec()) {
        response.fields.resize(specs.entries.size());
        for (size_t i = 0; i < specs.entries.size(); ++i) {
            const auto& req_entry = specs.entries[i];
            response.fields[i].original_index = req_entry.original_index;
            response.fields[i].opcode = static_cast<protocol::subdoc_opcode>(req_entry.opcode);
            response.fields[i].path = req_entry.path;
            response.fields[i].status = key_value_status_code::success;
        }
        for (size_t i = 0; i < encoded.body().fields().size(); ++i) {
            const auto& res_entry = encoded.body().fields()[i];
            response.fields[i].status = res_entry.status;
            response.fields[i].ec =
              protocol::map_status_code(protocol::client_opcode::subdoc_multi_mutation, static_cast<std::uint16_t>(res_entry.status));
            response.fields[i].exists =
              res_entry.status == key_value_status_code::success || res_entry.status == key_value_status_code::subdoc_success_deleted;
            response.fields[i].value = res_entry.value;
            if (!response.fields[i].ec && !response.ctx.ec()) {
                response.ctx.override_ec(response.fields[i].ec);
            }
        }
        if (!response.ctx.ec()) {
            response.cas = encoded.cas();
        }
        std::sort(response.fields.begin(), response.fields.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.original_index < rhs.original_index;
        });
    }
    return response;
}
} // namespace couchbase::core::operations
