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

#include <couchbase/errors.hxx>
#include <couchbase/operations/document_mutate_in.hxx>

#include <algorithm>

namespace couchbase::operations
{
std::error_code
mutate_in_request::encode_to(mutate_in_request::encoded_request_type& encoded, mcbp_context&& context)
{
    if (store_semantics == protocol::mutate_in_request_body::store_semantics_type::upsert && !cas.empty()) {
        return error::common_errc::invalid_argument;
    }
    if (create_as_deleted && !context.supports_feature(protocol::hello_feature::subdoc_create_as_deleted)) {
        return error::common_errc::unsupported_operation;
    }
    for (std::size_t i = 0; i < specs.entries.size(); ++i) {
        auto& entry = specs.entries[i];
        entry.original_index = i;
    }
    std::stable_sort(specs.entries.begin(), specs.entries.end(), [](const auto& lhs, const auto& rhs) {
        return (lhs.flags & protocol::mutate_in_request_body::mutate_in_specs::path_flag_xattr) >
               (rhs.flags & protocol::mutate_in_request_body::mutate_in_specs::path_flag_xattr);
    });

    encoded.opaque(opaque);
    encoded.partition(partition);
    encoded.body().id(id);
    encoded.cas(cas);
    if (expiry) {
        encoded.body().expiry(*expiry);
    }
    encoded.body().access_deleted(access_deleted);
    encoded.body().create_as_deleted(create_as_deleted);
    encoded.body().store_semantics(store_semantics);
    encoded.body().specs(specs);
    if (preserve_expiry) {
        encoded.body().preserve_expiry();
    }
    return {};
}

mutate_in_response
mutate_in_request::make_response(error_context::key_value&& ctx, const encoded_response_type& encoded) const
{
    mutate_in_response response{ std::move(ctx) };
    if (encoded.status() == protocol::status::subdoc_success_deleted ||
        encoded.status() == protocol::status::subdoc_multi_path_failure_deleted) {
        response.deleted = true;
    }
    if (!response.ctx.ec) {
        response.fields.resize(specs.entries.size());
        for (size_t i = 0; i < specs.entries.size(); ++i) {
            const auto& req_entry = specs.entries[i];
            response.fields[i].original_index = req_entry.original_index;
            response.fields[i].opcode = protocol::subdoc_opcode(req_entry.opcode);
            response.fields[i].path = req_entry.path;
            response.fields[i].status = protocol::status::success;
        }
        for (const auto& entry : encoded.body().fields()) {
            if (entry.status == protocol::status::success) {
                response.fields[entry.index].value = entry.value;
            } else {
                response.fields[entry.index].status = entry.status;
                response.fields[entry.index].ec =
                  protocol::map_status_code(protocol::client_opcode::subdoc_multi_mutation, std::uint16_t(entry.status));
                response.first_error_index = entry.index;
                response.ctx.ec = response.fields[entry.index].ec;
                break;
            }
        }
        if (!response.ctx.ec) {
            response.cas = encoded.cas();
            response.token = encoded.body().token();
            response.token.partition_id = partition;
            response.token.bucket_name = response.ctx.id.bucket();
        }
        std::sort(response.fields.begin(), response.fields.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.original_index < rhs.original_index;
        });
    } else if (store_semantics == protocol::mutate_in_request_body::store_semantics_type::insert &&
               response.ctx.ec == error::common_errc::cas_mismatch) {
        response.ctx.ec = error::key_value_errc::document_exists;
    }
    return response;
}
} // namespace couchbase::operations
