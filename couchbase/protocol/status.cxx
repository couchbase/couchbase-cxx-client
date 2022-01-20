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
#include <couchbase/protocol/status.hxx>
#include <couchbase/protocol/status_fmt.hxx>

#include <fmt/core.h>

namespace couchbase::protocol
{
std::string
status_to_string(std::uint16_t code)
{
    if (is_valid_status(code)) {
        return fmt::format("{} ({})", code, static_cast<status>(code));
    }
    return fmt::format("{} (unknown)", code);
}

[[nodiscard]] std::error_code
map_status_code(protocol::client_opcode opcode, uint16_t status)
{
    switch (protocol::status(status)) {
        case protocol::status::success:
        case protocol::status::subdoc_multi_path_failure:
        case protocol::status::subdoc_success_deleted:
        case protocol::status::subdoc_multi_path_failure_deleted:
            return {};

        case protocol::status::not_found:
        case protocol::status::not_stored:
            return error::key_value_errc::document_not_found;

        case protocol::status::exists:
            if (opcode == protocol::client_opcode::insert) {
                return error::key_value_errc::document_exists;
            }
            return error::common_errc::cas_mismatch;

        case protocol::status::too_big:
            return error::key_value_errc::value_too_large;

        case protocol::status::invalid:
        case protocol::status::xattr_invalid:
        case protocol::status::subdoc_invalid_combo:
        case protocol::status::subdoc_deleted_document_cannot_have_value:
            return error::common_errc::invalid_argument;

        case protocol::status::delta_bad_value:
            return error::key_value_errc::delta_invalid;

        case protocol::status::no_bucket:
            return error::common_errc::bucket_not_found;

        case protocol::status::locked:
            return error::key_value_errc::document_locked;

        case protocol::status::auth_stale:
        case protocol::status::auth_error:
        case protocol::status::no_access:
            return error::common_errc::authentication_failure;

        case protocol::status::not_supported:
        case protocol::status::unknown_command:
            return error::common_errc::unsupported_operation;

        case protocol::status::internal:
            return error::common_errc::internal_server_failure;

        case protocol::status::busy:
        case protocol::status::temporary_failure:
        case protocol::status::no_memory:
        case protocol::status::not_initialized:
            return error::common_errc::temporary_failure;

        case protocol::status::unknown_collection:
            return error::common_errc::collection_not_found;

        case protocol::status::unknown_scope:
            return error::common_errc::scope_not_found;

        case protocol::status::durability_invalid_level:
            return error::key_value_errc::durability_level_not_available;

        case protocol::status::durability_impossible:
            return error::key_value_errc::durability_impossible;

        case protocol::status::sync_write_in_progress:
            return error::key_value_errc::durable_write_in_progress;

        case protocol::status::sync_write_ambiguous:
            return error::key_value_errc::durability_ambiguous;

        case protocol::status::sync_write_re_commit_in_progress:
            return error::key_value_errc::durable_write_re_commit_in_progress;

        case protocol::status::subdoc_path_not_found:
            return error::key_value_errc::path_not_found;

        case protocol::status::subdoc_path_mismatch:
            return error::key_value_errc::path_mismatch;

        case protocol::status::subdoc_path_invalid:
            return error::key_value_errc::path_invalid;

        case protocol::status::subdoc_path_too_big:
            return error::key_value_errc::path_too_big;

        case protocol::status::subdoc_doc_too_deep:
            return error::key_value_errc::value_too_deep;

        case protocol::status::subdoc_value_cannot_insert:
            return error::key_value_errc::value_invalid;

        case protocol::status::subdoc_doc_not_json:
            return error::key_value_errc::document_not_json;

        case protocol::status::subdoc_num_range_error:
            return error::key_value_errc::number_too_big;

        case protocol::status::subdoc_delta_invalid:
            return error::key_value_errc::delta_invalid;

        case protocol::status::subdoc_path_exists:
            return error::key_value_errc::path_exists;

        case protocol::status::subdoc_value_too_deep:
            return error::key_value_errc::value_too_deep;

        case protocol::status::subdoc_xattr_invalid_flag_combo:
        case protocol::status::subdoc_xattr_invalid_key_combo:
            return error::key_value_errc::xattr_invalid_key_combo;

        case protocol::status::subdoc_xattr_unknown_macro:
        case protocol::status::subdoc_xattr_unknown_vattr_macro:
            return error::key_value_errc::xattr_unknown_macro;

        case protocol::status::subdoc_xattr_unknown_vattr:
            return error::key_value_errc::xattr_unknown_virtual_attribute;

        case protocol::status::subdoc_xattr_cannot_modify_vattr:
            return error::key_value_errc::xattr_cannot_modify_virtual_attribute;

        case protocol::status::subdoc_can_only_revive_deleted_documents:
            return error::key_value_errc::cannot_revive_living_document;

        case protocol::status::rate_limited_network_ingress:
        case protocol::status::rate_limited_network_egress:
        case protocol::status::rate_limited_max_connections:
        case protocol::status::rate_limited_max_commands:
            return error::common_errc::rate_limited;

        case protocol::status::scope_size_limit_exceeded:
            return error::common_errc::quota_limited;

        case protocol::status::subdoc_invalid_xattr_order:
        case protocol::status::not_my_vbucket:
        case protocol::status::auth_continue:
        case protocol::status::range_error:
        case protocol::status::rollback:
        case protocol::status::unknown_frame_info:
        case protocol::status::no_collections_manifest:
        case protocol::status::cannot_apply_collections_manifest:
        case protocol::status::collections_manifest_is_ahead:
        case protocol::status::dcp_stream_id_invalid:
        case protocol::status::dcp_stream_not_found:
        case protocol::status::opaque_no_match:
            break;
    }
    return error::network_errc::protocol_error;
}
} // namespace couchbase::protocol
