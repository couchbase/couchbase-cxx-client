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

#include <couchbase/protocol/client_opcode.hxx>

#include <cstdint>
#include <string>
#include <system_error>

namespace couchbase::protocol
{
enum class status : std::uint16_t {
    success = 0x00,
    not_found = 0x01,
    exists = 0x02,
    too_big = 0x03,
    invalid = 0x04,
    not_stored = 0x05,
    delta_bad_value = 0x06,
    not_my_vbucket = 0x07,
    no_bucket = 0x08,
    dcp_stream_not_found = 0x0a,
    opaque_no_match = 0x0b,
    locked = 0x09,
    auth_stale = 0x1f,
    auth_error = 0x20,
    auth_continue = 0x21,
    range_error = 0x22,
    rollback = 0x23,
    no_access = 0x24,
    not_initialized = 0x25,
    rate_limited_network_ingress = 0x30,
    rate_limited_network_egress = 0x31,
    rate_limited_max_connections = 0x32,
    rate_limited_max_commands = 0x33,
    scope_size_limit_exceeded = 0x34,
    unknown_frame_info = 0x80,
    unknown_command = 0x81,
    no_memory = 0x82,
    not_supported = 0x83,
    internal = 0x84,
    busy = 0x85,
    temporary_failure = 0x86,
    xattr_invalid = 0x87,
    unknown_collection = 0x88,
    no_collections_manifest = 0x89,
    cannot_apply_collections_manifest = 0x8a,
    collections_manifest_is_ahead = 0x8b,
    unknown_scope = 0x8c,
    dcp_stream_id_invalid = 0x8d,
    durability_invalid_level = 0xa0,
    durability_impossible = 0xa1,
    sync_write_in_progress = 0xa2,
    sync_write_ambiguous = 0xa3,
    sync_write_re_commit_in_progress = 0xa4,
    subdoc_path_not_found = 0xc0,
    subdoc_path_mismatch = 0xc1,
    subdoc_path_invalid = 0xc2,
    subdoc_path_too_big = 0xc3,
    subdoc_doc_too_deep = 0xc4,
    subdoc_value_cannot_insert = 0xc5,
    subdoc_doc_not_json = 0xc6,
    subdoc_num_range_error = 0xc7,
    subdoc_delta_invalid = 0xc8,
    subdoc_path_exists = 0xc9,
    subdoc_value_too_deep = 0xca,
    subdoc_invalid_combo = 0xcb,
    subdoc_multi_path_failure = 0xcc,
    subdoc_success_deleted = 0xcd,
    subdoc_xattr_invalid_flag_combo = 0xce,
    subdoc_xattr_invalid_key_combo = 0xcf,
    subdoc_xattr_unknown_macro = 0xd0,
    subdoc_xattr_unknown_vattr = 0xd1,
    subdoc_xattr_cannot_modify_vattr = 0xd2,
    subdoc_multi_path_failure_deleted = 0xd3,
    subdoc_invalid_xattr_order = 0xd4,
    subdoc_xattr_unknown_vattr_macro = 0xd5,
    subdoc_can_only_revive_deleted_documents = 0xd6,
    subdoc_deleted_document_cannot_have_value = 0xd7,
};

constexpr bool
is_valid_status(uint16_t code)
{
    switch (status(code)) {
        case status::success:
        case status::not_found:
        case status::exists:
        case status::too_big:
        case status::invalid:
        case status::not_stored:
        case status::delta_bad_value:
        case status::not_my_vbucket:
        case status::no_bucket:
        case status::locked:
        case status::auth_stale:
        case status::auth_error:
        case status::auth_continue:
        case status::range_error:
        case status::rollback:
        case status::no_access:
        case status::not_initialized:
        case status::unknown_frame_info:
        case status::unknown_command:
        case status::no_memory:
        case status::not_supported:
        case status::internal:
        case status::busy:
        case status::temporary_failure:
        case status::xattr_invalid:
        case status::unknown_collection:
        case status::no_collections_manifest:
        case status::cannot_apply_collections_manifest:
        case status::collections_manifest_is_ahead:
        case status::unknown_scope:
        case status::dcp_stream_id_invalid:
        case status::durability_invalid_level:
        case status::durability_impossible:
        case status::sync_write_in_progress:
        case status::sync_write_ambiguous:
        case status::sync_write_re_commit_in_progress:
        case status::subdoc_path_not_found:
        case status::subdoc_path_mismatch:
        case status::subdoc_path_invalid:
        case status::subdoc_path_too_big:
        case status::subdoc_doc_too_deep:
        case status::subdoc_value_cannot_insert:
        case status::subdoc_doc_not_json:
        case status::subdoc_num_range_error:
        case status::subdoc_delta_invalid:
        case status::subdoc_path_exists:
        case status::subdoc_value_too_deep:
        case status::subdoc_invalid_combo:
        case status::subdoc_multi_path_failure:
        case status::subdoc_success_deleted:
        case status::subdoc_xattr_invalid_flag_combo:
        case status::subdoc_xattr_invalid_key_combo:
        case status::subdoc_xattr_unknown_macro:
        case status::subdoc_xattr_unknown_vattr:
        case status::subdoc_xattr_cannot_modify_vattr:
        case status::subdoc_multi_path_failure_deleted:
        case status::subdoc_invalid_xattr_order:
        case status::dcp_stream_not_found:
        case status::opaque_no_match:
        case status::rate_limited_network_ingress:
        case status::rate_limited_network_egress:
        case status::rate_limited_max_connections:
        case status::rate_limited_max_commands:
        case status::scope_size_limit_exceeded:
        case status::subdoc_xattr_unknown_vattr_macro:
        case status::subdoc_can_only_revive_deleted_documents:
        case status::subdoc_deleted_document_cannot_have_value:
            return true;
    }
    return false;
}

[[nodiscard]] std::string
status_to_string(uint16_t code);

[[nodiscard]] std::error_code
map_status_code(protocol::client_opcode opcode, uint16_t status);

} // namespace couchbase::protocol
