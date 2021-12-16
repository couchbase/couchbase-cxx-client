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

#include <couchbase/protocol/status.hxx>

#include <fmt/core.h>

template<>
struct fmt::formatter<couchbase::protocol::status> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(couchbase::protocol::status opcode, FormatContext& ctx) const
    {
        string_view name = "unknown";
        switch (opcode) {
            case couchbase::protocol::status::success:
                name = "success (0x00)";
                break;
            case couchbase::protocol::status::not_found:
                name = "not_found (0x01)";
                break;
            case couchbase::protocol::status::exists:
                name = "exists (0x02)";
                break;
            case couchbase::protocol::status::too_big:
                name = "too_big (0x03)";
                break;
            case couchbase::protocol::status::invalid:
                name = "invalid (0x04)";
                break;
            case couchbase::protocol::status::not_stored:
                name = "not_started (0x05)";
                break;
            case couchbase::protocol::status::delta_bad_value:
                name = "delta_bad_value (0x06)";
                break;
            case couchbase::protocol::status::not_my_vbucket:
                name = "not_my_vbucket (0x07)";
                break;
            case couchbase::protocol::status::no_bucket:
                name = "no_bucket (0x08)";
                break;
            case couchbase::protocol::status::locked:
                name = "locked (0x09)";
                break;
            case couchbase::protocol::status::auth_stale:
                name = "auth_stale (0x1f)";
                break;
            case couchbase::protocol::status::auth_error:
                name = "auth_error (0x20)";
                break;
            case couchbase::protocol::status::auth_continue:
                name = "auth_continue (0x21)";
                break;
            case couchbase::protocol::status::range_error:
                name = "range_error (0x22)";
                break;
            case couchbase::protocol::status::rollback:
                name = "rollback (0x23)";
                break;
            case couchbase::protocol::status::no_access:
                name = "no_access (0x24)";
                break;
            case couchbase::protocol::status::not_initialized:
                name = "not_initialized (0x25)";
                break;
            case couchbase::protocol::status::unknown_frame_info:
                name = "unknown_frame_info (0x80)";
                break;
            case couchbase::protocol::status::unknown_command:
                name = "unknown_command (0x81)";
                break;
            case couchbase::protocol::status::no_memory:
                name = "no_memory (0x82)";
                break;
            case couchbase::protocol::status::not_supported:
                name = "not_supported (0x83)";
                break;
            case couchbase::protocol::status::internal:
                name = "internal (0x84)";
                break;
            case couchbase::protocol::status::busy:
                name = "busy (0x85)";
                break;
            case couchbase::protocol::status::temporary_failure:
                name = "temporary_failure (0x86)";
                break;
            case couchbase::protocol::status::xattr_invalid:
                name = "xattr_invalid (0x87)";
                break;
            case couchbase::protocol::status::unknown_collection:
                name = "unknown_collection (0x88)";
                break;
            case couchbase::protocol::status::no_collections_manifest:
                name = "no_collections_manifest (0x89)";
                break;
            case couchbase::protocol::status::cannot_apply_collections_manifest:
                name = "cannot_apply_collections_manifest (0x8a)";
                break;
            case couchbase::protocol::status::collections_manifest_is_ahead:
                name = "collections_manifest_is_ahead (0x8b)";
                break;
            case couchbase::protocol::status::unknown_scope:
                name = "unknown_scope (0x8c)";
                break;
            case couchbase::protocol::status::dcp_stream_id_invalid:
                name = "dcp_stream_id_invalid (0x8d)";
                break;
            case couchbase::protocol::status::durability_invalid_level:
                name = "durability_invalid_level (0xa0)";
                break;
            case couchbase::protocol::status::durability_impossible:
                name = "durability_impossible (0xa1)";
                break;
            case couchbase::protocol::status::sync_write_in_progress:
                name = "sync_write_in_progress (0xa2)";
                break;
            case couchbase::protocol::status::sync_write_ambiguous:
                name = "sync_write_ambiguous (0xa3)";
                break;
            case couchbase::protocol::status::sync_write_re_commit_in_progress:
                name = "sync_write_re_commit_in_progress (0xa4)";
                break;
            case couchbase::protocol::status::subdoc_path_not_found:
                name = "subdoc_path_not_found (0xc0)";
                break;
            case couchbase::protocol::status::subdoc_path_mismatch:
                name = "subdoc_path_mismatch (0xc1)";
                break;
            case couchbase::protocol::status::subdoc_path_invalid:
                name = "subdoc_path_invalid (0xc2)";
                break;
            case couchbase::protocol::status::subdoc_path_too_big:
                name = "subdoc_path_too_big (0xc3)";
                break;
            case couchbase::protocol::status::subdoc_doc_too_deep:
                name = "subdoc_doc_too_deep (0xc4)";
                break;
            case couchbase::protocol::status::subdoc_value_cannot_insert:
                name = "subdoc_value_cannot_insert (0xc5)";
                break;
            case couchbase::protocol::status::subdoc_doc_not_json:
                name = "subdoc_doc_not_json (0xc6)";
                break;
            case couchbase::protocol::status::subdoc_num_range_error:
                name = "subdoc_num_range_error (0xc7)";
                break;
            case couchbase::protocol::status::subdoc_delta_invalid:
                name = "subdoc_delta_invalid (0xc8)";
                break;
            case couchbase::protocol::status::subdoc_path_exists:
                name = "subdoc_path_exists (0xc9)";
                break;
            case couchbase::protocol::status::subdoc_value_too_deep:
                name = "subdoc_value_too_deep (0xca)";
                break;
            case couchbase::protocol::status::subdoc_invalid_combo:
                name = "subdoc_invalid_combo (0xcb)";
                break;
            case couchbase::protocol::status::subdoc_multi_path_failure:
                name = "subdoc_multi_path_failure (0xcc)";
                break;
            case couchbase::protocol::status::subdoc_success_deleted:
                name = "subdoc_success_deleted (0xcd)";
                break;
            case couchbase::protocol::status::subdoc_xattr_invalid_flag_combo:
                name = "subdoc_xattr_invalid_flag_combo (0xce)";
                break;
            case couchbase::protocol::status::subdoc_xattr_invalid_key_combo:
                name = "subdoc_xattr_invalid_key_combo (0xcf)";
                break;
            case couchbase::protocol::status::subdoc_xattr_unknown_macro:
                name = "subdoc_xattr_unknown_macro (0xd0)";
                break;
            case couchbase::protocol::status::subdoc_xattr_unknown_vattr:
                name = "subdoc_xattr_unknown_vattr (0xd1)";
                break;
            case couchbase::protocol::status::subdoc_xattr_cannot_modify_vattr:
                name = "subdoc_xattr_cannot_modify_vattr (0xd2)";
                break;
            case couchbase::protocol::status::subdoc_multi_path_failure_deleted:
                name = "subdoc_multi_path_failure_deleted (0xd3)";
                break;
            case couchbase::protocol::status::subdoc_invalid_xattr_order:
                name = "subdoc_invalid_xattr_order (0xd4)";
                break;
            case couchbase::protocol::status::dcp_stream_not_found:
                name = "dcp_stream_not_found (0x0a)";
                break;
            case couchbase::protocol::status::opaque_no_match:
                name = "opaque_no_match (0x0b)";
                break;
            case couchbase::protocol::status::rate_limited_network_ingress:
                name = "rate_limited_network_ingress (0x30)";
                break;
            case couchbase::protocol::status::rate_limited_network_egress:
                name = "opaque_no_match (0x31)";
                break;
            case couchbase::protocol::status::rate_limited_max_connections:
                name = "rate_limited_max_connections (0x32)";
                break;
            case couchbase::protocol::status::rate_limited_max_commands:
                name = "rate_limited_max_commands (0x33)";
                break;
            case couchbase::protocol::status::scope_size_limit_exceeded:
                name = "scope_size_limit_exceeded (0x34)";
                break;
            case couchbase::protocol::status::subdoc_xattr_unknown_vattr_macro:
                name = "subdoc_xattr_unknown_vattr_macro (0xd5)";
                break;
            case couchbase::protocol::status::subdoc_can_only_revive_deleted_documents:
                name = "subdoc_can_only_revive_deleted_documents (0xd6)";
                break;
            case couchbase::protocol::status::subdoc_deleted_document_cannot_have_value:
                name = "subdoc_deleted_document_cannot_have_value (0xd7)";
                break;
        }
        return format_to(ctx.out(), "{}", name);
    }
};
