/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

// map_status_code() carries a KV wire status into the error the application sees.
//
// Error mapping authority: sdk-rfcs/rfc/0058-error-handling.md
// KV wire status codes:    kv_engine/include/mcbp/protocol/status.h
// Sub-document spec:       sdk-rfcs/rfc/0053-sdk3-crud.md
//
// Each case cites the RFC-0058 error ID where applicable.

#include "framework/test_registry.hxx"

#include "core/protocol/status.hxx"

#include <couchbase/error_codes.hxx>

#include <cstdint>
#include <system_error>

namespace couchbase::test
{
namespace
{
using couchbase::core::key_value_status_code;
using couchbase::core::protocol::client_opcode;
using couchbase::core::protocol::map_status_code;
namespace errc = couchbase::errc;

auto
map(key_value_status_code code, client_opcode opcode = client_opcode::get) -> std::error_code
{
  return map_status_code(opcode, static_cast<std::uint16_t>(code));
}

void
subdoc_invalid_xattr_order_maps_to_invalid_argument([[maybe_unused]] context& ctx)
{
  // RFC-0058 §3 (InvalidArgument) lists 0xcb (subdoc_invalid_combo) as the canonical KV subdoc
  // trigger. subdoc_invalid_xattr_order (0xd4) is not explicitly listed, but the server describes
  // it as "Invalid XATTR order (xattrs should come first)" -- a client-ordering violation.
  // RFC-0053 §MutateIn / §LookupIn both mandate: "The server requires that all Xattr operations
  // must come before any regular operations when sent to the server." Sending them out of order is
  // therefore an invalid argument from the client.
  //
  // Regression: CXXCBC-787, where this status fell through to errc::network::protocol_error.
  assert_eq(map(key_value_status_code::subdoc_invalid_xattr_order),
            errc::common::invalid_argument,
            "xattr operations sent after body operations");
}

void
client_side_violations_map_to_invalid_argument([[maybe_unused]] context& ctx)
{
  // 0x04 generic invalid -- RFC-0058 §3
  assert_eq(map(key_value_status_code::invalid),
            errc::common::invalid_argument,
            "a request the server calls invalid");
  // 0x87 xattr_invalid -- RFC-0058 §3 (client sent malformed xattr data)
  assert_eq(map(key_value_status_code::xattr_invalid),
            errc::common::invalid_argument,
            "malformed xattr data");
  // 0xcb subdoc_invalid_combo -- RFC-0058 §3 explicitly: "KV Subdoc: 0xcb"
  assert_eq(map(key_value_status_code::subdoc_invalid_combo),
            errc::common::invalid_argument,
            "a subdoc spec combination the server rejects");
  // 0xd4 subdoc_invalid_xattr_order -- xattr ops must precede body ops
  // (RFC-0053 §MutateIn / §LookupIn ordering requirement)
  assert_eq(map(key_value_status_code::subdoc_invalid_xattr_order),
            errc::common::invalid_argument,
            "xattr operations sent after body operations");
  // 0xd7 subdoc_deleted_document_cannot_have_value -- invalid mutation request
  assert_eq(map(key_value_status_code::subdoc_deleted_document_cannot_have_value),
            errc::common::invalid_argument,
            "a value staged on a document the same request deletes");
}

void
success_codes_map_to_no_error([[maybe_unused]] context& ctx)
{
  // 0x00 success -- RFC-0058 implicit (no error)
  assert_eq(map(key_value_status_code::success), std::error_code{}, "success");
  // 0xcc subdoc_multi_path_failure -- top-level "success" for multi-spec, per-path errors
  // reported separately; RFC-0053 §LookupIn / §MutateIn
  assert_eq(map(key_value_status_code::subdoc_multi_path_failure),
            std::error_code{},
            "a multi-spec request whose per-path errors are reported per path");
  // 0xcd subdoc_success_deleted -- successful operation on a tombstone
  assert_eq(map(key_value_status_code::subdoc_success_deleted),
            std::error_code{},
            "a subdoc operation on a tombstone");
  // 0xd3 subdoc_multi_path_failure_deleted -- same as 0xcc on a tombstone
  assert_eq(map(key_value_status_code::subdoc_multi_path_failure_deleted),
            std::error_code{},
            "a multi-spec request on a tombstone");
  assert_eq(map(key_value_status_code::range_scan_complete),
            std::error_code{},
            "a range scan that ran to completion");
  assert_eq(map(key_value_status_code::range_scan_more),
            std::error_code{},
            "a range scan with more to deliver");
}

void
document_status_codes_map_to_their_document_errors([[maybe_unused]] context& ctx)
{
  // 0x01 -- RFC-0058 §101 DocumentNotFound
  assert_eq(map(key_value_status_code::not_found),
            errc::key_value::document_not_found,
            "a missing document");
  // 0x03 -- RFC-0058 §104 ValueTooLarge
  assert_eq(
    map(key_value_status_code::too_big), errc::key_value::value_too_large, "an oversized value");
  // 0x09 -- RFC-0058 §103 DocumentLocked
  assert_eq(
    map(key_value_status_code::locked), errc::key_value::document_locked, "a locked document");
  // 0x0e -- not_locked (no RFC-0058 ID; SDK-specific)
  assert_eq(map(key_value_status_code::not_locked),
            errc::key_value::document_not_locked,
            "an unlock of a document that is not locked");
}

void
exists_maps_by_opcode_to_document_exists_or_cas_mismatch([[maybe_unused]] context& ctx)
{
  // Insert + exists -> document already present: RFC-0058 §105 DocumentExists
  assert_eq(map(key_value_status_code::exists, client_opcode::insert),
            errc::key_value::document_exists,
            "an insert onto an existing document");
  // Replace/remove + exists -> CAS mismatch: RFC-0058 §9 CasMismatch
  // "KV: ERR_EXISTS (0x02) when replace or remove with cas"
  assert_eq(map(key_value_status_code::exists, client_opcode::replace),
            errc::common::cas_mismatch,
            "a replace whose CAS no longer matches");
}

void
not_stored_maps_by_opcode_to_document_exists_or_not_found([[maybe_unused]] context& ctx)
{
  // Add (insert) fails because key exists -> RFC-0058 §105 DocumentExists
  assert_eq(map(key_value_status_code::not_stored, client_opcode::insert),
            errc::key_value::document_exists,
            "an insert onto an existing document");
  // Other writes fail because key missing -> RFC-0058 §101 DocumentNotFound
  assert_eq(map(key_value_status_code::not_stored, client_opcode::replace),
            errc::key_value::document_not_found,
            "a replace of a missing document");
}

void
locked_maps_by_opcode_to_cas_mismatch_or_document_locked([[maybe_unused]] context& ctx)
{
  // Unlock with wrong CAS -> RFC-0058 §9 CasMismatch
  assert_eq(map(key_value_status_code::locked, client_opcode::unlock),
            errc::common::cas_mismatch,
            "an unlock presenting the wrong CAS");
  // Any other operation on a locked doc -> RFC-0058 §103 DocumentLocked
  assert_eq(map(key_value_status_code::locked, client_opcode::get),
            errc::key_value::document_locked,
            "a read of a locked document");
}

void
subdoc_path_status_codes_map_to_their_path_errors([[maybe_unused]] context& ctx)
{
  // 0xc0 -- RFC-0058 §113 PathNotFound
  assert_eq(map(key_value_status_code::subdoc_path_not_found),
            errc::key_value::path_not_found,
            "a path that is not in the document");
  // 0xc1 -- RFC-0058 §114 PathMismatch
  assert_eq(map(key_value_status_code::subdoc_path_mismatch),
            errc::key_value::path_mismatch,
            "a path that meets the wrong kind of node");
  // 0xc2 -- RFC-0058 §115 PathInvalid
  assert_eq(map(key_value_status_code::subdoc_path_invalid),
            errc::key_value::path_invalid,
            "a syntactically invalid path");
  // 0xc3 -- RFC-0058 §116 PathTooBig
  assert_eq(map(key_value_status_code::subdoc_path_too_big),
            errc::key_value::path_too_big,
            "a path longer than the server accepts");
  // 0xc4 -- RFC-0058 §117 PathTooDeep
  assert_eq(map(key_value_status_code::subdoc_doc_too_deep),
            errc::key_value::path_too_deep,
            "a document nested deeper than the server walks");
  // 0xc9 -- RFC-0058 §123 PathExists
  assert_eq(map(key_value_status_code::subdoc_path_exists),
            errc::key_value::path_exists,
            "a path an insert requires to be absent");
}

void
subdoc_value_status_codes_map_to_their_value_errors([[maybe_unused]] context& ctx)
{
  // 0xc5 -- RFC-0058 §119 ValueInvalid
  assert_eq(map(key_value_status_code::subdoc_value_cannot_insert),
            errc::key_value::value_invalid,
            "a value the server cannot insert at the path");
  // 0xc6 -- RFC-0058 §120 DocumentNotJson
  assert_eq(map(key_value_status_code::subdoc_doc_not_json),
            errc::key_value::document_not_json,
            "a subdoc operation on a document that is not JSON");
  // 0xc7 -- RFC-0058 §121 NumberTooBig
  assert_eq(map(key_value_status_code::subdoc_num_range_error),
            errc::key_value::number_too_big,
            "a counter beyond the range the server holds");
  // 0xc8 -- RFC-0058 §122 DeltaInvalid
  assert_eq(map(key_value_status_code::subdoc_delta_invalid),
            errc::key_value::delta_invalid,
            "a counter delta the server rejects");
  // 0xca -- RFC-0058 §118 ValueTooDeep
  assert_eq(map(key_value_status_code::subdoc_value_too_deep),
            errc::key_value::value_too_deep,
            "a value nested deeper than the server accepts");
}

void
subdoc_xattr_status_codes_map_to_their_xattr_errors([[maybe_unused]] context& ctx)
{
  // 0xce -- RFC-0058 §126 XattrInvalidKeyCombo
  assert_eq(map(key_value_status_code::subdoc_xattr_invalid_flag_combo),
            errc::key_value::xattr_invalid_key_combo,
            "an xattr flag combination the server rejects");
  // 0xcf -- RFC-0058 §126 XattrInvalidKeyCombo
  assert_eq(map(key_value_status_code::subdoc_xattr_invalid_key_combo),
            errc::key_value::xattr_invalid_key_combo,
            "an xattr key combination the server rejects");
  // 0xd0 -- RFC-0058 §124 XattrUnknownMacro
  assert_eq(map(key_value_status_code::subdoc_xattr_unknown_macro),
            errc::key_value::xattr_unknown_macro,
            "a macro the server does not expand");
  // 0xd5 -- RFC-0058 §124 XattrUnknownMacro (vattr macro variant)
  assert_eq(map(key_value_status_code::subdoc_xattr_unknown_vattr_macro),
            errc::key_value::xattr_unknown_macro,
            "a virtual-attribute macro the server does not expand");
  // 0xd1 -- RFC-0058 §127 XattrUnknownVirtualAttribute
  assert_eq(map(key_value_status_code::subdoc_xattr_unknown_vattr),
            errc::key_value::xattr_unknown_virtual_attribute,
            "a virtual attribute the server does not provide");
  // 0xd2 -- RFC-0058 §128 XattrCannotModifyVirtualAttribute
  assert_eq(map(key_value_status_code::subdoc_xattr_cannot_modify_vattr),
            errc::key_value::xattr_cannot_modify_virtual_attribute,
            "a write to a read-only virtual attribute");
  // 0xd6 -- no RFC-0058 ID; SDK-specific CannotReviveLivingDocument
  assert_eq(map(key_value_status_code::subdoc_can_only_revive_deleted_documents),
            errc::key_value::cannot_revive_living_document,
            "a revive of a document that is not a tombstone");
}

void
durability_status_codes_map_to_their_durability_errors([[maybe_unused]] context& ctx)
{
  // 0xa0 -- RFC-0058 §107 DurabilityLevelNotAvailable
  assert_eq(map(key_value_status_code::durability_invalid_level),
            errc::key_value::durability_level_not_available,
            "a durability level the cluster does not offer");
  // 0xa1 -- RFC-0058 §108 DurabilityImpossible
  assert_eq(map(key_value_status_code::durability_impossible),
            errc::key_value::durability_impossible,
            "a durability level the topology cannot satisfy");
  // 0xa2 -- RFC-0058 §110 DurableWriteInProgress
  assert_eq(map(key_value_status_code::sync_write_in_progress),
            errc::key_value::durable_write_in_progress,
            "a durable write already in flight for the key");
  // 0xa3 -- RFC-0058 §109 DurabilityAmbiguous
  assert_eq(map(key_value_status_code::sync_write_ambiguous),
            errc::key_value::durability_ambiguous,
            "a durable write whose outcome the server cannot report");
  // 0xa4 -- RFC-0058 §111 DurableWriteReCommitInProgress
  assert_eq(map(key_value_status_code::sync_write_re_commit_in_progress),
            errc::key_value::durable_write_re_commit_in_progress,
            "a durable write being re-committed after a topology change");
}

void
limit_status_codes_map_to_rate_limited_or_quota_limited([[maybe_unused]] context& ctx)
{
  // 0x30 -- RFC-0058 §21 RateLimited (network ingress)
  assert_eq(map(key_value_status_code::rate_limited_network_ingress),
            errc::common::rate_limited,
            "an ingress rate limit");
  // 0x31 -- RFC-0058 §21 RateLimited (network egress)
  assert_eq(map(key_value_status_code::rate_limited_network_egress),
            errc::common::rate_limited,
            "an egress rate limit");
  // 0x32 -- RFC-0058 §21 RateLimited (max connections)
  assert_eq(map(key_value_status_code::rate_limited_max_connections),
            errc::common::rate_limited,
            "a connection count limit");
  // 0x33 -- RFC-0058 §21 RateLimited (max commands)
  assert_eq(map(key_value_status_code::rate_limited_max_commands),
            errc::common::rate_limited,
            "a command rate limit");
  // 0x34 -- RFC-0058 §22 QuotaLimited (scope size limit)
  assert_eq(map(key_value_status_code::scope_size_limit_exceeded),
            errc::common::quota_limited,
            "a scope that has reached its size quota");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(subdoc_invalid_xattr_order_maps_to_invalid_argument) },
      { CASE(client_side_violations_map_to_invalid_argument) },
      { CASE(success_codes_map_to_no_error) },
      { CASE(document_status_codes_map_to_their_document_errors) },
      { CASE(exists_maps_by_opcode_to_document_exists_or_cas_mismatch) },
      { CASE(not_stored_maps_by_opcode_to_document_exists_or_not_found) },
      { CASE(locked_maps_by_opcode_to_cas_mismatch_or_document_locked) },
      { CASE(subdoc_path_status_codes_map_to_their_path_errors) },
      { CASE(subdoc_value_status_codes_map_to_their_value_errors) },
      { CASE(subdoc_xattr_status_codes_map_to_their_xattr_errors) },
      { CASE(durability_status_codes_map_to_their_durability_errors) },
      { CASE(limit_status_codes_map_to_rate_limited_or_quota_limited) },
    },
  };
}

} // namespace couchbase::test
