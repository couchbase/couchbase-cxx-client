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

// Tests for core/protocol/status.cxx: map_status_code()
//
// Error mapping authority: sdk-rfcs/rfc/0058-error-handling.md
// KV wire status codes:    kv_engine/include/mcbp/protocol/status.h
// Sub-document spec:       sdk-rfcs/rfc/0053-sdk3-crud.md
//
// Each test section cites the RFC-0058 error ID where applicable.

#include "core/protocol/status.hxx"

#include <catch2/catch_test_macros.hpp>
#include <couchbase/error_codes.hxx>

using couchbase::core::key_value_status_code;
using couchbase::core::protocol::client_opcode;
using couchbase::core::protocol::map_status_code;
namespace errc = couchbase::errc;

namespace
{
auto
map(key_value_status_code code, client_opcode opcode = client_opcode::get) -> std::error_code
{
  return map_status_code(opcode, static_cast<std::uint16_t>(code));
}
} // namespace

// ---------------------------------------------------------------------------
// Regression: CXXCBC-787
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code subdoc_invalid_xattr_order maps to invalid_argument", "[unit]")
{
  // RFC-0058 §3 (InvalidArgument) lists 0xcb (subdoc_invalid_combo) as the
  // canonical KV subdoc trigger.  subdoc_invalid_xattr_order (0xd4) is not
  // explicitly listed, but the server describes it as "Invalid XATTR order
  // (xattrs should come first)" — a client-ordering violation.
  // RFC-0053 §MutateIn / §LookupIn both mandate: "The server requires that all
  // Xattr operations must come before any regular operations when sent to the
  // server."  Sending them out of order is therefore an invalid argument from
  // the client, and the correct mapping is errc::common::invalid_argument.
  //
  // Previously this status fell through to errc::network::protocol_error
  // (CXXCBC-787); this test guards against that regression.
  CHECK(map(key_value_status_code::subdoc_invalid_xattr_order) == errc::common::invalid_argument);
}

// ---------------------------------------------------------------------------
// RFC-0058 §3 InvalidArgument
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code invalid_argument group", "[unit]")
{
  // 0x04 generic invalid — RFC-0058 §3
  CHECK(map(key_value_status_code::invalid) == errc::common::invalid_argument);
  // 0x87 xattr_invalid — RFC-0058 §3 (client sent malformed xattr data)
  CHECK(map(key_value_status_code::xattr_invalid) == errc::common::invalid_argument);
  // 0xcb subdoc_invalid_combo — RFC-0058 §3 explicitly: "KV Subdoc: 0xcb"
  CHECK(map(key_value_status_code::subdoc_invalid_combo) == errc::common::invalid_argument);
  // 0xd4 subdoc_invalid_xattr_order — xattr ops must precede body ops
  // (RFC-0053 §MutateIn / §LookupIn ordering requirement)
  CHECK(map(key_value_status_code::subdoc_invalid_xattr_order) == errc::common::invalid_argument);
  // 0xd7 subdoc_deleted_document_cannot_have_value — invalid mutation request
  CHECK(map(key_value_status_code::subdoc_deleted_document_cannot_have_value) ==
        errc::common::invalid_argument);
}

// ---------------------------------------------------------------------------
// Success / partial-success codes — no error
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code success codes return empty error", "[unit]")
{
  // 0x00 success — RFC-0058 implicit (no error)
  CHECK_FALSE(map(key_value_status_code::success));
  // 0xcc subdoc_multi_path_failure — top-level "success" for multi-spec, per-path errors
  // reported separately; RFC-0053 §LookupIn / §MutateIn
  CHECK_FALSE(map(key_value_status_code::subdoc_multi_path_failure));
  // 0xcd subdoc_success_deleted — successful operation on a tombstone
  CHECK_FALSE(map(key_value_status_code::subdoc_success_deleted));
  // 0xd3 subdoc_multi_path_failure_deleted — same as 0xcc on a tombstone
  CHECK_FALSE(map(key_value_status_code::subdoc_multi_path_failure_deleted));
  // range scan terminal codes
  CHECK_FALSE(map(key_value_status_code::range_scan_complete));
  CHECK_FALSE(map(key_value_status_code::range_scan_more));
}

// ---------------------------------------------------------------------------
// RFC-0058 §101 DocumentNotFound / §104 ValueTooLarge / §103 DocumentLocked
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code document errors", "[unit]")
{
  // 0x01 — RFC-0058 §101 DocumentNotFound
  CHECK(map(key_value_status_code::not_found) == errc::key_value::document_not_found);
  // 0x03 — RFC-0058 §104 ValueTooLarge
  CHECK(map(key_value_status_code::too_big) == errc::key_value::value_too_large);
  // 0x09 — RFC-0058 §103 DocumentLocked
  CHECK(map(key_value_status_code::locked) == errc::key_value::document_locked);
  // 0x0e — not_locked (no RFC-0058 ID; SDK-specific)
  CHECK(map(key_value_status_code::not_locked) == errc::key_value::document_not_locked);
}

// ---------------------------------------------------------------------------
// RFC-0058 §105 DocumentExists  /  §9 CasMismatch
// 0x02 (exists) mapping is opcode-dependent
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code exists depends on opcode", "[unit]")
{
  // Insert + exists → document already present: RFC-0058 §105 DocumentExists
  CHECK(map(key_value_status_code::exists, client_opcode::insert) ==
        errc::key_value::document_exists);
  // Replace/remove + exists → CAS mismatch: RFC-0058 §9 CasMismatch
  // "KV: ERR_EXISTS (0x02) when replace or remove with cas"
  CHECK(map(key_value_status_code::exists, client_opcode::replace) == errc::common::cas_mismatch);
}

// ---------------------------------------------------------------------------
// 0x05 (not_stored) is also opcode-dependent
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code not_stored depends on opcode", "[unit]")
{
  // Add (insert) fails because key exists → RFC-0058 §105 DocumentExists
  CHECK(map(key_value_status_code::not_stored, client_opcode::insert) ==
        errc::key_value::document_exists);
  // Other writes fail because key missing → RFC-0058 §101 DocumentNotFound
  CHECK(map(key_value_status_code::not_stored, client_opcode::replace) ==
        errc::key_value::document_not_found);
}

// ---------------------------------------------------------------------------
// 0x09 (locked) is opcode-dependent
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code locked depends on opcode", "[unit]")
{
  // Unlock with wrong CAS → RFC-0058 §9 CasMismatch
  CHECK(map(key_value_status_code::locked, client_opcode::unlock) == errc::common::cas_mismatch);
  // Any other operation on a locked doc → RFC-0058 §103 DocumentLocked
  CHECK(map(key_value_status_code::locked, client_opcode::get) == errc::key_value::document_locked);
}

// ---------------------------------------------------------------------------
// RFC-0058 subdoc path errors: §113–§117, §123
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code subdoc path errors", "[unit]")
{
  // 0xc0 — RFC-0058 §113 PathNotFound
  CHECK(map(key_value_status_code::subdoc_path_not_found) == errc::key_value::path_not_found);
  // 0xc1 — RFC-0058 §114 PathMismatch
  CHECK(map(key_value_status_code::subdoc_path_mismatch) == errc::key_value::path_mismatch);
  // 0xc2 — RFC-0058 §115 PathInvalid
  CHECK(map(key_value_status_code::subdoc_path_invalid) == errc::key_value::path_invalid);
  // 0xc3 — RFC-0058 §116 PathTooBig
  CHECK(map(key_value_status_code::subdoc_path_too_big) == errc::key_value::path_too_big);
  // 0xc4 — RFC-0058 §117 PathTooDeep
  CHECK(map(key_value_status_code::subdoc_doc_too_deep) == errc::key_value::path_too_deep);
  // 0xc9 — RFC-0058 §123 PathExists
  CHECK(map(key_value_status_code::subdoc_path_exists) == errc::key_value::path_exists);
}

// ---------------------------------------------------------------------------
// RFC-0058 subdoc value errors: §118, §119, §120, §121, §122
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code subdoc value errors", "[unit]")
{
  // 0xc5 — RFC-0058 §119 ValueInvalid
  CHECK(map(key_value_status_code::subdoc_value_cannot_insert) == errc::key_value::value_invalid);
  // 0xc6 — RFC-0058 §120 DocumentNotJson
  CHECK(map(key_value_status_code::subdoc_doc_not_json) == errc::key_value::document_not_json);
  // 0xc7 — RFC-0058 §121 NumberTooBig
  CHECK(map(key_value_status_code::subdoc_num_range_error) == errc::key_value::number_too_big);
  // 0xc8 — RFC-0058 §122 DeltaInvalid
  CHECK(map(key_value_status_code::subdoc_delta_invalid) == errc::key_value::delta_invalid);
  // 0xca — RFC-0058 §118 ValueTooDeep
  CHECK(map(key_value_status_code::subdoc_value_too_deep) == errc::key_value::value_too_deep);
}

// ---------------------------------------------------------------------------
// RFC-0058 xattr errors: §124 XattrUnknownMacro, §126 XattrInvalidKeyCombo,
//                        §127 XattrUnknownVirtualAttribute,
//                        §128 XattrCannotModifyVirtualAttribute
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code subdoc xattr errors", "[unit]")
{
  // 0xce — RFC-0058 §126 XattrInvalidKeyCombo
  CHECK(map(key_value_status_code::subdoc_xattr_invalid_flag_combo) ==
        errc::key_value::xattr_invalid_key_combo);
  // 0xcf — RFC-0058 §126 XattrInvalidKeyCombo
  CHECK(map(key_value_status_code::subdoc_xattr_invalid_key_combo) ==
        errc::key_value::xattr_invalid_key_combo);
  // 0xd0 — RFC-0058 §124 XattrUnknownMacro
  CHECK(map(key_value_status_code::subdoc_xattr_unknown_macro) ==
        errc::key_value::xattr_unknown_macro);
  // 0xd5 — RFC-0058 §124 XattrUnknownMacro (vattr macro variant)
  CHECK(map(key_value_status_code::subdoc_xattr_unknown_vattr_macro) ==
        errc::key_value::xattr_unknown_macro);
  // 0xd1 — RFC-0058 §127 XattrUnknownVirtualAttribute
  CHECK(map(key_value_status_code::subdoc_xattr_unknown_vattr) ==
        errc::key_value::xattr_unknown_virtual_attribute);
  // 0xd2 — RFC-0058 §128 XattrCannotModifyVirtualAttribute
  CHECK(map(key_value_status_code::subdoc_xattr_cannot_modify_vattr) ==
        errc::key_value::xattr_cannot_modify_virtual_attribute);
  // 0xd6 — no RFC-0058 ID; SDK-specific CannotReviveLivingDocument
  CHECK(map(key_value_status_code::subdoc_can_only_revive_deleted_documents) ==
        errc::key_value::cannot_revive_living_document);
}

// ---------------------------------------------------------------------------
// RFC-0058 durability errors: §107–§111
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code durability errors", "[unit]")
{
  // 0xa0 — RFC-0058 §107 DurabilityLevelNotAvailable
  CHECK(map(key_value_status_code::durability_invalid_level) ==
        errc::key_value::durability_level_not_available);
  // 0xa1 — RFC-0058 §108 DurabilityImpossible
  CHECK(map(key_value_status_code::durability_impossible) ==
        errc::key_value::durability_impossible);
  // 0xa2 — RFC-0058 §110 DurableWriteInProgress
  CHECK(map(key_value_status_code::sync_write_in_progress) ==
        errc::key_value::durable_write_in_progress);
  // 0xa3 — RFC-0058 §109 DurabilityAmbiguous
  CHECK(map(key_value_status_code::sync_write_ambiguous) == errc::key_value::durability_ambiguous);
  // 0xa4 — RFC-0058 §111 DurableWriteReCommitInProgress
  CHECK(map(key_value_status_code::sync_write_re_commit_in_progress) ==
        errc::key_value::durable_write_re_commit_in_progress);
}

// ---------------------------------------------------------------------------
// RFC-0058 §21 RateLimited / §22 QuotaLimited
// ---------------------------------------------------------------------------

TEST_CASE("unit: map_status_code rate limiting errors", "[unit]")
{
  // 0x30 — RFC-0058 §21 RateLimited (network ingress)
  CHECK(map(key_value_status_code::rate_limited_network_ingress) == errc::common::rate_limited);
  // 0x31 — RFC-0058 §21 RateLimited (network egress)
  CHECK(map(key_value_status_code::rate_limited_network_egress) == errc::common::rate_limited);
  // 0x32 — RFC-0058 §21 RateLimited (max connections)
  CHECK(map(key_value_status_code::rate_limited_max_connections) == errc::common::rate_limited);
  // 0x33 — RFC-0058 §21 RateLimited (max commands)
  CHECK(map(key_value_status_code::rate_limited_max_commands) == errc::common::rate_limited);
  // 0x34 — RFC-0058 §22 QuotaLimited (scope size limit)
  CHECK(map(key_value_status_code::scope_size_limit_exceeded) == errc::common::quota_limited);
}
