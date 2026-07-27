/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// Translates the core KV operation request/response structs to and from the couchbase.kv.v1
// protobuf messages. The response builders take an already-populated key_value_error_context (the
// component derives it from the gRPC status) and fill the value/cas/token, deliberately not
// reusing the MCBP-bound encode_to/make_response.

#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/operations/document_append.hxx"
#include "core/operations/document_decrement.hxx"
#include "core/operations/document_exists.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_and_lock.hxx"
#include "core/operations/document_get_and_touch.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/operations/document_increment.hxx"
#include "core/operations/document_insert.hxx"
#include "core/operations/document_prepend.hxx"
#include "core/operations/document_remove.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_touch.hxx"
#include "core/operations/document_unlock.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/cas.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/mutation_token.hxx>

#include <couchbase/kv/v1/kv.pb.h>

#include <cstdint>
#include <optional>
#include <string>

namespace couchbase::core::protostellar::kv
{
namespace v1 = ::couchbase::kv::v1;

// Value bytes go through core::utils::to_binary / to_string so the couchbase2 path shares one
// conversion with the rest of core rather than carrying its own memcpy helpers.

template<typename Request>
void
set_location(Request& proto, const document_id& id)
{
  proto.set_bucket_name(id.bucket());
  proto.set_scope_name(id.scope());
  proto.set_collection_name(id.collection());
  proto.set_key(id.key());
}

// The expiry encoding below is dictated by the gateway, which is what translates these messages
// into MCBP and is therefore the authority on what each shape means. Line references are to
// couchbase/stellar-gateway at commit 910af15aefcc094378636972dd22875c5e485ee9, and are pinned to
// the exact file contents by blob hash so they stay checkable after the files move:
//
//   kvserver.go  git cat-file -p dde6353d06439fa4d8e0445f2362e8b460ff1265
//   helpers.go   git cat-file -p 5bd3ad7dcaf3bf2ada354ad9846cf4a87aa73a31
//
// The core request carries expiry with the memcached convention: a value of at most the 30-day
// cutoff is a relative duration in seconds; above it, it is an absolute Unix-epoch timestamp (this
// is how core/impl/expiry.cxx encodes durations >= 30 days and absolute time points).
//
// The proto oneof is not that shape. expiry_secs is *always* a duration: the gateway passes it
// through when it is <= 2592000 and otherwise re-bases it onto "now + N seconds"
// (helpers.go:48-67, secsExpiryToGocbcorex), so an already-absolute core value sent through
// expiry_secs would be read as a ~50-year duration. Absolute values therefore have to use
// expiry_time, which the gateway takes as an epoch second (helpers.go:44-46). The cutoff itself is
// relative on both sides: the gateway's own boundary is `expiry <= 2592000`.
inline constexpr std::uint32_t relative_expiry_cutoff_seconds = 60U * 60U * 24U * 30U; // 2,592,000

// An absent expiry oneof is the gateway's encoding for "preserve the document's existing expiry",
// not for "no expiry": both Upsert and Replace map a nil oneof to opts.PreserveExpiry = true
// (kvserver.go:478-484 and :580-581). "No expiry" must therefore be an explicit zero, which the
// gateway maps to never-expires (helpers.go:49-51); the classic transport serialises the same zero
// into the extras (core/protocol/cmd_upsert.cxx:82-91). Touch and GetAndTouch reject an absent
// oneof outright -- "Expiry time specification is unknown." (kvserver.go:297-303, :131-137) -- so
// the field is not optional there either. The oneof is omitted only when the caller asked to
// preserve, which for ReplaceRequest is the only available encoding: unlike UpsertRequest it has no
// preserve_expiry_on_existing field at all.
template<typename Proto>
void
set_expiry(Proto& proto, std::uint32_t expiry, bool preserve_expiry = false)
{
  if (preserve_expiry) {
    return;
  }
  if (expiry <= relative_expiry_cutoff_seconds) {
    proto.set_expiry_secs(expiry);
  } else {
    proto.mutable_expiry_time()->set_seconds(static_cast<std::int64_t>(expiry));
  }
}

// couchbase::durability_level::none means "no requirement" — the proto field is left unset.
inline auto
to_proto_durability(couchbase::durability_level level) -> std::optional<v1::DurabilityLevel>
{
  switch (level) {
    case couchbase::durability_level::none:
      return std::nullopt;
    case couchbase::durability_level::majority:
      return v1::DURABILITY_LEVEL_MAJORITY;
    case couchbase::durability_level::majority_and_persist_to_active:
      return v1::DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE;
    case couchbase::durability_level::persist_to_majority:
      return v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY;
  }
  return std::nullopt;
}

inline auto
to_core_token(const v1::MutationToken& token) -> couchbase::mutation_token
{
  return couchbase::mutation_token{ token.vbucket_uuid(),
                                    token.seq_no(),
                                    static_cast<std::uint16_t>(token.vbucket_id()),
                                    token.bucket_name() };
}

// Every response that carries a document body puts it in a content oneof, and the generated
// accessor for the inactive arm returns the empty-string singleton. Reading content_uncompressed()
// while the compressed arm is active would therefore produce an empty value with a valid CAS and
// no error -- indistinguishable from an empty document. Compression negotiation lands in
// CXXCBC-905; until then every body-bearing decoder refuses the compressed arm instead of losing
// the value, which is the same fail-closed rule the request side applies.
template<typename Proto>
[[nodiscard]] auto
content_is_compressed(const Proto& proto) -> bool
{
  return proto.content_case() == Proto::kContentCompressed;
}

// ── Get ───────────────────────────────────────────────────────────────────────

inline auto
encode(const operations::get_request& request) -> v1::GetRequest
{
  v1::GetRequest proto;
  set_location(proto, request.id);
  return proto;
}

inline auto
decode(const v1::GetResponse& proto, key_value_error_context ctx) -> operations::get_response
{
  operations::get_response response;
  response.ctx = std::move(ctx);
  if (content_is_compressed(proto)) {
    response.ctx.override_ec(errc::common::feature_not_available);
    return response;
  }
  response.value = utils::to_binary(proto.content_uncompressed());
  response.cas = couchbase::cas{ proto.cas() };
  response.flags = proto.content_flags();
  return response;
}

inline auto
encode(const operations::get_projected_request& request) -> v1::GetRequest
{
  v1::GetRequest proto;
  set_location(proto, request.id);
  for (const auto& path : request.projections) {
    proto.add_project(path);
  }
  return proto;
}

inline auto
decode(const v1::GetResponse& proto,
       key_value_error_context ctx,
       const operations::get_projected_request& /* request */) -> operations::get_projected_response
{
  operations::get_projected_response response;
  response.ctx = std::move(ctx);
  if (content_is_compressed(proto)) {
    response.ctx.override_ec(errc::common::feature_not_available);
    return response;
  }
  response.value = utils::to_binary(proto.content_uncompressed());
  response.cas = couchbase::cas{ proto.cas() };
  response.flags = proto.content_flags();
  if (proto.has_expiry()) {
    response.expiry = static_cast<std::uint32_t>(proto.expiry().seconds());
  }
  return response;
}

// ── Mutations (upsert / insert / replace / remove) ─────────────────────────────

template<typename Response, typename Proto>
inline auto
decode_mutation(const Proto& proto, key_value_error_context ctx) -> Response
{
  Response response;
  response.ctx = std::move(ctx);
  response.cas = couchbase::cas{ proto.cas() };
  if (proto.has_mutation_token()) {
    response.token = to_core_token(proto.mutation_token());
  }
  return response;
}

inline auto
encode(const operations::upsert_request& request) -> v1::UpsertRequest
{
  v1::UpsertRequest proto;
  set_location(proto, request.id);
  proto.set_content_uncompressed(utils::to_string(request.value));
  proto.set_content_flags(request.flags);
  // preserve_expiry is expressed by omitting the oneof, NOT through the proto's
  // preserve_expiry_on_existing flag. The gateway rejects that flag outright whenever the oneof is
  // absent -- "Cannot specify preserve expiry with no expiry, leave expiry undefined to preserve
  // expiry." (kvserver.go:478-482) -- and again when the oneof carries a zero (:498-501), while a
  // nil oneof already means preserve (:484). Setting both is an InvalidArgument, not a redundancy.
  set_expiry(proto, request.expiry, request.preserve_expiry);
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
encode(const operations::insert_request& request) -> v1::InsertRequest
{
  v1::InsertRequest proto;
  set_location(proto, request.id);
  proto.set_content_uncompressed(utils::to_string(request.value));
  proto.set_content_flags(request.flags);
  // insert has no preserve_expiry: a document that does not exist yet has no expiry to preserve.
  set_expiry(proto, request.expiry);
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
encode(const operations::replace_request& request) -> v1::ReplaceRequest
{
  v1::ReplaceRequest proto;
  set_location(proto, request.id);
  proto.set_content_uncompressed(utils::to_string(request.value));
  proto.set_content_flags(request.flags);
  if (request.cas.value() != 0) {
    proto.set_cas(request.cas.value());
  }
  set_expiry(proto, request.expiry, request.preserve_expiry);
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
encode(const operations::remove_request& request) -> v1::RemoveRequest
{
  v1::RemoveRequest proto;
  set_location(proto, request.id);
  if (request.cas.value() != 0) {
    proto.set_cas(request.cas.value());
  }
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
decode(const v1::UpsertResponse& proto, key_value_error_context ctx) -> operations::upsert_response
{
  return decode_mutation<operations::upsert_response>(proto, std::move(ctx));
}

inline auto
decode(const v1::InsertResponse& proto, key_value_error_context ctx) -> operations::insert_response
{
  return decode_mutation<operations::insert_response>(proto, std::move(ctx));
}

inline auto
decode(const v1::ReplaceResponse& proto, key_value_error_context ctx)
  -> operations::replace_response
{
  return decode_mutation<operations::replace_response>(proto, std::move(ctx));
}

inline auto
decode(const v1::RemoveResponse& proto, key_value_error_context ctx) -> operations::remove_response
{
  return decode_mutation<operations::remove_response>(proto, std::move(ctx));
}

// ── Touch / Exists / lock / counters / append / prepend ────────────────────────

inline auto
encode(const operations::touch_request& request) -> v1::TouchRequest
{
  v1::TouchRequest proto;
  set_location(proto, request.id);
  set_expiry(proto, request.expiry);
  return proto;
}

inline auto
decode(const v1::TouchResponse& proto, key_value_error_context ctx) -> operations::touch_response
{
  operations::touch_response response;
  response.ctx = std::move(ctx);
  response.cas = couchbase::cas{ proto.cas() };
  return response;
}

inline auto
encode(const operations::exists_request& request) -> v1::ExistsRequest
{
  v1::ExistsRequest proto;
  set_location(proto, request.id);
  return proto;
}

inline auto
decode(const v1::ExistsResponse& proto, key_value_error_context ctx) -> operations::exists_response
{
  operations::exists_response response;
  response.ctx = std::move(ctx);
  response.document_exists = proto.result();
  response.cas = couchbase::cas{ proto.cas() };
  return response;
}

inline auto
encode(const operations::get_and_lock_request& request) -> v1::GetAndLockRequest
{
  v1::GetAndLockRequest proto;
  set_location(proto, request.id);
  proto.set_lock_time_secs(request.lock_time);
  return proto;
}

inline auto
decode(const v1::GetAndLockResponse& proto, key_value_error_context ctx)
  -> operations::get_and_lock_response
{
  operations::get_and_lock_response response;
  response.ctx = std::move(ctx);
  if (content_is_compressed(proto)) {
    response.ctx.override_ec(errc::common::feature_not_available);
    return response;
  }
  response.value = utils::to_binary(proto.content_uncompressed());
  response.cas = couchbase::cas{ proto.cas() };
  response.flags = proto.content_flags();
  return response;
}

inline auto
encode(const operations::unlock_request& request) -> v1::UnlockRequest
{
  v1::UnlockRequest proto;
  set_location(proto, request.id);
  proto.set_cas(request.cas.value());
  return proto;
}

inline auto
decode(const v1::UnlockResponse& /* proto */, key_value_error_context ctx)
  -> operations::unlock_response
{
  operations::unlock_response response;
  response.ctx = std::move(ctx);
  return response;
}

inline auto
encode(const operations::get_and_touch_request& request) -> v1::GetAndTouchRequest
{
  v1::GetAndTouchRequest proto;
  set_location(proto, request.id);
  set_expiry(proto, request.expiry);
  return proto;
}

inline auto
decode(const v1::GetAndTouchResponse& proto, key_value_error_context ctx)
  -> operations::get_and_touch_response
{
  operations::get_and_touch_response response;
  response.ctx = std::move(ctx);
  if (content_is_compressed(proto)) {
    response.ctx.override_ec(errc::common::feature_not_available);
    return response;
  }
  response.value = utils::to_binary(proto.content_uncompressed());
  response.cas = couchbase::cas{ proto.cas() };
  response.flags = proto.content_flags();
  return response;
}

template<typename Request, typename Proto>
inline void
set_counter_fields(Proto& proto, const Request& request)
{
  set_location(proto, request.id);
  proto.set_delta(request.delta);
  set_expiry(proto, request.expiry);
  if (request.initial_value.has_value()) {
    proto.set_initial(static_cast<std::int64_t>(request.initial_value.value()));
  }
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
}

template<typename Response, typename Proto>
inline auto
decode_counter(const Proto& proto, key_value_error_context ctx) -> Response
{
  Response response;
  response.ctx = std::move(ctx);
  response.content = static_cast<std::uint64_t>(proto.content());
  response.cas = couchbase::cas{ proto.cas() };
  if (proto.has_mutation_token()) {
    response.token = to_core_token(proto.mutation_token());
  }
  return response;
}

inline auto
encode(const operations::increment_request& request) -> v1::IncrementRequest
{
  v1::IncrementRequest proto;
  set_counter_fields(proto, request);
  return proto;
}

inline auto
decode(const v1::IncrementResponse& proto, key_value_error_context ctx)
  -> operations::increment_response
{
  return decode_counter<operations::increment_response>(proto, std::move(ctx));
}

inline auto
encode(const operations::decrement_request& request) -> v1::DecrementRequest
{
  v1::DecrementRequest proto;
  set_counter_fields(proto, request);
  return proto;
}

inline auto
decode(const v1::DecrementResponse& proto, key_value_error_context ctx)
  -> operations::decrement_response
{
  return decode_counter<operations::decrement_response>(proto, std::move(ctx));
}

inline auto
encode(const operations::append_request& request) -> v1::AppendRequest
{
  v1::AppendRequest proto;
  set_location(proto, request.id);
  proto.set_content(utils::to_string(request.value));
  if (request.cas.value() != 0) {
    proto.set_cas(request.cas.value());
  }
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
decode(const v1::AppendResponse& proto, key_value_error_context ctx) -> operations::append_response
{
  return decode_mutation<operations::append_response>(proto, std::move(ctx));
}

inline auto
encode(const operations::prepend_request& request) -> v1::PrependRequest
{
  v1::PrependRequest proto;
  set_location(proto, request.id);
  proto.set_content(utils::to_string(request.value));
  if (request.cas.value() != 0) {
    proto.set_cas(request.cas.value());
  }
  if (const auto level = to_proto_durability(request.durability_level); level) {
    proto.set_durability_level(*level);
  }
  return proto;
}

inline auto
decode(const v1::PrependResponse& proto, key_value_error_context ctx)
  -> operations::prepend_response
{
  return decode_mutation<operations::prepend_response>(proto, std::move(ctx));
}

} // namespace couchbase::core::protostellar::kv
