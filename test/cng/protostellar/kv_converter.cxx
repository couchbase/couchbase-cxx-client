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

// Unit tests for the KV converter (CXXCBC-892). Pure translation, no server (env-agnostic).

#include "framework/test_runner.hxx"

#include "core/protostellar/kv_converter.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/error_codes.hxx>

#include <array>
#include <cstdint>
#include <string>

namespace couchbase::cng::test
{
namespace
{
namespace pk = ::couchbase::core::protostellar::kv;
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::kv::v1;
namespace cu = ::couchbase::core::utils;
using ::couchbase::core::document_id;
using ::couchbase::core::key_value_error_context;

auto
test_id() -> document_id
{
  return document_id{ "b", "s", "c", "k" };
}

void
get_request_encodes_location()
{
  ops::get_request request;
  request.id = document_id{ "travel-sample", "inventory", "airline", "airline_10" };
  const auto proto = pk::encode(request);
  assert_eq(proto.bucket_name(), std::string{ "travel-sample" }, "bucket");
  assert_eq(proto.scope_name(), std::string{ "inventory" }, "scope");
  assert_eq(proto.collection_name(), std::string{ "airline" }, "collection");
  assert_eq(proto.key(), std::string{ "airline_10" }, "key");
}

void
get_response_decodes_value_cas_flags()
{
  v1::GetResponse proto;
  proto.set_content_uncompressed("{\"type\":\"airline\"}");
  proto.set_cas(0x1234'5678'9abc'def0ULL);
  proto.set_content_flags(0x0200'0006U);
  const auto response = pk::decode(proto, key_value_error_context{});
  assert_false(static_cast<bool>(response.ctx.ec()), "uncompressed content decodes without error");
  assert_eq(cu::to_string(response.value), std::string{ "{\"type\":\"airline\"}" }, "value");
  assert_eq(response.cas.value(), 0x1234'5678'9abc'def0ULL, "cas");
  assert_eq(response.flags, static_cast<std::uint32_t>(0x0200'0006), "flags");
}

// GetResponse.content is a oneof, and the accessor for the inactive arm returns the empty-string
// singleton. Reading content_uncompressed() while the compressed arm is active would yield an empty
// value with a valid CAS and no error, which no caller can tell apart from an empty document. Until
// snappy negotiation lands (CXXCBC-905) the decoder has to fail closed.
void
get_response_refuses_compressed_content()
{
  v1::GetResponse proto;
  proto.set_content_compressed("not-really-snappy");
  proto.set_cas(0x1234ULL);
  proto.set_content_flags(0x0200'0006U);

  const auto response = pk::decode(proto, key_value_error_context{});
  assert_true(response.ctx.ec() == errc::common::feature_not_available,
              "the compressed arm is refused rather than decoded as an empty value");
  assert_true(response.value.empty(), "no partial value is handed back");
}

// The three branches of set_expiry, plus the exact cutoff. Every expectation below is behaviour
// read out of the gateway; core/protostellar/kv_converter.hxx pins the commit and the blob hashes
// the kvserver.go / helpers.go line references are against.
//
//   * a nil expiry oneof means "preserve the existing expiry" for both Upsert and Replace
//     (kvserver.go:478-484, :580-581), so "no expiry" has to be an explicit zero or a default
//     upsert/replace inherits a TTL that couchbase:// clears;
//   * expiry_secs == 0 is never-expires (helpers.go:49-51);
//   * expiry_secs is always a *duration* -- passed through when <= 2592000 and otherwise re-based
//     onto now (helpers.go:60-66) -- so a core value above the cutoff, which is already an absolute
//     epoch second, must travel in expiry_time instead (helpers.go:44-46);
//   * the gateway's own relative/absolute boundary is `expiry <= 2592000`, which is why 2592000
//     itself belongs on the relative arm.
struct expiry_case {
  std::uint32_t expiry;
  bool relative;
  std::uint32_t seconds;
  std::int64_t timestamp_seconds;
};

constexpr std::array<expiry_case, 4> expiry_cases{ {
  { 0U, true, 0U, 0 },
  { 1U, true, 1U, 0 },
  { 2'592'000U, true, 2'592'000U, 0 }, // exactly 30 days is still relative
  { 2'592'001U, false, 0U, 2'592'001 },
} };

template<typename Proto>
void
assert_expiry(const Proto& proto, const expiry_case& expected, const std::string& what)
{
  assert_true(proto.expiry_case() != Proto::EXPIRY_NOT_SET,
              what + ": the expiry oneof must be set unless preserve_expiry was requested");
  if (expected.relative) {
    assert_true(proto.expiry_case() == Proto::kExpirySecs, what + ": relative arm selected");
    assert_eq(proto.expiry_secs(), expected.seconds, what + ": expiry seconds");
  } else {
    assert_true(proto.expiry_case() == Proto::kExpiryTime, what + ": absolute arm selected");
    assert_eq(proto.expiry_time().seconds(), expected.timestamp_seconds, what + ": expiry time");
  }
}

// upsert, insert and replace all share set_expiry, so every branch is pinned on each of them.
void
expiry_encodes_every_branch_explicitly()
{
  for (const auto& expected : expiry_cases) {
    const auto label = "expiry=" + std::to_string(expected.expiry);

    ops::upsert_request upsert;
    upsert.id = test_id();
    upsert.value = cu::to_binary("v");
    upsert.expiry = expected.expiry;
    assert_expiry(pk::encode(upsert), expected, "upsert " + label);

    ops::insert_request insert;
    insert.id = test_id();
    insert.value = cu::to_binary("v");
    insert.expiry = expected.expiry;
    assert_expiry(pk::encode(insert), expected, "insert " + label);

    ops::replace_request replace;
    replace.id = test_id();
    replace.value = cu::to_binary("v");
    replace.expiry = expected.expiry;
    assert_expiry(pk::encode(replace), expected, "replace " + label);
  }
}

// preserve_expiry wins over expiry, which is what upsert_options and replace_options document, and
// the gateway's encoding for it is an omitted oneof (kvserver.go:484 for upsert, :581 for replace).
void
preserve_expiry_omits_the_expiry_oneof()
{
  ops::upsert_request upsert;
  upsert.id = test_id();
  upsert.value = cu::to_binary("v");
  upsert.expiry = 300U;
  upsert.preserve_expiry = true;
  const auto upsert_proto = pk::encode(upsert);
  assert_true(upsert_proto.expiry_case() == v1::UpsertRequest::EXPIRY_NOT_SET,
              "upsert expresses preserve by omitting the expiry oneof");

  // ReplaceRequest has no preserve_expiry_on_existing field at all, so an absent oneof is the only
  // available encoding there.
  ops::replace_request replace;
  replace.id = test_id();
  replace.value = cu::to_binary("v");
  replace.expiry = 300U;
  replace.preserve_expiry = true;
  assert_true(pk::encode(replace).expiry_case() == v1::ReplaceRequest::EXPIRY_NOT_SET,
              "replace expresses preserve by omitting the expiry oneof");

  // ...and with the flag off, a default replace must clear the TTL rather than inherit it.
  ops::replace_request plain;
  plain.id = test_id();
  plain.value = cu::to_binary("v");
  const auto plain_proto = pk::encode(plain);
  assert_true(plain_proto.expiry_case() == v1::ReplaceRequest::kExpirySecs,
              "a default replace sends an explicit expiry");
  assert_eq(plain_proto.expiry_secs(), static_cast<std::uint32_t>(0), "...and it is zero");
}

// The gateway rejects two Upsert shapes with InvalidArgument, so the encoder must never emit them
// for any combination of the two inputs it derives them from:
//
//   * preserve_expiry_on_existing set while the expiry oneof is absent -- "Cannot specify preserve
//     expiry with no expiry, leave expiry undefined to preserve expiry." (kvserver.go:478-482);
//   * preserve_expiry_on_existing set while the oneof carries a zero (kvserver.go:498-501).
//
// Both are avoided by never setting the flag: an omitted oneof already means preserve (:484). The
// flag is only legal alongside a non-zero expiry, where it would mean the same thing, so it carries
// no information this encoder needs.
void
upsert_never_emits_a_gateway_rejected_expiry_shape()
{
  for (const bool preserve : { false, true }) {
    for (const auto& expected : expiry_cases) {
      ops::upsert_request request;
      request.id = test_id();
      request.value = cu::to_binary("v");
      request.expiry = expected.expiry;
      request.preserve_expiry = preserve;
      const auto proto = pk::encode(request);

      const std::string label = std::string{ "preserve=" } + (preserve ? "true" : "false") +
                                " expiry=" + std::to_string(expected.expiry);
      assert_false(proto.has_preserve_expiry_on_existing(),
                   label + ": preserve_expiry_on_existing is never set");
      if (preserve) {
        assert_true(proto.expiry_case() == v1::UpsertRequest::EXPIRY_NOT_SET,
                    label + ": preserve omits the oneof");
      } else {
        assert_true(proto.expiry_case() != v1::UpsertRequest::EXPIRY_NOT_SET,
                    label + ": a non-preserving upsert always sets the oneof");
      }
    }
  }
}

void
upsert_request_encodes_all_fields()
{
  ops::upsert_request request;
  request.id = test_id();
  request.value = cu::to_binary("document-body");
  request.flags = 0x0000'0006U;
  request.expiry = 300U;
  request.durability_level = couchbase::durability_level::majority;

  const auto proto = pk::encode(request);
  assert_eq(proto.content_uncompressed(), std::string{ "document-body" }, "content");
  assert_eq(proto.content_flags(), static_cast<std::uint32_t>(6), "flags");
  assert_true(proto.expiry_case() == v1::UpsertRequest::kExpirySecs, "expiry uses seconds");
  assert_eq(proto.expiry_secs(), static_cast<std::uint32_t>(300), "expiry seconds");
  assert_false(proto.preserve_expiry_on_existing(), "preserve expiry is off unless requested");
  assert_true(proto.has_durability_level(), "durability set");
  assert_true(proto.durability_level() == v1::DURABILITY_LEVEL_MAJORITY, "durability majority");
}

void
mutation_response_decodes_cas_and_token()
{
  v1::UpsertResponse proto;
  proto.set_cas(0xaa55ULL);
  auto* token = proto.mutable_mutation_token();
  token->set_bucket_name("b");
  token->set_vbucket_id(42U);
  token->set_vbucket_uuid(0xdead'beefULL);
  token->set_seq_no(1234ULL);

  const auto response = pk::decode(proto, key_value_error_context{});
  assert_eq(response.cas.value(), 0xaa55ULL, "cas");
  assert_eq(response.token.bucket_name(), std::string{ "b" }, "token bucket");
  assert_eq(response.token.partition_id(), static_cast<std::uint16_t>(42), "token vbucket id");
  assert_eq(response.token.partition_uuid(), 0xdead'beefULL, "token vbucket uuid");
  assert_eq(response.token.sequence_number(), static_cast<std::uint64_t>(1234), "token seqno");
}

void
replace_and_remove_encode_cas()
{
  ops::replace_request replace;
  replace.id = test_id();
  replace.value = cu::to_binary("v");
  replace.cas = couchbase::cas{ 0x99ULL };
  const auto replace_proto = pk::encode(replace);
  assert_true(replace_proto.has_cas(), "replace carries cas");
  assert_eq(replace_proto.cas(), 0x99ULL, "replace cas value");

  ops::remove_request remove;
  remove.id = test_id();
  remove.cas = couchbase::cas{ 0x77ULL };
  remove.durability_level = couchbase::durability_level::persist_to_majority;
  const auto remove_proto = pk::encode(remove);
  assert_true(remove_proto.has_cas(), "remove carries cas");
  assert_eq(remove_proto.cas(), 0x77ULL, "remove cas value");
  assert_true(remove_proto.durability_level() == v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
              "remove durability");
}

void
durability_none_is_left_unset()
{
  assert_false(pk::to_proto_durability(couchbase::durability_level::none).has_value(),
               "none -> unset");

  ops::insert_request insert;
  insert.id = test_id();
  insert.value = cu::to_binary("v");
  insert.durability_level = couchbase::durability_level::none;
  const auto proto = pk::encode(insert);
  assert_false(proto.has_durability_level(), "insert leaves durability unset for none");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_kv_converter",
    {
      { "get_request_encodes_location", get_request_encodes_location },
      { "get_response_decodes_value_cas_flags", get_response_decodes_value_cas_flags },
      { "get_response_refuses_compressed_content", get_response_refuses_compressed_content },
      { "expiry_encodes_every_branch_explicitly", expiry_encodes_every_branch_explicitly },
      { "preserve_expiry_omits_the_expiry_oneof", preserve_expiry_omits_the_expiry_oneof },
      { "upsert_never_emits_a_gateway_rejected_expiry_shape",
        upsert_never_emits_a_gateway_rejected_expiry_shape },
      { "upsert_request_encodes_all_fields", upsert_request_encodes_all_fields },
      { "mutation_response_decodes_cas_and_token", mutation_response_decodes_cas_and_token },
      { "replace_and_remove_encode_cas", replace_and_remove_encode_cas },
      { "durability_none_is_left_unset", durability_none_is_left_unset },
    },
  };
}

} // namespace couchbase::cng::test
