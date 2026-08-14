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

#include "core/cluster_options.hxx"
#include "core/protostellar/dispatcher.hxx"
#include "core/protostellar/kv_converter.hxx"
#include "core/utils/binary.hxx"

#include <couchbase/error_codes.hxx>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace couchbase::test
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
get_request_encodes_location([[maybe_unused]] context& ctx)
{
  ops::get_request request;
  request.id = document_id{ "travel-sample", "inventory", "airline", "airline_10" };
  const auto proto = pk::encode(request, pk::compression_settings{});
  assert_eq(proto.bucket_name(), std::string{ "travel-sample" }, "bucket");
  assert_eq(proto.scope_name(), std::string{ "inventory" }, "scope");
  assert_eq(proto.collection_name(), std::string{ "airline" }, "collection");
  assert_eq(proto.key(), std::string{ "airline_10" }, "key");
}

void
get_response_decodes_value_cas_flags([[maybe_unused]] context& ctx)
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
expiry_encodes_every_branch_explicitly([[maybe_unused]] context& ctx)
{
  for (const auto& expected : expiry_cases) {
    const auto label = "expiry=" + std::to_string(expected.expiry);

    ops::upsert_request upsert;
    upsert.id = test_id();
    upsert.value = cu::to_binary("v");
    upsert.expiry = expected.expiry;
    assert_expiry(pk::encode(upsert, pk::compression_settings{}), expected, "upsert " + label);

    ops::insert_request insert;
    insert.id = test_id();
    insert.value = cu::to_binary("v");
    insert.expiry = expected.expiry;
    assert_expiry(pk::encode(insert, pk::compression_settings{}), expected, "insert " + label);

    ops::replace_request replace;
    replace.id = test_id();
    replace.value = cu::to_binary("v");
    replace.expiry = expected.expiry;
    assert_expiry(pk::encode(replace, pk::compression_settings{}), expected, "replace " + label);
  }
}

// The counter seed is optional in the proto, so "not supplied" and "supplied as zero" are different
// wire messages -- gocb always sends a seed and is the outlier; jvm and .NET send it only when the
// caller asked for one, which is what is pinned here. INT64_MAX is the largest representable seed
// and must still be encoded exactly; anything above it is refused by the component before encoding
// (see a_counter_seed_above_int64_max_is_rejected_before_dispatch), because this converter returns
// the request message by value and has no error channel of its own.
void
counter_initial_value_is_optional_and_encodes_its_upper_bound([[maybe_unused]] context& ctx)
{
  ops::increment_request without_seed;
  without_seed.id = test_id();
  const auto encoded_without = pk::encode(without_seed);
  assert_true(!encoded_without.has_initial(),
              "no seed was supplied, so the optional initial field stays absent");

  ops::increment_request with_zero;
  with_zero.id = test_id();
  with_zero.initial_value = 0U;
  const auto encoded_zero = pk::encode(with_zero);
  assert_true(encoded_zero.has_initial(), "a seed of zero is still a seed and must be present");
  assert_eq(encoded_zero.initial(), std::int64_t{ 0 }, "zero seed value");

  ops::increment_request at_max;
  at_max.id = test_id();
  at_max.initial_value = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  assert_eq(pk::encode(at_max).initial(),
            std::numeric_limits<std::int64_t>::max(),
            "the largest representable seed survives the narrowing unchanged");

  ops::decrement_request decrement_at_max;
  decrement_at_max.id = test_id();
  decrement_at_max.initial_value =
    static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
  assert_eq(pk::encode(decrement_at_max).initial(),
            std::numeric_limits<std::int64_t>::max(),
            "decrement shares set_counter_fields, so it shares the bound");
}

// The remaining expiry-bearing operations added here. `touch` is pinned separately by
// touch_always_sends_an_expiry_arm; these three shared the same defect and had no expiry assertion
// at all. Like touch, none of them has a preserve_expiry option -- GetAndTouchRequest,
// IncrementRequest and DecrementRequest have no such field in the proto -- so the oneof is the only
// channel for the expiry and it must always be set. An omitted oneof would leave the gateway
// nothing to act on, so get_and_touch(id, 0) would report success and leave the old TTL in place.
void
expiry_encodes_every_branch_for_get_and_touch_and_the_counters([[maybe_unused]] context& ctx)
{
  for (const auto& expected : expiry_cases) {
    const auto label = "expiry=" + std::to_string(expected.expiry);

    ops::get_and_touch_request get_and_touch;
    get_and_touch.id = test_id();
    get_and_touch.expiry = expected.expiry;
    assert_expiry(
      pk::encode(get_and_touch, pk::compression_settings{}), expected, "get_and_touch " + label);

    ops::increment_request increment;
    increment.id = test_id();
    increment.expiry = expected.expiry;
    assert_expiry(pk::encode(increment), expected, "increment " + label);

    ops::decrement_request decrement;
    decrement.id = test_id();
    decrement.expiry = expected.expiry;
    assert_expiry(pk::encode(decrement), expected, "decrement " + label);
  }
}

// preserve_expiry wins over expiry, which is what upsert_options and replace_options document, and
// the gateway's encoding for it is an omitted oneof (kvserver.go:484 for upsert, :581 for replace).
void
preserve_expiry_omits_the_expiry_oneof([[maybe_unused]] context& ctx)
{
  ops::upsert_request upsert;
  upsert.id = test_id();
  upsert.value = cu::to_binary("v");
  upsert.expiry = 300U;
  upsert.preserve_expiry = true;
  const auto upsert_proto = pk::encode(upsert, pk::compression_settings{});
  assert_true(upsert_proto.expiry_case() == v1::UpsertRequest::EXPIRY_NOT_SET,
              "upsert expresses preserve by omitting the expiry oneof");

  // ReplaceRequest has no preserve_expiry_on_existing field at all, so an absent oneof is the only
  // available encoding there.
  ops::replace_request replace;
  replace.id = test_id();
  replace.value = cu::to_binary("v");
  replace.expiry = 300U;
  replace.preserve_expiry = true;
  assert_true(pk::encode(replace, pk::compression_settings{}).expiry_case() ==
                v1::ReplaceRequest::EXPIRY_NOT_SET,
              "replace expresses preserve by omitting the expiry oneof");

  // ...and with the flag off, a default replace must clear the TTL rather than inherit it.
  ops::replace_request plain;
  plain.id = test_id();
  plain.value = cu::to_binary("v");
  const auto plain_proto = pk::encode(plain, pk::compression_settings{});
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
upsert_never_emits_a_gateway_rejected_expiry_shape([[maybe_unused]] context& ctx)
{
  for (const bool preserve : { false, true }) {
    for (const auto& expected : expiry_cases) {
      ops::upsert_request request;
      request.id = test_id();
      request.value = cu::to_binary("v");
      request.expiry = expected.expiry;
      request.preserve_expiry = preserve;
      const auto proto = pk::encode(request, pk::compression_settings{});

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

// Touch and GetAndTouch have no preserve concept and reject an absent oneof outright -- their arm
// dispatch falls through to "Expiry time specification is unknown." (kvserver.go:297-303 for Touch,
// :131-137 for GetAndTouch). A zero expiry therefore has to be sent, not omitted; the gateway maps
// it to never-expires (helpers.go:49-51), which is what touch(0) means on the classic path too.
void
touch_always_sends_an_expiry_arm([[maybe_unused]] context& ctx)
{
  for (const auto& expected : expiry_cases) {
    ops::touch_request touch;
    touch.id = test_id();
    touch.expiry = expected.expiry;
    assert_expiry(pk::encode(touch), expected, "touch expiry=" + std::to_string(expected.expiry));
  }
}

void
upsert_request_encodes_all_fields([[maybe_unused]] context& ctx)
{
  ops::upsert_request request;
  request.id = test_id();
  request.value = cu::to_binary("document-body");
  request.flags = 0x0000'0006U;
  request.expiry = 300U;
  request.durability_level = couchbase::durability_level::majority;

  const auto proto = pk::encode(request, pk::compression_settings{});
  assert_eq(proto.content_uncompressed(), std::string{ "document-body" }, "content");
  assert_eq(proto.content_flags(), static_cast<std::uint32_t>(6), "flags");
  assert_true(proto.expiry_case() == v1::UpsertRequest::kExpirySecs, "expiry uses seconds");
  assert_eq(proto.expiry_secs(), static_cast<std::uint32_t>(300), "expiry seconds");
  assert_false(proto.preserve_expiry_on_existing(), "preserve expiry is off unless requested");
  assert_true(proto.has_durability_level(), "durability set");
  assert_true(proto.durability_level() == v1::DURABILITY_LEVEL_MAJORITY, "durability majority");
}

void
mutation_response_decodes_cas_and_token([[maybe_unused]] context& ctx)
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
replace_and_remove_encode_cas([[maybe_unused]] context& ctx)
{
  ops::replace_request replace;
  replace.id = test_id();
  replace.value = cu::to_binary("v");
  replace.cas = couchbase::cas{ 0x99ULL };
  const auto replace_proto = pk::encode(replace, pk::compression_settings{});
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

// Each durable level must map onto its own proto level, `majority_and_persist_to_active` included —
// it was the one level nothing asserted.
//
// This is the mapping's only guard, and no live case can stand in for it. The proto enum has no
// NONE member and starts at MAJORITY = 0, so the three durable levels occupy 0, 1, 2 in declaration
// order; a level shifted by one is still a value the gateway accepts, so the write succeeds and the
// caller silently gets weaker or stronger guarantees than were asked for.
void
durability_levels_map_to_their_own_proto_levels([[maybe_unused]] context& ctx)
{
  assert_true(pk::to_proto_durability(couchbase::durability_level::majority) ==
                v1::DURABILITY_LEVEL_MAJORITY,
              "majority");
  assert_true(
    pk::to_proto_durability(couchbase::durability_level::majority_and_persist_to_active) ==
      v1::DURABILITY_LEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE,
    "majority_and_persist_to_active");
  assert_true(pk::to_proto_durability(couchbase::durability_level::persist_to_majority) ==
                v1::DURABILITY_LEVEL_PERSIST_TO_MAJORITY,
              "persist_to_majority");
}

void
durability_none_is_left_unset([[maybe_unused]] context& ctx)
{
  assert_false(pk::to_proto_durability(couchbase::durability_level::none).has_value(),
               "none -> unset");

  ops::insert_request insert;
  insert.id = test_id();
  insert.value = cu::to_binary("v");
  insert.durability_level = couchbase::durability_level::none;
  const auto proto = pk::encode(insert, pk::compression_settings{});
  assert_false(proto.has_durability_level(), "insert leaves durability unset for none");
}

void
lifecycle_ops_encode_expected_fields([[maybe_unused]] context& ctx)
{
  ops::touch_request touch;
  touch.id = document_id{ "b", "s", "c", "k" };
  touch.expiry = 120U;
  const auto touch_proto = pk::encode(touch);
  assert_eq(touch_proto.key(), std::string{ "k" }, "touch key");
  assert_true(touch_proto.expiry_case() == v1::TouchRequest::kExpirySecs, "touch expiry secs");
  assert_eq(touch_proto.expiry_secs(), static_cast<std::uint32_t>(120), "touch expiry value");

  ops::get_and_lock_request lock;
  lock.id = document_id{ "b", "s", "c", "k" };
  lock.lock_time = 30U;
  assert_eq(pk::encode(lock, pk::compression_settings{}).lock_time_secs(),
            static_cast<std::uint32_t>(30),
            "lock time");

  ops::unlock_request unlock;
  unlock.id = document_id{ "b", "s", "c", "k" };
  unlock.cas = couchbase::cas{ 0x42ULL };
  assert_eq(pk::encode(unlock).cas(), 0x42ULL, "unlock cas");
}

void
exists_and_lock_responses_decode([[maybe_unused]] context& ctx)
{
  v1::ExistsResponse exists_proto;
  exists_proto.set_result(true);
  exists_proto.set_cas(0x9ULL);
  const auto exists = pk::decode(exists_proto, key_value_error_context{});
  assert_true(exists.exists(), "exists result");
  assert_eq(exists.cas.value(), 0x9ULL, "exists cas");

  v1::GetAndLockResponse lock_proto;
  lock_proto.set_content_uncompressed("locked-value");
  lock_proto.set_cas(0xabcULL);
  lock_proto.set_content_flags(0x06U);
  const auto locked = pk::decode(lock_proto, key_value_error_context{});
  assert_eq(cu::to_string(locked.value), std::string{ "locked-value" }, "get_and_lock value");
  assert_eq(locked.cas.value(), 0xabcULL, "get_and_lock cas");
}

void
counter_encodes_and_decodes([[maybe_unused]] context& ctx)
{
  ops::increment_request inc;
  inc.id = document_id{ "b", "s", "c", "counter" };
  inc.delta = 5ULL;
  inc.initial_value = 42ULL;
  inc.expiry = 100U;
  inc.durability_level = couchbase::durability_level::majority;
  const auto proto = pk::encode(inc);
  assert_eq(proto.delta(), 5ULL, "counter delta");
  assert_true(proto.has_initial(), "counter initial set");
  assert_eq(proto.initial(), std::int64_t{ 42 }, "counter initial value");
  assert_true(proto.expiry_case() == v1::IncrementRequest::kExpirySecs, "counter expiry secs");
  assert_true(proto.has_durability_level(), "counter durability set");

  v1::IncrementResponse resp;
  resp.set_cas(0x1ULL);
  resp.set_content(43);
  const auto result = pk::decode(resp, key_value_error_context{});
  assert_eq(result.content, static_cast<std::uint64_t>(43), "counter content");
  assert_eq(result.cas.value(), 0x1ULL, "counter cas");
}

void
append_encodes_content_and_cas([[maybe_unused]] context& ctx)
{
  ops::append_request append;
  append.id = document_id{ "b", "s", "c", "k" };
  append.value = cu::to_binary("-tail");
  append.cas = couchbase::cas{ 0x55ULL };
  const auto proto = pk::encode(append);
  assert_eq(proto.content(), std::string{ "-tail" }, "append content");
  assert_true(proto.has_cas(), "append cas set");
  assert_eq(proto.cas(), 0x55ULL, "append cas value");
}

// Every read that can carry a document body opts in, get_projected included: the gateway serves
// projections from the same Get handler as a full read, so a projected read that stayed silent
// would be the one path unable to accept a compressed body. gocb, the Java SDK and the .NET SDK all
// put the projection paths and the compression mode on the same request.
void
read_compression_opt_in_follows_settings([[maybe_unused]] context& ctx)
{
  ops::get_request get;
  get.id = test_id();
  ops::get_projected_request projected;
  projected.id = test_id();
  projected.projections = { "foo" };
  ops::get_and_lock_request lock;
  lock.id = test_id();
  ops::get_and_touch_request touch;
  touch.id = test_id();

  const auto opted_in = [](const auto& proto, const std::string& label) {
    assert_true(proto.has_compression(), label + ": enabled -> compression field present");
    assert_true(proto.compression() == v1::COMPRESSION_ENABLED_OPTIONAL,
                label + ": enabled -> OPTIONAL, never ALWAYS");
  };
  const pk::compression_settings enabled{ true };
  opted_in(pk::encode(get, enabled), "get");
  opted_in(pk::encode(projected, enabled), "get_projected");
  opted_in(pk::encode(lock, enabled), "get_and_lock");
  opted_in(pk::encode(touch, enabled), "get_and_touch");

  const pk::compression_settings disabled{ false };
  assert_false(pk::encode(get, disabled).has_compression(), "get: disabled -> field unset");
  assert_false(pk::encode(projected, disabled).has_compression(),
               "get_projected: disabled -> field unset");
  assert_false(pk::encode(lock, disabled).has_compression(),
               "get_and_lock: disabled -> field unset");
  assert_false(pk::encode(touch, disabled).has_compression(),
               "get_and_touch: disabled -> field unset");
}

// The gateway may answer any opted-in read with the compressed arm, so every read decoder has to
// inflate it. A path that read content_uncompressed directly would hand back the empty-string
// singleton with a success status.
void
every_read_path_decodes_compressed_content([[maybe_unused]] context& ctx)
{
  const std::string plain(256, 'x');
  const auto frame = pk::maybe_compress(cu::to_binary(plain), pk::compression_settings{});
  assert_true(frame.has_value(), "the fixture value is compressible");

  v1::GetResponse get_proto;
  get_proto.set_content_compressed(*frame);
  assert_eq(cu::to_string(pk::decode(get_proto, key_value_error_context{}).value), plain, "get");

  ops::get_projected_request projected;
  projected.id = test_id();
  assert_eq(cu::to_string(pk::decode(get_proto, key_value_error_context{}, projected).value),
            plain,
            "get_projected");

  v1::GetAndLockResponse lock_proto;
  lock_proto.set_content_compressed(*frame);
  assert_eq(
    cu::to_string(pk::decode(lock_proto, key_value_error_context{}).value), plain, "get_and_lock");

  v1::GetAndTouchResponse touch_proto;
  touch_proto.set_content_compressed(*frame);
  assert_eq(cu::to_string(pk::decode(touch_proto, key_value_error_context{}).value),
            plain,
            "get_and_touch");
}

void
compression_round_trips_large_values([[maybe_unused]] context& ctx)
{
  const std::string big(1024, 'a'); // well over min_size and highly compressible
  ops::upsert_request request;
  request.id = document_id{ "b", "s", "c", "k" };
  request.value = cu::to_binary(big);

  const auto compressed = pk::encode(request, pk::compression_settings{ true });
  assert_true(compressed.content_case() == v1::UpsertRequest::kContentCompressed,
              "large compressible value is snappy-compressed");

  // A compressed response must snappy-decode back to the original (never empty-as-success).
  v1::GetResponse response;
  response.set_content_compressed(compressed.content_compressed());
  const auto decoded = pk::decode(response, key_value_error_context{});
  assert_eq(cu::to_string(decoded.value), big, "content_compressed decodes to the original value");

  const auto raw = pk::encode(request, pk::compression_settings{ false });
  assert_true(raw.content_case() == v1::UpsertRequest::kContentUncompressed,
              "compression disabled sends raw content");
}

// A gateway emitting an undecodable snappy frame must not surface as an empty document -- an empty
// value with a valid CAS is indistinguishable from a real empty one, which is the same silent
// data-loss trap the unguarded content oneof had before compression landed. decode_content returns
// nullopt and the decoder reports the server-side fault instead of inventing a value.
void
malformed_compressed_content_fails_closed([[maybe_unused]] context& ctx)
{
  v1::GetResponse proto;
  proto.set_content_compressed(std::string(64, '\xff')); // not a snappy frame
  proto.set_cas(0x1234ULL);
  proto.set_content_flags(0x0200'0006U);

  const auto response = pk::decode(proto, key_value_error_context{});
  assert_true(response.ctx.ec() == errc::common::internal_server_failure,
              "undecodable compressed content is reported rather than silently emptied");
  assert_true(response.value.empty(), "no invented value is handed back");

  ops::get_projected_request projected;
  projected.id = test_id();
  const auto projected_response = pk::decode(proto, key_value_error_context{}, projected);
  assert_true(projected_response.ctx.ec() == errc::common::internal_server_failure,
              "a projected read reports it too");
  assert_true(projected_response.value.empty(), "no invented projected value is handed back");
}

// snappy sizes its output buffer from the length declared in the frame header before it validates
// the body, so an inflated length is an allocation request. Nothing larger than the transport's
// maximum message size can have been carried here, and a std::bad_alloc raised inside a gRPC
// completion would take the process down rather than fail the operation.
void
an_oversized_declared_length_is_refused([[maybe_unused]] context& ctx)
{
  std::optional<std::string> frame;
  {
    const std::string oversized(couchbase::core::protostellar::max_receive_message_size + 1, 'a');
    frame = pk::maybe_compress(cu::to_binary(oversized), pk::compression_settings{});
  }
  assert_true(frame.has_value(), "the fixture compresses");
  assert_false(pk::snappy_uncompress(*frame).has_value(),
               "a frame declaring more than the transport can carry is refused, not inflated");

  v1::GetResponse proto;
  proto.set_content_compressed(*frame);
  const auto response = pk::decode(proto, key_value_error_context{});
  assert_true(response.ctx.ec() == errc::common::internal_server_failure,
              "the refusal surfaces as an error rather than an empty value");
}

void
the_min_size_threshold_admits_a_value_of_exactly_min_size([[maybe_unused]] context& ctx)
{
  const pk::compression_settings settings{};
  const auto compresses = [&settings](std::size_t size) {
    ops::upsert_request request;
    request.id = test_id();
    request.value = cu::to_binary(std::string(size, 'a'));
    return pk::encode(request, settings).content_case() == v1::UpsertRequest::kContentCompressed;
  };
  assert_false(compresses(settings.min_size - 1), "one byte below min_size is sent raw");
  assert_true(compresses(settings.min_size), "a value of exactly min_size is compressed");
}

// min_ratio is the largest compressed/original ratio still worth sending, so a value landing
// exactly on it is compressed. The threshold is derived from the fixture's own compressed size over
// a power-of-two original, which makes both the division and the comparison exact in binary
// floating point -- an approximate ratio would not tell the inclusive bound from the exclusive one.
void
the_min_ratio_threshold_is_inclusive([[maybe_unused]] context& ctx)
{
  const std::string original(1024, 'a');
  const auto value = cu::to_binary(original);
  const auto measured = pk::maybe_compress(value, pk::compression_settings{ true, 32, 1.0 });
  assert_true(measured.has_value(), "the fixture compresses at ratio 1.0");
  const auto exact_ratio = static_cast<double>(measured->size()) / static_cast<double>(1024);

  assert_true(
    pk::maybe_compress(value, pk::compression_settings{ true, 32, exact_ratio }).has_value(),
    "a value landing exactly on min_ratio is compressed");
  const auto below = static_cast<double>(measured->size() - 1) / static_cast<double>(1024);
  assert_false(pk::maybe_compress(value, pk::compression_settings{ true, 32, below }).has_value(),
               "a value that misses min_ratio is sent raw");
}

// A value can clear min_size and still be worth sending raw. Without the ratio check every
// incompressible payload would go out larger than it arrived.
void
an_incompressible_value_is_sent_uncompressed([[maybe_unused]] context& ctx)
{
  // A fixed linear congruential sequence rather than <random>: the bytes have to be the same on
  // every platform for the ratio the assertion depends on to be the same.
  std::uint32_t state{ 20260806U };
  std::string noise;
  noise.reserve(4096);
  while (noise.size() < 4096) {
    state = (state * 1103515245U) + 12345U;
    noise.push_back(static_cast<char>((state >> 16U) & 0xFFU));
  }

  ops::upsert_request request;
  request.id = test_id();
  request.value = cu::to_binary(noise);
  const auto proto = pk::encode(request, pk::compression_settings{});
  assert_true(proto.content_case() == v1::UpsertRequest::kContentUncompressed,
              "a value snappy cannot shrink past min_ratio is sent raw");
}

void
cluster_options_maps_compression_settings([[maybe_unused]] context& ctx)
{
  couchbase::core::cluster_options options;
  options.enable_compression = false;
  options.compression_min_size = 128;
  options.compression_min_ratio = 0.5;

  const auto settings = couchbase::core::make_compression_settings(options);
  assert_false(settings.enabled, "enable_compression mapped");
  assert_eq(settings.min_size, std::size_t{ 128 }, "min_size mapped");
  assert_eq(settings.min_ratio, 0.5, "min_ratio mapped");
}

void
custom_compression_thresholds_are_honoured([[maybe_unused]] context& ctx)
{
  const std::string text(256, 'a');
  ops::upsert_request request;
  request.id = test_id();
  request.value = cu::to_binary(text);

  const pk::compression_settings custom{ true, 512, 0.83 };
  const auto proto = pk::encode(request, custom);
  assert_true(proto.content_case() == v1::UpsertRequest::kContentUncompressed,
              "payload below custom min_size is sent uncompressed");
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
      { "expiry_encodes_every_branch_explicitly", expiry_encodes_every_branch_explicitly },
      { "expiry_encodes_every_branch_for_get_and_touch_and_the_counters",
        expiry_encodes_every_branch_for_get_and_touch_and_the_counters },
      { "counter_initial_value_is_optional_and_encodes_its_upper_bound",
        counter_initial_value_is_optional_and_encodes_its_upper_bound },
      { "preserve_expiry_omits_the_expiry_oneof", preserve_expiry_omits_the_expiry_oneof },
      { "touch_always_sends_an_expiry_arm", touch_always_sends_an_expiry_arm },
      { "upsert_never_emits_a_gateway_rejected_expiry_shape",
        upsert_never_emits_a_gateway_rejected_expiry_shape },
      { "upsert_request_encodes_all_fields", upsert_request_encodes_all_fields },
      { "mutation_response_decodes_cas_and_token", mutation_response_decodes_cas_and_token },
      { "replace_and_remove_encode_cas", replace_and_remove_encode_cas },
      { "durability_none_is_left_unset", durability_none_is_left_unset },
      { "durability_levels_map_to_their_own_proto_levels",
        durability_levels_map_to_their_own_proto_levels },
      { "lifecycle_ops_encode_expected_fields", lifecycle_ops_encode_expected_fields },
      { "exists_and_lock_responses_decode", exists_and_lock_responses_decode },
      { "counter_encodes_and_decodes", counter_encodes_and_decodes },
      { "append_encodes_content_and_cas", append_encodes_content_and_cas },
      { "read_compression_opt_in_follows_settings", read_compression_opt_in_follows_settings },
      { "every_read_path_decodes_compressed_content", every_read_path_decodes_compressed_content },
      { "compression_round_trips_large_values", compression_round_trips_large_values },
      { "the_min_size_threshold_admits_a_value_of_exactly_min_size",
        the_min_size_threshold_admits_a_value_of_exactly_min_size },
      { "the_min_ratio_threshold_is_inclusive", the_min_ratio_threshold_is_inclusive },
      { "an_incompressible_value_is_sent_uncompressed",
        an_incompressible_value_is_sent_uncompressed },
      { "malformed_compressed_content_fails_closed", malformed_compressed_content_fails_closed },
      { "an_oversized_declared_length_is_refused", an_oversized_declared_length_is_refused },
      { "cluster_options_maps_compression_settings", cluster_options_maps_compression_settings },
      { "custom_compression_thresholds_are_honoured", custom_compression_thresholds_are_honoured },
    },
  };
}

} // namespace couchbase::test
