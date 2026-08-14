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

// Unit tests for the N1QL query <-> couchbase.query.v1 converter (CXXCBC-897). Pure, no server.
//
// The expectations here are read off couchbase/query/v1/query.proto and the gateway's
// queryserver.go, not off the converter -- an assertion derived from the code it tests only
// notices that the code changed.

#include "framework/test_registry.hxx"

#include "core/protostellar/query_converter.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace pq = ::couchbase::core::protostellar::query;
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::query::v1;

// A json_string holding the *binary* arm, which is the arm the public API produces:
// couchbase/query_options.hxx declares parameters as std::vector<codec::binary> and
// core/impl/query.cxx moves each one into the request. Building the string arm here instead --
// json_string{std::string} -- would exercise a path no caller reaches.
[[nodiscard]] auto
binary_json(const std::string& json) -> couchbase::core::json_string
{
  std::vector<std::byte> bytes;
  bytes.reserve(json.size());
  for (const char c : json) {
    bytes.push_back(static_cast<std::byte>(c));
  }
  return couchbase::core::json_string{ std::move(bytes) };
}

void
encode_maps_core_fields([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT 1";
  request.client_context_id = "ctx-42";
  request.readonly = true;
  request.adhoc = false;
  request.flex_index = true;
  request.preserve_expiry = true;
  request.scan_consistency = couchbase::query_scan_consistency::request_plus;
  request.profile = couchbase::query_profile::timings;
  request.max_parallelism = 4;
  request.metrics = true;

  const auto proto = pq::encode(request);
  assert_eq(proto.statement(), std::string{ "SELECT 1" }, "statement mapped");
  assert_eq(proto.client_context_id(), std::string{ "ctx-42" }, "client_context_id mapped");
  assert_true(proto.read_only(), "readonly mapped");
  assert_true(proto.flex_index(), "flex_index mapped");
  assert_true(proto.preserve_expiry(), "preserve_expiry mapped");
  assert_true(proto.prepared(), "adhoc=false requests a prepared statement");
  assert_true(proto.scan_consistency() ==
                v1::QueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS,
              "scan_consistency mapped");
  assert_true(proto.profile_mode() == v1::QueryRequest_ProfileMode_PROFILE_MODE_TIMINGS,
              "profile mapped");
  assert_true(proto.has_tuning_options(), "tuning options present");
  assert_eq(proto.tuning_options().max_parallelism(), std::uint32_t{ 4 }, "max_parallelism mapped");
  assert_false(proto.tuning_options().disable_metrics(),
               "metrics=true disables the disable_metrics flag");
}

// The blocker from the #1036 review. json_string::str() returns a reference to a static empty
// string for the binary arm, so reading parameters through it sends "" for every one of them --
// and the binary arm is the only arm the public API produces. Asserting on the payload rather
// than on "a parameter is present" is what separates the two.
void
positional_parameters_carry_their_json_payload([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT * FROM b WHERE id = $1 AND n = $2";
  request.positional_parameters.emplace_back(binary_json(R"("abc")"));
  request.positional_parameters.emplace_back(binary_json("42"));

  const auto proto = pq::encode(request);
  assert_eq(proto.positional_parameters_size(), 2, "both positional parameters encoded");
  assert_eq(proto.positional_parameters(0), std::string{ R"("abc")" }, "first payload preserved");
  assert_eq(proto.positional_parameters(1), std::string{ "42" }, "second payload preserved");
}

void
named_parameters_carry_their_json_payload([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT * FROM b WHERE id = $id";
  request.named_parameters.emplace("id", binary_json(R"("abc")"));
  request.named_parameters.emplace("limit", binary_json("10"));

  const auto proto = pq::encode(request);
  const auto& named = proto.named_parameters();
  assert_eq(named.size(), std::size_t{ 2 }, "both named parameters encoded");
  assert_true(named.count("id") == 1, "named parameter present");
  assert_eq(named.at("id"), std::string{ R"("abc")" }, "named payload preserved");
  assert_eq(named.at("limit"), std::string{ "10" }, "numeric named payload preserved");
}

// The proto field is `repeated bytes`, so a payload is copied verbatim rather than re-encoded.
// A parameter whose JSON contains a NUL or a non-ASCII byte must survive intact and keep its
// length -- treating the payload as a C string would truncate at the NUL.
void
a_parameter_payload_is_copied_byte_for_byte([[maybe_unused]] context& ctx)
{
  // "a<NUL>b<U+00E9>" as JSON: an embedded NUL and a multi-byte UTF-8 sequence. Built with an
  // explicit length because a NUL would otherwise end the literal.
  const std::string payload{ "\"a\0b\xc3\xa9\"", 7 };
  ops::query_request request;
  request.statement = "SELECT $1";
  request.positional_parameters.emplace_back(binary_json(payload));

  const auto proto = pq::encode(request);
  assert_eq(proto.positional_parameters(0).size(), std::size_t{ 7 }, "payload length preserved");
  assert_true(proto.positional_parameters(0) == payload, "payload bytes preserved");
}

// The string arm is unreachable from the public API but is what a direct core caller would build,
// and json_payload() has to handle both.
void
a_string_arm_parameter_is_encoded_too([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT $1";
  request.positional_parameters.emplace_back(std::string{ "7" });

  const auto proto = pq::encode(request);
  assert_eq(proto.positional_parameters(0), std::string{ "7" }, "string-arm payload encoded");
}

// A default-constructed json_string holds neither arm. It must encode as empty rather than trip
// over the variant.
void
an_empty_parameter_encodes_as_empty([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT $1";
  request.positional_parameters.emplace_back();

  const auto proto = pq::encode(request);
  assert_eq(proto.positional_parameters_size(), 1, "the parameter is still sent");
  assert_true(proto.positional_parameters(0).empty(), "an unset parameter encodes as empty");
}

// queryserver.go:93 sets the query context only when bucket AND scope are both present, and
// rejects a scope without a bucket outright (:89). A context that is not exactly two
// backtick-delimited identifiers therefore has to leave both fields unset rather than send half
// of one.
void
query_context_sets_both_fields_or_neither([[maybe_unused]] context& ctx)
{
  ops::query_request base;
  base.statement = "SELECT 1";

  auto scoped = base;
  scoped.query_context = "default:`travel-sample`.`inventory`";
  const auto proto = pq::encode(scoped);
  assert_eq(proto.bucket_name(), std::string{ "travel-sample" }, "query_context bucket parsed");
  assert_eq(proto.scope_name(), std::string{ "inventory" }, "query_context scope parsed");

  for (const auto* malformed : {
         "default:`travel-sample`",         // one identifier
         "default:`a`.`b`.`c`",             // three
         "default:`unbalanced",             // unterminated
         "default:travel-sample.inventory", // no backticks
         "",                                // empty
       }) {
    auto request = base;
    request.query_context = malformed;
    const auto encoded = pq::encode(request);
    assert_true(encoded.bucket_name().empty() && encoded.scope_name().empty(),
                std::string{ "a context that is not two identifiers sets neither field: " } +
                  malformed);
  }
}

void
encode_maps_mutation_state_to_consistent_with([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT 1";
  request.mutation_state.emplace_back(0x1234ULL, 42ULL, std::uint16_t{ 7 }, "default");

  const auto proto = pq::encode(request);
  assert_eq(proto.consistent_with_size(), 1, "one scan-vector token");
  const auto& token = proto.consistent_with(0);
  assert_eq(token.bucket_name(), std::string{ "default" }, "token bucket mapped");
  assert_eq(token.vbucket_id(), std::uint32_t{ 7 }, "token vbucket id mapped");
  assert_eq(token.vbucket_uuid(), std::uint64_t{ 0x1234 }, "token uuid mapped");
  assert_eq(token.seq_no(), std::uint64_t{ 42 }, "token seq no mapped");
}

// Every value of both enums has to map: a value added to the SDK enum without a case here would
// otherwise fall through and send the proto default (NOT_BOUNDED / OFF), silently downgrading the
// caller's consistency or profiling request.
void
every_scan_consistency_and_profile_value_maps([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT 1";

  request.scan_consistency = couchbase::query_scan_consistency::not_bounded;
  assert_true(pq::encode(request).scan_consistency() ==
                v1::QueryRequest_ScanConsistency_SCAN_CONSISTENCY_NOT_BOUNDED,
              "not_bounded maps");
  request.scan_consistency = couchbase::query_scan_consistency::request_plus;
  assert_true(pq::encode(request).scan_consistency() ==
                v1::QueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS,
              "request_plus maps");

  request.scan_consistency.reset();
  request.profile = couchbase::query_profile::off;
  assert_true(pq::encode(request).profile_mode() == v1::QueryRequest_ProfileMode_PROFILE_MODE_OFF,
              "profile off maps");
  request.profile = couchbase::query_profile::phases;
  assert_true(pq::encode(request).profile_mode() ==
                v1::QueryRequest_ProfileMode_PROFILE_MODE_PHASES,
              "profile phases maps");
  request.profile = couchbase::query_profile::timings;
  assert_true(pq::encode(request).profile_mode() ==
                v1::QueryRequest_ProfileMode_PROFILE_MODE_TIMINGS,
              "profile timings maps");
}

// scan_wait is a google.protobuf.Duration, whose seconds and nanos are separate fields. A value
// with a sub-second remainder is where a lossy cast shows up.
void
scan_wait_splits_into_seconds_and_nanos([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT 1";
  request.scan_wait = std::chrono::milliseconds{ 2500 };

  const auto proto = pq::encode(request);
  assert_true(proto.tuning_options().has_scan_wait(), "scan_wait present");
  assert_eq(proto.tuning_options().scan_wait().seconds(), std::int64_t{ 2 }, "seconds split out");
  assert_eq(proto.tuning_options().scan_wait().nanos(),
            std::int32_t{ 500'000'000 },
            "sub-second remainder kept as nanos");
}

void
tuning_fields_map_to_their_proto_counterparts([[maybe_unused]] context& ctx)
{
  ops::query_request request;
  request.statement = "SELECT 1";
  request.max_parallelism = 3;
  request.pipeline_batch = 5;
  request.pipeline_cap = 7;
  request.scan_cap = 9;

  const auto proto = pq::encode(request);
  const auto& tuning = proto.tuning_options();
  assert_eq(tuning.max_parallelism(), std::uint32_t{ 3 }, "max_parallelism mapped");
  assert_eq(tuning.pipeline_batch(), std::uint32_t{ 5 }, "pipeline_batch mapped");
  assert_eq(tuning.pipeline_cap(), std::uint32_t{ 7 }, "pipeline_cap mapped");
  assert_eq(tuning.scan_cap(), std::uint32_t{ 9 }, "scan_cap mapped");
  assert_true(tuning.disable_metrics(), "metrics defaults off, so disable_metrics is set");
}

// The tuning fields are uint64 in the core request and uint32 in the proto. Encoding a value that
// does not fit would wrap and ask the gateway to use a different number than the caller asked
// for, so can_encode rejects it -- the same convention the converter uses for every other
// "cannot express this" case.
void
can_encode_rejects_tuning_values_that_do_not_fit_uint32([[maybe_unused]] context& ctx)
{
  constexpr auto max32 = std::uint64_t{ std::numeric_limits<std::uint32_t>::max() };

  ops::query_request base;
  base.statement = "SELECT 1";

  auto at_limit = base;
  at_limit.max_parallelism = max32;
  at_limit.pipeline_batch = max32;
  at_limit.pipeline_cap = max32;
  at_limit.scan_cap = max32;
  assert_true(pq::can_encode(at_limit), "UINT32_MAX itself still encodes");
  assert_eq(pq::encode(at_limit).tuning_options().max_parallelism(),
            std::numeric_limits<std::uint32_t>::max(),
            "the boundary value survives encoding");

  for (auto field : std::initializer_list<std::optional<std::uint64_t> ops::query_request::*>{
         &ops::query_request::max_parallelism,
         &ops::query_request::pipeline_batch,
         &ops::query_request::pipeline_cap,
         &ops::query_request::scan_cap,
       }) {
    auto request = base;
    request.*field = max32 + 1;
    assert_false(pq::can_encode(request), "a tuning value above UINT32_MAX is not encodable");
  }
}

void
can_encode_rejects_unsupported_features([[maybe_unused]] context& ctx)
{
  ops::query_request base;
  base.statement = "SELECT 1";
  assert_true(pq::can_encode(base), "plain query encodes");

  ops::query_request with_raw = base;
  with_raw.raw.emplace("timeout", couchbase::core::json_string{ std::string{ "\"1s\"" } });
  assert_false(pq::can_encode(with_raw), "raw passthrough is not supported");

  ops::query_request with_replica = base;
  with_replica.use_replica = true;
  assert_false(pq::can_encode(with_replica), "use_replica is not supported");

  // An explicit `false` is a request for the default, not for a feature the gateway lacks.
  ops::query_request without_replica = base;
  without_replica.use_replica = false;
  assert_true(pq::can_encode(without_replica), "use_replica=false is not a rejection");

  // Wired as of CXXCBC-910: rows are handed to the callback as they arrive rather than buffered,
  // so this is no longer a reason to refuse the request.
  ops::query_request with_row_callback = base;
  with_row_callback.row_callback = [](std::string) {
    return couchbase::core::utils::json::stream_control::next_row;
  };
  assert_true(pq::can_encode(with_row_callback), "the streaming row callback is wired");

  // There is no proto field for node targeting and the gateway routes on its own, so honouring
  // the request is impossible; couchbase-jvm-clients raises the same refusal.
  ops::query_request with_target = base;
  with_target.send_to_node = "node1:8093";
  assert_false(pq::can_encode(with_target), "targeting a specific query node is not supported");
}

void
decode_meta_data_maps_status_metrics_warnings([[maybe_unused]] context& ctx)
{
  v1::QueryResponse_MetaData proto;
  proto.set_request_id("req-7");
  proto.set_client_context_id("ctx-7");
  proto.set_status(v1::QueryResponse_MetaData_Status_STATUS_SUCCESS);
  proto.set_signature(R"({"greeting":"string"})");
  proto.set_profile(R"({"phaseTimes":{}})");
  auto* metrics = proto.mutable_metrics();
  metrics->set_result_count(3);
  metrics->set_result_size(128);
  metrics->set_sort_count(1);
  metrics->set_mutation_count(2);
  metrics->set_error_count(0);
  metrics->set_warning_count(1);
  metrics->mutable_elapsed_time()->set_seconds(1);
  metrics->mutable_elapsed_time()->set_nanos(500'000'000);
  metrics->mutable_execution_time()->set_nanos(250'000'000);
  auto* warning = proto.add_warnings();
  warning->set_code(4321);
  warning->set_message("deprecated");

  ops::query_response::query_meta_data meta;
  pq::decode_meta_data(proto, meta);

  assert_eq(meta.request_id, std::string{ "req-7" }, "request_id decoded");
  assert_eq(meta.client_context_id, std::string{ "ctx-7" }, "client_context_id decoded");
  assert_eq(meta.status, std::string{ "success" }, "status enum -> string");
  assert_true(meta.signature.has_value(), "signature decoded");
  assert_true(meta.profile.has_value(), "profile decoded");
  assert_true(meta.metrics.has_value(), "metrics present");
  assert_eq(meta.metrics->result_count, std::uint64_t{ 3 }, "result_count decoded");
  assert_eq(meta.metrics->result_size, std::uint64_t{ 128 }, "result_size decoded");
  assert_eq(meta.metrics->sort_count, std::uint64_t{ 1 }, "sort_count decoded");
  assert_eq(meta.metrics->mutation_count, std::uint64_t{ 2 }, "mutation_count decoded");
  assert_eq(meta.metrics->warning_count, std::uint64_t{ 1 }, "warning_count decoded");
  assert_true(meta.metrics->elapsed_time == std::chrono::nanoseconds{ 1'500'000'000 },
              "elapsed_time duration decoded");
  assert_true(meta.metrics->execution_time == std::chrono::nanoseconds{ 250'000'000 },
              "execution_time duration decoded");
  assert_true(meta.warnings.has_value(), "warnings present");
  assert_eq(meta.warnings->at(0).code, std::uint64_t{ 4321 }, "warning code decoded");
  assert_eq(meta.warnings->at(0).message, std::string{ "deprecated" }, "warning message decoded");
}

// metrics, signature, profile and warnings are all optional in the schema. A response without
// them must leave the corresponding optionals unset rather than fabricate empty values, because
// the SDK surface distinguishes "not requested" from "requested and empty".
void
decode_meta_data_leaves_absent_optionals_unset([[maybe_unused]] context& ctx)
{
  v1::QueryResponse_MetaData proto;
  proto.set_request_id("req-8");
  proto.set_status(v1::QueryResponse_MetaData_Status_STATUS_SUCCESS);

  ops::query_response::query_meta_data meta;
  pq::decode_meta_data(proto, meta);

  assert_false(meta.metrics.has_value(), "absent metrics stay unset");
  assert_false(meta.signature.has_value(), "absent signature stays unset");
  assert_false(meta.profile.has_value(), "absent profile stays unset");
  assert_false(meta.warnings.has_value(), "absent warnings stay unset");
}

// Every Status the schema defines has a string. A value with no case would decode as "unknown",
// which the SDK surface cannot distinguish from the schema's own STATUS_UNKNOWN.
void
every_metadata_status_decodes_to_its_own_string([[maybe_unused]] context& ctx)
{
  const std::vector<std::pair<v1::QueryResponse_MetaData_Status, std::string>> expected{
    { v1::QueryResponse_MetaData_Status_STATUS_RUNNING, "running" },
    { v1::QueryResponse_MetaData_Status_STATUS_SUCCESS, "success" },
    { v1::QueryResponse_MetaData_Status_STATUS_ERRORS, "errors" },
    { v1::QueryResponse_MetaData_Status_STATUS_COMPLETED, "completed" },
    { v1::QueryResponse_MetaData_Status_STATUS_STOPPED, "stopped" },
    { v1::QueryResponse_MetaData_Status_STATUS_TIMEOUT, "timeout" },
    { v1::QueryResponse_MetaData_Status_STATUS_CLOSED, "closed" },
    { v1::QueryResponse_MetaData_Status_STATUS_FATAL, "fatal" },
    { v1::QueryResponse_MetaData_Status_STATUS_ABORTED, "aborted" },
    { v1::QueryResponse_MetaData_Status_STATUS_UNKNOWN, "unknown" },
  };
  // Guards against the schema gaining a value that nothing here covers.
  assert_eq(expected.size(),
            static_cast<std::size_t>(v1::QueryResponse_MetaData_Status_Status_ARRAYSIZE),
            "every status the schema defines is covered");

  for (const auto& [status, text] : expected) {
    v1::QueryResponse_MetaData proto;
    proto.set_status(status);
    ops::query_response::query_meta_data meta;
    pq::decode_meta_data(proto, meta);
    assert_eq(meta.status, text, "status " + text + " decoded");
  }
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(encode_maps_core_fields) },
      { CASE(positional_parameters_carry_their_json_payload) },
      { CASE(named_parameters_carry_their_json_payload) },
      { CASE(a_parameter_payload_is_copied_byte_for_byte) },
      { CASE(a_string_arm_parameter_is_encoded_too) },
      { CASE(an_empty_parameter_encodes_as_empty) },
      { CASE(query_context_sets_both_fields_or_neither) },
      { CASE(encode_maps_mutation_state_to_consistent_with) },
      { CASE(every_scan_consistency_and_profile_value_maps) },
      { CASE(scan_wait_splits_into_seconds_and_nanos) },
      { CASE(tuning_fields_map_to_their_proto_counterparts) },
      { CASE(can_encode_rejects_tuning_values_that_do_not_fit_uint32) },
      { CASE(can_encode_rejects_unsupported_features) },
      { CASE(decode_meta_data_maps_status_metrics_warnings) },
      { CASE(decode_meta_data_leaves_absent_optionals_unset) },
      { CASE(every_metadata_status_decodes_to_its_own_string) },
    },
  };
}

} // namespace couchbase::test
