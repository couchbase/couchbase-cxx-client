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

// Unit tests for the analytics <-> couchbase.analytics.v1 converter (CXXCBC-898). Pure, no
// server.

#include "framework/test_runner.hxx"

#include "core/protostellar/analytics_converter.hxx"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
namespace pa = ::couchbase::core::protostellar::analytics;
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::analytics::v1;

// Builds the variant arm the public API actually populates. couchbase/analytics_options.hxx carries
// parameters as codec::binary, so a test that constructs a json_string from a std::string exercises
// an arm production never selects -- and passes whether or not the converter reads the right one.
auto
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
  ops::analytics_request request;
  request.statement = "SELECT 1";
  request.client_context_id = "ctx-9";
  request.readonly = true;
  request.priority = true;
  request.scan_consistency = couchbase::core::analytics_scan_consistency::request_plus;
  request.positional_parameters.emplace_back(R"("pos")");
  request.named_parameters.emplace("a", couchbase::core::json_string{ R"("named")" });

  const auto proto = pa::encode(request);
  assert_eq(proto.statement(), std::string{ "SELECT 1" }, "statement mapped");
  assert_eq(proto.client_context_id(), std::string{ "ctx-9" }, "client_context_id mapped");
  assert_true(proto.read_only(), "readonly mapped");
  assert_true(proto.priority(), "priority mapped");
  assert_true(proto.scan_consistency() ==
                v1::AnalyticsQueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS,
              "scan_consistency mapped");
  assert_eq(proto.positional_parameters_size(), 1, "one positional parameter");
  assert_eq(proto.positional_parameters(0), std::string{ R"("pos")" }, "positional value mapped");
  assert_eq(proto.named_parameters().at("a"), std::string{ R"("named")" }, "named value mapped");
}

// The regression guard for the parameter defect: json_string::str() returns a reference to a static
// empty string unless the std::string arm is active, so reading a binary-arm parameter with it
// sends
// "". Asserting the length as well as the value keeps an empty-vs-empty comparison from passing.
void
parameters_from_the_binary_arm_carry_their_payload([[maybe_unused]] context& ctx)
{
  const std::string payload{ R"({"a":1})" };

  ops::analytics_request request;
  request.statement = "SELECT $1, $named";
  request.positional_parameters.emplace_back(binary_json(payload));
  request.named_parameters.emplace("named", binary_json(payload));

  const auto proto = pa::encode(request);
  assert_eq(proto.positional_parameters(0).size(), std::size_t{ 7 }, "positional length preserved");
  assert_true(proto.positional_parameters(0) == payload, "positional payload bytes preserved");
  assert_eq(
    proto.named_parameters().at("named").size(), std::size_t{ 7 }, "named length preserved");
  assert_true(proto.named_parameters().at("named") == payload, "named payload bytes preserved");
}

// A default-constructed json_string holds neither arm; it must encode as empty rather than trip
// over the variant.
void
an_empty_parameter_encodes_as_empty([[maybe_unused]] context& ctx)
{
  ops::analytics_request request;
  request.statement = "SELECT $1";
  request.positional_parameters.emplace_back();

  const auto proto = pa::encode(request);
  assert_eq(proto.positional_parameters_size(), 1, "the parameter is still sent");
  assert_true(proto.positional_parameters(0).empty(), "an unset parameter encodes as empty");
}

void
can_encode_rejects_unsupported_features([[maybe_unused]] context& ctx)
{
  ops::analytics_request base;
  base.statement = "SELECT 1";
  assert_true(pa::can_encode(base), "plain analytics encodes");

  ops::analytics_request with_raw = base;
  with_raw.raw.emplace("timeout", couchbase::core::json_string{ "\"1s\"" });
  assert_false(pa::can_encode(with_raw), "raw passthrough is not supported");

  ops::analytics_request with_row_callback = base;
  with_row_callback.row_callback = [](std::string) {
    return couchbase::core::utils::json::stream_control::next_row;
  };
  assert_true(pa::can_encode(with_row_callback), "the analytics streaming row callback is wired");
}

// Every way of naming a scope has to be refused, not just the one the original test covered. The
// pinned schema reserves field 8 (the old bucket_name) and field 9 is a single
// analytics_scope_name, so there is nowhere to put a bucket/scope pair; encoding any of these would
// drop the qualification and run the statement against the wrong scope.
void
can_encode_rejects_every_scope_qualification([[maybe_unused]] context& ctx)
{
  ops::analytics_request base;
  base.statement = "SELECT 1";

  ops::analytics_request with_bucket = base;
  with_bucket.bucket_name = "b";
  assert_false(pa::can_encode(with_bucket), "bucket_name alone is refused");

  ops::analytics_request with_scope = base;
  with_scope.scope_name = "s";
  assert_false(pa::can_encode(with_scope), "scope_name alone is refused");

  ops::analytics_request with_qualifier = base;
  with_qualifier.scope_qualifier = "default:`b`.`s`";
  assert_false(pa::can_encode(with_qualifier), "scope_qualifier is refused");
}

void
decode_meta_data_maps_status_metrics_warnings([[maybe_unused]] context& ctx)
{
  v1::AnalyticsQueryResponse_MetaData proto;
  proto.set_request_id("req-9");
  proto.set_client_context_id("ctx-9");
  proto.set_status("success");
  auto* metrics = proto.mutable_metrics();
  metrics->set_result_count(2);
  metrics->set_processed_objects(10);
  metrics->mutable_execution_time()->set_seconds(2);
  auto* warning = proto.add_warnings();
  warning->set_code(99);
  warning->set_message("careful");

  ops::analytics_response::analytics_meta_data meta;
  pa::decode_meta_data(proto, meta);

  assert_eq(meta.request_id, std::string{ "req-9" }, "request_id decoded");
  assert_true(meta.status == ops::analytics_response::success, "status string -> enum");
  assert_eq(meta.metrics.result_count, std::uint64_t{ 2 }, "result_count decoded");
  assert_eq(meta.metrics.processed_objects, std::uint64_t{ 10 }, "processed_objects decoded");
  assert_true(meta.metrics.execution_time == std::chrono::seconds{ 2 }, "execution_time decoded");
  assert_eq(meta.warnings.size(), std::size_t{ 1 }, "one warning decoded");
  assert_eq(meta.warnings.at(0).code, std::uint64_t{ 99 }, "warning code decoded");
}

// decode_meta_data replaces the warning list rather than appending to it, matching the query
// converter. Appending would duplicate warnings on this path only, and would grow without bound if
// a gateway ever sent more than one MetaData message on a stream.
void
warnings_are_replaced_when_metadata_is_decoded_again([[maybe_unused]] context& ctx)
{
  v1::AnalyticsQueryResponse_MetaData proto;
  proto.set_status("success");
  auto* warning = proto.add_warnings();
  warning->set_code(99);
  warning->set_message("careful");

  ops::analytics_response::analytics_meta_data meta;
  pa::decode_meta_data(proto, meta);
  pa::decode_meta_data(proto, meta);

  assert_eq(meta.warnings.size(), std::size_t{ 1 }, "the second decode replaced rather than added");
  assert_eq(
    meta.warnings.at(0).code, std::uint64_t{ 99 }, "the surviving warning is the decoded one");
}

// The gateway sends the analytics status as a lowercase string. "timeout" and "timedout" both have
// to reach the same enumerator, and anything unrecognised must land on `unknown` rather than on the
// zero-valued `running`, which would read as a query still in flight.
void
status_strings_map_to_the_core_enum([[maybe_unused]] context& ctx)
{
  const auto decode = [](const char* status) {
    v1::AnalyticsQueryResponse_MetaData proto;
    proto.set_status(status);
    ops::analytics_response::analytics_meta_data meta;
    pa::decode_meta_data(proto, meta);
    return meta.status;
  };

  assert_true(decode("success") == ops::analytics_response::success, "success mapped");
  assert_true(decode("errors") == ops::analytics_response::errors, "errors mapped");
  assert_true(decode("fatal") == ops::analytics_response::fatal, "fatal mapped");
  assert_true(decode("aborted") == ops::analytics_response::aborted, "aborted mapped");
  assert_true(decode("timeout") == ops::analytics_response::timedout, "timeout aliased");
  assert_true(decode("timedout") == ops::analytics_response::timedout, "timedout mapped");
  assert_true(decode("something-new") == ops::analytics_response::unknown,
              "an unrecognised status is unknown, not running");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_analytics_converter",
    {
      { "encode_maps_core_fields", encode_maps_core_fields },
      { "parameters_from_the_binary_arm_carry_their_payload",
        parameters_from_the_binary_arm_carry_their_payload },
      { "an_empty_parameter_encodes_as_empty", an_empty_parameter_encodes_as_empty },
      { "can_encode_rejects_unsupported_features", can_encode_rejects_unsupported_features },
      { "can_encode_rejects_every_scope_qualification",
        can_encode_rejects_every_scope_qualification },
      { "decode_meta_data_maps_status_metrics_warnings",
        decode_meta_data_maps_status_metrics_warnings },
      { "warnings_are_replaced_when_metadata_is_decoded_again",
        warnings_are_replaced_when_metadata_is_decoded_again },
      { "status_strings_map_to_the_core_enum", status_strings_map_to_the_core_enum },
    },
  };
}

} // namespace couchbase::test
