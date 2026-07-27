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

// Translates the core N1QL query request/response to and from the couchbase.query.v1 protobuf
// messages. Rows arrive across streamed QueryResponse messages and are buffered into
// query_response::rows; the terminal message carries the metadata. Deliberately does not reuse the
// MCBP-bound encode_to/make_response (those build an HTTP body + parse a JSON reply).
//
// Query features with no couchbase2 equivalent are reported by can_encode() so the component can
// surface feature_not_available rather than silently dropping them: the `raw` passthrough map,
// use_replica, the streaming row_callback (only the buffered result path is wired), node targeting
// via send_to_node, and tuning values too large for the proto's uint32 fields.

#include "core/json_string.hxx"
#include "core/operations/document_query.hxx"
#include "core/protostellar/json_payload.hxx"
#include "core/protostellar/query_proto.hxx"

#include <couchbase/query_profile.hxx>
#include <couchbase/query_scan_consistency.hxx>

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace couchbase::core::protostellar::query
{
namespace v1 = ::couchbase::query::v1;

// The proto's tuning fields are uint32 while the core request holds them as uint64, so a value
// that does not fit is not encodable: casting it would ask the gateway to use the wrapped number.
[[nodiscard]] inline auto
fits_uint32(const std::optional<std::uint64_t>& value) -> bool
{
  return !value.has_value() || *value <= std::numeric_limits<std::uint32_t>::max();
}

// Query request features the couchbase2 gateway schema cannot express yet. The component maps this
// to feature_not_available instead of silently ignoring the caller's intent.
[[nodiscard]] inline auto
can_encode(const operations::query_request& request) -> bool
{
  return request.raw.empty() && !request.use_replica.value_or(false) &&
         !request.row_callback.has_value() && !request.send_to_node.has_value() &&
         fits_uint32(request.max_parallelism) && fits_uint32(request.pipeline_batch) &&
         fits_uint32(request.pipeline_cap) && fits_uint32(request.scan_cap);
}

// Split the SDK query_context ("default:`bucket`.`scope`") into the proto's bucket_name/scope_name.
// Any pair of backtick-delimited identifiers is accepted; a value that does not parse leaves both
// unset (the statement is then expected to be fully qualified).
inline void
apply_query_context(const std::string& query_context, v1::QueryRequest& proto)
{
  std::string bucket;
  std::string scope;
  std::string* current = &bucket;
  bool inside = false;
  int identifiers = 0;
  for (const char c : query_context) {
    if (c == '`') {
      if (inside) { // closing backtick ends an identifier
        ++identifiers;
        if (identifiers == 1) {
          current = &scope;
        }
      }
      inside = !inside;
      continue;
    }
    if (inside) {
      current->push_back(c);
    }
  }
  // A well-formed context is exactly two balanced backtick-delimited identifiers
  // (`bucket`.`scope`). Anything else -- unbalanced, too few, or too many -- leaves both unset
  // rather than routing on a half- or mis-parsed context.
  if (!inside && identifiers == 2 && !bucket.empty() && !scope.empty()) {
    proto.set_bucket_name(bucket);
    proto.set_scope_name(scope);
  }
}

inline auto
encode(const operations::query_request& request) -> v1::QueryRequest
{
  v1::QueryRequest proto;
  proto.set_statement(request.statement);
  if (request.client_context_id.has_value()) {
    proto.set_client_context_id(*request.client_context_id);
  }
  if (request.query_context.has_value()) {
    apply_query_context(*request.query_context, proto);
  }
  proto.set_read_only(request.readonly);
  proto.set_flex_index(request.flex_index);
  proto.set_preserve_expiry(request.preserve_expiry);
  // adhoc=false means "use a prepared statement" on the MCBP path; the gateway prepares/caches on
  // its side, so we only signal intent.
  if (!request.adhoc) {
    proto.set_prepared(true);
  }

  if (request.scan_consistency.has_value()) {
    switch (*request.scan_consistency) {
      case query_scan_consistency::not_bounded:
        proto.set_scan_consistency(v1::QueryRequest_ScanConsistency_SCAN_CONSISTENCY_NOT_BOUNDED);
        break;
      case query_scan_consistency::request_plus:
        proto.set_scan_consistency(v1::QueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS);
        break;
    }
  }
  // A non-empty mutation_state carries at_plus scan vectors; the gateway takes them as
  // consistent_with mutation tokens.
  for (const auto& token : request.mutation_state) {
    auto* proto_token = proto.add_consistent_with();
    proto_token->set_bucket_name(token.bucket_name());
    proto_token->set_vbucket_id(token.partition_id());
    proto_token->set_vbucket_uuid(token.partition_uuid());
    proto_token->set_seq_no(token.sequence_number());
  }

  if (request.profile.has_value()) {
    switch (*request.profile) {
      case query_profile::off:
        proto.set_profile_mode(v1::QueryRequest_ProfileMode_PROFILE_MODE_OFF);
        break;
      case query_profile::phases:
        proto.set_profile_mode(v1::QueryRequest_ProfileMode_PROFILE_MODE_PHASES);
        break;
      case query_profile::timings:
        proto.set_profile_mode(v1::QueryRequest_ProfileMode_PROFILE_MODE_TIMINGS);
        break;
    }
  }

  for (const auto& parameter : request.positional_parameters) {
    proto.add_positional_parameters(json_payload(parameter));
  }
  for (const auto& [name, value] : request.named_parameters) {
    (*proto.mutable_named_parameters())[name] = json_payload(value);
  }

  // Tuning options: the SDK's `metrics` flag defaults to off, so disable_metrics is its inverse.
  auto* tuning = proto.mutable_tuning_options();
  tuning->set_disable_metrics(!request.metrics);
  if (request.max_parallelism.has_value()) {
    tuning->set_max_parallelism(static_cast<std::uint32_t>(*request.max_parallelism));
  }
  if (request.pipeline_batch.has_value()) {
    tuning->set_pipeline_batch(static_cast<std::uint32_t>(*request.pipeline_batch));
  }
  if (request.pipeline_cap.has_value()) {
    tuning->set_pipeline_cap(static_cast<std::uint32_t>(*request.pipeline_cap));
  }
  if (request.scan_cap.has_value()) {
    tuning->set_scan_cap(static_cast<std::uint32_t>(*request.scan_cap));
  }
  if (request.scan_wait.has_value()) {
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(*request.scan_wait);
    const auto nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(*request.scan_wait - secs);
    auto* scan_wait = tuning->mutable_scan_wait();
    scan_wait->set_seconds(secs.count());
    scan_wait->set_nanos(static_cast<std::int32_t>(nanos.count()));
  }

  return proto;
}

// google.protobuf.Duration -> nanoseconds.
[[nodiscard]] inline auto
to_nanoseconds(const google::protobuf::Duration& duration) -> std::chrono::nanoseconds
{
  return std::chrono::seconds{ duration.seconds() } + std::chrono::nanoseconds{ duration.nanos() };
}

[[nodiscard]] inline auto
status_to_string(v1::QueryResponse_MetaData_Status status) -> std::string
{
  switch (status) {
    case v1::QueryResponse_MetaData_Status_STATUS_RUNNING:
      return "running";
    case v1::QueryResponse_MetaData_Status_STATUS_SUCCESS:
      return "success";
    case v1::QueryResponse_MetaData_Status_STATUS_ERRORS:
      return "errors";
    case v1::QueryResponse_MetaData_Status_STATUS_COMPLETED:
      return "completed";
    case v1::QueryResponse_MetaData_Status_STATUS_STOPPED:
      return "stopped";
    case v1::QueryResponse_MetaData_Status_STATUS_TIMEOUT:
      return "timeout";
    case v1::QueryResponse_MetaData_Status_STATUS_CLOSED:
      return "closed";
    case v1::QueryResponse_MetaData_Status_STATUS_FATAL:
      return "fatal";
    case v1::QueryResponse_MetaData_Status_STATUS_ABORTED:
      return "aborted";
    default:
      return "unknown";
  }
}

// Fill a query_meta_data from the terminal QueryResponse.MetaData message.
inline void
decode_meta_data(const v1::QueryResponse_MetaData& proto,
                 operations::query_response::query_meta_data& meta)
{
  meta.request_id = proto.request_id();
  meta.client_context_id = proto.client_context_id();
  meta.status = status_to_string(proto.status());
  if (!proto.signature().empty()) {
    meta.signature = proto.signature();
  }
  if (!proto.profile().empty()) {
    meta.profile = proto.profile();
  }
  if (proto.has_metrics()) {
    const auto& m = proto.metrics();
    operations::query_response::query_metrics metrics;
    metrics.elapsed_time = to_nanoseconds(m.elapsed_time());
    metrics.execution_time = to_nanoseconds(m.execution_time());
    metrics.result_count = m.result_count();
    metrics.result_size = m.result_size();
    metrics.sort_count = m.sort_count();
    metrics.mutation_count = m.mutation_count();
    metrics.error_count = m.error_count();
    metrics.warning_count = m.warning_count();
    meta.metrics = metrics;
  }
  if (proto.warnings_size() > 0) {
    std::vector<operations::query_response::query_problem> warnings;
    warnings.reserve(static_cast<std::size_t>(proto.warnings_size()));
    for (const auto& w : proto.warnings()) {
      operations::query_response::query_problem problem;
      problem.code = w.code();
      problem.message = w.message();
      warnings.push_back(std::move(problem));
    }
    meta.warnings = std::move(warnings);
  }
}

} // namespace couchbase::core::protostellar::query
