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

// Translates the core analytics request/response to and from the couchbase.analytics.v1 protobuf
// messages, mirroring query_converter. Rows arrive across streamed AnalyticsQueryResponse messages
// and are buffered into analytics_response::rows; the terminal message carries the metadata.
//
// Features with no couchbase2 equivalent are reported by can_encode() so the component can surface
// feature_not_available rather than silently dropping them: the `raw` passthrough map and scoped
// analytics. The streaming row_callback IS wired (CXXCBC-910), so it no longer gates here.
//
// Scoped analytics is refused because the schema cannot express it, not merely because it is
// unimplemented. AnalyticsQueryRequest at the pinned protostellar revision retired the field the
// older schema used for this: field 8 is `reserved // bucket_name`, and field 9 was renamed from
// `scope_name` to `analytics_scope_name` -- one analytics-scope identifier rather than the
// bucket/scope pair the core request carries in bucket_name/scope_name/scope_qualifier. Collapsing
// a two-part qualification into that single name would be a guess about which the gateway means, so
// the request is refused instead. (Vendored copies in couchbase-jvm-clients and
// couchbase-net-client still carry the older bucket_name=8/scope_name=9 shape, so a mapping cribbed
// from them would target a schema this client does not speak.)

#include "core/operations/document_analytics.hxx"
#include "core/protostellar/json_payload.hxx"

#include <couchbase/analytics_scan_consistency.hxx>

#include <couchbase/analytics/v1/analytics.pb.h>

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace couchbase::core::protostellar::analytics
{
namespace v1 = ::couchbase::analytics::v1;

[[nodiscard]] inline auto
can_encode(const operations::analytics_request& request) -> bool
{
  return request.raw.empty() && !request.bucket_name.has_value() &&
         !request.scope_name.has_value() && !request.scope_qualifier.has_value();
}

inline auto
encode(const operations::analytics_request& request) -> v1::AnalyticsQueryRequest
{
  v1::AnalyticsQueryRequest proto;
  proto.set_statement(request.statement);
  proto.set_read_only(request.readonly);
  proto.set_priority(request.priority);
  if (request.client_context_id.has_value()) {
    proto.set_client_context_id(*request.client_context_id);
  }
  if (request.scan_consistency.has_value()) {
    switch (*request.scan_consistency) {
      case analytics_scan_consistency::not_bounded:
        proto.set_scan_consistency(
          v1::AnalyticsQueryRequest_ScanConsistency_SCAN_CONSISTENCY_NOT_BOUNDED);
        break;
      case analytics_scan_consistency::request_plus:
        proto.set_scan_consistency(
          v1::AnalyticsQueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS);
        break;
    }
  }
  for (const auto& parameter : request.positional_parameters) {
    proto.add_positional_parameters(json_payload(parameter));
  }
  for (const auto& [name, value] : request.named_parameters) {
    (*proto.mutable_named_parameters())[name] = json_payload(value);
  }
  return proto;
}

[[nodiscard]] inline auto
to_nanoseconds(const google::protobuf::Duration& duration) -> std::chrono::nanoseconds
{
  return std::chrono::seconds{ duration.seconds() } + std::chrono::nanoseconds{ duration.nanos() };
}

// The gateway reports status as a lowercase string; map it onto the core enum.
[[nodiscard]] inline auto
status_from_string(const std::string& status) -> operations::analytics_response::analytics_status
{
  using s = operations::analytics_response::analytics_status;
  if (status == "running") {
    return s::running;
  }
  if (status == "success") {
    return s::success;
  }
  if (status == "errors") {
    return s::errors;
  }
  if (status == "completed") {
    return s::completed;
  }
  if (status == "stopped") {
    return s::stopped;
  }
  if (status == "timeout" || status == "timedout") {
    return s::timedout;
  }
  if (status == "closed") {
    return s::closed;
  }
  if (status == "fatal") {
    return s::fatal;
  }
  if (status == "aborted") {
    return s::aborted;
  }
  return s::unknown;
}

inline void
decode_meta_data(const v1::AnalyticsQueryResponse_MetaData& proto,
                 operations::analytics_response::analytics_meta_data& meta)
{
  meta.request_id = proto.request_id();
  meta.client_context_id = proto.client_context_id();
  meta.status = status_from_string(proto.status());
  if (!proto.signature().empty()) {
    meta.signature = proto.signature();
  }
  if (proto.has_metrics()) {
    const auto& m = proto.metrics();
    meta.metrics.elapsed_time = to_nanoseconds(m.elapsed_time());
    meta.metrics.execution_time = to_nanoseconds(m.execution_time());
    meta.metrics.result_count = m.result_count();
    meta.metrics.result_size = m.result_size();
    meta.metrics.error_count = m.error_count();
    meta.metrics.processed_objects = m.processed_objects();
    meta.metrics.warning_count = m.warning_count();
  }
  // Replaced rather than appended, matching query_converter: decode_meta_data runs once per
  // MetaData message, and appending would accumulate duplicates without bound if a gateway ever
  // sent more than one.
  if (proto.warnings_size() > 0) {
    std::vector<operations::analytics_response::analytics_problem> warnings;
    warnings.reserve(static_cast<std::size_t>(proto.warnings_size()));
    for (const auto& w : proto.warnings()) {
      operations::analytics_response::analytics_problem problem;
      problem.code = w.code();
      problem.message = w.message();
      warnings.push_back(std::move(problem));
    }
    meta.warnings = std::move(warnings);
  }
}

} // namespace couchbase::core::protostellar::analytics
