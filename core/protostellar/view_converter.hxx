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

// Views are deprecated in the core API but still routable over couchbase2; suppress the core
// deprecation attribute for this header. push/pop the macro so an including translation unit that
// already defined it keeps its state restored, rather than having it clobbered at the bottom.
#pragma push_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
#define COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS

// Translates the core map/reduce view request/response to and from couchbase.view.v1. Rows arrive
// across streamed ViewQueryResponse messages and are buffered into document_view_response::rows;
// the terminal message carries the metadata.
//
// The couchbase2 ViewQueryRequest has no home for the core request's `raw`, `query_string`,
// `full_set`, or streaming `row_callback` fields; can_encode() reports those so the component
// surfaces feature_not_available rather than silently dropping them. The proto `limit`/`skip` are
// uint32 while core carries uint64, so can_encode() also rejects values that would truncate.
// `client_context_id`/`timeout` likewise have no proto field but do not change results, so they are
// simply not sent.

#include "core/operations/document_view.hxx"

#include <couchbase/view/v1/view.pb.h>

#include <cstdint>
#include <limits>
#include <string>

namespace couchbase::core::protostellar::view
{
namespace v1 = ::couchbase::view::v1;

[[nodiscard]] inline auto
can_encode(const operations::document_view_request& request) -> bool
{
  constexpr auto u32_max = (std::numeric_limits<std::uint32_t>::max)();
  return request.raw.empty() && request.query_string.empty() && !request.full_set.value_or(false) &&
         !request.row_callback.has_value() && request.limit.value_or(0) <= u32_max &&
         request.skip.value_or(0) <= u32_max;
}

inline auto
encode(const operations::document_view_request& request) -> v1::ViewQueryRequest
{
  v1::ViewQueryRequest proto;
  proto.set_bucket_name(request.bucket_name);
  proto.set_design_document_name(request.document_name);
  proto.set_view_name(request.view_name);
  proto.set_namespace_(request.ns == design_document_namespace::development
                         ? v1::ViewQueryRequest_Namespace_NAMESPACE_DEVELOPMENT
                         : v1::ViewQueryRequest_Namespace_NAMESPACE_PRODUCTION);

  for (const auto& key : request.keys) {
    proto.add_keys(key);
  }
  if (request.key.has_value()) {
    proto.set_key(*request.key);
  }
  if (request.start_key.has_value()) {
    proto.set_start_key(*request.start_key);
  }
  if (request.end_key.has_value()) {
    proto.set_end_key(*request.end_key);
  }
  if (request.start_key_doc_id.has_value()) {
    proto.set_start_key_doc_id(*request.start_key_doc_id);
  }
  if (request.end_key_doc_id.has_value()) {
    proto.set_end_key_doc_id(*request.end_key_doc_id);
  }
  if (request.limit.has_value()) {
    proto.set_limit(static_cast<std::uint32_t>(*request.limit));
  }
  if (request.skip.has_value()) {
    proto.set_skip(static_cast<std::uint32_t>(*request.skip));
  }
  if (request.reduce.has_value()) {
    proto.set_reduce(*request.reduce);
  }
  if (request.group.has_value()) {
    proto.set_group(*request.group);
  }
  if (request.group_level.has_value()) {
    proto.set_group_level(*request.group_level);
  }
  if (request.inclusive_end.has_value()) {
    proto.set_inclusive_end(*request.inclusive_end);
  }
  proto.set_debug(request.debug);
  if (request.consistency.has_value()) {
    switch (*request.consistency) {
      case view_scan_consistency::not_bounded:
        proto.set_scan_consistency(
          v1::ViewQueryRequest_ScanConsistency_SCAN_CONSISTENCY_NOT_BOUNDED);
        break;
      case view_scan_consistency::update_after:
        proto.set_scan_consistency(
          v1::ViewQueryRequest_ScanConsistency_SCAN_CONSISTENCY_UPDATE_AFTER);
        break;
      case view_scan_consistency::request_plus:
        proto.set_scan_consistency(
          v1::ViewQueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS);
        break;
    }
  }
  if (request.order.has_value()) {
    proto.set_order(*request.order == view_sort_order::descending
                      ? v1::ViewQueryRequest_Order_ORDER_DESCENDING
                      : v1::ViewQueryRequest_Order_ORDER_ASCENDING);
  }
  if (request.on_error.has_value()) {
    proto.set_on_error(*request.on_error == view_on_error::stop
                         ? v1::ViewQueryRequest_ErrorMode_ERROR_MODE_STOP
                         : v1::ViewQueryRequest_ErrorMode_ERROR_MODE_CONTINUE);
  }
  return proto;
}

// Append the rows from one streamed message onto the buffered response.
inline void
decode_rows(const v1::ViewQueryResponse& message, operations::document_view_response& response)
{
  for (const auto& row : message.rows()) {
    operations::document_view_response::row out;
    if (!row.id().empty()) {
      out.id = row.id();
    }
    out.key = row.key();
    out.value = row.value();
    response.rows.push_back(std::move(out));
  }
  if (message.has_meta_data()) {
    response.meta.total_rows = message.meta_data().total_rows();
    if (!message.meta_data().debug().empty()) {
      response.meta.debug_info = message.meta_data().debug();
    }
  }
}

} // namespace couchbase::core::protostellar::view

#pragma pop_macro("COUCHBASE_CXX_CLIENT_IGNORE_CORE_DEPRECATIONS")
