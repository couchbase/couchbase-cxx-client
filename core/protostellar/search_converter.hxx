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

// Translates the core FTS search request/response to and from couchbase.search.v1. Hits arrive
// across streamed SearchQueryResponse messages and are buffered into search_response::rows; the
// terminal message carries the metadata.
//
// The couchbase2 SearchQueryRequest models the FTS query as a structured `Query` oneof of 21 types,
// whereas the core request carries an opaque pre-serialized JSON `query`. This converter translates
// the trivial forms -- query_string (optionally boosted), match_all and match_none -- and reports
// every other shape as unmappable so the component surfaces feature_not_available. Structured
// facets, structured sort specs, vector search, mutation-token consistency, and the `raw`
// passthrough have no couchbase2 home and are likewise gated. Translating the remaining eighteen
// query arms is a follow-up; the gate is what keeps an untranslated one from being dropped in
// silence in the meantime.

#include "core/operations/document_search.hxx"
#include "core/utils/json.hxx"

#include <couchbase/search/v1/search.pb.h>

// core/utils/json.hxx only forward-declares tao::json::value; this header instantiates one and
// calls its members, so it needs the full definition.
#include <tao/json/value.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::core::protostellar::search
{
namespace v1 = ::couchbase::search::v1;

// Coarse, cheap gate for request features with no couchbase2 mapping. Query-shape support is
// decided in encode() (which must parse the query JSON).
[[nodiscard]] inline auto
can_encode(const operations::search_request& request) -> bool
{
  return request.raw.empty() && request.facets.empty() && request.sort_specs.empty() &&
         request.mutation_state.empty() && !request.vector_search.has_value() &&
         !request.vector_query_combination.has_value() && !request.row_callback.has_value();
}

// True when request.query does not parse as JSON at all, as opposed to parsing into a shape this
// converter does not translate. The component maps the two to different errors: a query the caller
// failed to serialize is their own invalid_argument, not a gap in couchbase2 support.
[[nodiscard]] inline auto
query_is_malformed(const operations::search_request& request) -> bool
{
  try {
    static_cast<void>(utils::json::parse(request.query));
  } catch (...) {
    return true;
  }
  return false;
}

// Translate the parsed core query JSON into the structured Query oneof, for the trivial forms only.
// Returns false, leaving proto.query unset, for every shape this converter does not translate.
[[nodiscard]] inline auto
apply_query(const tao::json::value& parsed, v1::SearchQueryRequest& proto) -> bool
{
  if (!parsed.is_object()) {
    return false;
  }
  // A boost is serialized beside the query key rather than inside it: query_string("foo").boost(2)
  // encodes as {"query":"foo","boost":2}. Rejecting every two-key object would therefore make an
  // ordinary boosted query unroutable, so a boost is read as a modifier of the recognized key.
  // Whether it can actually be carried depends on the arm, and is decided below. Any other sibling
  // (an analyzer, a field) would be lost in translation, so a wider object is reported as
  // unmappable and the component surfaces feature_not_available.
  const auto* boost = parsed.find("boost");
  if (parsed.get_object().size() > (boost == nullptr ? 1U : 2U)) {
    return false;
  }
  const auto* match_all = parsed.find("match_all");
  const auto* match_none = parsed.find("match_none");
  if (match_all != nullptr || match_none != nullptr) {
    // MatchAllQuery and MatchNoneQuery are empty messages, so a boost on either has nowhere to go.
    // Refusing the request keeps it from being run at a weight the caller did not ask for.
    if (boost != nullptr) {
      return false;
    }
    if (match_all != nullptr) {
      proto.mutable_query()->mutable_match_all_query();
    } else {
      proto.mutable_query()->mutable_match_none_query();
    }
    return true;
  }
  if (const auto* q = parsed.find("query"); q != nullptr && q->is_string()) {
    auto* query_string = proto.mutable_query()->mutable_query_string_query();
    query_string->set_query_string(q->get_string());
    if (boost != nullptr) {
      if (!boost->is_number()) {
        return false;
      }
      query_string->set_boost(static_cast<float>(boost->as<double>()));
    }
    return true;
  }
  return false;
}

// Encode the request; returns nullopt for a gated feature, an untranslated query shape, or a query
// that is not JSON. The component separates the last case with query_is_malformed().
[[nodiscard]] inline auto
encode(const operations::search_request& request) -> std::optional<v1::SearchQueryRequest>
{
  if (!can_encode(request)) {
    return std::nullopt;
  }
  v1::SearchQueryRequest proto;
  proto.set_index_name(request.index_name);
  if (request.bucket_name.has_value()) {
    proto.set_bucket_name(*request.bucket_name);
  }
  if (request.scope_name.has_value()) {
    proto.set_scope_name(*request.scope_name);
  }
  tao::json::value parsed;
  try {
    parsed = utils::json::parse(request.query);
  } catch (...) {
    return std::nullopt;
  }
  if (!apply_query(parsed, proto)) {
    return std::nullopt;
  }
  if (request.limit.has_value()) {
    proto.set_limit(*request.limit);
  }
  if (request.skip.has_value()) {
    proto.set_skip(*request.skip);
  }
  if (request.explain.value_or(false)) {
    proto.set_include_explanation(true);
  }
  proto.set_disable_scoring(request.disable_scoring);
  proto.set_include_locations(request.include_locations);
  proto.set_scan_consistency(v1::SearchQueryRequest_ScanConsistency_SCAN_CONSISTENCY_NOT_BOUNDED);
  if (request.highlight_style.has_value()) {
    proto.set_highlight_style(*request.highlight_style == search_highlight_style::ansi
                                ? v1::SearchQueryRequest_HighlightStyle_HIGHLIGHT_STYLE_ANSI
                                : v1::SearchQueryRequest_HighlightStyle_HIGHLIGHT_STYLE_HTML);
  }
  for (const auto& field : request.highlight_fields) {
    proto.add_highlight_fields(field);
  }
  for (const auto& field : request.fields) {
    proto.add_fields(field);
  }
  for (const auto& collection : request.collections) {
    proto.add_collections(collection);
  }
  return proto;
}

// Append the hits from one streamed message onto the buffered response and, if present, decode the
// terminal metadata. Structured facets are gated by can_encode(), so a response cannot carry any.
inline void
decode(const v1::SearchQueryResponse& message, operations::search_response& response)
{
  for (const auto& hit : message.hits()) {
    operations::search_response::search_row row;
    row.index = hit.index();
    row.id = hit.id();
    row.score = hit.score();
    row.explanation = hit.explanation();
    for (const auto& [field, fragment] : hit.fragments()) {
      row.fragments[field] =
        std::vector<std::string>{ fragment.content().begin(), fragment.content().end() };
    }
    // The proto keys each requested field to its value as raw JSON, whereas the core row carries
    // the whole set as one JSON object, so the object is reassembled here.
    if (!hit.fields().empty()) {
      tao::json::value fields = tao::json::empty_object;
      for (const auto& [name, value] : hit.fields()) {
        try {
          fields[name] = utils::json::parse(value);
        } catch (...) {
          // decode() runs on the io_context out of a stream callback, where an escaping exception
          // would take down the loop. A value that is not JSON is kept verbatim as a string so the
          // field still reaches the caller.
          fields[name] = value;
        }
      }
      row.fields = utils::json::generate(fields);
    }
    for (const auto& location : hit.locations()) {
      operations::search_response::search_location out;
      out.field = location.field();
      out.term = location.term();
      out.position = location.position();
      out.start_offset = location.start();
      out.end_offset = location.end();
      if (location.array_positions_size() > 0) {
        out.array_positions = std::vector<std::uint64_t>{ location.array_positions().begin(),
                                                          location.array_positions().end() };
      }
      row.locations.push_back(std::move(out));
    }
    response.rows.push_back(std::move(row));
  }
  if (message.has_meta_data()) {
    const auto& meta = message.meta_data();
    if (meta.has_metrics()) {
      const auto& m = meta.metrics();
      response.meta.metrics.took = std::chrono::seconds{ m.execution_time().seconds() } +
                                   std::chrono::nanoseconds{ m.execution_time().nanos() };
      response.meta.metrics.total_rows = m.total_rows();
      response.meta.metrics.max_score = m.max_score();
      response.meta.metrics.success_partition_count = m.success_partition_count();
      response.meta.metrics.error_partition_count = m.error_partition_count();
    }
    for (const auto& [location, msg] : meta.errors()) {
      response.meta.errors.try_emplace(location, msg);
    }
  }
}

} // namespace couchbase::core::protostellar::search
