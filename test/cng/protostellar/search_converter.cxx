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

// Unit tests for the FTS search <-> couchbase.search.v1 converter (CXXCBC-899). Pure, no
// server. Only the trivial query forms are translated; everything else is reported as unmappable.

#include "framework/test_registry.hxx"

#include "core/protostellar/search_converter.hxx"

#include "core/utils/json.hxx"

#include <cstdint>
#include <string>

namespace couchbase::test
{
namespace
{
namespace ps = ::couchbase::core::protostellar::search;
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::search::v1;

auto
base_request() -> ops::search_request
{
  ops::search_request request;
  request.index_name = "idx";
  request.query = couchbase::core::json_string{ R"({"query":"foo"})" };
  return request;
}

void
encode_maps_envelope_and_query_string([[maybe_unused]] context& ctx)
{
  auto request = base_request();
  request.limit = 10;
  request.skip = 5;
  request.fields = { "name" };
  request.collections = { "c1" };
  request.include_locations = true;

  const auto encoded = ps::encode(request);
  assert_true(encoded.has_value(), "query_string request encodes");
  assert_eq(encoded->index_name(), std::string{ "idx" }, "index_name mapped");
  assert_eq(encoded->limit(), std::uint32_t{ 10 }, "limit mapped");
  assert_eq(encoded->skip(), std::uint32_t{ 5 }, "skip mapped");
  assert_true(encoded->include_locations(), "include_locations mapped");
  assert_eq(encoded->fields_size(), 1, "fields mapped");
  assert_eq(encoded->collections_size(), 1, "collections mapped");
  assert_true(encoded->has_query(), "query set");
  assert_true(encoded->query().has_query_string_query(), "query_string form");
  assert_eq(encoded->query().query_string_query().query_string(),
            std::string{ "foo" },
            "query string carried");
}

void
encode_maps_match_all_and_match_none([[maybe_unused]] context& ctx)
{
  auto all = base_request();
  all.query = couchbase::core::json_string{ R"({"match_all":{}})" };
  const auto encoded_all = ps::encode(all);
  assert_true(encoded_all.has_value() && encoded_all->query().has_match_all_query(),
              "match_all mapped");

  auto none = base_request();
  none.query = couchbase::core::json_string{ R"({"match_none":{}})" };
  const auto encoded_none = ps::encode(none);
  assert_true(encoded_none.has_value() && encoded_none->query().has_match_none_query(),
              "match_none mapped");
}

void
encode_returns_nullopt_for_unmappable_query([[maybe_unused]] context& ctx)
{
  auto request = base_request();
  request.query = couchbase::core::json_string{ R"({"term":"x","field":"f"})" };
  assert_false(ps::encode(request).has_value(), "structured term query is not yet mapped");

  auto extra = base_request();
  extra.query = couchbase::core::json_string{ R"({"query":"foo","analyzer":"standard"})" };
  assert_false(ps::encode(extra).has_value(),
               "a recognized query with an unmappable sibling key is not mapped");

  // MatchAllQuery and MatchNoneQuery are empty proto messages, so a boost on either would be lost.
  auto boosted_match_all = base_request();
  boosted_match_all.query = couchbase::core::json_string{ R"({"match_all":{},"boost":2})" };
  assert_false(ps::encode(boosted_match_all).has_value(),
               "match_all cannot carry a boost, so a boosted one is refused rather than flattened");
}

// A boost is serialized beside the query key, so rejecting every two-key object would make
// query_string("foo").boost(2) -- an ordinary supported query -- unroutable.
void
encode_maps_a_boosted_query_string([[maybe_unused]] context& ctx)
{
  auto request = base_request();
  request.query = couchbase::core::json_string{ R"({"query":"foo","boost":2.5})" };

  const auto encoded = ps::encode(request);
  assert_true(encoded.has_value(), "a boosted query_string encodes");
  assert_eq(encoded->query().query_string_query().query_string(),
            std::string{ "foo" },
            "query string mapped alongside the boost");
  assert_true(encoded->query().query_string_query().boost() == 2.5F, "boost mapped");
}

// Reported as invalid_argument by the component rather than feature_not_available: a query the
// caller failed to serialize is their own error, not a gap in couchbase2 support.
void
malformed_query_json_is_distinguished_from_an_unmappable_shape([[maybe_unused]] context& ctx)
{
  auto malformed = base_request();
  malformed.query = couchbase::core::json_string{ R"({"match_all":)" };
  assert_false(ps::encode(malformed).has_value(), "a malformed query does not encode");
  assert_true(ps::query_is_malformed(malformed), "a malformed query is reported as malformed");

  auto unmappable = base_request();
  unmappable.query = couchbase::core::json_string{ R"({"term":"x","field":"f"})" };
  assert_false(ps::query_is_malformed(unmappable),
               "a well-formed query this converter cannot translate is not malformed");
}

void
can_encode_rejects_gated_features([[maybe_unused]] context& ctx)
{
  assert_true(ps::can_encode(base_request()), "plain search passes the coarse gate");

  auto with_facets = base_request();
  with_facets.facets.emplace("f", "{}");
  assert_false(ps::can_encode(with_facets), "facets are gated");

  auto with_sort = base_request();
  with_sort.sort_specs = { R"("-_score")" };
  assert_false(ps::can_encode(with_sort), "sort specs are gated");

  auto with_vector = base_request();
  with_vector.vector_search = couchbase::core::json_string{ "{}" };
  assert_false(ps::can_encode(with_vector), "vector search is gated");

  auto with_combo = base_request();
  with_combo.vector_query_combination = couchbase::core::vector_query_combination::combination_and;
  assert_false(ps::can_encode(with_combo), "vector query combination is gated");

  auto with_rrf = base_request();
  with_rrf.scoring = couchbase::core::search_scoring_reciprocal_rank_fusion{};
  assert_false(ps::can_encode(with_rrf), "reciprocal rank fusion is gated");

  auto with_rsf = base_request();
  with_rsf.scoring = couchbase::core::search_scoring_relative_score_fusion{};
  assert_false(ps::can_encode(with_rsf), "relative score fusion is gated");

  // "none" predates fusion and maps onto a field the protocol already has, so it is not gated.
  auto with_none = base_request();
  with_none.scoring = couchbase::core::search_scoring_none{};
  assert_true(ps::can_encode(with_none), "disabled scoring is not gated");
}

void
scoring_none_maps_onto_the_protocol_disable_scoring_field([[maybe_unused]] context& ctx)
{
  auto request = base_request();
  request.scoring = couchbase::core::search_scoring_none{};

  const auto encoded = ps::encode(request);
  assert_true(encoded.has_value(), "a request scoring none encodes");
  assert_true(encoded->disable_scoring(), "scoring(none) reaches the protocol as disable_scoring");
}

void
the_deprecated_disable_scoring_flag_still_reaches_the_protocol([[maybe_unused]] context& ctx)
{
  auto request = base_request();
  request.disable_scoring = true;

  const auto encoded = ps::encode(request);
  assert_true(encoded.has_value(), "a request disabling scoring encodes");
  assert_true(encoded->disable_scoring(), "disable_scoring reaches the protocol");
}

void
decode_maps_hits_and_metrics([[maybe_unused]] context& ctx)
{
  v1::SearchQueryResponse message;
  auto* hit = message.add_hits();
  hit->set_id("doc-1");
  hit->set_index("idx");
  hit->set_score(1.5);
  hit->set_explanation("why");
  auto* loc = hit->add_locations();
  loc->set_field("name");
  loc->set_term("foo");
  loc->set_position(2);
  loc->set_start(3);
  loc->set_end(6);
  auto* metrics = message.mutable_meta_data()->mutable_metrics();
  metrics->set_total_rows(42);
  metrics->set_max_score(1.5);
  metrics->set_success_partition_count(8);
  metrics->mutable_execution_time()->set_nanos(500);

  ops::search_response response;
  ps::decode(message, response);

  assert_eq(response.rows.size(), std::size_t{ 1 }, "one hit decoded");
  assert_eq(response.rows.at(0).id, std::string{ "doc-1" }, "hit id decoded");
  assert_true(response.rows.at(0).score == 1.5, "hit score decoded");
  assert_eq(response.rows.at(0).locations.size(), std::size_t{ 1 }, "location decoded");
  assert_eq(response.rows.at(0).locations.at(0).field, std::string{ "name" }, "location field");
  assert_eq(response.meta.metrics.total_rows, std::uint64_t{ 42 }, "total_rows decoded");
  assert_eq(response.meta.metrics.success_partition_count,
            std::uint64_t{ 8 },
            "success_partition_count decoded");
}

// An FTS query whose partitions did not all answer still returns the hits it has, and reports the
// rest through the per-partition error map and the failed count. That is a successful response with
// incomplete results on both transports, not an error: the HTTP path records the same two things
// and leaves ctx.ec unset, so mapping them to an error code here would make couchbase2 fail a query
// that couchbase:// answers. Carrying them is what lets a caller notice the results are partial.
void
decode_carries_partial_partition_failures([[maybe_unused]] context& ctx)
{
  v1::SearchQueryResponse message;
  auto* metrics = message.mutable_meta_data()->mutable_metrics();
  metrics->set_success_partition_count(5);
  metrics->set_error_partition_count(2);
  (*message.mutable_meta_data()->mutable_errors())["pindex_3"] = "context deadline exceeded";
  (*message.mutable_meta_data()->mutable_errors())["pindex_7"] = "pindex not available";

  ops::search_response response;
  ps::decode(message, response);

  assert_eq(response.meta.metrics.error_partition_count,
            std::uint64_t{ 2 },
            "the failed partition count is carried");
  assert_eq(response.meta.errors.size(), std::size_t{ 2 }, "both partition errors are carried");
  assert_eq(response.meta.errors.at("pindex_7"),
            std::string{ "pindex not available" },
            "the partition's own message is carried verbatim");
  assert_false(static_cast<bool>(response.ctx.ec),
               "a partial result is not turned into an error, as on the HTTP path");
}

// can_encode() accepts highlight_style, highlight_fields and fields, so a response can carry
// fragments and per-hit field values. Dropping them here would answer such a request with rows
// whose highlighting is silently empty -- an outcome the caller cannot distinguish from a document
// that simply had no match in that field.
void
decode_maps_fragments_fields_and_array_positions([[maybe_unused]] context& ctx)
{
  v1::SearchQueryResponse message;
  auto* hit = message.add_hits();
  hit->set_id("doc-1");
  (*hit->mutable_fragments())["name"].add_content("<em>foo</em> bar");
  (*hit->mutable_fragments())["name"].add_content("baz <em>foo</em>");
  // The gateway keys each requested field to its value as raw JSON.
  (*hit->mutable_fields())["name"] = R"("foo")";
  (*hit->mutable_fields())["age"] = "42";
  auto* loc = hit->add_locations();
  loc->set_field("name");
  loc->set_term("foo");
  loc->add_array_positions(1);
  loc->add_array_positions(7);

  ops::search_response response;
  ps::decode(message, response);

  const auto& row = response.rows.at(0);
  assert_eq(row.fragments.size(), std::size_t{ 1 }, "fragment field decoded");
  assert_eq(row.fragments.at("name").size(), std::size_t{ 2 }, "both fragments decoded");
  assert_eq(row.fragments.at("name").at(0),
            std::string{ "<em>foo</em> bar" },
            "fragment content decoded in order");

  // Reassembled as one JSON object, which is how the core row carries the field set.
  const auto fields = couchbase::core::utils::json::parse(row.fields);
  assert_eq(fields.at("name").get_string(), std::string{ "foo" }, "string field value decoded");
  assert_eq(
    fields.at("age").as<std::uint64_t>(), std::uint64_t{ 42 }, "numeric field stays a number");

  const auto& positions = row.locations.at(0).array_positions;
  assert_true(positions.has_value(), "array positions decoded");
  assert_eq(positions->size(), std::size_t{ 2 }, "both array positions decoded");
  assert_eq(positions->at(1), std::uint64_t{ 7 }, "array position value decoded");
}

// A hit with no highlighting must leave the row's optional members unset rather than carrying an
// empty object, so a caller can tell "no fragments were requested" from "the field did not match".
void
decode_leaves_absent_fragments_and_positions_unset([[maybe_unused]] context& ctx)
{
  v1::SearchQueryResponse message;
  auto* hit = message.add_hits();
  hit->set_id("doc-1");
  hit->add_locations()->set_field("name");

  ops::search_response response;
  ps::decode(message, response);

  const auto& row = response.rows.at(0);
  assert_true(row.fragments.empty(), "no fragments decoded when none were sent");
  assert_true(row.fields.empty(), "fields stays empty rather than becoming an empty JSON object");
  assert_false(row.locations.at(0).array_positions.has_value(),
               "array positions stays unset when the location carried none");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(encode_maps_envelope_and_query_string) },
      { CASE(encode_maps_match_all_and_match_none) },
      { CASE(encode_returns_nullopt_for_unmappable_query) },
      { CASE(encode_maps_a_boosted_query_string) },
      { CASE(malformed_query_json_is_distinguished_from_an_unmappable_shape) },
      { CASE(can_encode_rejects_gated_features) },
      { CASE(scoring_none_maps_onto_the_protocol_disable_scoring_field) },
      { CASE(the_deprecated_disable_scoring_flag_still_reaches_the_protocol) },
      { CASE(decode_maps_hits_and_metrics) },
      { CASE(decode_carries_partial_partition_failures) },
      { CASE(decode_maps_fragments_fields_and_array_positions) },
      { CASE(decode_leaves_absent_fragments_and_positions_unset) },
    },
  };
}

} // namespace couchbase::test
