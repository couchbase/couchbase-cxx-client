/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include "core/cluster_options.hxx"
#include "core/impl/encoded_search_query.hxx"
#include "core/impl/encoded_search_sort.hxx"
#include "core/io/http_context.hxx"
#include "core/io/query_cache.hxx"
#include "core/operations/document_search.hxx"
#include "core/topology/configuration_json.hxx"
#include "core/utils/json.hxx"

#include <couchbase/boolean_field_query.hxx>
#include <couchbase/boolean_query.hxx>
#include <couchbase/conjunction_query.hxx>
#include <couchbase/date_range_query.hxx>
#include <couchbase/disjunction_query.hxx>
#include <couchbase/doc_id_query.hxx>
#include <couchbase/error_codes.hxx>
#include <couchbase/geo_bounding_box_query.hxx>
#include <couchbase/geo_distance_query.hxx>
#include <couchbase/geo_polygon_query.hxx>
#include <couchbase/match_phrase_query.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/phrase_query.hxx>
#include <couchbase/prefix_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/regexp_query.hxx>
#include <couchbase/search_options.hxx>
#include <couchbase/search_scoring_none.hxx>
#include <couchbase/search_scoring_reciprocal_rank_fusion.hxx>
#include <couchbase/search_scoring_relative_score_fusion.hxx>
#include <couchbase/search_sort_geo_distance.hxx>
#include <couchbase/term_query.hxx>
#include <couchbase/term_range_query.hxx>
#include <couchbase/vector_search.hxx>
#include <couchbase/wildcard_query.hxx>

#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

#include <cstdint>
#include <ctime>
#include <initializer_list>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

namespace couchbase::test
{
// Every case here compares one JSON document against another, so a failure that omits the operands
// says only that the encoder produced something else.
template<>
struct operand_printer<tao::json::value> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const tao::json::value& value) -> std::string
  {
    return tao::json::to_string(value);
  }
};

namespace
{
using namespace tao::json::literals;

void
query_string_queries_encode_verbatim_with_an_optional_boost([[maybe_unused]] context& ctx)
{
  {
    //! [search-query-string-boosting]
    couchbase::query_string_query query(R"(description:pool name:pool^5)");
    //! [search-query-string-boosting]
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              "{\"query\":\"description:pool name:pool^5\"}"_json,
              "a query string is carried verbatim");
  }
  {
    //! [search-query-string-date-range]
    couchbase::query_string_query query(R"(created:>"2016-09-21")");
    //! [search-query-string-date-range]
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              "{\"query\":\"created:>\\\"2016-09-21\\\"\"}"_json,
              "an escaped quote survives encoding");
  }

  {
    //! [search-query-string-numeric-range]
    couchbase::query_string_query query(R"(reviews.ratings.Cleanliness:>4)");
    //! [search-query-string-numeric-range]
    query.boost(1.42);
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              "{\"boost\":1.42,\"query\":\"reviews.ratings.Cleanliness:>4\"}"_json,
              "the boost is emitted alongside the query");
  }
}

void
a_match_query_encodes_its_analyzer_fuzziness_and_operator([[maybe_unused]] context& ctx)
{
  {
    //! [search-match]
    auto query = couchbase::match_query("location hostel")
                   .field("reviews.content")
                   .analyzer("standard")
                   .fuzziness(2)
                   .prefix_length(4)
                   .match_operator(couchbase::match_operator::logical_and);
    //! [search-match]
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              R"(
{"analyzer":"standard","field":"reviews.content","fuzziness":2,
 "match":"location hostel","operator":"and","prefix_length":4}
)"_json,
              "the analyzer, fuzziness, prefix length and operator");
  }
}

void
a_conjunction_query_encodes_its_conjuncts([[maybe_unused]] context& ctx)
{
  //! [search-conjunction]
  auto query = couchbase::conjunction_query{
    couchbase::match_query("location hostel").field("reviews.content"),
    couchbase::boolean_field_query(true).field("free_breakfast")
  };
  //! [search-conjunction]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"conjuncts":[
    {"field":"reviews.content","match":"location hostel"},
    {"bool":true,"field":"free_breakfast"}
]}
)"_json,
            "the conjuncts");
}

void
a_disjunction_query_encodes_its_disjuncts_and_minimum([[maybe_unused]] context& ctx)
{
  //! [search-disjunction]
  auto query =
    couchbase::disjunction_query{
      couchbase::match_query("location hostel").field("reviews.content"),
      couchbase::boolean_field_query(true).field("free_breakfast"),
      couchbase::boolean_field_query(true).field("late_check_in"),
    }
      .min(2);
  //! [search-disjunction]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"disjuncts":[
    {"field":"reviews.content","match":"location hostel"},
    {"bool":true,"field":"free_breakfast"},
    {"bool":true,"field":"late_check_in"}
],
"min": 2}
)"_json,
            "the disjuncts and the minimum that must match");
}

void
a_doc_id_query_encodes_its_ids([[maybe_unused]] context& ctx)
{
  //! [search-docid]
  auto query =
    couchbase::doc_id_query(std::initializer_list<std::string>{ "airport_1258", "hotel_10160" });
  //! [search-docid]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"ids": ["airport_1258", "hotel_10160"]}
)"_json,
            "the document ids");
}

void
a_boolean_query_encodes_must_should_and_must_not([[maybe_unused]] context& ctx)
{
  //! [search-boolean]
  couchbase::boolean_query query;
  query.must(couchbase::match_query("hostel room").field("reviews.content"),
             couchbase::boolean_field_query(true).field("free_breakfast"));
  query.should(couchbase::numeric_range_query().field("reviews.ratings.Overall").min(4),
               couchbase::numeric_range_query().field("reviews.ratings.Service").min(5));
  query.must_not(couchbase::match_query("Padfield Gilingham").field("city"));
  //! [search-boolean]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"must":     {"conjuncts":[{"field":"reviews.content","match":"hostel room"},
                           {"bool":true,"field":"free_breakfast"}]},
 "must_not": {"disjuncts":[{"field":"city","match":"Padfield Gilingham"}], "min": 1},
 "should":   {"disjuncts":[{"field":"reviews.ratings.Overall","min":4},
                           {"field":"reviews.ratings.Service","min":5}], "min": 1}}
)"_json,
            "must, must_not and should");
}

void
a_term_query_encodes_its_term_and_field([[maybe_unused]] context& ctx)
{
  //! [search-term]
  auto query = couchbase::term_query("locate").field("reviews.content");
  //! [search-term]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"term": "locate", "field": "reviews.content"}
)"_json,
            "the term and the field");
}

void
a_match_phrase_query_encodes_its_phrase_and_field([[maybe_unused]] context& ctx)
{
  //! [search-match-phrase]
  auto query = couchbase::match_phrase_query("nice view").field("reviews.content");
  //! [search-match-phrase]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"match_phrase": "nice view", "field": "reviews.content"}
)"_json,
            "the phrase and the field");
}

void
a_phrase_query_encodes_its_terms_and_field([[maybe_unused]] context& ctx)
{
  //! [search-phrase]
  auto query = couchbase::phrase_query({ "nice", "view" }).field("reviews.content");
  //! [search-phrase]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"terms": ["nice", "view"], "field": "reviews.content"}
)"_json,
            "the terms and the field");
}

void
a_prefix_query_encodes_its_prefix_and_field([[maybe_unused]] context& ctx)
{
  //! [search-prefix]
  auto query = couchbase::prefix_query("inter").field("reviews.content");
  //! [search-prefix]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"prefix": "inter", "field": "reviews.content"}
)"_json,
            "the prefix and the field");
}

void
a_regexp_query_encodes_its_pattern_and_field([[maybe_unused]] context& ctx)
{
  //! [search-regexp]
  auto query = couchbase::regexp_query("inter.+").field("reviews.content");
  //! [search-regexp]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"regexp": "inter.+", "field": "reviews.content"}
)"_json,
            "the pattern and the field");
}

void
a_wildcard_query_encodes_its_pattern_and_field([[maybe_unused]] context& ctx)
{
  //! [search-wildcard]
  auto query = couchbase::wildcard_query("inter*").field("reviews.content");
  //! [search-wildcard]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"wildcard": "inter*", "field": "reviews.content"}
)"_json,
            "the pattern and the field");
}

void
a_numeric_range_query_encodes_its_bounds_and_inclusivity([[maybe_unused]] context& ctx)
{
  //! [search-numeric-range]
  auto query = couchbase::numeric_range_query()
                 .field("id")
                 .min(100, /* inclusive= */ false)
                 .max(1000, /* inclusive= */ false);
  //! [search-numeric-range]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"min": 100, "inclusive_min": false, "max": 1000, "inclusive_max": false, "field": "id"}
)"_json,
            "the bounds and their inclusivity");
}

void
a_date_range_query_encodes_string_and_tm_bounds([[maybe_unused]] context& ctx)
{
  {
    //! [search-date-range]
    auto query = couchbase::date_range_query()
                   .field("review_date")
                   .start("2001-10-09T10:20:30-08:00", /* inclusive= */ false)
                   .end("2016-10-31", /* inclusive= */ false);
    //! [search-date-range]
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              R"(
{"start": "2001-10-09T10:20:30-08:00", "inclusive_start": false,
 "end": "2016-10-31", "inclusive_end": false, "field": "review_date"}
)"_json,
              "string bounds and their inclusivity");
  }

  {
    //! [search-date-range-tm]
    std::tm start_tm{};
    start_tm.tm_year = 2001 - 1900;
    start_tm.tm_mon = 9;
    start_tm.tm_mday = 9;
    start_tm.tm_hour = 10;
    start_tm.tm_min = 20;
    start_tm.tm_sec = 30;

    std::tm end_tm{};
    end_tm.tm_year = 2001 - 1900;
    end_tm.tm_mon = 9;
    end_tm.tm_mday = 31;

    auto query = couchbase::date_range_query().field("review_date").start(start_tm).end(end_tm);
    // equivalent of
    // {"field":"review_date","start":"2001-10-09T10:20:30+0000","end":"2001-10-31T00:00:00+0000"}
    //! [search-date-range-tm]
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query,
              R"(
{"end":"2001-10-31T00:00:00+0000","field":"review_date","start":"2001-10-09T10:20:30+0000"}
)"_json,
              "a std::tm is rendered in the wire format");
  }
}

void
a_term_range_query_encodes_its_bounds_and_inclusivity([[maybe_unused]] context& ctx)
{
  //! [search-term-range]
  auto query = couchbase::term_range_query()
                 .field("desc")
                 .min("foo", /* inclusive= */ false)
                 .max("foof", /* inclusive= */ false);
  //! [search-term-range]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{"min": "foo", "inclusive_min": false, "max": "foof", "inclusive_max": false, "field": "desc"}
)"_json,
            "the bounds and their inclusivity");
}

void
match_none_and_match_all_encode_as_empty_objects([[maybe_unused]] context& ctx)
{
  {
    auto query = couchbase::match_none_query();
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query, R"({"match_none": {}})"_json, "match_none");
  }
  {
    auto query = couchbase::match_all_query();
    const auto encoded = query.encode();
    assert_success(encoded.ec, "the query encodes");
    assert_eq(encoded.query, R"({"match_all": {}})"_json, "match_all");
  }
}

void
a_geo_distance_query_encodes_its_location_and_distance([[maybe_unused]] context& ctx)
{
  //! [search-geo-distance]
  auto query = couchbase::geo_distance_query(53.482358, -2.235143, "100mi").field("geo");
  //! [search-geo-distance]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{
  "location": {
    "lon": -2.235143,
    "lat": 53.482358
   },
    "distance": "100mi",
    "field": "geo"
}
)"_json,
            "the location and the distance");
}

void
a_geo_bounding_box_query_encodes_its_corners([[maybe_unused]] context& ctx)
{
  //! [search-geo-bounding-box]
  auto query = couchbase::geo_bounding_box_query(couchbase::geo_point{ 53.482358, -2.235143 },
                                                 couchbase::geo_point{ 40.991862, 28.955043 })
                 .field("geo");
  //! [search-geo-bounding-box]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{
  "top_left": {
    "lon": -2.235143,
    "lat": 53.482358
   },
  "bottom_right": {
    "lon": 28.955043,
    "lat": 40.991862
   },
    "field": "geo"
}
)"_json,
            "the two corners");
}

void
a_geo_polygon_query_encodes_its_points([[maybe_unused]] context& ctx)
{
  //! [search-geo-polygon]
  auto query =
    couchbase::geo_polygon_query({
                                   couchbase::geo_point{ 37.79393211306212, -122.44234633404847 },
                                   couchbase::geo_point{ 37.77995881733997, -122.43977141339417 },
                                   couchbase::geo_point{ 37.788031092020155, -122.42925715405579 },
                                   couchbase::geo_point{ 37.79026946582319, -122.41149020154114 },
                                   couchbase::geo_point{ 37.79571192027403, -122.40735054016113 },
                                   couchbase::geo_point{ 37.79393211306212, -122.44234633404847 },
                                 })
      .field("geo");
  //! [search-geo-polygon]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");
  assert_eq(encoded.query,
            R"(
{
    "field": "geo",
    "polygon_points": [
      {"lat": 37.79393211306212, "lon": -122.44234633404847},
      {"lat": 37.77995881733997, "lon": -122.43977141339417},
      {"lat": 37.788031092020155, "lon": -122.42925715405579},
      {"lat": 37.79026946582319, "lon": -122.41149020154114},
      {"lat": 37.79571192027403, "lon": -122.40735054016113},
      {"lat": 37.79393211306212, "lon": -122.44234633404847}
    ]
}
)"_json,
            "the polygon points");
}

void
a_geo_distance_sort_encodes_its_location_and_unit([[maybe_unused]] context& ctx)
{
  //! [search-sort-geo-distance]
  auto geo_distance = couchbase::search_sort_geo_distance(
                        couchbase::geo_point{ 37.79393211306212, -122.44234633404847 }, "hotel")
                        .unit(couchbase::search_geo_distance_units::nautical_miles);
  //! [search-sort-geo-distance]
  const auto encoded = geo_distance.encode();
  assert_success(encoded.ec, "the sort encodes");
  assert_eq(encoded.sort,
            R"(
{
    "by": "geo_distance",
    "field": "hotel",
    "location": {
      "lat": 37.79393211306212,
      "lon": -122.44234633404847
    },
    "unit": "nauticalmiles"
}
)"_json,
            "the sort location and its unit");
}

void
a_vector_query_encodes_its_vector_boost_and_candidates([[maybe_unused]] context& ctx)
{
  //! [vector-query]
  auto query = couchbase::vector_query("foo", std::vector<double>{ 0.352, 0.6238, -0.32226 })
                 .boost(0.5)
                 .num_candidates(4);
  //! [vector-query]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");

  assert_eq(encoded.query,
            R"(
{
    "boost": 0.5,
    "field": "foo",
    "k": 4,
    "vector": [
      0.352,
      0.6238,
      -0.32226
    ]
}
)"_json,
            "the vector, the boost and the candidate count");
}

void
a_base64_vector_query_encodes_its_encoded_vector([[maybe_unused]] context& ctx)
{
  //! [base64-vector-query]
  std::string base64_encoded_query = "RWFzdGVyIGVnZyE=";
  auto query = couchbase::vector_query("foo", base64_encoded_query).boost(0.5).num_candidates(4);
  //! [base64-vector-query]
  const auto encoded = query.encode();
  assert_success(encoded.ec, "the query encodes");

  assert_eq(encoded.query,
            R"(
{
    "boost": 0.5,
    "field": "foo",
    "k": 4,
    "vector_base64": "RWFzdGVyIGVnZyE="
}
)"_json,
            "a base64 vector is sent under its own key");
}

// The scoring cases below drive a whole search_request through encode_to(), which takes an
// http_context. Nothing in the encoder consults the cluster's capabilities: score fusion is gated
// before dispatch, in cluster.cxx, and what that gate reads is held by the two capability cases at
// the end of this file.
auto
make_search_http_context(couchbase::core::topology::configuration& config)
  -> couchbase::core::http_context
{
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  std::string canonical_hostname{};
  std::uint16_t canonical_port{};
  return couchbase::core::http_context{
    config, cluster_options, query_cache, hostname, port, canonical_hostname, canonical_port,
  };
}

auto
encode_scoring(const couchbase::core::search_scoring_mode& scoring, bool disable_scoring)
  -> std::pair<std::error_code, tao::json::value>
{
  couchbase::core::topology::configuration config{};
  auto ctx = make_search_http_context(config);

  couchbase::core::operations::search_request request{};
  request.index_name = "idx";
  request.query = couchbase::core::json_string{ R"({"match":"wireless headphones"})" };
  request.scoring = scoring;
  request.disable_scoring = disable_scoring;

  couchbase::core::io::http_request encoded;
  if (const auto ec = request.encode_to(encoded, ctx); ec) {
    return { ec, {} };
  }
  return { {}, couchbase::core::utils::json::parse(encoded.body) };
}

void
no_scoring_option_sends_no_score_and_no_params([[maybe_unused]] context& ctx)
{
  auto [ec, body] = encode_scoring({}, false);
  assert_success(ec, "the request encodes");
  assert_false(body.get_object().count("score") != 0, "no score is sent");
  assert_false(body.get_object().count("params") != 0, "no scoring parameters are sent");
}

void
reciprocal_rank_fusion_carries_both_tuning_parameters([[maybe_unused]] context& ctx)
{
  auto [ec, body] =
    encode_scoring(couchbase::core::search_scoring_reciprocal_rank_fusion{ 60U, 200U }, false);
  assert_success(ec, "the request encodes");
  assert_eq(body.at("score").get_string(), std::string{ "rrf" }, "the score names the mode");
  assert_eq(body.at("params").at("score_rank_constant").get_unsigned(),
            std::uint64_t{ 60 },
            "the rank constant is sent");
  assert_eq(body.at("params").at("score_window_size").get_unsigned(),
            std::uint64_t{ 200 },
            "the window size is sent");
}

void
reciprocal_rank_fusion_drops_params_when_neither_is_set([[maybe_unused]] context& ctx)
{
  auto [ec, body] = encode_scoring(couchbase::core::search_scoring_reciprocal_rank_fusion{}, false);
  assert_success(ec, "the request encodes");
  assert_eq(body.at("score").get_string(), std::string{ "rrf" }, "the score names the mode");
  assert_false(body.get_object().count("params") != 0,
               "an empty parameter set is omitted rather than sent empty");
}

void
reciprocal_rank_fusion_omits_the_parameter_that_is_unset([[maybe_unused]] context& ctx)
{
  auto [ec, body] =
    encode_scoring(couchbase::core::search_scoring_reciprocal_rank_fusion{ 17U, {} }, false);
  assert_success(ec, "the request encodes");
  assert_eq(body.at("params").at("score_rank_constant").get_unsigned(),
            std::uint64_t{ 17 },
            "the parameter that was set is sent");
  assert_false(body.at("params").get_object().count("score_window_size") != 0,
               "the parameter that was not set is absent");
}

void
relative_score_fusion_has_no_rank_constant_to_send([[maybe_unused]] context& ctx)
{
  auto [ec, body] =
    encode_scoring(couchbase::core::search_scoring_relative_score_fusion{ 50U }, false);
  assert_success(ec, "the request encodes");
  assert_eq(body.at("score").get_string(), std::string{ "rsf" }, "the score names the mode");
  assert_eq(body.at("params").at("score_window_size").get_unsigned(),
            std::uint64_t{ 50 },
            "the window size is sent");
  assert_false(body.at("params").get_object().count("score_rank_constant") != 0,
               "relative score fusion has no rank constant");
}

void
search_scoring_none_sends_the_same_score_as_the_deprecated_option([[maybe_unused]] context& ctx)
{
  auto [none_ec, none_body] = encode_scoring(couchbase::core::search_scoring_none{}, false);
  assert_success(none_ec, "the request encodes with the scoring mode");
  auto [legacy_ec, legacy_body] = encode_scoring({}, true);
  assert_success(legacy_ec, "the request encodes with the deprecated flag");
  assert_eq(none_body.at("score").get_string(), std::string{ "none" }, "the score is none");
  assert_eq(none_body.at("score"),
            legacy_body.at("score"),
            "the mode and the deprecated flag put the same score on the wire");
  assert_false(none_body.get_object().count("params") != 0, "no scoring parameters accompany none");
}

void
disable_scoring_with_a_scoring_mode_is_rejected_before_sending([[maybe_unused]] context& ctx)
{
  auto [ec, body] = encode_scoring(couchbase::core::search_scoring_reciprocal_rank_fusion{}, true);
  assert_error(
    ec, couchbase::errc::common::invalid_argument, "the conflict is refused rather than encoded");
}

// disable_scoring() is deprecated in favour of scoring(search_scoring_none{}), but the conflict it
// guards against is load-bearing until it is removed, so these cases call it deliberately.
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#elif defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4996)
#endif
void
scoring_after_disable_scoring_is_refused([[maybe_unused]] context& ctx)
{
  couchbase::search_options options{};
  options.disable_scoring(true);
  assert_throws<std::invalid_argument>(
    [&options]() {
      options.scoring(couchbase::search_scoring_none{});
    },
    "a scoring mode cannot follow disable_scoring(true)");
}

void
disable_scoring_after_a_scoring_mode_is_refused([[maybe_unused]] context& ctx)
{
  couchbase::search_options options{};
  options.scoring(couchbase::search_scoring_reciprocal_rank_fusion{});
  assert_throws<std::invalid_argument>(
    [&options]() {
      options.disable_scoring(true);
    },
    "disable_scoring(true) cannot follow a scoring mode");
}

void
disable_scoring_false_does_not_conflict_with_a_scoring_mode([[maybe_unused]] context& ctx)
{
  couchbase::search_options options{};
  options.scoring(couchbase::search_scoring_reciprocal_rank_fusion{});
  assert_no_throw(
    [&options]() {
      options.disable_scoring(false);
    },
    "turning the deprecated flag off is not a conflict");
  assert_true(options.build().scoring.has_value(), "the scoring mode survives");
}

void
a_scoring_mode_is_accepted_once_disable_scoring_is_off_again([[maybe_unused]] context& ctx)
{
  couchbase::search_options options{};
  options.disable_scoring(true);
  options.disable_scoring(false);
  assert_no_throw(
    [&options]() {
      options.scoring(couchbase::search_scoring_relative_score_fusion{});
    },
    "the conflict is on the state, not on the call order");
}
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#elif defined(_MSC_VER)
#pragma warning(pop)
#endif

void
the_scoring_parameters_survive_build([[maybe_unused]] context& ctx)
{
  auto built =
    couchbase::search_options{}
      .scoring(
        couchbase::search_scoring_reciprocal_rank_fusion{}.rank_constant(60).window_size(200))
      .build();
  assert_true(built.scoring.has_value(), "the built options carry a scoring mode");
  const auto* fusion =
    std::get_if<couchbase::search_scoring::built::reciprocal_rank_fusion>(&built.scoring->mode);
  assert_true(fusion != nullptr, "the mode is reciprocal rank fusion");
  assert_eq(fusion->rank_constant, std::optional<std::uint32_t>{ 60 }, "the rank constant");
  assert_eq(fusion->window_size, std::optional<std::uint32_t>{ 200 }, "the window size");
}

void
score_fusion_is_read_from_the_search_cluster_capabilities([[maybe_unused]] context& ctx)
{
  const auto without = couchbase::core::utils::json::parse(
    R"({"nodes":[],"clusterCapabilities":{"search":["vectorSearch","scopedSearchIndex"]}})");
  assert_false(
    without.as<couchbase::core::topology::configuration>().capabilities.supports_score_fusion(),
    "a search capability list without scoreFusion does not enable it");

  const auto with = couchbase::core::utils::json::parse(
    R"({"nodes":[],"clusterCapabilities":{"search":["vectorSearch","scoreFusion"]}})");
  assert_true(
    with.as<couchbase::core::topology::configuration>().capabilities.supports_score_fusion(),
    "the capability is read from the search list");

  const auto elsewhere = couchbase::core::utils::json::parse(
    R"({"nodes":[],"clusterCapabilities":{"n1ql":["scoreFusion"]}})");
  assert_false(
    elsewhere.as<couchbase::core::topology::configuration>().capabilities.supports_score_fusion(),
    "the same name under another service does not enable it");
}

void
only_the_fusion_modes_are_gated_by_the_capability([[maybe_unused]] context& ctx)
{
  couchbase::core::configuration_capabilities capabilities{};
  assert_false(capabilities.supports_score_fusion(), "the capability is off by default");
  capabilities.cluster.insert(couchbase::core::cluster_capability::search_score_fusion);
  assert_true(capabilities.supports_score_fusion(), "inserting the capability turns it on");

  assert_true(
    couchbase::core::is_score_fusion(couchbase::core::search_scoring_reciprocal_rank_fusion{}),
    "reciprocal rank fusion is score fusion");
  assert_true(
    couchbase::core::is_score_fusion(couchbase::core::search_scoring_relative_score_fusion{}),
    "relative score fusion is score fusion");
  assert_false(couchbase::core::is_score_fusion(couchbase::core::search_scoring_none{}),
               "none is not score fusion, so it needs no capability");
  assert_false(couchbase::core::is_score_fusion(couchbase::core::search_scoring_mode{}),
               "an unset mode is not score fusion");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(query_string_queries_encode_verbatim_with_an_optional_boost) },
      { CASE(a_match_query_encodes_its_analyzer_fuzziness_and_operator) },
      { CASE(a_conjunction_query_encodes_its_conjuncts) },
      { CASE(a_disjunction_query_encodes_its_disjuncts_and_minimum) },
      { CASE(a_doc_id_query_encodes_its_ids) },
      { CASE(a_boolean_query_encodes_must_should_and_must_not) },
      { CASE(a_term_query_encodes_its_term_and_field) },
      { CASE(a_match_phrase_query_encodes_its_phrase_and_field) },
      { CASE(a_phrase_query_encodes_its_terms_and_field) },
      { CASE(a_prefix_query_encodes_its_prefix_and_field) },
      { CASE(a_regexp_query_encodes_its_pattern_and_field) },
      { CASE(a_wildcard_query_encodes_its_pattern_and_field) },
      { CASE(a_numeric_range_query_encodes_its_bounds_and_inclusivity) },
      { CASE(a_date_range_query_encodes_string_and_tm_bounds) },
      { CASE(a_term_range_query_encodes_its_bounds_and_inclusivity) },
      { CASE(match_none_and_match_all_encode_as_empty_objects) },
      { CASE(a_geo_distance_query_encodes_its_location_and_distance) },
      { CASE(a_geo_bounding_box_query_encodes_its_corners) },
      { CASE(a_geo_polygon_query_encodes_its_points) },
      { CASE(a_geo_distance_sort_encodes_its_location_and_unit) },
      { CASE(a_vector_query_encodes_its_vector_boost_and_candidates) },
      { CASE(a_base64_vector_query_encodes_its_encoded_vector) },
      { CASE(no_scoring_option_sends_no_score_and_no_params) },
      { CASE(reciprocal_rank_fusion_carries_both_tuning_parameters) },
      { CASE(reciprocal_rank_fusion_drops_params_when_neither_is_set) },
      { CASE(reciprocal_rank_fusion_omits_the_parameter_that_is_unset) },
      { CASE(relative_score_fusion_has_no_rank_constant_to_send) },
      { CASE(search_scoring_none_sends_the_same_score_as_the_deprecated_option) },
      { CASE(disable_scoring_with_a_scoring_mode_is_rejected_before_sending) },
      { CASE(scoring_after_disable_scoring_is_refused) },
      { CASE(disable_scoring_after_a_scoring_mode_is_refused) },
      { CASE(disable_scoring_false_does_not_conflict_with_a_scoring_mode) },
      { CASE(a_scoring_mode_is_accepted_once_disable_scoring_is_off_again) },
      { CASE(the_scoring_parameters_survive_build) },
      { CASE(score_fusion_is_read_from_the_search_cluster_capabilities) },
      { CASE(only_the_fusion_modes_are_gated_by_the_capability) },
    },
  };
}

} // namespace couchbase::test
