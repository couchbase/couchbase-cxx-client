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

#include "test_helper.hxx"

#include "core/impl/encoded_search_query.hxx"
#include "core/impl/encoded_search_sort.hxx"

#include <couchbase/boolean_field_query.hxx>
#include <couchbase/boolean_query.hxx>
#include <couchbase/conjunction_query.hxx>
#include <couchbase/date_range_query.hxx>
#include <couchbase/disjunction_query.hxx>
#include <couchbase/doc_id_query.hxx>
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
#include <couchbase/term_query.hxx>
#include <couchbase/term_range_query.hxx>
#include <couchbase/vector_search.hxx>
#include <couchbase/wildcard_query.hxx>

#include <couchbase/search_sort_geo_distance.hxx>

#include <tao/json.hpp>

using namespace tao::json::literals;

TEST_CASE("unit: query string search query", "[unit]")
{
  {
    //! [search-query-string-boosting]
    couchbase::query_string_query query(R"(description:pool name:pool^5)");
    //! [search-query-string-boosting]
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == "{\"query\":\"description:pool name:pool^5\"}"_json);
  }
  {
    //! [search-query-string-date-range]
    couchbase::query_string_query query(R"(created:>"2016-09-21")");
    //! [search-query-string-date-range]
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == "{\"query\":\"created:>\\\"2016-09-21\\\"\"}"_json);
  }

  {
    //! [search-query-string-numeric-range]
    couchbase::query_string_query query(R"(reviews.ratings.Cleanliness:>4)");
    //! [search-query-string-numeric-range]
    query.boost(1.42);
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == "{\"boost\":1.42,\"query\":\"reviews.ratings.Cleanliness:>4\"}"_json);
  }
}

TEST_CASE("unit: match search query", "[unit]")
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
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == R"(
{"analyzer":"standard","field":"reviews.content","fuzziness":2,"match":"location hostel","operator":"and","prefix_length":4}
)"_json);
  }
}

TEST_CASE("unit: conjunction search query", "[unit]")
{
  //! [search-conjunction]
  auto query = couchbase::conjunction_query{
    couchbase::match_query("location hostel").field("reviews.content"),
    couchbase::boolean_field_query(true).field("free_breakfast")
  };
  //! [search-conjunction]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"conjuncts":[
    {"field":"reviews.content","match":"location hostel"},
    {"bool":true,"field":"free_breakfast"}
]}
)"_json);
}

TEST_CASE("unit: disjunction search query", "[unit]")
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
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"disjuncts":[
    {"field":"reviews.content","match":"location hostel"},
    {"bool":true,"field":"free_breakfast"},
    {"bool":true,"field":"late_check_in"}
],
"min": 2}
)"_json);
}

TEST_CASE("unit: doc id search query", "[unit]")
{
  //! [search-docid]
  auto query =
    couchbase::doc_id_query(std::initializer_list<std::string>{ "airport_1258", "hotel_10160" });
  //! [search-docid]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"ids": ["airport_1258", "hotel_10160"]}
)"_json);
}

TEST_CASE("unit: boolean search query", "[unit]")
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
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"must":     {"conjuncts":[{"field":"reviews.content","match":"hostel room"},{"bool":true,"field":"free_breakfast"}]},
 "must_not": {"disjuncts":[{"field":"city","match":"Padfield Gilingham"}], "min": 1},
 "should":   {"disjuncts":[{"field":"reviews.ratings.Overall","min":4},{"field":"reviews.ratings.Service","min":5}], "min": 1}}
)"_json);
}

TEST_CASE("unit: term search query", "[unit]")
{
  //! [search-term]
  auto query = couchbase::term_query("locate").field("reviews.content");
  //! [search-term]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"term": "locate", "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: match phrase search query", "[unit]")
{
  //! [search-match-phrase]
  auto query = couchbase::match_phrase_query("nice view").field("reviews.content");
  //! [search-match-phrase]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"match_phrase": "nice view", "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: phrase search query", "[unit]")
{
  //! [search-phrase]
  auto query = couchbase::phrase_query({ "nice", "view" }).field("reviews.content");
  //! [search-phrase]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"terms": ["nice", "view"], "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: prefix search query", "[unit]")
{
  //! [search-prefix]
  auto query = couchbase::prefix_query("inter").field("reviews.content");
  //! [search-prefix]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"prefix": "inter", "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: regexp search query", "[unit]")
{
  //! [search-regexp]
  auto query = couchbase::regexp_query("inter.+").field("reviews.content");
  //! [search-regexp]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"regexp": "inter.+", "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: wildcard search query", "[unit]")
{
  //! [search-wildcard]
  auto query = couchbase::wildcard_query("inter*").field("reviews.content");
  //! [search-wildcard]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"wildcard": "inter*", "field": "reviews.content"}
)"_json);
}

TEST_CASE("unit: numeric range search query", "[unit]")
{
  //! [search-numeric-range]
  auto query = couchbase::numeric_range_query()
                 .field("id")
                 .min(100, /* inclusive= */ false)
                 .max(1000, /* inclusive= */ false);
  //! [search-numeric-range]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"min": 100, "inclusive_min": false, "max": 1000, "inclusive_max": false, "field": "id"}
)"_json);
}

TEST_CASE("unit: date range search query", "[unit]")
{
  {
    //! [search-date-range]
    auto query = couchbase::date_range_query()
                   .field("review_date")
                   .start("2001-10-09T10:20:30-08:00", /* inclusive= */ false)
                   .end("2016-10-31", /* inclusive= */ false);
    //! [search-date-range]
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == R"(
{"start": "2001-10-09T10:20:30-08:00", "inclusive_start": false, "end": "2016-10-31", "inclusive_end": false, "field": "review_date"}
)"_json);
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
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == R"(
{"end":"2001-10-31T00:00:00+0000","field":"review_date","start":"2001-10-09T10:20:30+0000"}
)"_json);
  }
}

TEST_CASE("unit: term range search query", "[unit]")
{
  //! [search-term-range]
  auto query = couchbase::term_range_query()
                 .field("desc")
                 .min("foo", /* inclusive= */ false)
                 .max("foof", /* inclusive= */ false);
  //! [search-term-range]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{"min": "foo", "inclusive_min": false, "max": "foof", "inclusive_max": false, "field": "desc"}
)"_json);
}

TEST_CASE("unit: special search query", "[unit]")
{
  {
    auto query = couchbase::match_none_query();
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == R"({"match_none": {}})"_json);
  }
  {
    auto query = couchbase::match_all_query();
    const auto encoded = query.encode();
    REQUIRE_FALSE(encoded.ec);
    REQUIRE(encoded.query == R"({"match_all": {}})"_json);
  }
}

TEST_CASE("unit: geo distance search query", "[unit]")
{
  //! [search-geo-distance]
  auto query = couchbase::geo_distance_query(53.482358, -2.235143, "100mi").field("geo");
  //! [search-geo-distance]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
{
  "location": {
    "lon": -2.235143,
    "lat": 53.482358
   },
    "distance": "100mi",
    "field": "geo"
}
)"_json);
}

TEST_CASE("unit: geo bounding box search query", "[unit]")
{
  //! [search-geo-bounding-box]
  auto query = couchbase::geo_bounding_box_query(couchbase::geo_point{ 53.482358, -2.235143 },
                                                 couchbase::geo_point{ 40.991862, 28.955043 })
                 .field("geo");
  //! [search-geo-bounding-box]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
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
)"_json);
}

TEST_CASE("unit: geo polygon search query", "[unit]")
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
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.query == R"(
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
)"_json);
}

TEST_CASE("unit: search sort geo distance", "[unit]")
{
  //! [search-sort-geo-distance]
  auto geo_distance = couchbase::search_sort_geo_distance(
                        couchbase::geo_point{ 37.79393211306212, -122.44234633404847 }, "hotel")
                        .unit(couchbase::search_geo_distance_units::nautical_miles);
  //! [search-sort-geo-distance]
  const auto encoded = geo_distance.encode();
  REQUIRE_FALSE(encoded.ec);
  REQUIRE(encoded.sort == R"(
{
    "by": "geo_distance",
    "field": "hotel",
    "location": {
      "lat": 37.79393211306212,
      "lon": -122.44234633404847
    },
    "unit": "nauticalmiles"
}
)"_json);
}

TEST_CASE("unit: vector query", "[unit]")
{
  //! [vector-query]
  auto query = couchbase::vector_query("foo", std::vector<double>{ 0.352, 0.6238, -0.32226 })
                 .boost(0.5)
                 .num_candidates(4);
  //! [vector-query]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);

  REQUIRE(encoded.query == R"(
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
)"_json);
}

TEST_CASE("unit: base64 vector query", "[unit]")
{
  //! [base64-vector-query]
  std::string base64_encoded_query = "RWFzdGVyIGVnZyE=";
  auto query = couchbase::vector_query("foo", base64_encoded_query).boost(0.5).num_candidates(4);
  //! [base64-vector-query]
  const auto encoded = query.encode();
  REQUIRE_FALSE(encoded.ec);

  REQUIRE(encoded.query == R"(
{
    "boost": 0.5,
    "field": "foo",
    "k": 4,
    "vector_base64": "RWFzdGVyIGVnZyE="
}
)"_json);
}
