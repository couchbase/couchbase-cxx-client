/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include "core/utils/json_streaming_lexer.hxx"

#include "utils/logger.hxx"

#include <cstddef>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
struct query_result {
  std::error_code ec{};
  std::size_t number_of_rows{};
  std::string meta{};
  std::vector<std::string> rows{};
  bool row_handler_ran{};
  bool complete_handler_ran{};
};

// The pointer selects the elements of a query response's "results" array, and the depth stops
// events below a row, so a row is reported as one string rather than as its own token stream.
auto
parse(const std::vector<std::string>& chunks) -> query_result
{
  ::test::utils::init_logger();

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4);
  query_result result{};
  lexer.on_row([&result](std::string&& row) {
    result.row_handler_ran = true;
    result.rows.emplace_back(std::move(row));
    return couchbase::core::utils::json::stream_control::next_row;
  });
  lexer.on_complete([&result](std::error_code ec, std::size_t number_of_rows, std::string&& meta) {
    result.complete_handler_ran = true;
    result.ec = ec;
    result.number_of_rows = number_of_rows;
    result.meta = std::move(meta);
  });
  for (const auto& chunk : chunks) {
    lexer.feed(chunk);
  }
  return result;
}

void
a_response_delivered_in_one_chunk_yields_every_row([[maybe_unused]] context& ctx)
{
  const std::string chunk = R"(
{
"requestID": "2640a5b5-2e67-44e7-86ec-31cc388b7427",
"clientContextID": "730ecac3-e8d0-4d6e-4ed9-e2d4abd1d7b9",
"signature": {"greeting":"string"},
"results": [
{"greeting":"C++"},
{"greeting":"ruby"},
null,1,false
],
"status": "success"
}
)";
  const auto result = parse({ chunk });

  assert_success(result.ec, "the payload parses");
  assert_eq(result.number_of_rows, std::size_t{ 5 }, "the number of rows reported at completion");
  assert_eq(result.rows.size(), std::size_t{ 5 }, "the number of rows handed to the row handler");
  const std::string expected_meta{ R"(
{
"requestID": "2640a5b5-2e67-44e7-86ec-31cc388b7427",
"clientContextID": "730ecac3-e8d0-4d6e-4ed9-e2d4abd1d7b9",
"signature": {"greeting":"string"},
"results": [
],
"status": "success"
}
)" };
  assert_eq(result.meta, expected_meta, "the metadata is the payload with the rows removed");
  assert_eq(result.rows[0], std::string{ R"({"greeting":"C++"})" }, "the first row");
  assert_eq(result.rows[1], std::string{ R"({"greeting":"ruby"})" }, "the second row");
  assert_eq(result.rows[2], std::string{ "null" }, "the third row");
  assert_eq(result.rows[3], std::string{ "1" }, "the fourth row");
  assert_eq(result.rows[4], std::string{ "false" }, "the fifth row");
}

void
rows_arriving_in_separate_chunks_are_each_reported([[maybe_unused]] context& ctx)
{
  const std::vector<std::string> chunks{
    /* 0 */
    R"({"requestID": "9739203f-9cd5-45cd-8e3a-31c27407d66a", "clientContextID": "2067c2c25c32545c", "signature": {"*":"*"}, "results": [)",
    /* 1 */
    R"({"beer-sample":{"name":"21st Amendment Brewery Cafe","city":"San Francisco","state":"California","code":"94107","country":"United States","phone":"1-415-369-0900","website":"http://www.21st-amendment.com/","type":"brewery","updated":"2010-10-24 13:54:07","description":"The 21st Amendment Brewery offers a variety of award winning house made brews and American grilled cuisine in a comfortable loft like setting. Join us before and after Giants baseball games in our outdoor beer garden. A great location for functions and parties in our semi-private Brewers Loft. See you soon at the 21A!","address":["563 Second Street"],"geo":{"accuracy":"ROOFTOP","lat":37.7825,"lon":-122.393}}},)",
    /* 2 */
    R"({"beer-sample":{"name":"21A IPA","abv":7.2,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"21st_amendment_brewery_cafe","updated":"2010-07-22 20:00:20","description":"Deep golden color. Citrus and piney hop aromas. Assertive malt backbone supporting the overwhelming bitterness. Dry hopped in the fermenter with four types of hops giving an explosive hop aroma. Many refer to this IPA as Nectar of the Gods. Judge for yourself. Now Available in Cans!","style":"American-Style India Pale Ale","category":"North American Ale"}},)",
    /* 3 */
    R"({"beer-sample":{"name":"563 Stout","abv":5.0,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"21st_amendment_brewery_cafe","updated":"2010-07-22 20:00:20","description":"Deep black color, toasted black burnt coffee flavors and aroma. Dispensed with Nitrogen through a slow-flow faucet giving it the characteristic cascading effect, resulting in a rich dense creamy head.","style":"American-Style Stout","category":"North American Ale"}})",
    /* 4 */
    R"(], "status": "success", "metrics": {"elapsedTime": "1.284307ms","executionTime": "1.231972ms","resultCount": 3,"resultSize": 1658,"serviceLoad": 3} })"
  };
  const auto result = parse(chunks);

  assert_success(result.ec, "the payload parses");
  assert_eq(result.number_of_rows, std::size_t{ 3 }, "the number of rows reported at completion");
  assert_eq(result.rows.size(), std::size_t{ 3 }, "the number of rows handed to the row handler");
  assert_eq(result.meta, chunks[0] + chunks[4], "the metadata is what surrounds the row array");
}

void
a_chunk_boundary_inside_a_metadata_value_is_reassembled([[maybe_unused]] context& ctx)
{
  const std::vector<std::string> chunks{
    /* 0 */
    R"({"requestID": "34a4e4b2-3f69-4bf8-a6e2-ae06798de3d9","clientContextID": "dfea5193-ead9-4ac2-5558-8fd5c4631959","signature": {"greeting":"string"},"results": [{"greeting":"ruby rules"}],"status": "success","profile": {"phaseTimes": {"authorize":"10.473µs","instantiate":"10.29µs","parse":"183.413µs","plan":"19.155µs","project":"5.712µs","run":"43.258µs","stream":"7.078µs"},"phaseOperators": {"authorize":1,"project":1,"stream":1},"requestTime": "2022-05-11T11:01:14.943Z","servicingHost": "10.112.220.101:8091","executionTimings": {"#operator":"Authorize","#stats":{"#phaseSwitches":4,"execTime":"1.372µs","servTime":"9.101µs"},"privileges":{"List":[]},"~child":{"#operator":"Sequence","#stats":{"#phaseSwitches":2,"execTime":"838ns"},"~children":[{"#operator":"DummyScan","#stats":{"#itemsOut":1,"#phaseSwitches":3,"execTime":"794ns","kernTime":"514ns"},"optimizer_estimates":{"cardinality":1,"cost":1.0842021724855044e-19,"fr_cost":1.0842021724855044e-19,"size":1}},{"#operator":"InitialProject","#stats":{"#itemsIn":1,"#itemsOut":1,"#phaseSwitches":8,"execTime":"110.717µs","kernTime":"4.786µs","state":"running"},"optimizer_estimates":{"cardinality":1,"cost":0.001,"fr_cost":0.001,"size":1},"result_terms":[{"as":"greeting","expr":"\"ruby rules\""}]},{"#operator":"Stream","#stats":{"#itemsIn":)",
    /* 1 */
    R"(1,"#itemsOut":1,"#phaseSwitches":2,"execTime":"7.078µs"},"optimizer_estimates":{"cardinality":1,"cost":0.001,"fr_cost":0.001,"size":1}}]},"~versions":["7.1.0-N1QL","7.1.0-2534-enterprise"]},"optimizerEstimates": {"cardinality":1,"cost":0.001}}})"
  };
  const auto result = parse(chunks);

  assert_success(result.ec, "the payload parses");
  assert_eq(result.number_of_rows, std::size_t{ 1 }, "the number of rows reported at completion");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "the number of rows handed to the row handler");
  const std::string expected_meta{
    R"({"requestID": "34a4e4b2-3f69-4bf8-a6e2-ae06798de3d9","clientContextID": "dfea5193-ead9-4ac2-5558-8fd5c4631959","signature": {"greeting":"string"},"results": [],"status": "success","profile": {"phaseTimes": {"authorize":"10.473µs","instantiate":"10.29µs","parse":"183.413µs","plan":"19.155µs","project":"5.712µs","run":"43.258µs","stream":"7.078µs"},"phaseOperators": {"authorize":1,"project":1,"stream":1},"requestTime": "2022-05-11T11:01:14.943Z","servicingHost": "10.112.220.101:8091","executionTimings": {"#operator":"Authorize","#stats":{"#phaseSwitches":4,"execTime":"1.372µs","servTime":"9.101µs"},"privileges":{"List":[]},"~child":{"#operator":"Sequence","#stats":{"#phaseSwitches":2,"execTime":"838ns"},"~children":[{"#operator":"DummyScan","#stats":{"#itemsOut":1,"#phaseSwitches":3,"execTime":"794ns","kernTime":"514ns"},"optimizer_estimates":{"cardinality":1,"cost":1.0842021724855044e-19,"fr_cost":1.0842021724855044e-19,"size":1}},{"#operator":"InitialProject","#stats":{"#itemsIn":1,"#itemsOut":1,"#phaseSwitches":8,"execTime":"110.717µs","kernTime":"4.786µs","state":"running"},"optimizer_estimates":{"cardinality":1,"cost":0.001,"fr_cost":0.001,"size":1},"result_terms":[{"as":"greeting","expr":"\"ruby rules\""}]},{"#operator":"Stream","#stats":{"#itemsIn":1,"#itemsOut":1,"#phaseSwitches":2,"execTime":"7.078µs"},"optimizer_estimates":{"cardinality":1,"cost":0.001,"fr_cost":0.001,"size":1}}]},"~versions":["7.1.0-N1QL","7.1.0-2534-enterprise"]},"optimizerEstimates": {"cardinality":1,"cost":0.001}}})"
  };
  assert_eq(result.meta, expected_meta, "the metadata spanning the chunk boundary is reassembled");
}

void
a_chunk_boundary_between_a_metadata_key_and_its_value_is_reassembled([[maybe_unused]] context& ctx)
{
  const std::vector<std::string> chunks{
    /* 0 */
    R"({"requestID": "2640a5b5-2e67-44e7-86ec-31cc388b7427","results": [42],)",
    /* 1 */
    R"("clientContextID":)",
    /* 2 */
    R"("730ecac3-e8d0-4d6e-4ed9-e2d4abd1d7b9",)",
    /* 3 */
    R"("status": "success"})",
  };
  const auto result = parse(chunks);

  assert_success(result.ec, "the payload parses");
  assert_eq(result.number_of_rows, std::size_t{ 1 }, "the number of rows reported at completion");
  assert_eq(result.rows.size(), std::size_t{ 1 }, "the number of rows handed to the row handler");
  const std::string expected_meta{
    R"({"requestID": "2640a5b5-2e67-44e7-86ec-31cc388b7427","results": [],"clientContextID":"730ecac3-e8d0-4d6e-4ed9-e2d4abd1d7b9","status": "success"})"
  };
  assert_eq(
    result.meta, expected_meta, "the metadata trailer arriving after the rows is reassembled");
  assert_eq(result.rows[0], std::string{ "42" }, "the only row");
}

void
a_payload_without_a_row_array_completes_with_no_rows([[maybe_unused]] context& ctx)
{
  const std::string chunk = R"(
{

	"requestID": "d07c0cde-cd80-4620-bb6b-d0641f272420",
	"clientContextID": "a7bbe750-20a2-4e46-eb67-315e3733b2a8",
	"signature": {
		"*": "*"
	},
	"plans":{},
	"status": "success",
	"metrics": {
		"elapsedTime": "6.56579ms",
		"executionTime": "5.552905ms",
		"resultCount": 0,
		"resultSize": 0,
		"processedObjects": 0
	}
}
)";
  const auto result = parse({ chunk });

  assert_false(result.row_handler_ran, "nothing matched the row pointer");
  assert_true(result.complete_handler_ran, "completion is still reported");
  assert_success(result.ec, "a payload with no row array is not an error");
  assert_eq(result.number_of_rows, std::size_t{ 0 }, "the number of rows reported at completion");
  assert_true(result.rows.empty(), "no row was handed to the row handler");
  assert_eq(result.meta, chunk, "the whole payload is metadata");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_response_delivered_in_one_chunk_yields_every_row) },
      { CASE(rows_arriving_in_separate_chunks_are_each_reported) },
      { CASE(a_chunk_boundary_inside_a_metadata_value_is_reassembled) },
      { CASE(a_chunk_boundary_between_a_metadata_key_and_its_value_is_reassembled) },
      { CASE(a_payload_without_a_row_array_completes_with_no_rows) },
    },
  };
}

} // namespace couchbase::test
