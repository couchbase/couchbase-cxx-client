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

#include "couchbase/logger/logger.hxx"
#include "test_helper.hxx"
#include "utils/logger.hxx"

#include <couchbase/utils/json_streaming_lexer.hxx>

struct query_result {
    std::error_code ec{};
    std::size_t number_of_rows{};
    std::string meta{};
    std::vector<std::string> rows{};
};

TEST_CASE("unit: json_streaming_lexer parse query result in single chunk", "[unit]")
{
    test::utils::init_logger();

    std::string chunk = R"(
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
    couchbase::utils::json::streaming_lexer lexer("/results/^", 4);
    query_result result{};
    lexer.on_row([&result](std::string&& row) {
        result.rows.emplace_back(std::move(row));
        return couchbase::utils::json::stream_control::next_row;
    });
    lexer.on_complete([&result](std::error_code ec, std::size_t number_of_rows, std::string&& meta) {
        result.ec = ec;
        result.number_of_rows = number_of_rows;
        result.meta = std::move(meta);
    });
    lexer.feed(chunk);
    REQUIRE_FALSE(result.ec);
    REQUIRE(result.number_of_rows == 5);
    REQUIRE(result.rows.size() == 5);
    REQUIRE(result.meta == R"(
{
"requestID": "2640a5b5-2e67-44e7-86ec-31cc388b7427",
"clientContextID": "730ecac3-e8d0-4d6e-4ed9-e2d4abd1d7b9",
"signature": {"greeting":"string"},
"results": [
],
"status": "success"
}
)");
    REQUIRE(result.rows[0] == R"({"greeting":"C++"})");
    REQUIRE(result.rows[1] == R"({"greeting":"ruby"})");
    REQUIRE(result.rows[2] == R"(null)");
    REQUIRE(result.rows[3] == R"(1)");
    REQUIRE(result.rows[4] == R"(false)");
}

TEST_CASE("unit: json_streaming_lexer parse query result", "[unit]")
{
    test::utils::init_logger();

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

    couchbase::utils::json::streaming_lexer lexer("/results/^", 4);
    query_result result{};
    lexer.on_row([&result](std::string&& row) {
        result.rows.emplace_back(std::move(row));
        return couchbase::utils::json::stream_control::next_row;
    });
    lexer.on_complete([&result](std::error_code ec, std::size_t number_of_rows, std::string&& meta) {
        result.ec = ec;
        result.number_of_rows = number_of_rows;
        result.meta = std::move(meta);
    });
    for (const auto& chunk : chunks) {
        lexer.feed(chunk);
    }
    REQUIRE_FALSE(result.ec);
    REQUIRE(result.number_of_rows == 3);
    REQUIRE(result.rows.size() == 3);
    REQUIRE(result.meta == chunks[0] + chunks[4]);
}
