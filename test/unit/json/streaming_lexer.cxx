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

#include "core/utils/json.hxx"
#include "core/utils/json_streaming_lexer.hxx"

#include "utils/logger.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json.hpp>

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <system_error>
#include <vector>

namespace
{
thread_local volatile long g_alloc_count{ 0 };
} // namespace

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
auto
operator new(std::size_t sz) -> void*
{
  g_alloc_count = g_alloc_count + 1;
  if (void* p = std::malloc(sz == 0 ? 1 : sz)) {
    return p;
  }
  throw std::bad_alloc{};
}

void
operator delete(void* p) noexcept
{
  std::free(p);
}

void
operator delete(void* p, std::size_t /* sz */) noexcept
{
  std::free(p);
}
#endif

namespace couchbase::test
{
namespace
{
// The allocation budgets read g_alloc_count, which only the replacement operator new above
// increments. A sanitized build compiles the replacement out, and under valgrind's memcheck the
// client library's allocations bypass it while this file's own still reach it. Either way the
// counter stays at zero for the code under test and every budget is met, so a case with a budget
// runs only where an allocation made inside the library is counted.
class needs_counted_allocations : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "a global operator new that counts the client library's allocations";
  }

  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
    const tao::json::value document{ { "probe", 1 } };
    g_alloc_count = 0;
    // A non-empty object is generated into a reserved buffer: at least one allocation.
    const auto generated = couchbase::core::utils::json::generate_binary(document);
    if (generated.empty() || g_alloc_count == 0) {
      return check_result::missing("allocations in the client library are not counted");
    }
    return check_result::ok();
  }
};
} // namespace

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

void
meta_header_callback_receives_prefix_and_preserves_full_meta([[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4);
  std::string meta_header{};
  std::error_code meta_header_ec{};
  int meta_header_calls{ 0 };
  int rows_before_meta_header{ 0 };
  std::vector<std::string> rows{};
  std::string full_meta{};
  std::error_code final_ec{};

  // The handlers only record: an assertion thrown from one would unwind through the jsonsl C code.
  lexer.on_metadata_header_complete([&](std::error_code ec, std::string&& header) {
    meta_header_ec = ec;
    ++meta_header_calls;
    meta_header = std::move(header);
  });
  lexer.on_row([&](std::string&& row) {
    if (meta_header_calls == 0) {
      ++rows_before_meta_header;
    }
    rows.emplace_back(std::move(row));
    return couchbase::core::utils::json::stream_control::next_row;
  });
  lexer.on_complete([&](std::error_code ec, std::size_t /* number_of_rows */, std::string&& meta) {
    final_ec = ec;
    full_meta = std::move(meta);
  });

  lexer.feed(R"({"requestID":"req-1","clientContextID":"ctx-1","results":[)");
  lexer.feed(R"({"id":1},{"id":2}],"status":"success"})");

  assert_success(meta_header_ec, "metadata header parses cleanly");
  assert_success(final_ec, "the payload parses cleanly");
  // row_streamer::start completes on the header, and back-pressure from buffered rows can stop
  // reads before the rest of the payload arrives.
  assert_eq(rows_before_meta_header, 0, "the metadata header arrives before the first row");
  assert_eq(meta_header_calls, 1, "on_meta_header_complete is invoked once");
  assert_eq(meta_header,
            std::string{ R"({"requestID":"req-1","clientContextID":"ctx-1","results":[)" },
            "on_meta_header_complete receives the pre-row metadata header");
  assert_eq(rows.size(), std::size_t{ 2 }, "both rows are emitted");
  assert_eq(
    full_meta,
    std::string{
      R"({"requestID":"req-1","clientContextID":"ctx-1","results":[],"status":"success"})" },
    "on_complete still receives the full metadata including the pre-row header and trailer");
}

void
a_feed_ending_after_the_row_key_keeps_the_key_across_buffer_growth([[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  // The matched key is kept as an offset into the lexer's buffer. The first feed ends right after
  // it, and the second one is large enough to reallocate the buffer before the key is used.
  std::string rows_chunk = R"(:[)";
  std::vector<std::string> expected_rows{};
  for (int i = 0; i < 128; ++i) {
    expected_rows.push_back(R"({"row":)" + std::to_string(i) + R"(,"pad":"0123456789abcdef"})");
    rows_chunk += (i == 0 ? "" : ",") + expected_rows.back();
  }
  rows_chunk += R"(],"status":"success"})";

  const auto result = parse({ R"({"requestID":"req-1","results")", rows_chunk });

  assert_success(result.ec, "the payload parses cleanly");
  assert_eq(result.rows, expected_rows, "every row is emitted as a row");
  assert_eq(result.meta,
            std::string{ R"({"requestID":"req-1","results":[],"status":"success"})" },
            "the metadata keeps the key and drops the rows");
}

struct long_metadata_keys {
  std::vector<std::string> chunks{};
  std::string expected_meta{};
};

// 32 long (non-SSO) metadata keys, half of them with a nested key, placed before and after the row
// array when there is one. The chunks are shorter than a key, so keys straddle feed() boundaries.
auto
long_metadata_keys_payload(bool with_results = true) -> long_metadata_keys
{
  std::string payload = "{";
  std::string expected_meta = "{";
  for (int i = 0; i < 16; ++i) {
    const std::string entry = "\"pre_metadata_attribute_with_long_non_sso_name_" +
                              std::to_string(i) + "\":{\"nested_long_metadata_key_" +
                              std::to_string(i) + "\":" + std::to_string(i) + "},";
    payload += entry;
    expected_meta += entry;
  }
  if (with_results) {
    payload += R"("results":[{"row":1},{"row":2},{"row":3}],)";
    expected_meta += R"("results":[],)";
  }
  for (int i = 0; i < 16; ++i) {
    const std::string entry = "\"post_metadata_attribute_with_long_non_sso_name_" +
                              std::to_string(i) + "\":\"value_" + std::to_string(i) + "\",";
    payload += entry;
    expected_meta += entry;
  }
  payload += R"("status":"success"})";
  expected_meta += R"("status":"success"})";

  long_metadata_keys result{};
  for (std::size_t pos = 0; pos < payload.size(); pos += 37) {
    result.chunks.push_back(payload.substr(pos, 37));
  }
  result.expected_meta = std::move(expected_meta);
  return result;
}

void
many_long_metadata_keys_without_header_callback_parse_cleanly([[maybe_unused]] context& ctx)
{
  const auto payload = long_metadata_keys_payload();
  const auto result = parse(payload.chunks);

  assert_success(result.ec, "chunked payload with many long metadata keys parses cleanly");
  assert_true(result.row_handler_ran, "row handler ran");
  assert_true(result.complete_handler_ran, "completion handler ran");
  assert_eq(result.number_of_rows, std::size_t{ 3 }, "all three rows counted");
  assert_eq(result.rows.size(), std::size_t{ 3 }, "all three rows emitted");
  assert_eq(result.rows[0], std::string{ R"({"row":1})" }, "first row");
  assert_eq(result.rows[1], std::string{ R"({"row":2})" }, "second row");
  assert_eq(result.rows[2], std::string{ R"({"row":3})" }, "third row");
  assert_eq(result.meta,
            payload.expected_meta,
            "full metadata is preserved without on_metadata_header_complete registered");
}

auto
count_feed_allocations(const std::vector<std::string>& chunks, bool with_header_handler) -> long
{
  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4);
  lexer.on_row([](std::string&& /* row */) {
    return couchbase::core::utils::json::stream_control::next_row;
  });
  lexer.on_complete([](std::error_code /* ec */, std::size_t /* rows */, std::string&& /* meta */) {
  });
  if (with_header_handler) {
    lexer.on_metadata_header_complete([](std::error_code /* ec */, std::string&& /* header */) {
    });
  }

  g_alloc_count = 0;
  for (const auto& chunk : chunks) {
    lexer.feed(chunk);
  }
  return g_alloc_count;
}

void
long_metadata_keys_are_matched_without_a_string_per_key([[maybe_unused]] context& ctx)
{
  // The logger allocates on its first initialisation, which must fall outside the measurement.
  ::test::utils::init_logger();
  const auto payload = long_metadata_keys_payload();

  // Only feeding is measured: collecting rows and metadata would add allocations of its own.
  const long allocs = count_feed_allocations(payload.chunks, false);

  // A std::string per matched key allocates once for each long key and exceeds the budget.
  assert_true(allocs <= 25L,
              "streaming_lexer compares metadata keys via string_view without per-key allocation");
}

void
only_a_registered_header_handler_gets_a_copy_of_the_header([[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();
  // The header is longer than the small-string buffer, so its copy allocates. MSVC debug iterators
  // add a proxy allocation to every string, so the difference is asserted positive, not exact. The
  // header is delivered as the first row starts; without a row, only the trailer delivers it.
  const auto with_rows = long_metadata_keys_payload(true);
  assert_true(count_feed_allocations(with_rows.chunks, true) >
                count_feed_allocations(with_rows.chunks, false),
              "with a row array, only a registered handler gets a copy of the header");

  const auto without_rows = long_metadata_keys_payload(false);
  assert_true(count_feed_allocations(without_rows.chunks, true) >
                count_feed_allocations(without_rows.chunks, false),
              "without a row array, only a registered handler gets a copy of the header");
}

void
an_oversized_payload_without_a_header_handler_completes_once_with_an_error(
  [[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4, 64);
  int completions{ 0 };
  std::error_code final_ec{};
  lexer.on_row([](std::string&& /* row */) {
    return couchbase::core::utils::json::stream_control::next_row;
  });
  lexer.on_complete([&](std::error_code ec, std::size_t /* rows */, std::string&& /* meta */) {
    ++completions;
    final_ec = ec;
  });

  // The first chunk alone exceeds the buffer limit; the second arrives after the lexer gave up.
  lexer.feed(R"({"requestID":")" + std::string(128, 'x'));
  lexer.feed(R"(","results":[]})");

  assert_eq(completions, 1, "the oversized payload completes exactly once");
  assert_error(final_ec,
               couchbase::errc::streaming_json_lexer::generic,
               "the oversized payload completes with the lexer error");
}

void
an_oversized_chunk_after_the_header_completes_once_with_an_error([[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4, 64);
  int meta_header_calls{ 0 };
  std::error_code meta_header_ec{};
  int rows{ 0 };
  int completions{ 0 };
  std::error_code final_ec{};
  lexer.on_metadata_header_complete([&](std::error_code ec, std::string&& /* header */) {
    ++meta_header_calls;
    meta_header_ec = ec;
  });
  lexer.on_row([&](std::string&& /* row */) {
    ++rows;
    return couchbase::core::utils::json::stream_control::next_row;
  });
  lexer.on_complete([&](std::error_code ec, std::size_t /* rows */, std::string&& /* meta */) {
    ++completions;
    final_ec = ec;
  });

  // The header and a row fit the limit, which clears the header handler; the next chunk does not.
  lexer.feed(R"({"requestID":"r","results":[{"id":1},)");
  lexer.feed(R"({"id":")" + std::string(128, 'x'));

  assert_eq(meta_header_calls, 1, "the header is delivered once");
  assert_success(meta_header_ec, "the header is delivered without an error");
  assert_eq(rows, 1, "the row before the oversized chunk is delivered");
  assert_eq(completions, 1, "the oversized chunk completes the payload once");
  assert_error(final_ec,
               couchbase::errc::streaming_json_lexer::generic,
               "the oversized chunk completes with the lexer error");
}

void
without_a_row_array_the_header_handler_and_completion_each_get_the_whole_payload(
  [[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  const std::string payload =
    R"({"requestID":"req-1","signature":{"*":"*"},"plans":{},"status":"success"})";

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4);
  int meta_header_calls{ 0 };
  std::error_code meta_header_ec{};
  std::string meta_header{};
  int completions{ 0 };
  std::error_code final_ec{};
  std::size_t number_of_rows{ 0 };
  std::string meta{};
  lexer.on_metadata_header_complete([&](std::error_code ec, std::string&& header) {
    ++meta_header_calls;
    meta_header_ec = ec;
    meta_header = std::move(header);
  });
  lexer.on_complete([&](std::error_code ec, std::size_t rows, std::string&& full_meta) {
    ++completions;
    final_ec = ec;
    number_of_rows = rows;
    meta = std::move(full_meta);
  });

  lexer.feed(payload.substr(0, 30));
  lexer.feed(payload.substr(30));

  assert_eq(meta_header_calls, 1, "the header handler is called once");
  assert_success(meta_header_ec, "the header is delivered without an error");
  assert_eq(meta_header, payload, "the header handler receives the whole payload");
  assert_eq(completions, 1, "the payload completes once");
  assert_success(final_ec, "the payload completes without an error");
  assert_eq(number_of_rows, std::size_t{ 0 }, "no row is counted");
  assert_eq(meta, payload, "the completion receives the whole payload as metadata");
}

void
malformed_json_before_the_row_array_reports_the_error_to_both_handlers_once(
  [[maybe_unused]] context& ctx)
{
  ::test::utils::init_logger();

  couchbase::core::utils::json::streaming_lexer lexer("/results/^", 4);
  int meta_header_calls{ 0 };
  std::error_code meta_header_ec{};
  int completions{ 0 };
  std::error_code final_ec{};
  lexer.on_metadata_header_complete([&](std::error_code ec, std::string&& /* header */) {
    ++meta_header_calls;
    meta_header_ec = ec;
  });
  lexer.on_complete([&](std::error_code ec, std::size_t /* rows */, std::string&& /* meta */) {
    ++completions;
    final_ec = ec;
  });

  // An object key must be a string, so the lexer fails before it reaches "results".
  lexer.feed(R"({"requestID":"req-1",42:"x",)");
  lexer.feed(R"("results":[{"id":1}],"status":"success"})");

  assert_eq(meta_header_calls, 1, "the header handler is called once");
  assert_error(meta_header_ec,
               couchbase::errc::streaming_json_lexer::object_key_expected,
               "the header handler receives the parse error");
  assert_eq(completions, 1, "the payload completes once");
  assert_error(final_ec,
               couchbase::errc::streaming_json_lexer::object_key_expected,
               "the completion receives the parse error");
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
      { CASE(meta_header_callback_receives_prefix_and_preserves_full_meta) },
      { CASE(a_feed_ending_after_the_row_key_keeps_the_key_across_buffer_growth) },
      { CASE(many_long_metadata_keys_without_header_callback_parse_cleanly) },
      { CASE(long_metadata_keys_are_matched_without_a_string_per_key),
        { std::make_shared<const needs_counted_allocations>() } },
      { CASE(only_a_registered_header_handler_gets_a_copy_of_the_header),
        { std::make_shared<const needs_counted_allocations>() } },
      { CASE(an_oversized_payload_without_a_header_handler_completes_once_with_an_error) },
      { CASE(an_oversized_chunk_after_the_header_completes_once_with_an_error) },
      { CASE(without_a_row_array_the_header_handler_and_completion_each_get_the_whole_payload) },
      { CASE(malformed_json_before_the_row_array_reports_the_error_to_both_handlers_once) },
    },
  };
}

} // namespace couchbase::test
