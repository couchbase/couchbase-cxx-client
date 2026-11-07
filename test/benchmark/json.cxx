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

#include "bench_registry.hxx"

#include "core/utils/json.hxx"
#include "core/utils/json_streaming_lexer.hxx"

#include <tao/json.hpp>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

namespace couchbase::test::bench
{
namespace
{
auto
small_document() -> tao::json::value
{
  return {
    { "name", "bench" },
    { "id", 42 },
    { "tags", tao::json::value::array({ "a", "b", "c" }) },
    { "nested", { { "ok", true }, { "ratio", 0.5 } } },
  };
}

// Nested objects and arrays.
auto
large_document() -> tao::json::value
{
  tao::json::value items = tao::json::empty_array;
  for (std::size_t i = 0; i < 80; ++i) {
    items.push_back({
      { "id", i },
      { "name", "item-" + std::to_string(i) },
      { "tags", tao::json::value::array({ "alpha", "beta", "gamma", "delta" }) },
      { "attributes",
        { { "active", i % 2 == 0 },
          { "score", static_cast<double>(i) / 7.0 },
          { "history", tao::json::value::array({ i, i + 1, i + 2, i + 3, i + 4 }) } } },
    });
  }
  return { { "items", items } };
}

void
json_generate_binary(state& st)
{
  const auto small = small_document();
  const auto large = large_document();

  st.batched("small", 1, [&] {
    do_not_optimize(core::utils::json::generate_binary(small));
  });
  st.batched("large", 1, [&] {
    do_not_optimize(core::utils::json::generate_binary(large));
  });
}

// A query response as the query service sends it: the preamble up to the opening of "results", the
// rows, then the trailer.
auto
query_response(std::size_t row_count) -> std::string
{
  std::string body =
    R"({"requestID":"9739203f-9cd5-45cd-8e3a-31c27407d66a","clientContextID":"2067c2c25c32545c",)"
    R"("signature":{"*":"*"},"results":[)";
  for (std::size_t i = 0; i < row_count; ++i) {
    if (i > 0) {
      body += ',';
    }
    body += R"({"id":"bench::)" + std::to_string(i) + R"(","type":"row","value":{"n":)" +
            std::to_string(i) + R"(,"tags":["a","b","c"],"text":"lorem ipsum dolor sit amet"}})";
  }
  body += R"(],"status":"success","metrics":{"elapsedTime":"1.234567ms",)"
          R"("executionTime":"1.112233ms","resultCount":)" +
          std::to_string(row_count) + R"(,"resultSize":)" + std::to_string(row_count * 90) +
          R"(,"serviceLoad":2}})";
  return body;
}

// Registers the three handlers the way core's row streamer does, then feeds the response in chunks
// the size of a socket read. One iteration parses the whole response. Throws unless the lexer
// completed without error after emitting expected_rows rows, so a lexer that stops early fails the
// case rather than reading as faster.
void
parse_response(const std::string& body, std::size_t expected_rows)
{
  constexpr std::size_t chunk_size = 4096;
  core::utils::json::streaming_lexer lexer("/results/^", 4);
  std::size_t rows = 0;
  lexer.on_metadata_header_complete([](std::error_code ec, std::string&& meta_header) {
    const auto last_non_ws = meta_header.find_last_not_of(" \t\f\v\n\r");
    if (last_non_ws != std::string::npos) {
      meta_header.erase(last_non_ws + 1);
    }
    if (!meta_header.empty() && meta_header.back() == '[') {
      meta_header.append("]}");
    }
    do_not_optimize(ec);
    do_not_optimize(meta_header);
  });
  lexer.on_row([&rows](std::string&& row) {
    do_not_optimize(row);
    ++rows;
    return core::utils::json::stream_control::next_row;
  });
  bool completed = false;
  lexer.on_complete(
    [&completed](std::error_code ec, std::size_t number_of_rows, std::string&& meta) {
      completed = !ec;
      do_not_optimize(number_of_rows);
      do_not_optimize(meta);
    });
  for (std::size_t offset = 0; offset < body.size(); offset += chunk_size) {
    lexer.feed(std::string_view{ body }.substr(offset, chunk_size));
  }
  if (!completed || rows != expected_rows) {
    throw std::runtime_error("the lexer did not complete without error after " +
                             std::to_string(expected_rows) + " rows");
  }
}

void
json_streaming_lexer(state& st)
{
  const auto small = query_response(10);
  const auto large = query_response(1000);
  parse_response(small, 10);
  parse_response(large, 1000);

  st.batched("rows_10", 1, [&] {
    parse_response(small, 10);
  });
  st.batched("rows_1000", 1, [&] {
    parse_response(large, 1000);
  });
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name, { { CASE(json_generate_binary) }, { CASE(json_streaming_lexer) } } };
}
} // namespace couchbase::test::bench
