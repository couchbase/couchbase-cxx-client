/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "core/operations/document_get_projected.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_lookup_in.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <tao/json/value.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::key_value_status_code;
using couchbase::core::operations::get_projected_request;
using couchbase::core::protocol::client_response;
using couchbase::core::protocol::lookup_in_response_body;

// Each field succeeds unless `statuses` gives its status.
auto
make_lookup_in_response(const std::vector<std::string>& values,
                        const std::vector<key_value_status_code>& statuses = {})
  -> client_response<lookup_in_response_body>
{
  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(lookup_in_response_body::opcode);
  msg.header.specific = couchbase::core::utils::host_to_network(static_cast<std::uint16_t>(
    statuses.empty() ? key_value_status_code::success
                     : key_value_status_code::subdoc_multi_path_failure));
  for (std::size_t i = 0; i < values.size(); ++i) {
    const auto& value = values[i];
    const std::uint16_t status = couchbase::core::utils::host_to_network(static_cast<std::uint16_t>(
      i < statuses.size() ? statuses[i] : key_value_status_code::success));
    const std::uint32_t len =
      couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(value.size()));
    const auto pos = msg.body.size();
    msg.body.resize(pos + sizeof(status) + sizeof(len) + value.size());
    std::memcpy(msg.body.data() + pos, &status, sizeof(status));
    std::memcpy(msg.body.data() + pos + sizeof(status), &len, sizeof(len));
    std::memcpy(msg.body.data() + pos + sizeof(status) + sizeof(len), value.data(), value.size());
  }
  msg.header.bodylen =
    couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(msg.body.size()));
  return client_response<lookup_in_response_body>(std::move(msg));
}

// With the flags spec, up to 15 projections fit in one lookup, and `values` holds the value of
// each. More exceed the 16 specs of one lookup. The request then fetches the whole document, and
// `values` holds only that document. subdoc_lookup() resolves each path locally.
auto
project(std::vector<std::string> projections, const std::vector<std::string>& values)
  -> tao::json::value
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc" };
  req.projections = std::move(projections);
  get_projected_request::encoded_request_type encoded_req{};
  assert_success(req.encode_to(encoded_req, { std::nullopt, {} }), "the request is encoded");
  assert_eq(req.effective_projections.empty(),
            req.projections.size() > 15,
            "the whole document is fetched only for more than 15 projections");

  std::vector<std::string> fields{ "0" };
  fields.insert(fields.end(), values.begin(), values.end());
  const auto resp = req.make_response({}, make_lookup_in_response(fields));
  assert_success(resp.ctx.ec(), "the response is decoded");
  return couchbase::core::utils::json::parse_binary(resp.value);
}

auto
project_full_document(std::vector<std::string> projections, const std::string& document)
  -> tao::json::value
{
  while (projections.size() < 16) {
    projections.push_back("missing" + std::to_string(projections.size()));
  }
  return project(std::move(projections), { document });
}

void
array_indices_select_the_indexed_element([[maybe_unused]] context& ctx)
{
  const auto projected =
    project_full_document({ "items[1].name", "tail[-1]", "scalars[1]", "matrix[1][0]" },
                          R"({"items": [{"name": "zero"}, {"name": "one"}, {"name": "two"}],
        "tail": [100, 200, 300],
        "scalars": ["a", "b", "c"],
        "matrix": [[1, 2], [3, 4]]})");

  assert_eq(projected,
            couchbase::core::utils::json::parse(
              R"({"items": [{"name": "one"}], "tail": [300], "scalars": ["b"], "matrix": [[3]]})"),
            "each path selects its own element");
}

void
unresolvable_paths_are_omitted([[maybe_unused]] context& ctx)
{
  const std::string document{ R"({"empty": [], "scalars": ["a", "b", "c"], "name": "value",
        "o": {"k": [5]}})" };
  assert_eq(project_full_document({ "empty[-1]",
                                    "empty[0]",
                                    "scalars[3]",
                                    "scalars[x]",
                                    "scalars[1x]",
                                    "scalars.key",
                                    "name.key",
                                    "scalars.0]",
                                    "o[k",
                                    "scalars[0]" },
                                  document),
            couchbase::core::utils::json::parse(R"({"scalars": ["a"]})"),
            "only the path that resolves is projected");
  // Apart from name.key, scalars.key and o[k, which enclose them and would leave them unapplied.
  assert_eq(
    project_full_document({ "name.key.sub", "scalars.key[0]", "o[k[0]", "scalars[0]" }, document),
    couchbase::core::utils::json::parse(R"({"scalars": ["a"]})"),
    "a step past a scalar or an unclosed index");
}

void
an_array_projected_with_its_element_is_returned_once([[maybe_unused]] context& ctx)
{
  assert_eq(project({ "items", "items[0]" }, { "[10,20]", "10" }),
            couchbase::core::utils::json::parse(R"({"items": [10, 20]})"),
            "the array first");
  assert_eq(project({ "items[0]", "items" }, { "10", "[10,20]" }),
            couchbase::core::utils::json::parse(R"({"items": [10, 20]})"),
            "the element first");
  assert_eq(project({ "a.items", "a.items[0]" }, { "[10,20]", "10" }),
            couchbase::core::utils::json::parse(R"({"a": {"items": [10, 20]}})"),
            "an array inside an object");
  assert_eq(project({ "items", "items[-1]" }, { "[10,20]", "20" }),
            couchbase::core::utils::json::parse(R"({"items": [10, 20]})"),
            "a negative index");
}

void
an_element_projected_with_a_path_inside_it_is_returned_once([[maybe_unused]] context& ctx)
{
  assert_eq(project({ "items[0]", "items[0].n" }, { R"({"n":1})", "1" }),
            couchbase::core::utils::json::parse(R"({"items": [{"n": 1}]})"),
            "the element first");
  assert_eq(project({ "items[0].n", "items[0]" }, { "1", R"({"n":1})" }),
            couchbase::core::utils::json::parse(R"({"items": [{"n": 1}]})"),
            "the path inside it first");
}

void
a_repeated_element_path_is_returned_once([[maybe_unused]] context& ctx)
{
  assert_eq(project({ "items[0]", "items[0]" }, { "10", "10" }),
            couchbase::core::utils::json::parse(R"({"items": [10]})"),
            "one element");
}

void
paths_that_do_not_enclose_one_another_are_all_projected([[maybe_unused]] context& ctx)
{
  assert_eq(project({ "item", "items[0]" }, { "1", "10" }),
            couchbase::core::utils::json::parse(R"({"item": 1, "items": [10]})"),
            "a key that extends another");
  assert_eq(project({ "items[0]", "items[1]" }, { "10", "20" }),
            couchbase::core::utils::json::parse(R"({"items": [10, 20]})"),
            "sibling elements");
  assert_eq(project({ "items[1]", "items[10]" }, { "20", "110" }),
            couchbase::core::utils::json::parse(R"({"items": [20, 110]})"),
            "an index that extends another");
}

void
the_full_document_path_returns_an_element_once([[maybe_unused]] context& ctx)
{
  assert_eq(project_full_document({ "items", "items[0]", "items[0].n", "items[0]" },
                                  R"({"items": [{"n": 1}, 20]})"),
            couchbase::core::utils::json::parse(R"({"items": [{"n": 1}, 20]})"),
            "an array with its element and a path inside it");
  assert_eq(project_full_document({ "items[0].n", "items[0]" }, R"({"items": [{"n": 1}, 20]})"),
            couchbase::core::utils::json::parse(R"({"items": [{"n": 1}]})"),
            "an element with a path inside it");
}
void
an_enclosed_projection_still_reports_its_error([[maybe_unused]] context& ctx)
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc" };
  req.projections = { "items", "items[0].n" };
  get_projected_request::encoded_request_type encoded_req{};
  assert_success(req.encode_to(encoded_req, { std::nullopt, {} }), "the request is encoded");

  const auto resp =
    req.make_response({},
                      make_lookup_in_response({ "0", "[10]", "" },
                                              { key_value_status_code::success,
                                                key_value_status_code::success,
                                                key_value_status_code::subdoc_path_mismatch }));
  assert_eq(resp.ctx.ec(),
            std::error_code{ couchbase::errc::key_value::path_mismatch },
            "the error of the enclosed path");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(array_indices_select_the_indexed_element) },
      { CASE(unresolvable_paths_are_omitted) },
      { CASE(an_array_projected_with_its_element_is_returned_once) },
      { CASE(an_element_projected_with_a_path_inside_it_is_returned_once) },
      { CASE(a_repeated_element_path_is_returned_once) },
      { CASE(paths_that_do_not_enclose_one_another_are_all_projected) },
      { CASE(the_full_document_path_returns_an_element_once) },
      { CASE(an_enclosed_projection_still_reports_its_error) },
    },
  };
}
} // namespace couchbase::test
