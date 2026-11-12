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

#include <tao/json/value.hpp>

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::key_value_status_code;
using couchbase::core::operations::get_projected_request;
using couchbase::core::protocol::client_response;
using couchbase::core::protocol::lookup_in_response_body;

auto
make_lookup_in_response(const std::vector<std::string>& values)
  -> client_response<lookup_in_response_body>
{
  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(lookup_in_response_body::opcode);
  msg.header.specific = couchbase::core::utils::host_to_network(
    static_cast<std::uint16_t>(key_value_status_code::success));
  for (const auto& value : values) {
    const std::uint16_t status = couchbase::core::utils::host_to_network(
      static_cast<std::uint16_t>(key_value_status_code::success));
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

// With the flags spec, more than 15 projections exceed the 16 specs of one lookup, so the request
// fetches the whole document and resolves each path locally in subdoc_lookup().
auto
project_full_document(std::vector<std::string> projections, const std::string& document)
  -> tao::json::value
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc" };
  req.projections = std::move(projections);
  while (req.projections.size() < 16) {
    req.projections.push_back("missing" + std::to_string(req.projections.size()));
  }
  get_projected_request::encoded_request_type encoded_req{};
  assert_success(req.encode_to(encoded_req, { std::nullopt, {} }), "the request is encoded");
  assert_true(req.effective_projections.empty(), "the whole document is fetched");

  const auto resp = req.make_response({}, make_lookup_in_response({ "0", document }));
  assert_success(resp.ctx.ec(), "the response is decoded");
  return couchbase::core::utils::json::parse_binary(resp.value);
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
  const auto projected =
    project_full_document({ "empty[-1]",
                            "empty[0]",
                            "scalars[3]",
                            "scalars[x]",
                            "scalars[1x]",
                            "scalars.key",
                            "name.key",
                            "name.key.sub",
                            "scalars.key[0]",
                            "scalars.0]",
                            "o[k[0]",
                            "o[k",
                            "scalars[0]" },
                          R"({"empty": [], "scalars": ["a", "b", "c"], "name": "value",
        "o": {"k": [5]}})");

  assert_eq(projected,
            couchbase::core::utils::json::parse(R"({"scalars": ["a"]})"),
            "only the path that resolves is projected");
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
    },
  };
}
} // namespace couchbase::test
