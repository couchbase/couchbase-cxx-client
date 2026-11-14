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

#include "framework/test_registry.hxx"

#include "core/operations/document_get_projected.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_lookup_in.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace couchbase::test
{
namespace
{
using couchbase::core::key_value_status_code;
using couchbase::core::operations::get_projected_request;
using couchbase::core::protocol::client_response;
using couchbase::core::protocol::lookup_in_response_body;

struct subdoc_field_spec {
  std::string value{};
  key_value_status_code status{ key_value_status_code::success };
};

auto
make_lookup_in_response_with_statuses(const std::vector<subdoc_field_spec>& fields)
  -> client_response<lookup_in_response_body>
{
  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(lookup_in_response_body::opcode);
  msg.header.specific = couchbase::core::utils::host_to_network(
    static_cast<std::uint16_t>(key_value_status_code::success));

  for (const auto& field : fields) {
    const std::uint16_t status =
      couchbase::core::utils::host_to_network(static_cast<std::uint16_t>(field.status));
    const std::uint32_t len =
      couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(field.value.size()));
    const auto pos = msg.body.size();
    msg.body.resize(pos + sizeof(status) + sizeof(len) + field.value.size());
    std::memcpy(msg.body.data() + pos, &status, sizeof(status));
    std::memcpy(msg.body.data() + pos + sizeof(status), &len, sizeof(len));
    if (!field.value.empty()) {
      std::memcpy(msg.body.data() + pos + sizeof(status) + sizeof(len),
                  field.value.data(),
                  field.value.size());
    }
  }
  msg.header.bodylen =
    couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(msg.body.size()));
  return client_response<lookup_in_response_body>(std::move(msg));
}

auto
make_lookup_in_response(const std::vector<std::string>& field_values)
  -> client_response<lookup_in_response_body>
{
  std::vector<subdoc_field_spec> specs;
  specs.reserve(field_values.size());
  for (const auto& val : field_values) {
    specs.push_back(subdoc_field_spec{ val, key_value_status_code::success });
  }
  return make_lookup_in_response_with_statuses(specs);
}

void
full_document_projection_resolves_array_elements_by_index_and_nested_fields(
  [[maybe_unused]] context& ctx)
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc-1" };
  req.preserve_array_indexes = true;
  req.projections = {
    "items[0]",
    "objects[0].name",
    "objects[0].code",
    "tail[-1]",
  };
  for (int i = 0; i < 16; ++i) {
    req.projections.push_back("f" + std::to_string(i));
  }

  const std::optional<couchbase::core::topology::configuration> no_config{};
  get_projected_request::encoded_request_type encoded_req{};
  assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
               "encode_to succeeds");
  assert_true(req.effective_projections.empty(), "more than 16 specs triggers full-document mode");

  const std::string full_doc = R"({
    "items": ["alpha", "beta", "gamma"],
    "objects": [
      {"name": "first-object", "code": 10, "unprojected": "skip-0"},
      {"name": "second-object", "code": 20, "unprojected": "skip-1"}
    ],
    "tail": [100, 200, 300],
    "f0": 1
  })";

  auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
  const auto resp = req.make_response({}, encoded_resp);
  assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds");

  const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
  const auto* items = projected.find("items");
  assert_true(items != nullptr && items->is_array(), "terminal array element is projected");
  assert_eq(items->get_array().size(), std::size_t{ 1 }, "one array element projected");
  assert_eq(items->get_array()[0].get_string(), "alpha", "index 0 selects the first element");

  const auto* objects = projected.find("objects");
  assert_true(objects != nullptr && objects->is_array(), "nested array object is projected");
  assert_eq(
    objects->get_array().size(), std::size_t{ 1 }, "single object entry in projected array");
  assert_eq(objects->get_array()[0].at("name").get_string(), "first-object", "projected name");
  assert_eq(
    objects->get_array()[0].at("code").get_unsigned(), std::uint64_t{ 10 }, "projected code");
  assert_true(objects->get_array()[0].find("unprojected") == nullptr,
              "unprojected sibling field is not included");

  const auto* tail = projected.find("tail");
  assert_true(tail != nullptr && tail->is_array(), "negative index array element is projected");
  assert_eq(tail->get_array().size(), std::size_t{ 3 }, "preserve_array_indexes keeps index 2");
  assert_true(tail->get_array()[0].is_null() && tail->get_array()[1].is_null(),
              "unprojected leading slots are null when preserve_array_indexes is true");
  assert_eq(tail->get_array()[2].get_unsigned(), std::uint64_t{ 300 }, "index -1 selects the back");
}

void
full_document_projection_handles_empty_arrays_and_invalid_paths_without_crashing(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};
  const std::string full_doc =
    R"({"empty": [], "items": ["x"], "valid": "ok", "arr.idx": ["first"], )"
    R"("BACKTICK`HAPPY": "yes", "\"quoted": "qval"})";

  // 1. Valid paths against empty arrays and valid backtick/JSON-escaped identifiers succeed in
  // over-16 full-document projection mode (and empty projection strings remain a no-op, matching
  // subdoc mode).
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-2-empty-and-escaped" };
    req.projections = {
      "",        "empty[-1]",   "empty[0]",          "`arr.idx`[0]",
      "items[]", R"(\"quoted)", "`BACKTICK``HAPPY`", "valid",
    };
    for (int i = 0; i < 15; ++i) {
      req.projections.push_back("f" + std::to_string(i));
    }

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "over-16 specs use full-document mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds on empty/escaped paths");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_eq(projected.at("valid").get_string(), "ok", "valid path is projected");
    assert_eq(projected.at("arr.idx").get_array().at(0).get_string(),
              "first",
              "backtick-escaped dotted key is unescaped and projected");
    assert_eq(projected.at("items").get_array().at(0).get_string(),
              "x",
              "empty bracket component items[] selects index 0 in full-document mode");
    assert_eq(projected.at("BACKTICK`HAPPY").get_string(),
              "yes",
              "doubled backtick inside identifier is unescaped");
    assert_eq(projected.at("\"quoted").get_string(),
              "qval",
              "JSON-escaped quote in path is unescaped and projected");
    assert_true(projected.find("empty") == nullptr, "empty array lookup yields no element");
  }

  // 2. Malformed paths in over-16 full-document mode report errc::key_value::path_invalid rather
  // than being silently omitted as missing paths.
  for (const auto* bad_path : {
         ".",
         ".leading",
         "items[notanumber]",
         "missing.items[notanumber]",
         "items[0]unexpected",
         "malformed[0",
         "malformed[",
         "items[-0]",
         "items[-2]",
         "invalid..path",
         "`unterminated",
         "``",
         R"("unquoted)",
         R"(bad\q)",
         R"(bad\u0041)",
         R"(trailing\)",
       }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-2-invalid-path" };
    req.projections = { "valid", bad_path };
    for (int i = 0; i < 16; ++i) {
      req.projections.push_back("f" + std::to_string(i));
    }

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "over-16 specs use full-document mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_invalid,
              std::string{ "malformed path in full-document mode reports path_invalid: " } +
                bad_path);
    assert_true(resp.value.empty(), "invalid path response does not return partial payload");
  }

  // 3. Paths exceeding the 31-component subjson limit in over-16 full-document mode report
  // errc::key_value::path_too_big.
  {
    std::string overdeep_path = "a0";
    for (int i = 1; i < 32; ++i) {
      overdeep_path.append(".a" + std::to_string(i));
    }

    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-2-overdeep-path" };
    req.projections = { "valid", overdeep_path };
    for (int i = 0; i < 16; ++i) {
      req.projections.push_back("f" + std::to_string(i));
    }

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "over-16 specs use full-document mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_too_big,
              "32-component path in full-document mode reports path_too_big");
    assert_true(resp.value.empty(), "overdeep path response has empty value");
  }

  // 4. Paths exceeding kv_engine's SUBDOC_PATH_MAX_LENGTH (1024 bytes) are rejected with
  // errc::common::invalid_argument (Status::Einval on the server, distinct from > 31 components
  // Status::SubdocPathE2big -> path_too_big) regardless of projection count, while 1024-byte paths
  // remain valid.
  {
    const std::string max_len_path(1024, 'a');
    const std::string overlong_path(1025, 'a');

    {
      get_projected_request ok_req{};
      ok_req.id = { "default", "_default", "_default", "doc-2-exact-1024-path" };
      ok_req.projections = { "valid", max_len_path };
      for (int i = 0; i < 16; ++i) {
        ok_req.projections.push_back("f" + std::to_string(i));
      }

      get_projected_request::encoded_request_type encoded_req{};
      assert_false(static_cast<bool>(ok_req.encode_to(encoded_req, { no_config, {} })),
                   "1024-byte path succeeds in encode_to");
      auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
      const auto resp = ok_req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()),
                   "1024-byte missing path succeeds in full-document mode");
    }

    for (const int extra_specs : { 0, 16 }) {
      get_projected_request bad_req{};
      bad_req.id = { "default", "_default", "_default", "doc-2-overlong-1025-path" };
      bad_req.projections = { "valid", overlong_path };
      for (int i = 0; i < extra_specs; ++i) {
        bad_req.projections.push_back("f" + std::to_string(i));
      }

      get_projected_request::encoded_request_type encoded_req{};
      assert_eq(bad_req.encode_to(encoded_req, { no_config, {} }),
                couchbase::errc::common::invalid_argument,
                "1025-byte path reports invalid_argument in encode_to");

      // Also verify make_response directly when effective_projections is empty (full-doc mode).
      bad_req.effective_projections.clear();
      auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
      const auto resp = bad_req.make_response({}, encoded_resp);
      assert_eq(resp.ctx.ec(),
                couchbase::errc::common::invalid_argument,
                "1025-byte path reports invalid_argument in full-document make_response");
      assert_true(resp.value.empty(), "overlong path response has empty value");
    }
  }
}

void
projected_get_response_with_missing_subdoc_fields_reports_parsing_failure(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-3" };
    req.projections = { "a", "b" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");

    // Empty response body with 0 subdoc fields
    auto empty_resp = make_lookup_in_response({});
    const auto resp = req.make_response({}, empty_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::common::parsing_failure,
              "missing subdoc fields report parsing_failure instead of reading out of bounds");

    for (const auto* bad_flags :
         { "not-a-number", "4294967296", "1700000000junk", "999999999999999999999999999999" }) {
      auto bad_flags_resp = make_lookup_in_response({ bad_flags, "1", "2" });
      const auto parsed = req.make_response({}, bad_flags_resp);
      assert_eq(parsed.ctx.ec(),
                couchbase::errc::common::parsing_failure,
                "invalid or overflowing flags value reports parsing_failure without throwing");
    }
  }

  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-3-expiry" };
    req.with_expiry = true;
    req.projections = { "a", "b" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to with_expiry succeeds");

    // Non-empty response that has flags + 2 projections (3 fields) but is missing the 4th field
    // required when with_expiry is true (flags + expiry + 2 projections = 4 fields)
    auto short_expiry_resp = make_lookup_in_response({ "33554432", "1", "2" });
    const auto short_resp = req.make_response({}, short_expiry_resp);
    assert_eq(short_resp.ctx.ec(),
              couchbase::errc::common::parsing_failure,
              "truncated with_expiry response returns parsing_failure");

    for (const auto* bad_expiry :
         { "not-an-expiry", "4294967296", "1700000000junk", "999999999999999999999999999999" }) {
      auto bad_expiry_resp = make_lookup_in_response({ "33554432", bad_expiry, "1", "2" });
      const auto parsed = req.make_response({}, bad_expiry_resp);
      assert_eq(parsed.ctx.ec(),
                couchbase::errc::common::parsing_failure,
                "invalid or overflowing expiry value reports parsing_failure without throwing");
    }

    auto valid_expiry_resp = make_lookup_in_response({ "33554432", "1700000000", "1", "2" });
    const auto ok_resp = req.make_response({}, valid_expiry_resp);
    assert_false(static_cast<bool>(ok_resp.ctx.ec()), "valid with_expiry response succeeds");
    assert_eq(ok_resp.flags, std::uint32_t{ 33554432 }, "flags parsed");
    assert_true(ok_resp.expiry.has_value(), "expiry populated");
    assert_eq(ok_resp.expiry.value_or(0), std::uint32_t{ 1700000000 }, "expiry value parsed");
    const auto projected = couchbase::core::utils::json::parse_binary(ok_resp.value);
    assert_eq(projected.at("a").get_unsigned(), std::uint64_t{ 1 }, "projected a after expiry");
    assert_eq(projected.at("b").get_unsigned(), std::uint64_t{ 2 }, "projected b after expiry");
  }

  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-3-full-failed-body" };
    req.with_expiry = true;
    req.projections = {};

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to full-document with_expiry succeeds");

    auto failed_body_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { "1700000000", key_value_status_code::success },
      { "", key_value_status_code::subdoc_doc_not_json },
    });
    const auto resp = req.make_response({}, failed_body_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::document_not_json,
              "failed full-document body field maps status code instead of returning empty value");
  }
}

void
nested_array_projections_merge_into_same_element_when_preserve_array_indexes_is_false(
  [[maybe_unused]] context& ctx)
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc-4" };
  req.preserve_array_indexes = false;
  req.projections = {
    "objects[0].name",
    "objects[0].code",
    "objects[2].name",
    "missing[0].name",
  };
  for (int i = 0; i < 15; ++i) {
    req.projections.push_back("f" + std::to_string(i));
  }

  const std::optional<couchbase::core::topology::configuration> no_config{};
  get_projected_request::encoded_request_type encoded_req{};
  assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
               "encode_to succeeds");
  assert_true(req.effective_projections.empty(), "more than 16 specs triggers full-document mode");

  const std::string full_doc = R"({
    "objects": [
      {"name": "first", "code": 10, "extra": true},
      {"name": "second", "code": 20},
      {"name": "third", "code": 30}
    ]
  })";

  auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
  const auto resp = req.make_response({}, encoded_resp);
  assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds");

  const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
  const auto* objects = projected.find("objects");
  assert_true(objects != nullptr && objects->is_array(), "objects array is projected");
  assert_eq(objects->get_array().size(),
            std::size_t{ 2 },
            "two projected array elements are compacted without splitting objects[0]");
  assert_eq(objects->get_array()[0].at("name").get_string(), "first", "objects[0].name");
  assert_eq(
    objects->get_array()[0].at("code").get_unsigned(), std::uint64_t{ 10 }, "objects[0].code");
  assert_eq(objects->get_array()[1].at("name").get_string(),
            "third",
            "objects[2].name compacted to index 1");
  assert_true(projected.find("missing") == nullptr,
              "missing array projections do not leave partial containers in the output");
}

void
negative_index_projection_does_not_overwrite_distinct_positive_index_slot(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  for (const bool preserve_indexes : { false, true }) {
    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-5-neg-only" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = { "objects[-1].name", "objects[-1].code" };

      get_projected_request::encoded_request_type encoded_req{};
      assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                   "negative-only encode_to succeeds");
      assert_false(req.effective_projections.empty(),
                   "negative-only projections stay in subdoc mode");

      auto encoded_resp = make_lookup_in_response({ "33554432", R"("last")", "20" });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()), "negative-only make_response succeeds");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& arr = projected.at("objects").get_array();
      assert_eq(arr.size(), std::size_t{ 1 }, "objects[-1] sibling fields coalesce in subdoc mode");
      assert_eq(arr[0].at("name").get_string(), "last", "objects[-1].name");
      assert_eq(arr[0].at("code").get_unsigned(), std::uint64_t{ 20 }, "objects[-1].code");
    }

    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-5-mixed-short" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = {
        "objects[0].name", "objects[-1].name", "objects[-1].code",
        "single[0].name",  "single[-1].code",
      };

      get_projected_request::encoded_request_type encoded_req{};
      assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                   "mixed-sign encode_to succeeds");
      assert_true(
        req.effective_projections.empty(),
        "mixing positive and negative indexes on the same array falls back to full-document mode");

      const std::string full_doc = R"({
        "objects": [{"name": "first"}, {"name": "last", "code": 20}],
        "single": [{"name": "only", "code": 42}]
      })";
      auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()), "mixed-sign make_response succeeds");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& arr = projected.at("objects").get_array();
      assert_eq(arr.size(),
                std::size_t{ 2 },
                "objects[0] and objects[-1] on 2-element array remain distinct");
      assert_eq(arr[0].at("name").get_string(), "first", "objects[0].name preserved");
      assert_eq(arr[1].at("name").get_string(), "last", "objects[-1].name in second slot");
      assert_eq(arr[1].at("code").get_unsigned(), std::uint64_t{ 20 }, "objects[-1].code merged");

      const auto& single = projected.at("single").get_array();
      assert_eq(single.size(),
                std::size_t{ 1 },
                "single[0] and single[-1] on 1-element array resolve to index 0 and coalesce");
      assert_eq(single[0].at("name").get_string(), "only", "single[0].name");
      assert_eq(single[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "single[-1].code");
    }

    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-5-direct-subdoc" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = { "objects[0].name", "objects[-1].name", "objects[-1].code" };
      req.effective_projections = req.projections;

      auto encoded_resp = make_lookup_in_response({ "33554432", R"("first")", R"("last")", "20" });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()), "direct subdoc make_response succeeds");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& arr = projected.at("objects").get_array();
      assert_eq(
        arr.size(), std::size_t{ 2 }, "objects[0] and objects[-1] remain distinct in subdoc mode");
      assert_eq(arr[0].at("name").get_string(), "first", "objects[0].name preserved");
      assert_eq(arr[1].at("name").get_string(), "last", "objects[-1].name in second slot");
      assert_eq(arr[1].at("code").get_unsigned(), std::uint64_t{ 20 }, "objects[-1].code merged");
    }
  }
}

void
whole_container_projection_synchronizes_cached_array_slots([[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  for (const bool preserve_indexes : { false, true }) {
    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-7-subdoc" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = { "objects[0].name", "objects[-1].code", "objects", "objects[-1].name" };
      req.effective_projections = req.projections;

      auto encoded_resp = make_lookup_in_response({
        "33554432",
        R"("only")",
        "42",
        R"([{"name":"only","code":42}])",
        R"("only")",
      });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()),
                   "overlapping subdoc projections with whole-array replacement do not throw");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& arr = projected.at("objects").get_array();
      assert_eq(arr.size(), std::size_t{ 1 }, "replaced 1-element array keeps size 1");
      assert_eq(arr[0].at("name").get_string(), "only", "objects[0].name after replacement");
      assert_eq(
        arr[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "objects[0].code after replacement");
    }

    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-7-encoded" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = { "objects[0].name", "objects[-1].code", "objects", "objects[-1].name" };

      get_projected_request::encoded_request_type encoded_req{};
      assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                   "encode_to succeeds");
      assert_true(req.effective_projections.empty(), "mixed signs fall back to full document");

      auto encoded_resp =
        make_lookup_in_response({ "33554432", R"({"objects":[{"name":"only","code":42}]})" });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& arr = projected.at("objects").get_array();
      assert_eq(arr.size(), std::size_t{ 1 }, "single element produced");
      assert_eq(arr[0].at("name").get_string(), "only", "objects[0].name");
      assert_eq(arr[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "objects[0].code");
    }

    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-7-nested" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = {
        "matrix",
        "matrix[-1]",
        "matrix[0][-1].name",
        "deep",
        "deep[-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1][-1].leaf",
      };
      req.effective_projections = req.projections;

      auto encoded_resp = make_lookup_in_response({
        "33554432",
        R"([[{"name":"first"}]])",
        R"([{"name":"first"},{"name":"second"}])",
        R"("second-updated")",
        R"([[[[[[[[[[[[[[[[[[[[{"leaf":1,"keep":2}]]]]]]]]]]]]]]]]]]]])",
        "99",
      });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()),
                   "nested container replacement and deep singleton array projection succeed");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& row = projected.at("matrix").get_array().at(0).get_array();
      assert_eq(
        row.size(), std::size_t{ 2 }, "matrix[0] has 2 elements after matrix[-1] replacement");
      assert_eq(row[0].at("name").get_string(), "first", "matrix[0][0].name preserved");
      assert_eq(row[1].at("name").get_string(),
                "second-updated",
                "matrix[0][-1].name updates canonical nested slot after matrix[-1] replacement");

      const tao::json::value* deep_cur = &projected.at("deep");
      for (int depth = 0; depth < 20; ++depth) {
        assert_true(deep_cur->is_array(), "deep level is an array");
        assert_eq(deep_cur->get_array().size(), std::size_t{ 1 }, "deep level is a singleton");
        deep_cur = &deep_cur->get_array().at(0);
      }
      assert_true(deep_cur->is_object(), "deep innermost node is an object");
      assert_eq(deep_cur->at("leaf").get_unsigned(),
                std::uint64_t{ 99 },
                "deep[-1]...[ -1 ].leaf updated to 99");
      assert_eq(deep_cur->at("keep").get_unsigned(),
                std::uint64_t{ 2 },
                "deep[-1]...[ -1 ].keep preserved");
    }

    {
      get_projected_request req{};
      req.id = { "default", "_default", "_default", "doc-7-dotted-member" };
      req.preserve_array_indexes = preserve_indexes;
      req.projections = {
        "container",
        "container.a.b[-1].v",
      };
      req.effective_projections = req.projections;

      auto encoded_resp = make_lookup_in_response({
        "33554432",
        R"({"a":{"b":[{"v":1},{"v":2}]},"a.b":[{"v":9}]})",
        "20",
      });
      const auto resp = req.make_response({}, encoded_resp);
      assert_false(static_cast<bool>(resp.ctx.ec()),
                   "dotted member sibling does not collide with nested array slot cache");

      const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
      const auto& nested_b = projected.at("container").at("a").at("b").get_array();
      assert_eq(nested_b.size(), std::size_t{ 2 }, "container.a.b keeps 2 elements");
      assert_eq(nested_b[0].at("v").get_unsigned(),
                std::uint64_t{ 1 },
                "container.a.b[0].v is not overwritten by container[\"a.b\"] slot collision");
      assert_eq(nested_b[1].at("v").get_unsigned(),
                std::uint64_t{ 20 },
                "container.a.b[-1].v updates second element");
    }
  }
}

void
compact_projection_of_high_array_index_coalesces_without_dense_allocation(
  [[maybe_unused]] context& ctx)
{
  get_projected_request req{};
  req.id = { "default", "_default", "_default", "doc-6" };
  req.preserve_array_indexes = false;
  req.projections = {
    "items[999999].sku",
    "items[999999].price",
    "huge[1000000000].sku",
    "huge[1000000000].price",
  };

  const std::optional<couchbase::core::topology::configuration> no_config{};
  get_projected_request::encoded_request_type encoded_req{};
  assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
               "encode_to succeeds");
  assert_false(req.effective_projections.empty(), "uses subdoc multi-lookup mode");

  auto encoded_resp =
    make_lookup_in_response({ "33554432", R"("SKU-999999")", "199", R"("SKU-1B")", "299" });
  const auto resp = req.make_response({}, encoded_resp);
  assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds");

  const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
  const auto& items = projected.at("items").get_array();
  assert_eq(items.size(), std::size_t{ 1 }, "single compact array slot produced");
  assert_eq(items[0].at("sku").get_string(), "SKU-999999", "sku projected");
  assert_eq(items[0].at("price").get_unsigned(), std::uint64_t{ 199 }, "price coalesced");

  const auto& huge = projected.at("huge").get_array();
  assert_eq(huge.size(), std::size_t{ 1 }, "single compact slot for index 1000000000");
  assert_eq(huge[0].at("sku").get_string(), "SKU-1B", "huge sku projected");
  assert_eq(huge[0].at("price").get_unsigned(), std::uint64_t{ 299 }, "huge price coalesced");
}

void
unfinished_array_bracket_projections_do_not_trigger_full_doc_fallback_or_partial_arrays(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  // 1. Malformed paths such as "items[0][", "items[", or "items[-0]" paired with "items[-1]" must
  // not be classified as valid positive-index projections on "items" that force full-document
  // fallback and hide the server's subdoc_path_invalid error.
  for (const auto* bad_path : { "items[0][", "items[", "items[-0]" }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-8-unfinished-bracket" };
    req.projections = { bad_path, "items[-1]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(req.effective_projections.empty(),
                 "malformed array bracket path does not trigger mixed-sign full-document fallback");

    auto server_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_invalid },
      { R"("tail")", key_value_status_code::success },
    });
    const auto resp = req.make_response({}, server_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_invalid,
              "subdoc mode surfaces path_invalid for malformed array bracket path");
  }

  // Even when valid sibling projections mix positive and negative indexes on the same array, a
  // malformed sibling path within the 16-spec subdoc limit must keep the request in subdoc mode so
  // the server's path_invalid error is not silently dropped by client-side full-document fallback.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-8-mixed-with-malformed-sibling" };
    req.projections = { "items[0]", "items[-1]", "bad[" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(
      req.effective_projections.empty(),
      "malformed sibling path prevents mixed-sign full-document fallback from hiding path_invalid");

    auto server_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { R"("head")", key_value_status_code::success },
      { R"("tail")", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_invalid },
    });
    const auto resp = req.make_response({}, server_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_invalid,
              "subdoc mode surfaces path_invalid when mixed-sign projections include bad[");
  }

  // 2. Trailing '[' paths ("malformed[" and "malformed_nested[0][") must be rejected as invalid
  // projection paths rather than leaving empty array containers in the projected document.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-8-no-partial-containers" };
    req.projections = { "malformed[", "malformed_nested[0][", "valid" };
    req.effective_projections = req.projections;

    auto encoded_resp = make_lookup_in_response({ "33554432", "1", "2", R"("ok")" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "make_response succeeds");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_eq(projected.at("valid").get_string(), "ok", "valid sibling path is projected");
    assert_true(projected.find("malformed") == nullptr,
                "trailing '[' does not materialize an empty array container");
    assert_true(projected.find("malformed_nested") == nullptr,
                "trailing '][' does not materialize a nested empty array container");
  }
}

void
full_document_and_mixed_sign_projections_report_path_mismatch_on_type_mismatch(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  // 1. Short mixed-sign array projection where the target field is a non-array JSON value must
  // propagate errc::key_value::path_mismatch instead of silently omitting the projections.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-9-non-array-field" };
    req.projections = { "valid", "items[0]", "items[-1]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign array projections trigger full-document fallback");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"({"valid":"ok","items":1})" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_mismatch,
              "indexing into a scalar field reports path_mismatch");
    assert_true(resp.value.empty(), "failed response does not return partial projected payload");
  }

  // 2. Nested member projection beneath a non-object array element reports path_mismatch.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-9-non-object-element" };
    req.projections = { "items[0].name", "items[-1].name" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign nested projections trigger full-document fallback");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"({"items":[1,2]})" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_mismatch,
              "field lookup on a scalar array element reports path_mismatch");
  }

  // 3. Non-object root JSON document reports path_mismatch when projecting object fields.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-9-array-root" };
    req.projections = { "items[0]", "items[-1]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign projections trigger full-document fallback");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"([1,2,3])" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_mismatch,
              "field lookup on a root JSON array reports path_mismatch");
  }

  // 4. Over-16 full-document projection mode also reports path_mismatch on type-incompatible steps.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-9-over-16-mismatch" };
    req.projections = { "a.b" };
    for (int i = 0; i < 16; ++i) {
      req.projections.push_back("f" + std::to_string(i));
    }

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "over-16 specs use full-document mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"({"a":42,"f0":1})" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_mismatch,
              "nested field lookup on scalar in over-16 full-document mode reports path_mismatch");
  }

  // 5. Missing fields, empty arrays, and out-of-bounds indexes remain omitted (not path_mismatch).
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-9-missing-vs-mismatch" };
    req.projections = { "missing.nested[0]", "items[0]", "items[-1]", "short[5].name" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign projections trigger full-document fallback");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"({"items":[],"short":[1]})" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()),
                 "missing fields, empty arrays, and out-of-bounds indexes succeed");
    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_eq(projected, tao::json::empty_object, "omitted paths produce an empty JSON object");
  }
}

void
quoted_or_escaped_or_overdeep_projections_stay_in_subdoc_mode([[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  for (const std::string& malformed_special_path : {
         std::string{ "`unterminated" },
         std::string{ "``" },
         std::string{ R"(bad\escape)" },
         std::string{ R"(bad"quote)" },
         std::string{ "bad\x01_ctrl" },
       }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-10-special-path" };
    req.projections = { "items[0]", "items[-1]", malformed_special_path };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(
      req.effective_projections.empty(),
      "malformed backtick, escaped, quoted, or control-char path stays in subdoc mode: " +
        malformed_special_path);

    auto server_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { R"("head")", key_value_status_code::success },
      { R"("tail")", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_invalid },
    });
    const auto resp = req.make_response({}, server_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_invalid,
              "subdoc mode surfaces path_invalid for malformed quoted/escaped path");
  }

  // Valid quoted or escaped sibling paths do NOT disable mixed-sign array alias resolution:
  // projecting objects[0].name, objects[-1].code, and `meta.key` on a 1-element array falls back
  // to full-document mode and merges the sibling fields into a single array element.
  for (const bool preserve_indexes : { false, true }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-10-mixed-sign-with-quoted-sibling" };
    req.preserve_array_indexes = preserve_indexes;
    req.projections = {
      "objects[0].name", "objects[-1].code", "`meta.key`",
      R"(\"quoted)",     "`arr.idx`[0].a",   "`arr.idx`[-1].b",
    };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "valid quoted/escaped siblings allow mixed-sign array alias resolution via "
                "full-document mode");

    const std::string full_doc = R"({
      "objects": [{"name": "only", "code": 42, "extra": true}],
      "meta.key": "mval",
      "\"quoted": "qval",
      "arr.idx": [{"a": 1, "b": 2}]
    })";
    auto server_resp = make_lookup_in_response({ "33554432", full_doc });
    const auto resp = req.make_response({}, server_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()),
                 "mixed-sign projection with valid quoted/escaped siblings succeeds");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    const auto& objects = projected.at("objects").get_array();
    assert_eq(objects.size(),
              std::size_t{ 1 },
              "objects[0] and objects[-1] on 1-element array merge into one object");
    assert_eq(objects[0].at("name").get_string(), "only", "objects[0].name preserved");
    assert_eq(objects[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "objects[-1].code merged");
    assert_eq(projected.at("meta.key").get_string(), "mval", "backtick-quoted sibling projected");
    assert_eq(projected.at("\"quoted").get_string(), "qval", "JSON-escaped sibling projected");

    const auto& quoted_arr = projected.at("arr.idx").get_array();
    assert_eq(quoted_arr.size(),
              std::size_t{ 1 },
              "mixed-sign projections on backtick-quoted array merge into one element");
    assert_eq(quoted_arr[0].at("a").get_unsigned(), std::uint64_t{ 1 }, "`arr.idx`[0].a");
    assert_eq(quoted_arr[0].at("b").get_unsigned(), std::uint64_t{ 2 }, "`arr.idx`[-1].b");
  }

  // Subjson allows at most 31 path components (Limits::MAX_COMPONENTS == 32 including root).
  // A 32-component sibling path must stay in subdoc mode so path_too_big is preserved.
  {
    std::string overdeep_path = "a0";
    for (int i = 1; i < 32; ++i) {
      overdeep_path.append(".a" + std::to_string(i));
    }

    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-10-overdeep" };
    req.projections = { "items[0]", "items[-1]", overdeep_path };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(req.effective_projections.empty(),
                 "32-component path stays in subdoc mode to preserve path_too_big");

    auto server_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { R"("head")", key_value_status_code::success },
      { R"("tail")", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_too_big },
    });
    const auto resp = req.make_response({}, server_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::path_too_big,
              "subdoc mode surfaces path_too_big for 32-component path");
  }

  // Valid backtick- and JSON-escaped paths in subdoc mode reconstruct unescaped object keys.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-10-valid-escaped-subdoc" };
    req.projections = { "`arr.idx`[0]", "`BACKTICK``HAPPY`", R"(\"quoted)", "items[]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(req.effective_projections.empty(), "escaped paths stay in subdoc mode");

    auto server_resp =
      make_lookup_in_response({ "33554432", R"("first")", R"("yes")", R"("qval")", R"("x")" });
    const auto resp = req.make_response({}, server_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "valid escaped subdoc projections succeed");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_eq(projected.at("arr.idx").get_array().at(0).get_string(),
              "first",
              "subdoc mode unescapes backtick-quoted dotted key");
    assert_eq(
      projected.at("BACKTICK`HAPPY").get_string(), "yes", "subdoc mode unescapes doubled backtick");
    assert_eq(
      projected.at("\"quoted").get_string(), "qval", "subdoc mode unescapes JSON-escaped quote");
    assert_eq(projected.at("items").get_array().at(0).get_string(),
              "x",
              "subdoc mode treats empty bracket component items[] as index 0");
  }
}

void
mixed_sign_and_full_document_projections_report_document_not_json_on_non_json_body(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  // 1. Short mixed-sign projection on a non-JSON document containing raw bytes "string" must
  // report document_not_json rather than parsing_failure.
  for (const std::string& raw_body : {
         std::string{ "string" },
         std::string{ "\xff\x00not-json", 10 },
       }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-11-non-json-mixed" };
    req.projections = { "items[0]", "items[-1]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign array projections trigger full-document fallback");

    auto encoded_resp = make_lookup_in_response({ "33554432", raw_body });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::document_not_json,
              "non-JSON document body in mixed-sign fallback reports document_not_json");
    assert_true(resp.value.empty(), "failed response has empty value");
  }

  // 2. Empty 0-byte document body causes whole-doc lookup ("") to return subdoc_path_not_found,
  // whereas subjson lookup reports document_not_json because an empty body is not JSON.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-11-empty-body-mixed" };
    req.projections = { "items[0]", "items[-1]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign array projections trigger full-document fallback");

    auto empty_doc_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_not_found },
    });
    const auto resp = req.make_response({}, empty_doc_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::document_not_json,
              "0-byte non-JSON document in projected get reports document_not_json");
  }

  // Expiry-only whole-document get (projections.empty(), with_expiry = true) on an existing
  // 0-byte document receives subdoc_path_not_found on "" and must succeed with empty value and
  // populated expiry rather than returning path_not_found or document_not_json.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-11-empty-body-expiry-only" };
    req.with_expiry = true;
    req.projections = {};

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to expiry-only succeeds");
    assert_true(req.effective_projections.empty(), "expiry-only uses whole-document lookup");

    auto empty_doc_expiry_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { "1700000000", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_not_found },
    });
    const auto resp = req.make_response({}, empty_doc_expiry_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "expiry-only get on 0-byte document succeeds");
    assert_eq(resp.flags, std::uint32_t{ 33554432 }, "0-byte document flags preserved");
    assert_true(resp.expiry.has_value(), "0-byte document expiry populated");
    assert_eq(resp.expiry.value_or(0), std::uint32_t{ 1700000000 }, "0-byte document expiry value");
    assert_true(resp.value.empty(), "0-byte document value is empty");
  }

  // 3. Over-16 full-document projection on raw non-JSON bytes also reports document_not_json.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-11-over-16-non-json" };
    for (int i = 0; i < 17; ++i) {
      req.projections.push_back("f" + std::to_string(i));
    }

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "over-16 specs use full-document mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", "string" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::key_value::document_not_json,
              "non-JSON document body in over-16 full-document mode reports document_not_json");
  }

  // 4. Corrupt non-JSON field payload in subdoc multi-lookup mode still reports parsing_failure.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-11-corrupt-subdoc-field" };
    req.projections = { "items[0]" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(req.effective_projections.empty(), "single projection stays in subdoc mode");

    auto corrupt_subdoc_resp = make_lookup_in_response({ "33554432", "not-valid-json-fragment" });
    const auto resp = req.make_response({}, corrupt_subdoc_resp);
    assert_eq(resp.ctx.ec(),
              couchbase::errc::common::parsing_failure,
              "corrupt subdoc field value in multi-lookup mode reports parsing_failure");
  }
}

void
root_array_projections_reconstruct_array_in_subdoc_and_full_document_modes(
  [[maybe_unused]] context& ctx)
{
  const std::optional<couchbase::core::topology::configuration> no_config{};

  // 1. Sub-document multi-lookup mode (<= 16 specs) on a root JSON array with both compact and
  // index-preserving array reconstruction.
  for (const bool preserve_indexes : { false, true }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-12-root-array-subdoc" };
    req.preserve_array_indexes = preserve_indexes;
    req.projections = { "[1].name", "[1].code" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_false(req.effective_projections.empty(), "two root-array specs stay in subdoc mode");

    auto encoded_resp = make_lookup_in_response({ "33554432", R"("second")", "20" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "root-array subdoc projection succeeds");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_true(projected.is_array(), "projected root document is a JSON array");
    const auto& arr = projected.get_array();
    if (preserve_indexes) {
      assert_eq(arr.size(), std::size_t{ 2 }, "preserve_array_indexes pads index 0 with null");
      assert_true(arr[0].is_null(), "unprojected leading root-array element is null");
      assert_eq(arr[1].at("name").get_string(), "second", "[1].name projected");
      assert_eq(arr[1].at("code").get_unsigned(), std::uint64_t{ 20 }, "[1].code merged");
    } else {
      assert_eq(arr.size(), std::size_t{ 1 }, "compact mode coalesces [1].name and [1].code");
      assert_eq(arr[0].at("name").get_string(), "second", "[1].name projected");
      assert_eq(arr[0].at("code").get_unsigned(), std::uint64_t{ 20 }, "[1].code merged");
    }
  }

  // 2. Full-document mode (> 16 specs) on a root JSON array with nested multidimensional elements.
  for (const bool preserve_indexes : { false, true }) {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-12-root-array-fulldoc" };
    req.preserve_array_indexes = preserve_indexes;
    for (int i = 0; i < 15; ++i) {
      req.projections.push_back("[1].f" + std::to_string(i));
    }
    req.projections.emplace_back("[1].matrix[1][0]");
    req.projections.emplace_back("[1].matrix[1][1]");

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(), "17 root-array specs use full-document mode");

    const tao::json::value elem0 = { { "unused", true } };
    tao::json::value elem1 = {
      { "matrix",
        tao::json::value::array({
          tao::json::value::array({ 1, 2 }),
          tao::json::value::array({ 30, 40 }),
        }) },
    };
    for (int i = 0; i < 15; ++i) {
      elem1["f" + std::to_string(i)] = i;
    }
    const auto full_doc =
      couchbase::core::utils::json::generate(tao::json::value::array({ elem0, elem1 }));
    auto encoded_resp = make_lookup_in_response({ "33554432", full_doc });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "root-array full-document projection succeeds");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_true(projected.is_array(), "full-document projected root is a JSON array");
    const auto& arr = projected.get_array();
    const std::size_t target_idx = preserve_indexes ? 1U : 0U;
    assert_eq(arr.size(), preserve_indexes ? std::size_t{ 2 } : std::size_t{ 1 }, "root size");
    if (preserve_indexes) {
      assert_true(arr[0].is_null(), "leading root element padded with null");
    }
    for (int i = 0; i < 15; ++i) {
      assert_eq(arr[target_idx].at("f" + std::to_string(i)).get_unsigned(),
                static_cast<std::uint64_t>(i),
                "projected field on root-array element");
    }
    const auto& matrix = arr[target_idx].at("matrix").get_array();
    const std::size_t row_idx = preserve_indexes ? 1U : 0U;
    assert_eq(
      matrix[row_idx].get_array().at(0).get_unsigned(), std::uint64_t{ 30 }, "matrix[1][0]");
    assert_eq(
      matrix[row_idx].get_array().at(1).get_unsigned(), std::uint64_t{ 40 }, "matrix[1][1]");
  }

  // 3. Mixed-sign root-array projections ([0].name and [-1].code) on a 1-element root array fall
  // back to full-document mode and merge into a single element.
  {
    get_projected_request req{};
    req.id = { "default", "_default", "_default", "doc-12-root-array-mixed-sign" };
    req.projections = { "[0].name", "[-1].code" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(req.effective_projections.empty(),
                "mixed-sign root-array projections trigger full-document fallback");

    auto encoded_resp =
      make_lookup_in_response({ "33554432", R"([{"name":"only","code":42,"extra":false}])" });
    const auto resp = req.make_response({}, encoded_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "mixed-sign root-array projection succeeds");

    const auto projected = couchbase::core::utils::json::parse_binary(resp.value);
    assert_true(projected.is_array(), "mixed-sign projected root is a JSON array");
    assert_eq(projected.get_array().size(), std::size_t{ 1 }, "single root element merged");
    assert_eq(projected.get_array()[0].at("name").get_string(), "only", "[0].name");
    assert_eq(projected.get_array()[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "[-1].code");
  }

  // 4. Out-of-bounds root-array projections return an empty JSON array [] in both subdoc and
  // full-document modes, whereas root-type mismatches still report path_mismatch.
  {
    get_projected_request subdoc_req{};
    subdoc_req.id = { "default", "_default", "_default", "doc-12-root-array-missing-subdoc" };
    subdoc_req.projections = { "[5].name" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(subdoc_req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    auto missing_subdoc_resp = make_lookup_in_response_with_statuses({
      { "33554432", key_value_status_code::success },
      { "", key_value_status_code::subdoc_path_not_found },
    });
    const auto resp = subdoc_req.make_response({}, missing_subdoc_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "missing root-array element succeeds");
    assert_eq(couchbase::core::utils::json::parse_binary(resp.value),
              tao::json::empty_array,
              "missing root-array element in subdoc mode returns empty array");
  }
  {
    get_projected_request fulldoc_req{};
    fulldoc_req.id = { "default", "_default", "_default", "doc-12-root-array-missing-fulldoc" };
    fulldoc_req.projections = { "[5].name", "[-1].missing" };

    get_projected_request::encoded_request_type encoded_req{};
    assert_false(static_cast<bool>(fulldoc_req.encode_to(encoded_req, { no_config, {} })),
                 "encode_to succeeds");
    assert_true(fulldoc_req.effective_projections.empty(), "mixed-sign uses full-document mode");

    auto missing_fulldoc_resp = make_lookup_in_response({ "33554432", R"([{"name":"only"}])" });
    const auto resp = fulldoc_req.make_response({}, missing_fulldoc_resp);
    assert_false(static_cast<bool>(resp.ctx.ec()), "missing root-array paths in full-doc succeed");
    assert_eq(couchbase::core::utils::json::parse_binary(resp.value),
              tao::json::empty_array,
              "missing root-array paths in full-document mode return empty array");

    auto mismatch_resp = make_lookup_in_response({ "33554432", R"({"name":"object_not_array"})" });
    const auto mismatch = fulldoc_req.make_response({}, mismatch_resp);
    assert_eq(mismatch.ctx.ec(),
              couchbase::errc::key_value::path_mismatch,
              "root-array projection on root JSON object reports path_mismatch");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(full_document_projection_resolves_array_elements_by_index_and_nested_fields) },
      { CASE(full_document_projection_handles_empty_arrays_and_invalid_paths_without_crashing) },
      { CASE(projected_get_response_with_missing_subdoc_fields_reports_parsing_failure) },
      { CASE(
        nested_array_projections_merge_into_same_element_when_preserve_array_indexes_is_false) },
      { CASE(negative_index_projection_does_not_overwrite_distinct_positive_index_slot) },
      { CASE(whole_container_projection_synchronizes_cached_array_slots) },
      { CASE(compact_projection_of_high_array_index_coalesces_without_dense_allocation) },
      { CASE(
        unfinished_array_bracket_projections_do_not_trigger_full_doc_fallback_or_partial_arrays) },
      { CASE(full_document_and_mixed_sign_projections_report_path_mismatch_on_type_mismatch) },
      { CASE(quoted_or_escaped_or_overdeep_projections_stay_in_subdoc_mode) },
      { CASE(mixed_sign_and_full_document_projections_report_document_not_json_on_non_json_body) },
      { CASE(root_array_projections_reconstruct_array_in_subdoc_and_full_document_modes) },
    },
  };
}

} // namespace couchbase::test
