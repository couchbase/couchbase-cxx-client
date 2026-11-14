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

#include "utils/integration_test_guard.hxx"
#include "utils/test_data.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json/value.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
void
projected_get_over_sixteen_paths_resolves_array_elements_from_full_document(
  [[maybe_unused]] context& ctx)
{
  const ::test::utils::integration_test_guard integration;

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  const auto id = ::test::utils::uniq_id("projected_get_array");
  tao::json::value doc = {
    { "items", tao::json::value::array({ "alpha", "beta", "gamma" }) },
    { "objects",
      tao::json::value::array({
        tao::json::value{ { "name", "zero" }, { "extra", 1 } },
        tao::json::value{ { "name", "one" }, { "extra", 2 } },
      }) },
    { "empty_arr", tao::json::empty_array },
  };
  for (std::uint8_t i = 0; i < 16; ++i) {
    doc["field_" + std::to_string(i)] = i;
  }

  {
    auto [err, upsert_res] = collection.insert(id, doc, {}).get();
    assert_success(err, "seed document with arrays and scalar fields");
    assert_ne(upsert_res.cas().value(), std::uint64_t{ 0 }, "insert populates a non-zero CAS");
  }

  // More than 16 projected paths switches get_projected_request to a full-document subdoc fetch
  // with client-side JSON projection.
  std::vector<std::string> paths{ "items[0]", "objects[0].name", "empty_arr[-1]" };
  for (std::uint8_t i = 0; i < 16; ++i) {
    paths.push_back("field_" + std::to_string(i));
  }

  auto [err, resp] = collection.get(id, couchbase::get_options{}.project(paths)).get();
  assert_success(err, "projected get succeeds");
  assert_ne(resp.cas().value(), std::uint64_t{ 0 }, "projected get returns a non-zero CAS");

  const auto content = resp.content_as<tao::json::value>();
  assert_true(content.find("items") != nullptr, "projected document includes items array");
  assert_eq(content.at("items").get_array().size(),
            std::size_t{ 1 },
            "items array contains only the projected element");
  assert_eq(content.at("items").get_array().at(0).get_string(),
            std::string{ "alpha" },
            "items[0] matches first source element");
  assert_true(content.find("objects") != nullptr, "projected document includes objects array");
  assert_eq(content.at("objects").get_array().size(),
            std::size_t{ 1 },
            "objects array contains only the projected element");
  assert_eq(content.at("objects").get_array().at(0).at("name").get_string(),
            std::string{ "zero" },
            "objects[0].name matches nested field of first source element");
  assert_true(content.at("objects").get_array().at(0).find("extra") == nullptr,
              "unprojected sibling field objects[0].extra is omitted");
  assert_true(content.find("empty_arr") == nullptr,
              "out-of-bounds negative index on empty array is omitted");
  for (std::uint8_t i = 0; i < 16; ++i) {
    assert_eq(content.at("field_" + std::to_string(i)).get_unsigned(),
              static_cast<std::uint64_t>(i),
              "projected scalar field is preserved");
  }
}

void
projected_get_preserves_mixed_sign_quoted_merges_and_subdoc_error_parity(
  [[maybe_unused]] context& ctx)
{
  const ::test::utils::integration_test_guard integration;

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  const auto id = ::test::utils::uniq_id("projected_get_parity");
  tao::json::value doc = {
    { "objects",
      tao::json::value::array({
        tao::json::value{ { "name", "only" }, { "code", 42 }, { "extra", true } },
      }) },
    { "meta.key", "mval" },
  };
  for (std::uint8_t i = 0; i < 16; ++i) {
    doc["f" + std::to_string(i)] = i;
  }

  {
    auto [err, upsert_res] = collection.insert(id, doc, {}).get();
    assert_success(err, "seed single-element array document");
    assert_ne(upsert_res.cas().value(), std::uint64_t{ 0 }, "insert populates CAS");
  }

  // 1. Mixed-sign projections (including subjson empty-bracket index-0 syntax objects[].name)
  // alongside a valid backtick-quoted sibling merge into one object, and subdoc mode also
  // preserves empty-bracket components across both <= 16 and > 16 projection counts.
  for (const int extra_specs : { 0, 16 }) {
    std::vector<std::string> empty_bracket_paths{ "objects[].name" };
    for (int i = 0; i < extra_specs; ++i) {
      empty_bracket_paths.push_back("f" + std::to_string(i));
    }
    auto [err, resp] =
      collection.get(id, couchbase::get_options{}.project(empty_bracket_paths)).get();
    assert_success(err, "empty-bracket index-0 projection succeeds in subdoc and full-doc modes");
    const auto content = resp.content_as<tao::json::value>();
    assert_eq(content.at("objects").get_array().at(0).at("name").get_string(),
              std::string{ "only" },
              "objects[].name selects element 0 in both subdoc and full-doc modes");
  }
  {
    auto [err, resp] = collection
                         .get(id,
                              couchbase::get_options{}.project(
                                { "objects[].name", "objects[-1].code", "`meta.key`" }))
                         .get();
    assert_success(err, "mixed-sign projected get with quoted sibling succeeds");
    const auto content = resp.content_as<tao::json::value>();
    const auto& objects = content.at("objects").get_array();
    assert_eq(objects.size(), std::size_t{ 1 }, "single source element merges into one slot");
    assert_eq(objects[0].at("name").get_string(), std::string{ "only" }, "objects[0].name");
    assert_eq(objects[0].at("code").get_unsigned(), std::uint64_t{ 42 }, "objects[-1].code");
    assert_eq(content.at("meta.key").get_string(), std::string{ "mval" }, "`meta.key` projected");
  }

  // 2. Error code parity between <= 16 subdoc mode and > 16 full-document mode:
  //    - > 1024-byte path -> errc::common::invalid_argument (kv_engine SUBDOC_PATH_MAX_LENGTH)
  //    - > 31-component path -> errc::key_value::path_too_big
  //    - malformed syntax -> errc::key_value::path_invalid
  const std::string overlong_path(1025, 'a');
  std::string overdeep_path = "a0";
  for (int i = 1; i < 32; ++i) {
    overdeep_path.append(".a" + std::to_string(i));
  }

  for (const int extra_specs : { 0, 16 }) {
    auto with_extra = [&](std::string target) {
      std::vector<std::string> paths{ std::move(target) };
      for (int i = 0; i < extra_specs; ++i) {
        paths.push_back("f" + std::to_string(i));
      }
      return paths;
    };

    {
      auto [err, _] =
        collection.get(id, couchbase::get_options{}.project(with_extra(overlong_path))).get();
      assert_eq(err.ec(),
                couchbase::errc::common::invalid_argument,
                "1025-byte path reports invalid_argument in both subdoc and full-doc modes");
    }
    {
      auto [err, _] =
        collection.get(id, couchbase::get_options{}.project(with_extra(overdeep_path))).get();
      assert_eq(err.ec(),
                couchbase::errc::key_value::path_too_big,
                "32-component path reports path_too_big in both subdoc and full-doc modes");
    }
    {
      auto [err, _] =
        collection.get(id, couchbase::get_options{}.project(with_extra("objects[notanumber]")))
          .get();
      assert_eq(err.ec(),
                couchbase::errc::key_value::path_invalid,
                "malformed path reports path_invalid in both subdoc and full-doc modes");
    }
  }

  // 3. Root-array documents reconstruct a JSON array in subdoc mode, > 16 full-document mode,
  // and mixed-sign mode, and report path_mismatch on root container type mismatches.
  const auto root_arr_id = ::test::utils::uniq_id("projected_get_root_array");
  const tao::json::value elem0 = { { "name", "first" }, { "code", 10 }, { "extra", true } };
  tao::json::value elem1 = { { "name", "second" }, { "code", 20 }, { "extra", false } };
  for (std::uint8_t i = 0; i < 16; ++i) {
    elem1["f" + std::to_string(i)] = i;
  }
  {
    auto [err, _] =
      collection.insert(root_arr_id, tao::json::value::array({ elem0, elem1 }), {}).get();
    assert_success(err, "seed root-array document");
  }
  {
    auto [err, resp] =
      collection.get(root_arr_id, couchbase::get_options{}.project({ "[1].name", "[1].code" }))
        .get();
    assert_success(err, "root-array subdoc projected get succeeds");
    const auto content = resp.content_as<tao::json::value>();
    assert_true(content.is_array(), "subdoc projected root is a JSON array");
    assert_eq(content.get_array().size(), std::size_t{ 1 }, "single projected element");
    assert_eq(content.get_array()[0].at("name").get_string(), std::string{ "second" }, "[1].name");
    assert_eq(content.get_array()[0].at("code").get_unsigned(), std::uint64_t{ 20 }, "[1].code");
  }
  {
    std::vector<std::string> many_root_paths{ "[1].name", "[1].code" };
    for (std::uint8_t i = 0; i < 16; ++i) {
      many_root_paths.push_back("[1].f" + std::to_string(i));
    }
    auto [err, resp] =
      collection.get(root_arr_id, couchbase::get_options{}.project(many_root_paths)).get();
    assert_success(err, "root-array full-document projected get succeeds");
    const auto content = resp.content_as<tao::json::value>();
    assert_true(content.is_array(), "full-document projected root is a JSON array");
    assert_eq(content.get_array().size(), std::size_t{ 1 }, "single projected element");
    assert_eq(content.get_array()[0].at("name").get_string(), std::string{ "second" }, "[1].name");
    assert_eq(content.get_array()[0].at("f15").get_unsigned(), std::uint64_t{ 15 }, "[1].f15");
  }
  {
    auto [err, resp] =
      collection.get(root_arr_id, couchbase::get_options{}.project({ "[5].name" })).get();
    assert_success(err, "missing root-array element succeeds");
    assert_eq(resp.content_as<tao::json::value>(),
              tao::json::empty_array,
              "missing root-array element returns empty array");
  }
  {
    auto [err, _] =
      collection.get(root_arr_id, couchbase::get_options{}.project({ "not_an_element" })).get();
    assert_eq(err.ec(),
              couchbase::errc::key_value::path_mismatch,
              "field projection on root-array document reports path_mismatch");
  }
  {
    auto [err, _] = collection.get(id, couchbase::get_options{}.project({ "[0].name" })).get();
    assert_eq(err.ec(),
              couchbase::errc::key_value::path_mismatch,
              "root-array projection on root-object document reports path_mismatch");
  }
}
} // namespace

auto
tests() -> test_suite
{
  const std::vector<requirement_ptr> cluster{ needs::real_cluster() };
  return {
    suite_name,
    {
      { CASE(projected_get_over_sixteen_paths_resolves_array_elements_from_full_document),
        cluster,
        timeout::integration },
      { CASE(projected_get_preserves_mixed_sign_quoted_merges_and_subdoc_error_parity),
        cluster,
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
