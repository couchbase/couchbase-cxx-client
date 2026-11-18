/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2024 Couchbase, Inc.
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

#include "core/impl/error.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_mutate_in.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/json.hxx"

#include <couchbase/mutate_in_specs.hxx>

#include <couchbase/store_semantics.hxx>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

namespace couchbase::test
{
namespace
{
using body = couchbase::core::protocol::mutate_in_request_body;

// extras() encodes the doc flags as its last byte, and omits the byte entirely when no flag is
// set. So the encoding is what these cases read: a flag that never reaches the wire is a flag the
// server never sees, whatever the body's own state says.
auto
encoded_flags(body& req) -> std::uint8_t
{
  const auto& extras = req.extras();
  assert_false(extras.empty(), "a set flag reaches the encoded extras");
  return static_cast<std::uint8_t>(extras.back());
}

void
doc_flag_constants_are_byte_wide([[maybe_unused]] context& ctx)
{
  assert_true(std::is_same_v<decltype(body::doc_flag_mkdoc), const std::uint8_t>, "doc_flag_mkdoc");
  assert_true(std::is_same_v<decltype(body::doc_flag_add), const std::uint8_t>, "doc_flag_add");
  assert_true(std::is_same_v<decltype(body::doc_flag_access_deleted), const std::uint8_t>,
              "doc_flag_access_deleted");
  assert_true(std::is_same_v<decltype(body::doc_flag_create_as_deleted), const std::uint8_t>,
              "doc_flag_create_as_deleted");
  assert_true(std::is_same_v<decltype(body::doc_flag_revive_document), const std::uint8_t>,
              "doc_flag_revive_document");
}

void
doc_flag_constants_have_the_expected_bit_values([[maybe_unused]] context& ctx)
{
  assert_eq(body::doc_flag_mkdoc, std::uint8_t{ 0b0000'0001U }, "doc_flag_mkdoc");
  assert_eq(body::doc_flag_add, std::uint8_t{ 0b0000'0010U }, "doc_flag_add");
  assert_eq(body::doc_flag_access_deleted, std::uint8_t{ 0b0000'0100U }, "doc_flag_access_deleted");
  assert_eq(
    body::doc_flag_create_as_deleted, std::uint8_t{ 0b0000'1000U }, "doc_flag_create_as_deleted");
  assert_eq(
    body::doc_flag_revive_document, std::uint8_t{ 0b0001'0000U }, "doc_flag_revive_document");
}

void
doc_flag_constants_occupy_distinct_bits([[maybe_unused]] context& ctx)
{
  assert_eq(body::doc_flag_mkdoc & body::doc_flag_add, 0, "mkdoc and add");
  assert_eq(body::doc_flag_mkdoc & body::doc_flag_access_deleted, 0, "mkdoc and access_deleted");
  assert_eq(
    body::doc_flag_mkdoc & body::doc_flag_create_as_deleted, 0, "mkdoc and create_as_deleted");
  assert_eq(body::doc_flag_mkdoc & body::doc_flag_revive_document, 0, "mkdoc and revive_document");
  assert_eq(body::doc_flag_add & body::doc_flag_access_deleted, 0, "add and access_deleted");
  assert_eq(body::doc_flag_add & body::doc_flag_create_as_deleted, 0, "add and create_as_deleted");
  assert_eq(body::doc_flag_add & body::doc_flag_revive_document, 0, "add and revive_document");
  assert_eq(body::doc_flag_access_deleted & body::doc_flag_create_as_deleted,
            0,
            "access_deleted and create_as_deleted");
  assert_eq(body::doc_flag_access_deleted & body::doc_flag_revive_document,
            0,
            "access_deleted and revive_document");
  assert_eq(body::doc_flag_create_as_deleted & body::doc_flag_revive_document,
            0,
            "create_as_deleted and revive_document");
}

void
replace_semantics_clear_the_store_semantics_bits([[maybe_unused]] context& ctx)
{
  body req{};
  req.store_semantics(couchbase::store_semantics::upsert);
  req.store_semantics(couchbase::store_semantics::replace);

  // With no expiry, no user flags and no doc flag left set, extras carries nothing at all.
  assert_true(req.extras().empty(), "replace leaves no doc flag to encode");
}

void
upsert_semantics_set_the_mkdoc_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.store_semantics(couchbase::store_semantics::upsert);

  const auto flags = encoded_flags(req);
  assert_ne(flags & body::doc_flag_mkdoc, 0, "upsert sets mkdoc");
  assert_eq(flags & body::doc_flag_add, 0, "upsert does not set add");
}

void
insert_semantics_set_the_add_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.store_semantics(couchbase::store_semantics::insert);

  const auto flags = encoded_flags(req);
  assert_ne(flags & body::doc_flag_add, 0, "insert sets add");
  assert_eq(flags & body::doc_flag_mkdoc, 0, "insert does not set mkdoc");
}

void
switching_semantics_leaves_the_other_flags_alone([[maybe_unused]] context& ctx)
{
  body req{};
  req.store_semantics(couchbase::store_semantics::upsert);
  req.access_deleted(true);
  req.store_semantics(couchbase::store_semantics::insert);

  const auto flags = encoded_flags(req);
  assert_ne(flags & body::doc_flag_add, 0, "insert sets add");
  assert_eq(flags & body::doc_flag_mkdoc, 0, "the earlier upsert's mkdoc is cleared");
  assert_ne(flags & body::doc_flag_access_deleted, 0, "access_deleted survives the switch");
}

void
enabling_access_deleted_sets_its_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.access_deleted(true);

  assert_ne(encoded_flags(req) & body::doc_flag_access_deleted, 0, "access_deleted is set");
}

void
disabling_access_deleted_clears_its_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.access_deleted(true);
  req.access_deleted(false);

  assert_true(req.extras().empty(), "no doc flag is left to encode");
}

void
toggling_access_deleted_leaves_the_other_flags_alone([[maybe_unused]] context& ctx)
{
  body req{};
  req.create_as_deleted(true);
  req.access_deleted(true);
  req.access_deleted(false);

  const auto flags = encoded_flags(req);
  assert_eq(flags & body::doc_flag_access_deleted, 0, "access_deleted is cleared");
  assert_ne(flags & body::doc_flag_create_as_deleted, 0, "create_as_deleted survives");
}

void
enabling_create_as_deleted_sets_its_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.create_as_deleted(true);

  assert_ne(encoded_flags(req) & body::doc_flag_create_as_deleted, 0, "create_as_deleted is set");
}

void
disabling_create_as_deleted_leaves_the_other_flags_alone([[maybe_unused]] context& ctx)
{
  body req{};
  req.access_deleted(true);
  req.create_as_deleted(true);
  req.create_as_deleted(false);

  const auto flags = encoded_flags(req);
  assert_eq(flags & body::doc_flag_create_as_deleted, 0, "create_as_deleted is cleared");
  assert_ne(flags & body::doc_flag_access_deleted, 0, "access_deleted survives");
}

void
enabling_revive_document_sets_its_bit([[maybe_unused]] context& ctx)
{
  body req{};
  req.revive_document(true);

  assert_ne(encoded_flags(req) & body::doc_flag_revive_document, 0, "revive_document is set");
}

void
disabling_revive_document_leaves_the_other_flags_alone([[maybe_unused]] context& ctx)
{
  body req{};
  req.access_deleted(true);
  req.revive_document(true);
  req.revive_document(false);

  const auto flags = encoded_flags(req);
  assert_eq(flags & body::doc_flag_revive_document, 0, "revive_document is cleared");
  assert_ne(flags & body::doc_flag_access_deleted, 0, "access_deleted survives");
}

using couchbase::core::key_value_status_code;
using couchbase::core::operations::mutate_in_request;
using couchbase::core::operations::mutate_in_response;
using couchbase::core::protocol::client_response;
using couchbase::core::protocol::mutate_in_response_body;

// Encodes the specs, which moves XATTR specs first, and decodes a multi-path failure whose one
// entry fails the spec at wire_index.
auto
fail_at_wire_index(couchbase::mutate_in_specs specs, std::uint8_t wire_index) -> mutate_in_response
{
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.specs = specs.specs();
  mutate_in_request::encoded_request_type encoded{};
  assert_success(req.encode_to(encoded, { std::nullopt, {} }), "the request is encoded");

  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(mutate_in_response_body::opcode);
  msg.header.specific = couchbase::core::utils::host_to_network(
    static_cast<std::uint16_t>(key_value_status_code::subdoc_multi_path_failure));
  const auto status = static_cast<std::uint16_t>(key_value_status_code::subdoc_path_not_found);
  // index, then the status; a failed entry carries no value length
  msg.body = { std::byte{ wire_index },
               static_cast<std::byte>(status >> 8U),
               static_cast<std::byte>(status & 0xffU) };
  msg.header.bodylen =
    couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(msg.body.size()));
  return req.make_response({}, client_response<mutate_in_response_body>(std::move(msg)));
}

auto
body_then_xattr() -> couchbase::mutate_in_specs
{
  return couchbase::mutate_in_specs{
    couchbase::mutate_in_specs::upsert("body_path", 1),
    couchbase::mutate_in_specs::upsert("xattr_path", 2).xattr(),
  };
}

void
a_failed_xattr_spec_after_a_body_spec_reports_its_caller_index([[maybe_unused]] context& ctx)
{
  const auto resp = fail_at_wire_index(body_then_xattr(), 0);

  assert_error(resp.ctx.ec(), couchbase::errc::key_value::path_not_found, "the spec fails");
  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 1 }, "first_error_index");
  assert_eq(
    resp.ctx.first_error_path(), std::optional<std::string>{ "xattr_path" }, "first_error_path");
}

void
a_failed_body_spec_before_an_xattr_spec_reports_its_caller_index([[maybe_unused]] context& ctx)
{
  const auto resp = fail_at_wire_index(body_then_xattr(), 1);

  assert_error(resp.ctx.ec(), couchbase::errc::key_value::path_not_found, "the spec fails");
  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 0 }, "first_error_index");
  assert_eq(
    resp.ctx.first_error_path(), std::optional<std::string>{ "body_path" }, "first_error_path");
}

void
the_public_error_context_reports_the_caller_index([[maybe_unused]] context& ctx)
{
  const auto resp = fail_at_wire_index(body_then_xattr(), 0);
  const auto error = couchbase::core::impl::make_error(resp.ctx);
  const std::string text = error.ctx().to_json();
  const auto json = couchbase::core::utils::json::parse(text);

  assert_eq(json.at("first_error_index").as<std::size_t>(), std::size_t{ 1 }, "first_error_index");
  assert_eq(
    json.at("first_error_path").get_string(), std::string{ "xattr_path" }, "first_error_path");
}

void
specs_already_in_encoded_order_keep_their_index([[maybe_unused]] context& ctx)
{
  const auto resp = fail_at_wire_index(
    couchbase::mutate_in_specs{
      couchbase::mutate_in_specs::upsert("xattr_path", 2).xattr(),
      couchbase::mutate_in_specs::upsert("body_path", 1),
    },
    1);

  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 1 }, "first_error_index");
  assert_eq(
    resp.ctx.first_error_path(), std::optional<std::string>{ "body_path" }, "first_error_path");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(doc_flag_constants_are_byte_wide), {}, timeout::instant },
      { CASE(doc_flag_constants_have_the_expected_bit_values), {}, timeout::instant },
      { CASE(doc_flag_constants_occupy_distinct_bits), {}, timeout::instant },
      { CASE(replace_semantics_clear_the_store_semantics_bits), {}, timeout::instant },
      { CASE(upsert_semantics_set_the_mkdoc_bit), {}, timeout::instant },
      { CASE(insert_semantics_set_the_add_bit), {}, timeout::instant },
      { CASE(switching_semantics_leaves_the_other_flags_alone), {}, timeout::instant },
      { CASE(enabling_access_deleted_sets_its_bit), {}, timeout::instant },
      { CASE(disabling_access_deleted_clears_its_bit), {}, timeout::instant },
      { CASE(toggling_access_deleted_leaves_the_other_flags_alone), {}, timeout::instant },
      { CASE(enabling_create_as_deleted_sets_its_bit), {}, timeout::instant },
      { CASE(disabling_create_as_deleted_leaves_the_other_flags_alone), {}, timeout::instant },
      { CASE(enabling_revive_document_sets_its_bit), {}, timeout::instant },
      { CASE(disabling_revive_document_leaves_the_other_flags_alone), {}, timeout::instant },
      { CASE(a_failed_xattr_spec_after_a_body_spec_reports_its_caller_index),
        {},
        timeout::instant },
      { CASE(a_failed_body_spec_before_an_xattr_spec_reports_its_caller_index),
        {},
        timeout::instant },
      { CASE(the_public_error_context_reports_the_caller_index), {}, timeout::instant },
      { CASE(specs_already_in_encoded_order_keep_their_index), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
