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
#include "core/impl/lookup_in_replica.hxx"
#include "core/impl/subdoc/path_flags.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_context.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/operations/document_mutate_in.hxx"
#include "core/operations/document_replace.hxx"
#include "core/operations/document_upsert.hxx"
#include "core/protocol/client_response.hxx"
#include "core/protocol/cmd_lookup_in.hxx"
#include "core/protocol/cmd_lookup_in_replica.hxx"
#include "core/protocol/cmd_mutate_in.hxx"
#include "core/protocol/datatype.hxx"
#include "core/protocol/magic.hxx"
#include "core/utils/byteswap.hxx"
#include "core/utils/json.hxx"

#include <couchbase/durability_level.hxx>
#include <couchbase/lookup_in_specs.hxx>
#include <couchbase/mutate_in_specs.hxx>

#include <couchbase/store_semantics.hxx>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

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
// entry fails the spec at wire_index. encodes is the number of dispatches.
auto
fail_at_wire_index(couchbase::mutate_in_specs specs, std::uint8_t wire_index, int encodes = 1)
  -> mutate_in_response
{
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.specs = specs.specs();
  mutate_in_request::encoded_request_type encoded{};
  for (int i = 0; i < encodes; ++i) {
    assert_success(req.encode_to(encoded, { std::nullopt, {} }), "the request is encoded");
  }

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

// A multi-lookup or multi-mutation response with the given outer status and entries, in wire order.
template<typename Body>
auto
subdoc_response(key_value_status_code status, std::vector<std::byte> body) -> client_response<Body>
{
  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(Body::opcode);
  msg.header.specific = couchbase::core::utils::host_to_network(static_cast<std::uint16_t>(status));
  msg.header.bodylen =
    couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(body.size()));
  msg.body = std::move(body);
  return client_response<Body>(std::move(msg));
}

// Appends one entry: the status, a 4-byte length and the value. A mutation entry starts with its
// index.
void
append_entry(std::vector<std::byte>& body,
             key_value_status_code status,
             const std::string& value,
             std::optional<std::uint8_t> index = {})
{
  if (index) {
    body.push_back(std::byte{ *index });
  }
  const auto code = static_cast<std::uint16_t>(status);
  body.push_back(static_cast<std::byte>(code >> 8U));
  body.push_back(static_cast<std::byte>(code & 0xffU));
  const auto size = static_cast<std::uint32_t>(value.size());
  body.push_back(static_cast<std::byte>(size >> 24U));
  body.push_back(static_cast<std::byte>(size >> 16U));
  body.push_back(static_cast<std::byte>(size >> 8U));
  body.push_back(static_cast<std::byte>(size & 0xffU));
  for (const char c : value) {
    body.push_back(static_cast<std::byte>(c));
  }
}

auto
value_of(const std::vector<std::byte>& value) -> std::string
{
  std::string text{};
  for (const auto b : value) {
    text.push_back(static_cast<char>(b));
  }
  return text;
}

// A retry dispatches the same request again, and every dispatch calls encode_to().
template<typename Request>
void
encode_twice(Request& req)
{
  typename Request::encoded_request_type encoded{};
  assert_success(req.encode_to(encoded, { std::nullopt, {} }), "the first dispatch");
  assert_success(req.encode_to(encoded, { std::nullopt, {} }), "the retry");
}

void
a_mutate_in_encoded_again_keeps_the_caller_index([[maybe_unused]] context& ctx)
{
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.specs =
    couchbase::mutate_in_specs{
      couchbase::mutate_in_specs::increment("count", 1),
      couchbase::mutate_in_specs::upsert("meta.x", 42).xattr(),
    }
      .specs();
  encode_twice(req);

  // the counter is at wire index 1, behind the XATTR spec
  std::vector<std::byte> body{};
  append_entry(body, key_value_status_code::success, "1", 1);
  const auto resp = req.make_response(
    {}, subdoc_response<mutate_in_response_body>(key_value_status_code::success, std::move(body)));

  assert_success(resp.ctx.ec(), "the response is decoded");
  assert_eq(resp.fields.at(0).original_index, std::size_t{ 0 }, "the first field");
  assert_eq(resp.fields.at(0).path, std::string{ "count" }, "the counter keeps index 0");
  assert_eq(value_of(resp.fields.at(0).value), std::string{ "1" }, "the counter's value");
  assert_eq(resp.fields.at(1).path, std::string{ "meta.x" }, "the XATTR spec keeps index 1");
}

void
a_failed_mutate_in_encoded_again_reports_the_caller_index([[maybe_unused]] context& ctx)
{
  const auto resp = fail_at_wire_index(body_then_xattr(), 0, 2);

  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 1 }, "first_error_index");
  assert_eq(
    resp.ctx.first_error_path(), std::optional<std::string>{ "xattr_path" }, "first_error_path");
  assert_eq(resp.fields.at(1).path, std::string{ "xattr_path" }, "the failed field keeps index 1");
}

auto
lookup_body_then_xattr() -> std::vector<couchbase::core::impl::subdoc::command>
{
  return couchbase::lookup_in_specs{
    couchbase::lookup_in_specs::get("body_path"),
    couchbase::lookup_in_specs::get("xattr_path").xattr(),
  }
    .specs();
}

void
a_lookup_in_encoded_again_keeps_the_caller_index([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::lookup_in_request req{
    { "default", "_default", "_default", "doc" }
  };
  req.specs = lookup_body_then_xattr();
  encode_twice(req);

  // wire order: the XATTR value, then the body value
  std::vector<std::byte> body{};
  append_entry(body, key_value_status_code::success, "\"x\"");
  append_entry(body, key_value_status_code::success, "\"b\"");
  const auto resp =
    req.make_response({},
                      subdoc_response<couchbase::core::protocol::lookup_in_response_body>(
                        key_value_status_code::success, std::move(body)));

  assert_success(resp.ctx.ec(), "the response is decoded");
  assert_eq(resp.fields.at(0).original_index, std::size_t{ 0 }, "the first field");
  assert_eq(resp.fields.at(0).path, std::string{ "body_path" }, "the body spec keeps index 0");
  assert_eq(value_of(resp.fields.at(0).value), std::string{ "\"b\"" }, "the body value");
  assert_eq(resp.fields.at(1).path, std::string{ "xattr_path" }, "the XATTR spec keeps index 1");
}

void
a_failed_lookup_in_encoded_again_reports_the_caller_index([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::lookup_in_request req{
    { "default", "_default", "_default", "doc" }
  };
  req.specs = lookup_body_then_xattr();
  encode_twice(req);

  std::vector<std::byte> body{};
  append_entry(body, key_value_status_code::success, "\"x\"");
  append_entry(body, key_value_status_code::subdoc_path_not_found, "");
  const auto resp =
    req.make_response({},
                      subdoc_response<couchbase::core::protocol::lookup_in_response_body>(
                        key_value_status_code::subdoc_multi_path_failure, std::move(body)));

  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 0 }, "first_error_index");
  assert_eq(
    resp.ctx.first_error_path(), std::optional<std::string>{ "body_path" }, "first_error_path");
}

void
a_replica_lookup_in_encoded_again_keeps_the_caller_index([[maybe_unused]] context& ctx)
{
  couchbase::core::impl::lookup_in_replica_request req{
    { "default", "_default", "_default", "doc" }
  };
  req.specs = lookup_body_then_xattr();
  encode_twice(req);

  std::vector<std::byte> body{};
  append_entry(body, key_value_status_code::success, "\"x\"");
  append_entry(body, key_value_status_code::subdoc_path_not_found, "");
  const auto resp =
    req.make_response({},
                      subdoc_response<couchbase::core::protocol::lookup_in_replica_response_body>(
                        key_value_status_code::subdoc_multi_path_failure, std::move(body)));

  assert_eq(resp.fields.at(0).original_index, std::size_t{ 0 }, "the first field");
  assert_eq(resp.fields.at(0).path, std::string{ "body_path" }, "the body spec keeps index 0");
  assert_eq(resp.fields.at(1).path, std::string{ "xattr_path" }, "the XATTR spec keeps index 1");
  assert_eq(value_of(resp.fields.at(1).value), std::string{ "\"x\"" }, "the XATTR value");
  assert_eq(resp.ctx.first_error_index(), std::optional<std::size_t>{ 0 }, "first_error_index");
}

// The flags byte of the first spec on the wire. XATTR specs are encoded first.
template<typename Encoded>
auto
first_wire_flags(Encoded& encoded) -> std::byte
{
  return encoded.body().value().at(1);
}

// Each dispatch sends the binary flag only to a node that supports binary XATTRs, whatever the
// earlier dispatches reached.
template<typename Request>
void
assert_binary_flag_follows_the_node(Request& req)
{
  typename Request::encoded_request_type encoded{};
  const auto binary_xattr = couchbase::core::feature_mask_bit(
    couchbase::core::protocol::hello_feature::subdoc_binary_xattr);
  assert_success(req.encode_to(encoded, { std::nullopt, binary_xattr }), "the first dispatch");
  assert_true(couchbase::core::impl::subdoc::has_binary_value_path_flag(first_wire_flags(encoded)),
              "a node with binary XATTRs gets the flag");
  assert_success(req.encode_to(encoded, { std::nullopt, 0 }), "the retry");
  assert_false(couchbase::core::impl::subdoc::has_binary_value_path_flag(first_wire_flags(encoded)),
               "a node without them does not");
  assert_success(req.encode_to(encoded, { std::nullopt, binary_xattr }), "a second retry");
  assert_true(couchbase::core::impl::subdoc::has_binary_value_path_flag(first_wire_flags(encoded)),
              "a node with binary XATTRs gets the flag again");
}

void
a_lookup_in_retry_sends_the_binary_flag_only_to_nodes_that_support_it([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::lookup_in_request req{
    { "default", "_default", "_default", "doc" }
  };
  req.specs =
    couchbase::lookup_in_specs{
      couchbase::lookup_in_specs::get("body_path"),
      couchbase::lookup_in_specs::get("xattr_path").xattr().binary(),
    }
      .specs();
  assert_binary_flag_follows_the_node(req);
}

void
a_mutate_in_retry_sends_the_binary_flag_only_to_nodes_that_support_it([[maybe_unused]] context& ctx)
{
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.specs =
    couchbase::mutate_in_specs{
      couchbase::mutate_in_specs::increment("count", 1),
      couchbase::mutate_in_specs::upsert("meta.x", 42).xattr().binary(),
    }
      .specs();
  assert_binary_flag_follows_the_node(req);
}

// Wire bytes of one dispatch from encode_for_dispatch(), which mcbp_command::send() calls for every
// dispatch.
template<typename Request>
auto
dispatch(Request& req, std::uint64_t features) -> std::vector<std::byte>
{
  auto [ec, data] = couchbase::core::operations::encode_for_dispatch(
    req, { std::nullopt, features }, std::chrono::milliseconds{ 2'500 });
  assert_success(ec, "the dispatch encodes");
  return data;
}

// Its encoded form returns a buffer whose address it records.
struct buffer_recording_request {
  inline static const std::byte* returned{ nullptr };

  struct encoded_request_type {
    [[nodiscard]] auto data(bool /* try_to_compress */) const -> std::vector<std::byte>
    {
      std::vector<std::byte> bytes(4096, std::byte{ 0x5a });
      returned = bytes.data();
      return bytes;
    }
  };

  [[nodiscard]] auto encode_to(encoded_request_type& /* encoded */,
                               couchbase::core::mcbp_context&& /* context */) const
    -> std::error_code
  {
    return {};
  }
};

void
the_encoded_bytes_are_moved_out_of_the_encoded_request([[maybe_unused]] context& ctx)
{
  buffer_recording_request req{};
  const auto [ec, wire] = couchbase::core::operations::encode_for_dispatch(
    req, { std::nullopt, 0 }, std::chrono::milliseconds{ 2'500 });
  assert_success(ec, "the dispatch encodes");
  assert_eq(wire.size(), std::size_t{ 4096 }, "the encoded bytes");
  assert_true(wire.data() == buffer_recording_request::returned, "the buffer is not copied");
}

constexpr std::size_t header_size{ 24 };
constexpr std::size_t framing_extras_length_offset{ 2 };
constexpr std::size_t extras_length_offset{ 4 };
// mutate_in extras: the expiry, then the user flags.
constexpr std::size_t expiry_size{ sizeof(std::uint32_t) };

auto
extras_length(const std::vector<std::byte>& wire) -> std::size_t
{
  return std::to_integer<std::size_t>(wire.at(extras_length_offset));
}

void
a_mutate_in_retry_sends_the_user_flags_only_to_nodes_that_support_them(
  [[maybe_unused]] context& ctx)
{
  const auto binary_xattr = couchbase::core::feature_mask_bit(
    couchbase::core::protocol::hello_feature::subdoc_binary_xattr);
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.flags = 0x01020304;
  // The expiry makes the first dispatch encode extras, which a reused body would resend.
  req.expiry = 3600;
  req.specs = couchbase::mutate_in_specs{ couchbase::mutate_in_specs::upsert("x", 42) }.specs();

  assert_eq(extras_length(dispatch(req, 0)), std::size_t{ 4 }, "the expiry alone");
  const auto wire = dispatch(req, binary_xattr);
  assert_eq(extras_length(wire), std::size_t{ 8 }, "the expiry and the user flags");
  const std::vector<std::byte> user_flags{ wire.begin() + header_size + expiry_size,
                                           wire.begin() + header_size + 2 * expiry_size };
  assert_eq(
    user_flags,
    std::vector<std::byte>{ std::byte{ 1 }, std::byte{ 2 }, std::byte{ 3 }, std::byte{ 4 } },
    "the user flags, in network order");
  assert_eq(extras_length(dispatch(req, 0)), std::size_t{ 4 }, "the expiry alone again");
}

// Past the dispatch whose accumulated frames would no longer fit the one-byte length.
constexpr int dispatches_past_the_length_byte{ 64 };

template<typename Request>
void
assert_each_dispatch_carries_one_frame_of_each_kind(Request& req)
{
  req.durability_level = couchbase::durability_level::majority;
  req.preserve_expiry = true;
  for (int i = 0; i < dispatches_past_the_length_byte; ++i) {
    const auto wire = dispatch(req, 0);
    assert_eq(static_cast<couchbase::core::protocol::magic>(wire.at(0)),
              couchbase::core::protocol::magic::alt_client_request,
              "framing extras are present");
    // A durability frame with a timeout takes 4 bytes, a preserve-expiry frame 1.
    assert_eq(std::to_integer<std::size_t>(wire.at(framing_extras_length_offset)),
              std::size_t{ 5 },
              "one durability frame and one preserve-expiry frame");
  }
}

void
each_dispatch_of_a_durable_mutate_in_carries_one_frame_of_each_kind([[maybe_unused]] context& ctx)
{
  mutate_in_request req{ { "default", "_default", "_default", "doc" } };
  req.specs =
    couchbase::mutate_in_specs{ couchbase::mutate_in_specs::increment("count", 1) }.specs();
  assert_each_dispatch_carries_one_frame_of_each_kind(req);
}

void
each_dispatch_of_a_durable_replace_carries_one_frame_of_each_kind([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::replace_request req{ { "default", "_default", "_default", "doc" },
                                                    couchbase::core::utils::to_binary("{}") };
  assert_each_dispatch_carries_one_frame_of_each_kind(req);
}

void
each_dispatch_of_a_durable_upsert_carries_one_frame_of_each_kind([[maybe_unused]] context& ctx)
{
  couchbase::core::operations::upsert_request req{ { "default", "_default", "_default", "doc" },
                                                   couchbase::core::utils::to_binary("{}") };
  assert_each_dispatch_carries_one_frame_of_each_kind(req);
}
void
a_value_is_compressed_only_for_a_node_with_snappy([[maybe_unused]] context& ctx)
{
  constexpr std::size_t datatype_offset{ 5 };
  const auto snappy_bit = static_cast<std::uint8_t>(couchbase::core::protocol::datatype::snappy);
  const std::string value(4096, 'a');
  couchbase::core::operations::upsert_request req{ { "default", "_default", "_default", "doc" },
                                                   couchbase::core::utils::to_binary(value) };

  const auto plain = dispatch(req, 0);
  assert_eq(std::to_integer<std::uint8_t>(plain.at(datatype_offset)) & snappy_bit,
            0,
            "no snappy datatype without snappy support");
  assert_eq(std::string(reinterpret_cast<const char*>(plain.data() + plain.size() - value.size()),
                        value.size()),
            value,
            "the value is sent as is");

  const auto compressed = dispatch(
    req, couchbase::core::feature_mask_bit(couchbase::core::protocol::hello_feature::snappy));
  assert_eq(std::to_integer<std::uint8_t>(compressed.at(datatype_offset)) & snappy_bit,
            int{ snappy_bit },
            "the snappy datatype with snappy support");
  assert_true(compressed.size() < plain.size(), "the value is compressed");
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
      { CASE(a_mutate_in_encoded_again_keeps_the_caller_index), {}, timeout::instant },
      { CASE(a_failed_mutate_in_encoded_again_reports_the_caller_index), {}, timeout::instant },
      { CASE(a_lookup_in_encoded_again_keeps_the_caller_index), {}, timeout::instant },
      { CASE(a_failed_lookup_in_encoded_again_reports_the_caller_index), {}, timeout::instant },
      { CASE(a_replica_lookup_in_encoded_again_keeps_the_caller_index), {}, timeout::instant },
      { CASE(a_lookup_in_retry_sends_the_binary_flag_only_to_nodes_that_support_it),
        {},
        timeout::instant },
      { CASE(a_mutate_in_retry_sends_the_binary_flag_only_to_nodes_that_support_it),
        {},
        timeout::instant },
      { CASE(the_encoded_bytes_are_moved_out_of_the_encoded_request), {}, timeout::instant },
      { CASE(a_value_is_compressed_only_for_a_node_with_snappy), {}, timeout::instant },
      { CASE(a_mutate_in_retry_sends_the_user_flags_only_to_nodes_that_support_them),
        {},
        timeout::instant },
      { CASE(each_dispatch_of_a_durable_mutate_in_carries_one_frame_of_each_kind),
        {},
        timeout::instant },
      { CASE(each_dispatch_of_a_durable_replace_carries_one_frame_of_each_kind),
        {},
        timeout::instant },
      { CASE(each_dispatch_of_a_durable_upsert_carries_one_frame_of_each_kind),
        {},
        timeout::instant },
    },
  };
}

} // namespace couchbase::test
