/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/document_id.hxx"
#include "core/error_context/key_value.hxx"
#include "core/operations/document_get_projected.hxx"
#include "core/utils/byteswap.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>

namespace couchbase::test
{
namespace
{
void
an_error_context_built_from_a_code_and_an_id_carries_nothing_else([[maybe_unused]] context& ctx)
{
  couchbase::core::document_id id{ "bucket", "_default", "_default", "my-doc" };
  auto ec = couchbase::errc::key_value::document_not_found;
  auto error_ctx = couchbase::core::make_key_value_error_context(ec, id);

  assert_eq(error_ctx.ec(), ec, "the code");
  assert_eq(error_ctx.id(), std::string{ "my-doc" }, "the document key");
  assert_eq(error_ctx.bucket(), std::string{ "bucket" }, "the bucket");
  assert_eq(error_ctx.scope(), std::string{ "_default" }, "the scope");
  assert_eq(error_ctx.collection(), std::string{ "_default" }, "the collection");
  assert_eq(error_ctx.retry_attempts(), std::size_t{ 0 }, "no retries were attempted");
  assert_true(error_ctx.retry_reasons().empty(), "no retry reason is recorded");
  assert_false(error_ctx.last_dispatched_to().has_value(), "nothing was dispatched to a node");
  assert_false(error_ctx.last_dispatched_from().has_value(), "nothing was dispatched from a node");
  assert_false(error_ctx.status_code().has_value(), "no server status was received");
  assert_false(error_ctx.error_map_info().has_value(), "no error map entry was matched");
  assert_false(error_ctx.extended_error_info().has_value(), "no extended error info was received");
}

void
an_error_context_carries_the_retry_attempts_and_reasons_it_was_given([[maybe_unused]] context& ctx)
{
  couchbase::core::document_id id{ "travel-sample", "inventory", "airline", "doc-key" };
  auto ec = couchbase::errc::key_value::document_locked;
  std::size_t retry_attempts = 3;
  std::set<couchbase::retry_reason> retry_reasons{
    couchbase::retry_reason::key_value_locked,
    couchbase::retry_reason::key_value_temporary_failure,
  };

  auto error_ctx =
    couchbase::core::make_key_value_error_context(ec, id, retry_attempts, retry_reasons);

  assert_eq(error_ctx.ec(), ec, "the code");
  assert_eq(error_ctx.id(), std::string{ "doc-key" }, "the document key");
  assert_eq(error_ctx.bucket(), std::string{ "travel-sample" }, "the bucket");
  assert_eq(error_ctx.scope(), std::string{ "inventory" }, "the scope");
  assert_eq(error_ctx.collection(), std::string{ "airline" }, "the collection");
  assert_eq(error_ctx.retry_attempts(), retry_attempts, "the retry attempt count");
  assert_true(error_ctx.retry_reasons() == retry_reasons, "the retry reasons");
  assert_true(error_ctx.retried_because_of(couchbase::retry_reason::key_value_locked),
              "a reason that was given is reported");
  assert_true(error_ctx.retried_because_of(couchbase::retry_reason::key_value_temporary_failure),
              "a reason that was given is reported");
  assert_false(error_ctx.retried_because_of(couchbase::retry_reason::socket_not_available),
               "a reason that was not given is not reported");
}

void
the_two_error_context_overloads_agree_on_observable_state([[maybe_unused]] context& ctx)
{
  couchbase::core::document_id id{ "bucket", "scope", "collection", "key" };
  auto ec = couchbase::errc::common::authentication_failure;

  auto simple = couchbase::core::make_key_value_error_context(ec, id);
  auto explicitly_zero = couchbase::core::make_key_value_error_context(ec, id, 0, {});

  assert_eq(simple.ec(), explicitly_zero.ec(), "the code");
  assert_eq(simple.id(), explicitly_zero.id(), "the document key");
  assert_eq(simple.bucket(), explicitly_zero.bucket(), "the bucket");
  assert_eq(simple.scope(), explicitly_zero.scope(), "the scope");
  assert_eq(simple.collection(), explicitly_zero.collection(), "the collection");
  assert_eq(simple.retry_attempts(), explicitly_zero.retry_attempts(), "the retry attempt count");
  assert_true(simple.retry_reasons() == explicitly_zero.retry_reasons(), "the retry reasons");
}

void
retry_attempts_are_carried_without_a_reason([[maybe_unused]] context& ctx)
{
  couchbase::core::document_id id{ "bucket", "_default", "_default", "key" };
  auto ec = couchbase::errc::key_value::document_exists;

  auto error_ctx = couchbase::core::make_key_value_error_context(ec, id, 5, {});

  assert_eq(error_ctx.retry_attempts(), std::size_t{ 5 }, "the retry attempt count");
  assert_true(error_ctx.retry_reasons().empty(), "no retry reason is recorded");
}

struct stub_session {
  [[nodiscard]] auto decode_error_code(std::uint16_t /* code */) const
    -> std::optional<couchbase::core::key_value_error_map_info>
  {
    return std::nullopt;
  }

  [[nodiscard]] auto node_uuid() const -> std::string
  {
    return {};
  }

  [[nodiscard]] auto canonical_hostname() const -> std::string
  {
    return {};
  }

  [[nodiscard]] auto canonical_port_number() const -> std::uint16_t
  {
    return 0;
  }
};

struct stub_command {
  struct request_type {
    couchbase::core::document_id id{ "bucket", "_default", "_default", "doc-1" };
    std::uint16_t partition{ 7 };
    std::uint32_t opaque{ 42U };
    couchbase::core::io::retry_context<true> retries{};
  };

  request_type request{};
  std::optional<stub_session> session_{};
  std::optional<std::string> last_dispatched_to_{ "127.0.0.1:11210" };
  std::optional<std::string> last_dispatched_from_{ "127.0.0.1:54321" };
  mutable int id_calls{ 0 };
  mutable std::string id_{};

  [[nodiscard]] auto id() const -> const std::string&
  {
    ++id_calls;
    if (id_.empty()) {
      id_ = "get/00000000-0000-4000-8000-000000000001";
    }
    return id_;
  }
};

struct stub_response {
  couchbase::core::protocol::client_opcode opcode_val{
    couchbase::core::protocol::client_opcode::get,
  };

  [[nodiscard]] auto opcode() const -> couchbase::core::protocol::client_opcode
  {
    return opcode_val;
  }

  [[nodiscard]] auto opaque() const -> std::uint32_t
  {
    return 42U;
  }

  [[nodiscard]] auto status() const -> couchbase::core::key_value_status_code
  {
    return couchbase::core::key_value_status_code::success;
  }

  [[nodiscard]] auto cas() const -> couchbase::cas
  {
    return couchbase::cas{ 100U };
  }

  [[nodiscard]] auto error_info() const
    -> std::optional<couchbase::core::key_value_extended_error_info>
  {
    return std::nullopt;
  }
};

void
clean_success_skips_lazy_command_id_generation_while_error_generates_it(
  [[maybe_unused]] context& ctx)
{
  {
    const stub_command cmd{};
    const stub_response resp{};
    auto ok_ctx = couchbase::core::make_key_value_error_context(
      std::error_code{}, std::uint16_t{ 0 }, &cmd, resp);
    assert_eq(cmd.id_calls, 0, "clean success (!ec && status_code == 0) does not materialize UUID");
    assert_true(ok_ctx.operation_id().empty(), "operation_id is empty on clean success");
    assert_eq(ok_ctx.last_dispatched_to().value_or(""),
              std::string{ "127.0.0.1:11210" },
              "last_dispatched_to is preserved");

    // Any post-wire failure that calls override_ec(ec) (such as with_legacy_durability or
    // collection transcoding/projection) lazily populates operation_id from the deferred opcode.
    ok_ctx.override_ec(couchbase::errc::key_value::durability_ambiguous);
    assert_eq(ok_ctx.operation_id().size(),
              std::size_t{ 39 },
              "override_ec(ec) lazily populates 39-char '<opcode>/<uuid>' operation_id");
    assert_eq(ok_ctx.operation_id().substr(0, 3),
              std::string{ "00/" },
              "operation_id carries get (0x00) opcode prefix");
  }

  {
    const stub_command cmd{};
    const stub_response resp{};
    auto err_ctx = couchbase::core::make_key_value_error_context(
      couchbase::errc::key_value::document_not_found, std::uint16_t{ 0x0001 }, &cmd, resp);
    assert_eq(cmd.id_calls, 1, "error path materializes command id once");
    assert_eq(err_ctx.operation_id(),
              std::string{ "get/00000000-0000-4000-8000-000000000001" },
              "operation_id carries the generated command id");
  }

  {
    // When wire status is success (0x0000) so make_key_value_error_context skips UUID generation,
    // a subsequent decode/parse failure inside get_projected_request::make_response or subdoc
    // field error inside make_subdocument_error_context must still populate operation_id.
    const stub_command cmd{};
    const stub_response resp{ couchbase::core::protocol::client_opcode::subdoc_multi_lookup };
    auto wire_ok_ctx = couchbase::core::make_key_value_error_context(
      std::error_code{}, std::uint16_t{ 0 }, &cmd, resp);
    assert_true(wire_ok_ctx.operation_id().empty(), "wire-success context starts with empty id");

    auto sub_err_ctx = couchbase::core::make_subdocument_error_context(
      wire_ok_ctx, couchbase::errc::key_value::path_not_found, "missing", 0, false);
    assert_eq(sub_err_ctx.operation_id().size(),
              std::size_t{ 39 },
              "make_subdocument_error_context populates 39-char operation_id on subdoc error");
    assert_eq(sub_err_ctx.operation_id().substr(0, 3),
              std::string{ "d0/" },
              "subdocument operation_id carries deferred opcode prefix");

    couchbase::core::operations::get_projected_request proj_req{
      couchbase::core::document_id{ "bucket", "_default", "_default", "doc-1" },
    };
    proj_req.projections = { "name" };
    proj_req.effective_projections = { "name" };

    // Build a subdoc_multi_lookup response with 2 fields:
    // field 0 ($document.flags): status=0, len=1, value="0"
    // field 1 (projected "name"): status=0, len=4, value="{bad" (triggers parsing_failure)
    couchbase::core::io::mcbp_message msg{};
    msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
    msg.header.opcode =
      static_cast<std::uint8_t>(couchbase::core::protocol::client_opcode::subdoc_multi_lookup);
    auto append_field = [&msg](std::uint16_t field_status, std::string_view val) {
      const auto st_be = couchbase::core::utils::host_to_network(field_status);
      const auto len_be =
        couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(val.size()));
      const auto* st_ptr = reinterpret_cast<const std::byte*>(&st_be);
      const auto* len_ptr = reinterpret_cast<const std::byte*>(&len_be);
      msg.body.insert(msg.body.end(), st_ptr, st_ptr + sizeof(st_be));
      msg.body.insert(msg.body.end(), len_ptr, len_ptr + sizeof(len_be));
      for (const char ch : val) {
        msg.body.push_back(static_cast<std::byte>(ch));
      }
    };
    append_field(0x0000, "0");
    append_field(0x0000, "{bad");
    msg.header.bodylen =
      couchbase::core::utils::host_to_network(static_cast<std::uint32_t>(msg.body.size()));

    couchbase::core::operations::get_projected_request::encoded_response_type encoded{
      std::move(msg),
    };
    auto proj_resp = proj_req.make_response(std::move(wire_ok_ctx), encoded);
    assert_eq(proj_resp.ctx.ec(),
              couchbase::errc::common::parsing_failure,
              "malformed JSON projection triggers parsing_failure");
    assert_eq(proj_resp.ctx.operation_id().size(),
              std::size_t{ 39 },
              "post-decode error populates 39-char '<opcode>/<uuid>' operation_id");
    assert_eq(proj_resp.ctx.operation_id().substr(0, 3),
              std::string{ "d0/" },
              "operation_id carries subdoc_multi_lookup (0xd0) opcode prefix");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_error_context_built_from_a_code_and_an_id_carries_nothing_else),
        {},
        timeout::instant },
      { CASE(an_error_context_carries_the_retry_attempts_and_reasons_it_was_given),
        {},
        timeout::instant },
      { CASE(the_two_error_context_overloads_agree_on_observable_state), {}, timeout::instant },
      { CASE(retry_attempts_are_carried_without_a_reason), {}, timeout::instant },
      { CASE(clean_success_skips_lazy_command_id_generation_while_error_generates_it),
        {},
        timeout::instant },
    },
  };
}

} // namespace couchbase::test
