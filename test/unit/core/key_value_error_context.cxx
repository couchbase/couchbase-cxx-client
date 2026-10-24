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
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_replica.hxx"
#include "core/utils/byteswap.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/retry_reason.hxx>

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
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

void
get_and_replica_make_response_move_value_buffer_without_copy([[maybe_unused]] context& ctx)
{
  auto make_msg =
    [](couchbase::core::protocol::client_opcode opcode) -> couchbase::core::io::mcbp_message {
    couchbase::core::io::mcbp_message msg{};
    msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
    msg.header.opcode = static_cast<std::uint8_t>(opcode);
    msg.header.extlen = 4;
    // Set network-order bodylen (68 = 0x44: 4-byte flags + 64-byte value)
    msg.header.bodylen = couchbase::core::utils::host_to_network(std::uint32_t{ 68U });
    msg.body.assign(68, std::byte{ 0x5a });
    return msg;
  };

  {
    const couchbase::core::operations::get_request req{
      couchbase::core::document_id{ "default", "_default", "_default", "k" },
    };
    couchbase::core::operations::get_request::encoded_response_type encoded{
      make_msg(couchbase::core::protocol::client_opcode::get),
    };
    const std::byte* original_data_ptr = encoded.body().value().data();
    assert_eq(encoded.body().value().size(), std::size_t{ 64 }, "parsed 64-byte value");

    auto resp = req.make_response({}, std::move(encoded));
    assert_eq(resp.value.size(), std::size_t{ 64 }, "moved 64-byte value");
    assert_true(resp.value.data() == original_data_ptr,
                "get_request::make_response moves value buffer without reallocating");
  }

  {
    const couchbase::core::operations::get_replica_request req{
      couchbase::core::document_id{ "default", "_default", "_default", "k" },
    };
    couchbase::core::operations::get_replica_request::encoded_response_type encoded{
      make_msg(couchbase::core::protocol::client_opcode::get_replica),
    };
    const std::byte* original_data_ptr = encoded.body().value().data();
    assert_eq(encoded.body().value().size(), std::size_t{ 64 }, "parsed 64-byte value");

    auto resp = req.make_response({}, std::move(encoded));
    assert_eq(resp.value.size(), std::size_t{ 64 }, "moved 64-byte value");
    assert_true(resp.value.data() == original_data_ptr,
                "get_replica_request::make_response moves value buffer without reallocating");
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
      { CASE(get_and_replica_make_response_move_value_buffer_without_copy), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
