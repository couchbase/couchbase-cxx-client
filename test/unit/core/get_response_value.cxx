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

/*
 * make_response of each get operation takes the encoded response as an rvalue and moves the value
 * buffer out of it. A copy would leave the response value at a different address.
 */

#include "framework/test_registry.hxx"

#include "core/document_id.hxx"
#include "core/operations/document_get.hxx"
#include "core/operations/document_get_and_lock.hxx"
#include "core/operations/document_get_and_touch.hxx"
#include "core/operations/document_get_replica.hxx"
#include "core/utils/byteswap.hxx"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace couchbase::test
{
namespace
{
template<typename Request>
void
assert_value_is_moved(couchbase::core::protocol::client_opcode opcode)
{
  // 4 bytes of flags as extras, then a 64-byte value.
  couchbase::core::io::mcbp_message msg{};
  msg.header.magic = static_cast<std::uint8_t>(couchbase::core::protocol::magic::client_response);
  msg.header.opcode = static_cast<std::uint8_t>(opcode);
  msg.header.extlen = 4;
  msg.header.bodylen = couchbase::core::utils::host_to_network(std::uint32_t{ 68U });
  msg.body.assign(68, std::byte{ 0x5a });

  const Request req{ couchbase::core::document_id{ "default", "_default", "_default", "k" } };
  typename Request::encoded_response_type encoded{ std::move(msg) };
  const std::byte* parsed = encoded.body().value().data();
  assert_eq(encoded.body().value().size(), std::size_t{ 64 }, "the value is parsed");

  auto resp = req.make_response({}, std::move(encoded));
  assert_eq(resp.value.size(), std::size_t{ 64 }, "the response carries the value");
  assert_true(resp.value.data() == parsed, "the value buffer is moved, not copied");
}

void
get_moves_the_value_buffer([[maybe_unused]] context& ctx)
{
  assert_value_is_moved<couchbase::core::operations::get_request>(
    couchbase::core::protocol::client_opcode::get);
}

void
get_replica_moves_the_value_buffer([[maybe_unused]] context& ctx)
{
  assert_value_is_moved<couchbase::core::operations::get_replica_request>(
    couchbase::core::protocol::client_opcode::get_replica);
}

void
get_and_lock_moves_the_value_buffer([[maybe_unused]] context& ctx)
{
  assert_value_is_moved<couchbase::core::operations::get_and_lock_request>(
    couchbase::core::protocol::client_opcode::get_and_lock);
}

void
get_and_touch_moves_the_value_buffer([[maybe_unused]] context& ctx)
{
  assert_value_is_moved<couchbase::core::operations::get_and_touch_request>(
    couchbase::core::protocol::client_opcode::get_and_touch);
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(get_moves_the_value_buffer), {}, timeout::instant },
      { CASE(get_replica_moves_the_value_buffer), {}, timeout::instant },
      { CASE(get_and_lock_moves_the_value_buffer), {}, timeout::instant },
      { CASE(get_and_touch_moves_the_value_buffer), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
