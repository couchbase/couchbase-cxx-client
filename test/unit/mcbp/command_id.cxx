/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/io/mcbp_command_id.hxx"
#include "core/protocol/client_opcode.hxx"

#include <algorithm>
#include <cstddef>
#include <string>

namespace couchbase::test
{
namespace
{
using couchbase::core::operations::make_command_id;
using couchbase::core::protocol::client_opcode;

void
make_command_id_renders_the_opcode_and_a_valid_uuid([[maybe_unused]] context& ctx)
{
  const auto id = make_command_id(client_opcode::upsert);

  // "<opcode:02x>/<36-char uuid>" == 2 + 1 + 36 == 39 chars
  assert_eq(id.size(), std::size_t{ 39 }, "the identifier is opcode, separator and uuid");
  assert_eq(id[2], '/', "the opcode is separated from the uuid by a slash");
  assert_eq(id.substr(0, 2), "01", "upsert is opcode 0x01");

  const auto uuid = id.substr(3);
  assert_eq(uuid.size(), std::size_t{ 36 }, "the uuid is rendered in full");
  assert_eq(std::count(uuid.begin(), uuid.end(), '-'),
            std::ptrdiff_t{ 4 },
            "the uuid carries its four group separators");
  assert_true(std::all_of(uuid.begin(),
                          uuid.end(),
                          [](char c) {
                            return c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                          }),
              "the uuid holds nothing but lowercase hex digits and hyphens");
}

void
make_command_id_encodes_the_opcode_byte([[maybe_unused]] context& ctx)
{
  assert_eq(make_command_id(client_opcode::get).substr(0, 2), "00", "get is opcode 0x00");
  assert_eq(make_command_id(client_opcode::remove).substr(0, 2), "04", "remove is opcode 0x04");
}

void
make_command_id_embeds_a_version_4_uuid([[maybe_unused]] context& ctx)
{
  // uuid::random() produces version-4 uuids, and the version nibble is the first character of the
  // uuid's third hyphen-delimited group. Asserting that generator invariant rather than comparing
  // two random values for inequality, which a uuid collision would fail.
  const auto uuid = make_command_id(client_opcode::get).substr(3);
  assert_eq(uuid.size(), std::size_t{ 36 }, "the uuid is rendered in full");
  assert_eq(uuid[14], '4', "the uuid announces version 4");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(make_command_id_renders_the_opcode_and_a_valid_uuid) },
      { CASE(make_command_id_encodes_the_opcode_byte) },
      { CASE(make_command_id_embeds_a_version_4_uuid) },
    },
  };
}

} // namespace couchbase::test
