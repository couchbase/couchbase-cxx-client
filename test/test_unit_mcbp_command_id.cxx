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

#include "test_helper.hxx"

#include "core/io/mcbp_command_id.hxx"
#include "core/protocol/client_opcode.hxx"

#include <spdlog/fmt/bundled/format.h>

#include <algorithm>
#include <cstdint>

TEST_CASE("unit: make_command_id renders opcode and a valid uuid", "[unit]")
{
  using couchbase::core::operations::make_command_id;
  using couchbase::core::protocol::client_opcode;

  const auto id = make_command_id(client_opcode::upsert);

  // "<opcode:02x>/<36-char uuid>" == 2 + 1 + 36 == 39 chars
  REQUIRE(id.size() == 39);
  REQUIRE(id[2] == '/');
  REQUIRE(id.substr(0, 2) ==
          fmt::format("{:02x}", static_cast<std::uint8_t>(client_opcode::upsert)));
  // the trailing 36 characters are a UUID: only lowercase hex and hyphens, four hyphens
  const auto uuid = id.substr(3);
  REQUIRE(uuid.size() == 36);
  REQUIRE(std::count(uuid.begin(), uuid.end(), '-') == 4);
  REQUIRE(std::all_of(uuid.begin(), uuid.end(), [](char c) {
    return c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  }));
}

TEST_CASE("unit: make_command_id encodes the opcode byte", "[unit]")
{
  using couchbase::core::operations::make_command_id;
  using couchbase::core::protocol::client_opcode;

  REQUIRE(make_command_id(client_opcode::get).substr(0, 2) ==
          fmt::format("{:02x}", static_cast<std::uint8_t>(client_opcode::get)));
  REQUIRE(make_command_id(client_opcode::remove).substr(0, 2) ==
          fmt::format("{:02x}", static_cast<std::uint8_t>(client_opcode::remove)));
}

TEST_CASE("unit: make_command_id embeds a version-4 uuid", "[unit]")
{
  using couchbase::core::operations::make_command_id;
  using couchbase::core::protocol::client_opcode;

  // The id is "<opcode>/<uuid>", and uuid::random() produces version-4 uuids. Assert that generator
  // invariant -- the version nibble is the first character of the uuid's third hyphen-delimited
  // group (index 14) and is '4' -- rather than comparing two random values for inequality, which is
  // theoretically non-deterministic (a uuid collision would fail the test).
  const auto uuid = make_command_id(client_opcode::get).substr(3);
  REQUIRE(uuid.size() == 36);
  REQUIRE(uuid[14] == '4');
}
