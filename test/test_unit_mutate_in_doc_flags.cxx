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

#include "test_helper.hxx"

#include "core/protocol/cmd_mutate_in.hxx"

#include <couchbase/store_semantics.hxx>

#include <cstdint>
#include <type_traits>

TEST_CASE("unit: mutate_in doc_flag constants are std::uint8_t", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  CHECK(std::is_same_v<decltype(body::doc_flag_mkdoc), const std::uint8_t>);
  CHECK(std::is_same_v<decltype(body::doc_flag_add), const std::uint8_t>);
  CHECK(std::is_same_v<decltype(body::doc_flag_access_deleted), const std::uint8_t>);
  CHECK(std::is_same_v<decltype(body::doc_flag_create_as_deleted), const std::uint8_t>);
  CHECK(std::is_same_v<decltype(body::doc_flag_revive_document), const std::uint8_t>);
}

TEST_CASE("unit: mutate_in doc_flag constants have expected bit values", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  CHECK(body::doc_flag_mkdoc == std::uint8_t{ 0b0000'0001U });
  CHECK(body::doc_flag_add == std::uint8_t{ 0b0000'0010U });
  CHECK(body::doc_flag_access_deleted == std::uint8_t{ 0b0000'0100U });
  CHECK(body::doc_flag_create_as_deleted == std::uint8_t{ 0b0000'1000U });
  CHECK(body::doc_flag_revive_document == std::uint8_t{ 0b0001'0000U });
}

TEST_CASE("unit: mutate_in doc_flag constants are mutually exclusive in their bit positions",
          "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  // No two flags share the same bit
  CHECK((body::doc_flag_mkdoc & body::doc_flag_add) == 0U);
  CHECK((body::doc_flag_mkdoc & body::doc_flag_access_deleted) == 0U);
  CHECK((body::doc_flag_mkdoc & body::doc_flag_create_as_deleted) == 0U);
  CHECK((body::doc_flag_mkdoc & body::doc_flag_revive_document) == 0U);
  CHECK((body::doc_flag_add & body::doc_flag_access_deleted) == 0U);
  CHECK((body::doc_flag_add & body::doc_flag_create_as_deleted) == 0U);
  CHECK((body::doc_flag_add & body::doc_flag_revive_document) == 0U);
  CHECK((body::doc_flag_access_deleted & body::doc_flag_create_as_deleted) == 0U);
  CHECK((body::doc_flag_access_deleted & body::doc_flag_revive_document) == 0U);
  CHECK((body::doc_flag_create_as_deleted & body::doc_flag_revive_document) == 0U);
}

TEST_CASE("unit: mutate_in store_semantics sets correct doc flags", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  SECTION("replace clears store-semantics bits")
  {
    body req{};
    // First set upsert, then switch to replace — bits must be cleared
    req.store_semantics(couchbase::store_semantics::upsert);
    req.store_semantics(couchbase::store_semantics::replace);
    // After replace the extras must not contain mkdoc or add bits.
    // We verify indirectly: calling extras() encodes the flags; a zero-flag
    // extras is shorter (no flags byte) than a non-zero one.
    const auto& extras = req.extras();
    // extras should be exactly 0 bytes when there is no expiry, no user_flags,
    // and flags == 0 (replace semantics).
    CHECK(extras.empty());
  }

  SECTION("upsert sets doc_flag_mkdoc bit")
  {
    body req{};
    req.store_semantics(couchbase::store_semantics::upsert);
    const auto& extras = req.extras();
    // extras has a flags byte when flags != 0
    REQUIRE_FALSE(extras.empty());
    // The flags byte is the last byte of extras
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_mkdoc) != 0U);
    CHECK((flags & body::doc_flag_add) == 0U);
  }

  SECTION("insert sets doc_flag_add bit")
  {
    body req{};
    req.store_semantics(couchbase::store_semantics::insert);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_add) != 0U);
    CHECK((flags & body::doc_flag_mkdoc) == 0U);
  }

  SECTION("switching semantics replaces the store-semantics bits without affecting others")
  {
    body req{};
    // Enable access_deleted alongside upsert
    req.store_semantics(couchbase::store_semantics::upsert);
    req.access_deleted(true);

    // Now switch to insert
    req.store_semantics(couchbase::store_semantics::insert);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());

    CHECK((flags & body::doc_flag_add) != 0U);
    CHECK((flags & body::doc_flag_mkdoc) == 0U);
    // access_deleted bit must still be set
    CHECK((flags & body::doc_flag_access_deleted) != 0U);
  }
}

TEST_CASE("unit: mutate_in access_deleted flag toggle", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  SECTION("enabling sets the bit")
  {
    body req{};
    req.access_deleted(true);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_access_deleted) != 0U);
  }

  SECTION("disabling clears the bit")
  {
    body req{};
    req.access_deleted(true);
    req.access_deleted(false);
    // flags should be zero → no flags byte in extras
    const auto& extras = req.extras();
    CHECK(extras.empty());
  }

  SECTION("toggling does not affect other flags")
  {
    body req{};
    req.create_as_deleted(true);
    req.access_deleted(true);
    req.access_deleted(false);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_access_deleted) == 0U);
    CHECK((flags & body::doc_flag_create_as_deleted) != 0U);
  }
}

TEST_CASE("unit: mutate_in create_as_deleted flag toggle", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  SECTION("enabling sets the bit")
  {
    body req{};
    req.create_as_deleted(true);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_create_as_deleted) != 0U);
  }

  SECTION("disabling clears the bit without affecting others")
  {
    body req{};
    req.access_deleted(true);
    req.create_as_deleted(true);
    req.create_as_deleted(false);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_create_as_deleted) == 0U);
    CHECK((flags & body::doc_flag_access_deleted) != 0U);
  }
}

TEST_CASE("unit: mutate_in revive_document flag toggle", "[unit]")
{
  using body = couchbase::core::protocol::mutate_in_request_body;

  SECTION("enabling sets the bit")
  {
    body req{};
    req.revive_document(true);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_revive_document) != 0U);
  }

  SECTION("disabling clears the bit without affecting others")
  {
    body req{};
    req.access_deleted(true);
    req.revive_document(true);
    req.revive_document(false);
    const auto& extras = req.extras();
    REQUIRE_FALSE(extras.empty());
    auto flags = static_cast<std::uint8_t>(extras.back());
    CHECK((flags & body::doc_flag_revive_document) == 0U);
    CHECK((flags & body::doc_flag_access_deleted) != 0U);
  }
}
