/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include "core/transactions/transaction_get_result.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <system_error>

using couchbase::core::document_id;
using couchbase::core::transactions::transaction_get_result;

TEST_CASE("transactions: get result row requires a usable CAS", "[unit]")
{
  const document_id id{ "default", "_default", "_default", "key" };

  SECTION("scas string populates the CAS")
  {
    const transaction_get_result doc(
      id, couchbase::core::utils::json::parse(R"({"scas":"1780000000000000000","doc":{"k":"v"}})"));
    REQUIRE(doc.cas().value() == 1780000000000000000ULL);
    REQUIRE_FALSE(doc.content().data.empty());
  }

  SECTION("numeric cas populates the CAS")
  {
    const transaction_get_result doc(
      id, couchbase::core::utils::json::parse(R"({"cas":42,"doc":{"k":"v"}})"));
    REQUIRE(doc.cas().value() == 42ULL);
  }

  SECTION("a row without scas or cas is rejected as a decoding failure")
  {
    try {
      [[maybe_unused]] const transaction_get_result doc(
        id, couchbase::core::utils::json::parse(R"({"doc":{"k":"v"}})"));
      FAIL("expected a decoding failure to be thrown");
    } catch (const std::system_error& e) {
      REQUIRE(e.code() == couchbase::errc::common::decoding_failure);
    }
  }

  SECTION("a row with scas but no doc is accepted with empty content")
  {
    const transaction_get_result doc(id, couchbase::core::utils::json::parse(R"({"scas":"123"})"));
    REQUIRE(doc.cas().value() == 123ULL);
    REQUIRE(doc.content().data.empty());
  }
}
