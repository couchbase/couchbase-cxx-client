/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include "core/transactions/staged_mutation.hxx"

#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

using namespace couchbase::core::transactions;

TEST_CASE("transactions: staged_mutation::type_as_string return type is string_view", "[unit]")
{
  // Verify at compile time that the return type is std::string_view, not std::string.
  static_assert(
    std::is_same_v<
      decltype(std::declval<couchbase::core::transactions::staged_mutation>().type_as_string()),
      std::string_view>,
    "type_as_string must return std::string_view");
}

TEST_CASE("transactions: staged_mutation::type_as_string returns correct labels", "[unit]")
{
  using couchbase::core::document_id;
  const document_id id{ "b", "s", "c", "key" };

  SECTION("INSERT")
  {
    staged_mutation sm{ staged_mutation_type::INSERT,
                        id,
                        /*cas=*/couchbase::cas{},
                        /*staged_content=*/std::optional<couchbase::codec::binary>{},
                        /*staged_flags=*/0U,
                        /*current_user_flags=*/0U,
                        /*doc_metadata=*/std::nullopt,
                        /*operation_id=*/"op1" };
    REQUIRE(sm.type_as_string() == "INSERT");
  }

  SECTION("REPLACE")
  {
    staged_mutation sm{ staged_mutation_type::REPLACE,
                        id,
                        /*cas=*/couchbase::cas{},
                        /*staged_content=*/std::optional<couchbase::codec::binary>{},
                        /*staged_flags=*/0U,
                        /*current_user_flags=*/0U,
                        /*doc_metadata=*/std::nullopt,
                        /*operation_id=*/"op2" };
    REQUIRE(sm.type_as_string() == "REPLACE");
  }

  SECTION("REMOVE")
  {
    staged_mutation sm{ staged_mutation_type::REMOVE,
                        id,
                        /*cas=*/couchbase::cas{},
                        /*staged_content=*/std::optional<couchbase::codec::binary>{},
                        /*staged_flags=*/0U,
                        /*current_user_flags=*/0U,
                        /*doc_metadata=*/std::nullopt,
                        /*operation_id=*/"op3" };
    REQUIRE(sm.type_as_string() == "REMOVE");
  }
}
