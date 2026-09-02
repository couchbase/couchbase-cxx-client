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

#include "framework/test_registry.hxx"

#include "core/transactions/staged_mutation.hxx"

#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace couchbase::test
{
namespace
{
using couchbase::core::transactions::staged_mutation;
using couchbase::core::transactions::staged_mutation_type;

auto
make_staged_mutation(staged_mutation_type type, std::string operation_id) -> staged_mutation
{
  return staged_mutation{
    type,
    couchbase::core::document_id{ "b", "s", "c", "key" },
    /*cas=*/couchbase::cas{},
    /*staged_content=*/std::optional<couchbase::codec::binary>{},
    /*staged_flags=*/0U,
    /*current_user_flags=*/0U,
    /*doc_metadata=*/std::nullopt,
    std::move(operation_id),
  };
}

// Checked at compile time, so this case asserts nothing at runtime. Returning std::string by value
// would leave every caller copying the label, and the ATR write in attempt_context_impl takes it
// by view.
void
type_as_string_returns_a_string_view([[maybe_unused]] context& ctx)
{
  static_assert(
    std::is_same_v<decltype(std::declval<staged_mutation>().type_as_string()), std::string_view>,
    "type_as_string must return std::string_view");
}

// The three labels below are the value of the ATR entry's "type" field, so a changed spelling is a
// wire-format change a server-side cleanup would no longer recognise.
void
an_insert_is_labelled_insert([[maybe_unused]] context& ctx)
{
  const auto sm = make_staged_mutation(staged_mutation_type::INSERT, "op1");

  assert_eq(sm.type_as_string(), "INSERT", "the ATR label for a staged insert");
}

void
a_replace_is_labelled_replace([[maybe_unused]] context& ctx)
{
  const auto sm = make_staged_mutation(staged_mutation_type::REPLACE, "op2");

  assert_eq(sm.type_as_string(), "REPLACE", "the ATR label for a staged replace");
}

void
a_remove_is_labelled_remove([[maybe_unused]] context& ctx)
{
  const auto sm = make_staged_mutation(staged_mutation_type::REMOVE, "op3");

  assert_eq(sm.type_as_string(), "REMOVE", "the ATR label for a staged remove");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(type_as_string_returns_a_string_view), {}, timeout::instant },
      { CASE(an_insert_is_labelled_insert), {}, timeout::instant },
      { CASE(a_replace_is_labelled_replace), {}, timeout::instant },
      { CASE(a_remove_is_labelled_remove), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
