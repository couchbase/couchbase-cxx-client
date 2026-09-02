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

#include "core/transactions/get_multi_transaction_id.hxx"

#include <cstddef>
#include <optional>
#include <set>

namespace couchbase::test
{
namespace
{
using couchbase::core::transactions::make_transaction_id;
using couchbase::core::transactions::transaction_id;

// The regression guard for the read-skew reset-and-retry hang: two documents staged by the same
// transaction attempt carry different per-mutation operation ids, but must still be recognised as
// belonging to a single transaction. If the operation id were part of the identity they would count
// as two transactions and disambiguation would loop until timeout.
void
transaction_identity_ignores_the_operation_id([[maybe_unused]] context& ctx)
{
  const auto a = make_transaction_id("txn-A", "attempt-1", "op-1");
  const auto b = make_transaction_id("txn-A", "attempt-1", "op-2");

  assert_true(a.has_value(), "a fully staged document yields an identity");
  assert_true(b.has_value(), "a fully staged document yields an identity");
  assert_true(a.value() == b.value(), "two operations of one attempt are one transaction");
}

void
transaction_identity_distinguishes_attempts_and_transactions([[maybe_unused]] context& ctx)
{
  const auto base = make_transaction_id("txn-A", "attempt-1", "op-1");
  const auto other_attempt = make_transaction_id("txn-A", "attempt-2", "op-1");
  const auto other_txn = make_transaction_id("txn-B", "attempt-1", "op-1");

  assert_true(base.has_value(), "a fully staged document yields an identity");
  assert_true(other_attempt.has_value(), "a fully staged document yields an identity");
  assert_true(other_txn.has_value(), "a fully staged document yields an identity");
  assert_false(base.value() == other_attempt.value(), "a second attempt is a distinct identity");
  assert_false(base.value() == other_txn.value(), "a second transaction is a distinct identity");
}

// A genuinely staged document always carries transaction + attempt + operation. A missing id means
// the document is not staged, so no transaction identity can be derived.
void
a_transaction_id_requires_the_full_staged_triplet([[maybe_unused]] context& ctx)
{
  assert_false(make_transaction_id("txn-A", "attempt-1", std::nullopt).has_value(),
               "a missing operation id yields no identity");
  assert_false(make_transaction_id("txn-A", std::nullopt, "op-1").has_value(),
               "a missing attempt id yields no identity");
  assert_false(make_transaction_id(std::nullopt, "attempt-1", "op-1").has_value(),
               "a missing transaction id yields no identity");
  assert_false(make_transaction_id(std::nullopt, std::nullopt, std::nullopt).has_value(),
               "a document with no staged ids at all yields no identity");
}

// Mirrors how read-skew disambiguation groups fetched documents: a std::set<transaction_id> keyed
// on (transaction, attempt). Two documents staged by the same attempt collapse to one entry; a
// document from a different attempt is a distinct transaction.
void
disambiguation_groups_documents_by_transaction_attempt([[maybe_unused]] context& ctx)
{
  std::set<transaction_id> transactions;
  transactions.insert(make_transaction_id("txn-A", "attempt-1", "op-1").value());
  transactions.insert(make_transaction_id("txn-A", "attempt-1", "op-2").value());
  transactions.insert(make_transaction_id("txn-A", "attempt-2", "op-3").value());

  assert_eq(transactions.size(), std::size_t{ 2 }, "three documents from two attempts");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(transaction_identity_ignores_the_operation_id), {}, timeout::instant },
      { CASE(transaction_identity_distinguishes_attempts_and_transactions), {}, timeout::instant },
      { CASE(a_transaction_id_requires_the_full_staged_triplet), {}, timeout::instant },
      { CASE(disambiguation_groups_documents_by_transaction_attempt), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
