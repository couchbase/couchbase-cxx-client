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

#include "core/transactions/get_multi_transaction_id.hxx"

#include <optional>
#include <set>
#include <string>

using couchbase::core::transactions::make_transaction_id;
using couchbase::core::transactions::transaction_id;

TEST_CASE("unit: get_multi transaction identity ignores the operation id", "[unit]")
{
  // This is the regression guard for the read-skew reset-and-retry hang: two documents staged by
  // the same transaction attempt carry different per-mutation operation ids, but must still be
  // recognised as belonging to a single transaction. If the operation id were part of the identity
  // they would count as two transactions and disambiguation would loop until timeout.
  const auto a = make_transaction_id("txn-A", "attempt-1", "op-1");
  const auto b = make_transaction_id("txn-A", "attempt-1", "op-2");
  REQUIRE(a.has_value());
  REQUIRE(b.has_value());
  REQUIRE(a.value() == b.value());
}

TEST_CASE("unit: get_multi transaction identity distinguishes attempts and transactions", "[unit]")
{
  const auto base = make_transaction_id("txn-A", "attempt-1", "op-1");
  const auto other_attempt = make_transaction_id("txn-A", "attempt-2", "op-1");
  const auto other_txn = make_transaction_id("txn-B", "attempt-1", "op-1");

  REQUIRE(base.has_value());
  REQUIRE(other_attempt.has_value());
  REQUIRE(other_txn.has_value());
  REQUIRE_FALSE(base.value() == other_attempt.value());
  REQUIRE_FALSE(base.value() == other_txn.value());
}

TEST_CASE("unit: get_multi transaction id requires the full staged triplet", "[unit]")
{
  // A genuinely staged document always carries transaction + attempt + operation. A missing id
  // means the document is not staged, so no transaction identity can be derived.
  REQUIRE_FALSE(make_transaction_id("txn-A", "attempt-1", std::nullopt).has_value());
  REQUIRE_FALSE(make_transaction_id("txn-A", std::nullopt, "op-1").has_value());
  REQUIRE_FALSE(make_transaction_id(std::nullopt, "attempt-1", "op-1").has_value());
  REQUIRE_FALSE(make_transaction_id(std::nullopt, std::nullopt, std::nullopt).has_value());
}

TEST_CASE("unit: get_multi disambiguation groups documents by transaction attempt", "[unit]")
{
  // Mirrors how read-skew disambiguation groups fetched documents: a std::set<transaction_id>
  // keyed on (transaction, attempt). Two documents staged by the same attempt collapse to one
  // entry; a document from a different attempt is a distinct transaction.
  std::set<transaction_id> transactions;
  transactions.insert(make_transaction_id("txn-A", "attempt-1", "op-1").value());
  transactions.insert(make_transaction_id("txn-A", "attempt-1", "op-2").value());
  transactions.insert(make_transaction_id("txn-A", "attempt-2", "op-3").value());

  REQUIRE(transactions.size() == 2);
}
