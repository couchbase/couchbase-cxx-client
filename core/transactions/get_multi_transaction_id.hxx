/*
 *     Copyright 2021-Present Couchbase, Inc.
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
#pragma once

#include <optional>
#include <string>

namespace couchbase::core::transactions
{
// Identifies the transaction attempt a document is staged in. Identity is the (transaction,
// attempt) pair only: the per-mutation operation id is deliberately NOT part of it. Read-skew
// detection groups the fetched documents by the *transaction* they belong to (spec "Document
// disambiguation"), and two documents staged by the same attempt always carry different operation
// ids -- including the operation id here would make them count as separate transactions and send
// disambiguation down the "two or more transactions, too complex" reset-and-retry path forever.
struct transaction_id {
  std::string transaction;
  std::string attempt;

  auto operator==(const transaction_id& other) const -> bool
  {
    return transaction == other.transaction && attempt == other.attempt;
  }
  auto operator<(const transaction_id& other) const -> bool
  {
    if (transaction != other.transaction) {
      return transaction < other.transaction;
    }
    return attempt < other.attempt;
  }
};

// Build a transaction_id from the staged (transaction, attempt, operation) ids carried on a
// document's transactional metadata. All three must be present -- a document genuinely staged in a
// transaction always carries the full triplet, so a missing id means the document is not staged --
// but the operation id is deliberately dropped from the resulting identity (see transaction_id).
// Returns nullopt if any of the three ids is absent.
[[nodiscard]] inline auto
make_transaction_id(const std::optional<std::string>& transaction,
                    const std::optional<std::string>& attempt,
                    const std::optional<std::string>& operation) -> std::optional<transaction_id>
{
  if (transaction && attempt && operation) {
    return transaction_id{ transaction.value(), attempt.value() };
  }
  return {};
}

} // namespace couchbase::core::transactions
