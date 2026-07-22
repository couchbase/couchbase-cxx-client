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

#include "core/document_id.hxx"
#include "error_class.hxx"
#include "exceptions.hxx"
#include "get_multi_transaction_id.hxx"
#include "internal/doc_record.hxx"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

// Pure decision helpers for the get_multi orchestrator, factored out of get_multi_orchestrator.cxx
// so the branching that the ExtGetMulti spec specifies can be unit-tested without a cluster. Each
// function operates on plain data and is free of I/O or orchestrator state.
namespace couchbase::core::transactions
{
enum class get_multi_fetch_outcome : std::uint8_t {
  document_absent,
  retry_after_backoff,
  bound_exceeded,
  fail_expired,
  fail_without_rollback,
  fail_with_rollback,
};

enum class bound_exceeded_action : std::uint8_t {
  preserve_prior_value,
  fail_retryable,
};

/**
 * Decides what a bound-exceeded fetch does with its slot. A document already fetched in an earlier
 * round is kept for the best-effort snapshot the spec's BoundExceeded return allows. But when no
 * value is held for the document -- e.g. reset_and_retry() cleared every slot and the re-fetch hit
 * a transient with the (fixed, already-elapsed) bound -- leaving the slot blank would misreport the
 * document as not-found: a transient is never a document-not-found. The spec's "Global signal
 * handling" instead raises a retryable error when a document is unresolved at the bound, so the
 * enclosing transaction retries rather than returning a wrong absent result.
 */
[[nodiscard]] inline auto
get_multi_bound_exceeded_action(bool has_prior_value) -> bound_exceeded_action
{
  return has_prior_value ? bound_exceeded_action::preserve_prior_value
                         : bound_exceeded_action::fail_retryable;
}

/**
 * Maps a failed individual get_multi fetch to the action required by the ExtGetMulti spec's
 * "Fetching an individual document / Error handling" section, adapted to this SDK's error model.
 *
 * The spec branches on a distinct TimeoutException, which this SDK does not have: a read timeout
 * classifies as FAIL_TRANSIENT (unambiguous_timeout) or FAIL_AMBIGUOUS (ambiguous_timeout), the
 * same classes as a genuine transient failure. Rather than guess which occurred, a transient class
 * is disambiguated by whether the operation's read-skew bound has elapsed: before the bound it is a
 * transient error to retry (spec FAIL_TRANSIENT: exponential backoff from 1ms), once the bound has
 * elapsed the SDK stops resolving and returns the best-effort snapshot (spec BoundExceeded).
 * A FAIL_EXPIRY is surfaced as fail_expired so the caller can mark the operation expired (raising
 * TRANSACTION_EXPIRED) rather than a generic failure.
 */
[[nodiscard]] inline auto
classify_get_multi_fetch_error(std::optional<error_class> ec,
                               std::optional<external_exception> cause,
                               bool read_skew_bound_exceeded) -> get_multi_fetch_outcome
{
  if (!ec.has_value()) {
    return get_multi_fetch_outcome::document_absent;
  }
  if (cause == DOCUMENT_UNRETRIEVABLE_EXCEPTION || cause == DOCUMENT_NOT_FOUND_EXCEPTION ||
      ec == FAIL_DOC_NOT_FOUND) {
    return get_multi_fetch_outcome::document_absent;
  }
  if (ec == FAIL_TRANSIENT || ec == FAIL_AMBIGUOUS) {
    return read_skew_bound_exceeded ? get_multi_fetch_outcome::bound_exceeded
                                    : get_multi_fetch_outcome::retry_after_backoff;
  }
  if (ec == FAIL_EXPIRY) {
    return get_multi_fetch_outcome::fail_expired;
  }
  if (ec == FAIL_HARD) {
    return get_multi_fetch_outcome::fail_without_rollback;
  }
  return get_multi_fetch_outcome::fail_with_rollback;
}

/**
 * Per-document read timeout: the smaller of the time left before the operation's read-skew bound
 * and the SDK's key-value read timeout (spec "Fetching an individual document": use an operation
 * timeout of min(remaining deadline, 2.5s)). Floored at 1ms so an already-elapsed bound still
 * issues a bounded read that fails fast rather than passing a zero or negative timeout.
 */
[[nodiscard]] inline auto
get_multi_fetch_timeout(std::chrono::steady_clock::duration remaining,
                        std::chrono::milliseconds key_value_timeout) -> std::chrono::milliseconds
{
  const auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
  if (remaining_ms < std::chrono::milliseconds{ 1 }) {
    return std::chrono::milliseconds{ 1 };
  }
  return std::min(remaining_ms, key_value_timeout);
}

/**
 * True when a fetched document is a read-skew victim (spec "Read skew resolution", T1 COMMITTED): a
 * document we fetched that is NOT part of T1 -- it carries no transactional metadata, or belongs to
 * a different attempt -- yet which T1's ATR entry records as mutated (inserted/replaced/removed).
 * Such a document was read before T1 committed and must be re-fetched for a consistent snapshot.
 */
[[nodiscard]] inline auto
contains_mutation(const std::optional<std::vector<doc_record>>& mutated_ids,
                  const core::document_id& id) -> bool
{
  if (!mutated_ids) {
    return false;
  }
  return std::any_of(mutated_ids->begin(), mutated_ids->end(), [&id](const auto& mutation) {
    return mutation == id;
  });
}

[[nodiscard]] inline auto
is_read_skew_victim(bool doc_exists,
                    const std::optional<transaction_id>& doc_transaction_id,
                    const std::string& t1_attempt_id,
                    const core::document_id& id,
                    const std::optional<std::vector<doc_record>>& t1_inserted_ids,
                    const std::optional<std::vector<doc_record>>& t1_replaced_ids,
                    const std::optional<std::vector<doc_record>>& t1_removed_ids) -> bool
{
  if (!doc_exists) {
    return false;
  }
  const bool fetched_as_part_of_t1 =
    doc_transaction_id.has_value() && doc_transaction_id->attempt == t1_attempt_id;
  if (fetched_as_part_of_t1) {
    return false;
  }
  return contains_mutation(t1_inserted_ids, id) || contains_mutation(t1_replaced_ids, id) ||
         contains_mutation(t1_removed_ids, id);
}
} // namespace couchbase::core::transactions
