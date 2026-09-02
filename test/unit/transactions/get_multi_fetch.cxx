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

#include "framework/test_registry.hxx"

#include "core/transactions/get_multi_fetch.hxx"

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
using namespace couchbase::core::transactions;
using couchbase::core::document_id;
using namespace std::chrono_literals;

void
no_error_class_classifies_as_document_absent([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(std::nullopt, std::nullopt, false),
            get_multi_fetch_outcome::document_absent,
            "a fetch that reported no error class found no document");
}

void
document_unretrievable_classifies_as_document_absent([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_OTHER, DOCUMENT_UNRETRIEVABLE_EXCEPTION, false),
            get_multi_fetch_outcome::document_absent,
            "replica mode reporting the document unretrievable is absence, not failure");
}

void
document_not_found_classifies_as_document_absent([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND, DOCUMENT_NOT_FOUND_EXCEPTION, false),
            get_multi_fetch_outcome::document_absent,
            "document-not-found carrying its cause is absence");
  assert_eq(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND, std::nullopt, false),
            get_multi_fetch_outcome::document_absent,
            "the error class alone is enough to call it absence");
}

void
a_transient_class_before_the_bound_retries_with_backoff([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, false),
            get_multi_fetch_outcome::retry_after_backoff,
            "a transient failure inside the read-skew bound is retried");
  assert_eq(classify_get_multi_fetch_error(FAIL_AMBIGUOUS, std::nullopt, false),
            get_multi_fetch_outcome::retry_after_backoff,
            "an ambiguous failure inside the read-skew bound is retried");
}

void
a_transient_class_after_the_bound_is_bound_exceeded([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, true),
            get_multi_fetch_outcome::bound_exceeded,
            "a transient failure past the read-skew bound stops resolving");
  assert_eq(classify_get_multi_fetch_error(FAIL_AMBIGUOUS, std::nullopt, true),
            get_multi_fetch_outcome::bound_exceeded,
            "an ambiguous failure past the read-skew bound stops resolving");
}

// The orchestrator records an absent document as empty but, on a mere bound-exceeded, must
// preserve the value already fetched for a best-effort snapshot. The two must not collapse to
// one outcome, or the orchestrator could no longer tell them apart.
void
document_absent_and_bound_exceeded_stay_distinct([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND, std::nullopt, true),
            get_multi_fetch_outcome::document_absent,
            "the bound does not turn a missing document into a bound-exceeded");
  assert_ne(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, true),
            get_multi_fetch_outcome::document_absent,
            "the bound does not turn a transient failure into a missing document");
}

void
fail_expiry_classifies_as_fail_expired([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_EXPIRY, std::nullopt, false),
            get_multi_fetch_outcome::fail_expired,
            "an expiry is surfaced as expiry so the transaction can be marked expired");
}

void
fail_hard_classifies_as_fail_without_rollback([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_HARD, std::nullopt, false),
            get_multi_fetch_outcome::fail_without_rollback,
            "a hard failure is not rolled back");
}

void
any_other_class_classifies_as_fail_with_rollback([[maybe_unused]] context& ctx)
{
  assert_eq(classify_get_multi_fetch_error(FAIL_OTHER, std::nullopt, false),
            get_multi_fetch_outcome::fail_with_rollback,
            "an unclassified failure is rolled back");
  assert_eq(classify_get_multi_fetch_error(FAIL_CAS_MISMATCH, std::nullopt, false),
            get_multi_fetch_outcome::fail_with_rollback,
            "a CAS mismatch is rolled back");
}

void
a_bound_exceeded_document_with_a_prior_value_keeps_it([[maybe_unused]] context& ctx)
{
  assert_eq(get_multi_bound_exceeded_action(true),
            bound_exceeded_action::preserve_prior_value,
            "a document fetched in an earlier round survives into the best-effort snapshot");
}

// Regression guard: after reset_and_retry clears the slots, a transient at the elapsed bound
// must not leave the slot blank (which the result mapping would misreport as not-found).
void
a_bound_exceeded_document_with_no_value_fails_retryably([[maybe_unused]] context& ctx)
{
  assert_eq(get_multi_bound_exceeded_action(false),
            bound_exceeded_action::fail_retryable,
            "a document with no fetched value fails retryably rather than reading as absent");
}

void
a_remaining_larger_than_the_key_value_default_is_capped([[maybe_unused]] context& ctx)
{
  assert_eq(get_multi_fetch_timeout(10s, 2500ms).count(),
            2500,
            "the individual fetch timeout in milliseconds: no fetch outlives the key-value one");
}

void
a_remaining_smaller_than_the_key_value_default_is_used_as_is([[maybe_unused]] context& ctx)
{
  assert_eq(get_multi_fetch_timeout(100ms, 2500ms).count(),
            100,
            "the individual fetch timeout in milliseconds: no fetch outlives the bound");
}

void
a_non_positive_remaining_collapses_to_the_millisecond_floor([[maybe_unused]] context& ctx)
{
  assert_eq(get_multi_fetch_timeout(0ms, 2500ms).count(),
            1,
            "an exhausted bound still issues a bounded read");
  assert_eq(get_multi_fetch_timeout(-5s, 2500ms).count(),
            1,
            "an overshot bound is never passed through as a negative timeout");
}

auto
victim_id() -> document_id
{
  return { "d", "s", "c", "k" };
}

auto
mutating_t1() -> std::optional<std::vector<doc_record>>
{
  return std::vector<doc_record>{ doc_record{ "d", "s", "c", "k" } };
}

auto
no_mutations() -> std::optional<std::vector<doc_record>>
{
  return {};
}

void
a_document_that_does_not_exist_is_never_a_victim([[maybe_unused]] context& ctx)
{
  assert_false(
    is_read_skew_victim(
      false, std::nullopt, "attempt-1", victim_id(), mutating_t1(), no_mutations(), no_mutations()),
    "there is nothing to re-fetch for a document that was not read");
}

void
a_document_already_fetched_as_part_of_t1_is_not_a_victim([[maybe_unused]] context& ctx)
{
  const std::optional<transaction_id> in_t1{ transaction_id{ "txn", "attempt-1" } };

  assert_false(
    is_read_skew_victim(
      true, in_t1, "attempt-1", victim_id(), mutating_t1(), no_mutations(), no_mutations()),
    "a document read as part of T1 is already consistent with T1");
}

void
a_document_outside_t1_that_t1_mutated_is_a_victim([[maybe_unused]] context& ctx)
{
  assert_true(
    is_read_skew_victim(
      true, std::nullopt, "attempt-1", victim_id(), mutating_t1(), no_mutations(), no_mutations()),
    "a document with no transactional metadata that T1 inserted was read too early");
}

void
a_document_from_another_attempt_that_t1_mutated_is_a_victim([[maybe_unused]] context& ctx)
{
  const std::optional<transaction_id> other{ transaction_id{ "txn", "attempt-2" } };

  assert_true(
    is_read_skew_victim(
      true, other, "attempt-1", victim_id(), mutating_t1(), no_mutations(), no_mutations()),
    "another attempt's metadata does not make the document part of T1");
}

void
a_document_in_none_of_t1s_mutation_lists_is_not_a_victim([[maybe_unused]] context& ctx)
{
  assert_false(
    is_read_skew_victim(
      true, std::nullopt, "attempt-1", victim_id(), no_mutations(), no_mutations(), no_mutations()),
    "a document T1 never touched cannot have been read before T1 committed");
}

void
the_replaced_and_removed_lists_are_matched_too([[maybe_unused]] context& ctx)
{
  assert_true(
    is_read_skew_victim(
      true, std::nullopt, "attempt-1", victim_id(), no_mutations(), mutating_t1(), no_mutations()),
    "a document T1 replaced is a victim");
  assert_true(
    is_read_skew_victim(
      true, std::nullopt, "attempt-1", victim_id(), no_mutations(), no_mutations(), mutating_t1()),
    "a document T1 removed is a victim");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(no_error_class_classifies_as_document_absent), {}, timeout::instant },
      { CASE(document_unretrievable_classifies_as_document_absent), {}, timeout::instant },
      { CASE(document_not_found_classifies_as_document_absent), {}, timeout::instant },
      { CASE(a_transient_class_before_the_bound_retries_with_backoff), {}, timeout::instant },
      { CASE(a_transient_class_after_the_bound_is_bound_exceeded), {}, timeout::instant },
      { CASE(document_absent_and_bound_exceeded_stay_distinct), {}, timeout::instant },
      { CASE(fail_expiry_classifies_as_fail_expired), {}, timeout::instant },
      { CASE(fail_hard_classifies_as_fail_without_rollback), {}, timeout::instant },
      { CASE(any_other_class_classifies_as_fail_with_rollback), {}, timeout::instant },
      { CASE(a_bound_exceeded_document_with_a_prior_value_keeps_it), {}, timeout::instant },
      { CASE(a_bound_exceeded_document_with_no_value_fails_retryably), {}, timeout::instant },
      { CASE(a_remaining_larger_than_the_key_value_default_is_capped), {}, timeout::instant },
      { CASE(a_remaining_smaller_than_the_key_value_default_is_used_as_is), {}, timeout::instant },
      { CASE(a_non_positive_remaining_collapses_to_the_millisecond_floor), {}, timeout::instant },
      { CASE(a_document_that_does_not_exist_is_never_a_victim), {}, timeout::instant },
      { CASE(a_document_already_fetched_as_part_of_t1_is_not_a_victim), {}, timeout::instant },
      { CASE(a_document_outside_t1_that_t1_mutated_is_a_victim), {}, timeout::instant },
      { CASE(a_document_from_another_attempt_that_t1_mutated_is_a_victim), {}, timeout::instant },
      { CASE(a_document_in_none_of_t1s_mutation_lists_is_not_a_victim), {}, timeout::instant },
      { CASE(the_replaced_and_removed_lists_are_matched_too), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
