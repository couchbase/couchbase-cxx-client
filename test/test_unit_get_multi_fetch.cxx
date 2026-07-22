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

#include "core/transactions/get_multi_fetch.hxx"

#include <chrono>
#include <optional>
#include <string>
#include <vector>

using namespace couchbase::core::transactions;
using couchbase::core::document_id;

TEST_CASE("transactions: get_multi individual-fetch error classification", "[unit]")
{
  SECTION("no error class means the document is simply absent")
  {
    REQUIRE(classify_get_multi_fetch_error(std::nullopt, std::nullopt, false) ==
            get_multi_fetch_outcome::document_absent);
  }
  SECTION("document-unretrievable (replica mode) is absent, not a failure")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_OTHER, DOCUMENT_UNRETRIEVABLE_EXCEPTION, false) ==
            get_multi_fetch_outcome::document_absent);
  }
  SECTION("document-not-found is absent")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND,
                                           DOCUMENT_NOT_FOUND_EXCEPTION,
                                           false) == get_multi_fetch_outcome::document_absent);
    REQUIRE(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND, std::nullopt, false) ==
            get_multi_fetch_outcome::document_absent);
  }
  SECTION("a transient class before the bound retries with backoff")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, false) ==
            get_multi_fetch_outcome::retry_after_backoff);
    REQUIRE(classify_get_multi_fetch_error(FAIL_AMBIGUOUS, std::nullopt, false) ==
            get_multi_fetch_outcome::retry_after_backoff);
  }
  SECTION("a transient class after the bound is bound-exceeded")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, true) ==
            get_multi_fetch_outcome::bound_exceeded);
    REQUIRE(classify_get_multi_fetch_error(FAIL_AMBIGUOUS, std::nullopt, true) ==
            get_multi_fetch_outcome::bound_exceeded);
  }
  SECTION("document-absent and bound-exceeded stay distinct outcomes")
  {
    // The orchestrator records an absent document as empty but, on a mere bound-exceeded, must
    // preserve the value already fetched for a best-effort snapshot. The two must not collapse to
    // one outcome, or the orchestrator could no longer tell them apart.
    REQUIRE(classify_get_multi_fetch_error(FAIL_DOC_NOT_FOUND, std::nullopt, true) ==
            get_multi_fetch_outcome::document_absent);
    REQUIRE(classify_get_multi_fetch_error(FAIL_TRANSIENT, std::nullopt, true) !=
            get_multi_fetch_outcome::document_absent);
  }
  SECTION("FAIL_EXPIRY fails as expired")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_EXPIRY, std::nullopt, false) ==
            get_multi_fetch_outcome::fail_expired);
  }
  SECTION("FAIL_HARD fails without rollback")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_HARD, std::nullopt, false) ==
            get_multi_fetch_outcome::fail_without_rollback);
  }
  SECTION("any other class fails with rollback")
  {
    REQUIRE(classify_get_multi_fetch_error(FAIL_OTHER, std::nullopt, false) ==
            get_multi_fetch_outcome::fail_with_rollback);
    REQUIRE(classify_get_multi_fetch_error(FAIL_CAS_MISMATCH, std::nullopt, false) ==
            get_multi_fetch_outcome::fail_with_rollback);
  }
}

TEST_CASE("transactions: get_multi bound-exceeded action", "[unit]")
{
  SECTION(
    "a document already fetched in an earlier round is preserved for the best-effort snapshot")
  {
    REQUIRE(get_multi_bound_exceeded_action(true) == bound_exceeded_action::preserve_prior_value);
  }
  SECTION("a document with no fetched value fails retryably rather than being reported absent")
  {
    // Regression guard: after reset_and_retry clears the slots, a transient at the elapsed bound
    // must not leave the slot blank (which the result mapping would misreport as not-found).
    REQUIRE(get_multi_bound_exceeded_action(false) == bound_exceeded_action::fail_retryable);
  }
}

TEST_CASE("transactions: get_multi per-document fetch timeout", "[unit]")
{
  using namespace std::chrono_literals;
  const auto kv = 2500ms;

  SECTION("remaining larger than the key-value default is capped at the default")
  {
    REQUIRE(get_multi_fetch_timeout(10s, kv) == kv);
  }
  SECTION("remaining smaller than the key-value default is used as-is")
  {
    REQUIRE(get_multi_fetch_timeout(100ms, kv) == 100ms);
  }
  SECTION("a non-positive remaining collapses to the 1ms floor")
  {
    REQUIRE(get_multi_fetch_timeout(0ms, kv) == 1ms);
    REQUIRE(get_multi_fetch_timeout(-5s, kv) == 1ms);
  }
}

TEST_CASE("transactions: get_multi read-skew victim selection", "[unit]")
{
  const document_id id{ "d", "s", "c", "k" };
  const std::string t1{ "attempt-1" };
  const std::optional<std::vector<doc_record>> mutated{ std::vector<doc_record>{
    doc_record{ "d", "s", "c", "k" } } };
  const std::optional<std::vector<doc_record>> none{};

  SECTION("a document that does not exist is never a victim")
  {
    REQUIRE_FALSE(is_read_skew_victim(false, std::nullopt, t1, id, mutated, none, none));
  }
  SECTION("a document already fetched as part of T1 is not a victim")
  {
    const std::optional<transaction_id> in_t1{ transaction_id{ "txn", t1 } };
    REQUIRE_FALSE(is_read_skew_victim(true, in_t1, t1, id, mutated, none, none));
  }
  SECTION("a document not in T1 but recorded as mutated by T1 is a victim")
  {
    REQUIRE(is_read_skew_victim(true, std::nullopt, t1, id, mutated, none, none));
  }
  SECTION("a document from a different attempt recorded as mutated by T1 is a victim")
  {
    const std::optional<transaction_id> other{ transaction_id{ "txn", "attempt-2" } };
    REQUIRE(is_read_skew_victim(true, other, t1, id, mutated, none, none));
  }
  SECTION("a document not recorded in any of T1's mutation lists is not a victim")
  {
    REQUIRE_FALSE(is_read_skew_victim(true, std::nullopt, t1, id, none, none, none));
  }
  SECTION("the replaced and removed lists are matched too")
  {
    REQUIRE(is_read_skew_victim(true, std::nullopt, t1, id, none, mutated, none));
    REQUIRE(is_read_skew_victim(true, std::nullopt, t1, id, none, none, mutated));
  }
}
