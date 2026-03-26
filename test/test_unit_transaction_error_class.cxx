/*
 *     Copyright 2024 Couchbase, Inc.
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

/**
 * Unit tests for the transaction utility functions introduced or changed in CXXCBC-810:
 *
 *  1. error_class_from_response<lookup_in_response> — the new specialization that
 *     inspects per-field subdoc errors when the document-level ctx.ec() is success.
 *  2. atr_entry::has_expired — the safety-guard that now requires both
 *     timestamp_start_ms_ and expires_after_ms_ to be present before declaring
 *     an entry expired.
 *  3. staged_mutation::type_as_string — returns std::string_view, not std::string.
 */

#include "test_helper.hxx"

#include "core/error_context/subdocument_error_context.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/transactions/error_class.hxx"
#include "core/transactions/internal/atr_entry.hxx"
#include "core/transactions/internal/utils.hxx"
#include "core/transactions/staged_mutation.hxx"

#include <couchbase/error_codes.hxx>

#include <optional>
#include <string_view>
#include <type_traits>

using namespace couchbase::core::transactions;
using couchbase::core::key_value_status_code;
using couchbase::core::subdocument_error_context;
using couchbase::core::operations::lookup_in_response;
using couchbase::core::protocol::subdoc_opcode;

namespace
{

/** Build a minimal subdocument_error_context with only the fields we care about. */
auto
make_subdoc_ctx(std::error_code doc_ec, std::optional<std::size_t> first_error_index = std::nullopt)
  -> subdocument_error_context
{
  return subdocument_error_context{
    /*operation_id=*/"op",
    /*ec=*/doc_ec,
    /*last_dispatched_to=*/std::nullopt,
    /*last_dispatched_from=*/std::nullopt,
    /*retry_attempts=*/0,
    /*retry_reasons=*/{},
    /*id=*/"key",
    /*bucket=*/"b",
    /*scope=*/"s",
    /*collection=*/"c",
    /*opaque=*/0,
    /*status_code=*/std::nullopt,
    /*cas=*/couchbase::cas{},
    /*error_map_info=*/std::nullopt,
    /*extended_error_info=*/std::nullopt,
    /*first_error_path=*/std::nullopt,
    /*first_error_index=*/first_error_index,
    /*deleted=*/false,
  };
}

/** Build a lookup_in_response::entry with a per-field error code. */
auto
make_field(std::error_code field_ec) -> lookup_in_response::entry
{
  return lookup_in_response::entry{
    /*path=*/"txn.id",
    /*value=*/{},
    /*original_index=*/0,
    /*exists=*/false,
    /*opcode=*/subdoc_opcode::get,
    /*status=*/key_value_status_code::success,
    /*ec=*/field_ec,
  };
}

/**
 * Build an atr_entry with CAS and optional start/expiry values.
 * The CAS encodes "current time" in nanoseconds; the entry start is in milliseconds.
 * elapsed_ms = (cas / 1_000_000) - start_ms
 */
auto
make_atr_entry(std::uint64_t cas_ns,
               std::optional<std::uint64_t> start_ms,
               std::optional<std::uint32_t> expires_after_ms) -> atr_entry
{
  return atr_entry{
    /*atr_bucket=*/"b",
    /*atr_id=*/"id",
    /*attempt_id=*/"attempt",
    /*state=*/attempt_state::PENDING,
    /*timestamp_start_ms=*/start_ms,
    /*timestamp_commit_ms=*/std::nullopt,
    /*timestamp_complete_ms=*/std::nullopt,
    /*timestamp_rollback_ms=*/std::nullopt,
    /*timestamp_rolled_back_ms=*/std::nullopt,
    /*expires_after_ms=*/expires_after_ms,
    /*inserted_ids=*/std::nullopt,
    /*replaced_ids=*/std::nullopt,
    /*removed_ids=*/std::nullopt,
    /*forward_compat=*/std::nullopt,
    /*cas=*/cas_ns,
    /*durability_level=*/std::nullopt,
  };
}

} // namespace

// ---------------------------------------------------------------------------
// 1.  error_class_from_response<lookup_in_response> — document-level errors
// ---------------------------------------------------------------------------

TEST_CASE("transactions: error_class_from_response(lookup_in): no error yields nullopt", "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({});
  REQUIRE_FALSE(error_class_from_response(resp).has_value());
}

TEST_CASE("transactions: error_class_from_response(lookup_in): document_not_found", "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::key_value::document_not_found);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_DOC_NOT_FOUND);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): document_exists", "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::key_value::document_exists);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_DOC_ALREADY_EXISTS);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): cas_mismatch", "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::common::cas_mismatch);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_CAS_MISMATCH);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): unambiguous_timeout is transient",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::common::unambiguous_timeout);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_TRANSIENT);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): temporary_failure is transient",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::common::temporary_failure);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_TRANSIENT);
}

TEST_CASE(
  "transactions: error_class_from_response(lookup_in): durable_write_in_progress is transient",
  "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::key_value::durable_write_in_progress);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_TRANSIENT);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): durability_ambiguous is ambiguous",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::key_value::durability_ambiguous);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_AMBIGUOUS);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): ambiguous_timeout is ambiguous",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::common::ambiguous_timeout);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_AMBIGUOUS);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): request_canceled is ambiguous",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx(couchbase::errc::common::request_canceled);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_AMBIGUOUS);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): unknown doc-level error is other",
          "[unit]")
{
  lookup_in_response resp;
  // value_too_large is not special-cased in the lookup_in specialization
  resp.ctx = make_subdoc_ctx(couchbase::errc::key_value::value_too_large);
  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_OTHER);
}

// ---------------------------------------------------------------------------
// 2.  error_class_from_response<lookup_in_response> — per-field (subdoc) errors
//     These fire when ctx.ec() == {} but a spec within the response failed.
// ---------------------------------------------------------------------------

TEST_CASE("transactions: error_class_from_response(lookup_in): per-field path_not_found is "
          "FAIL_PATH_NOT_FOUND",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/0);
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_not_found));

  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_PATH_NOT_FOUND);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): per-field path_exists is "
          "FAIL_PATH_ALREADY_EXISTS",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/0);
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_exists));

  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_PATH_ALREADY_EXISTS);
}

TEST_CASE("transactions: error_class_from_response(lookup_in): per-field unknown error is other",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/0);
  resp.fields.push_back(make_field(couchbase::errc::key_value::value_too_large));

  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_OTHER);
}

TEST_CASE(
  "transactions: error_class_from_response(lookup_in): per-field success with first_error_index "
  "yields nullopt",
  "[unit]")
{
  // first_error_index points to a field whose ec is success — should return nullopt
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/0);
  resp.fields.push_back(make_field({})); // ec == success

  const auto ec = error_class_from_response(resp);
  REQUIRE_FALSE(ec.has_value());
}

TEST_CASE(
  "transactions: error_class_from_response(lookup_in): first_error_index out of range yields "
  "nullopt",
  "[unit]")
{
  // first_error_index points beyond the fields vector — should not crash and returns nullopt
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/5);
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_not_found));

  const auto ec = error_class_from_response(resp);
  REQUIRE_FALSE(ec.has_value());
}

TEST_CASE(
  "transactions: error_class_from_response(lookup_in): no first_error_index but fields present "
  "yields nullopt",
  "[unit]")
{
  // No per-field error signalled via first_error_index — nullopt even if fields contain errors
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, std::nullopt);
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_not_found));

  const auto ec = error_class_from_response(resp);
  REQUIRE_FALSE(ec.has_value());
}

TEST_CASE("transactions: error_class_from_response(lookup_in): second field error reported via "
          "first_error_index",
          "[unit]")
{
  // Two fields; only index 1 failed
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/1);
  resp.fields.push_back(make_field({}));                                         // index 0: ok
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_not_found)); // index 1: fail

  const auto ec = error_class_from_response(resp);
  REQUIRE(ec.has_value());
  REQUIRE(*ec == FAIL_PATH_NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 3.  atr_entry::has_expired — fixed guard for missing expires_after_ms_
// ---------------------------------------------------------------------------

TEST_CASE("transactions: atr_entry::has_expired: clearly expired entry", "[unit]")
{
  // start=1000ms, ttl=500ms, cas encodes 2000ms → elapsed=1000ms > 500ms → expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 2000ULL * 1'000'000ULL; // 2000ms as nanoseconds
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);
  REQUIRE(entry.has_expired());
}

TEST_CASE("transactions: atr_entry::has_expired: entry not yet expired", "[unit]")
{
  // start=1000ms, ttl=5000ms, cas encodes 1100ms → elapsed=100ms < 5000ms → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 5000U;
  const std::uint64_t cas_ns = 1100ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);
  REQUIRE_FALSE(entry.has_expired());
}

TEST_CASE("transactions: atr_entry::has_expired: returns false when expires_after_ms is absent",
          "[unit]")
{
  // Before the fix this could lead to UB or incorrect expiry detection.
  // Now it must conservatively return false.
  const std::uint64_t start_ms = 1000ULL;
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL; // far in the future
  const auto entry = make_atr_entry(cas_ns, start_ms, /*expires_after_ms=*/std::nullopt);
  REQUIRE_FALSE(entry.has_expired());
}

TEST_CASE("transactions: atr_entry::has_expired: returns false when timestamp_start_ms is absent",
          "[unit]")
{
  const std::uint32_t ttl_ms = 100U;
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, /*start_ms=*/std::nullopt, ttl_ms);
  REQUIRE_FALSE(entry.has_expired());
}

TEST_CASE("transactions: atr_entry::has_expired: returns false when both timestamps absent",
          "[unit]")
{
  const std::uint64_t cas_ns = 9999ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, std::nullopt, std::nullopt);
  REQUIRE_FALSE(entry.has_expired());
}

TEST_CASE(
  "transactions: atr_entry::has_expired: returns false when cas has not advanced past start",
  "[unit]")
{
  // cas_ms <= start_ms — the guard (cas_ms > start_ms) is false → returns false
  const std::uint64_t start_ms = 2000ULL;
  const std::uint32_t ttl_ms = 100U;
  const std::uint64_t cas_ns = 1500ULL * 1'000'000ULL; // cas_ms=1500 < start_ms=2000
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);
  REQUIRE_FALSE(entry.has_expired());
}

TEST_CASE("transactions: atr_entry::has_expired: safety_margin extends effective TTL", "[unit]")
{
  // start=1000ms, ttl=500ms, cas encodes 1600ms → elapsed=600ms
  // Without margin: 600 > 500 → expired
  // With margin 200: 600 > 700? No → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 1600ULL * 1'000'000ULL;
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);
  REQUIRE(entry.has_expired(/*safety_margin=*/0));
  REQUIRE_FALSE(entry.has_expired(/*safety_margin=*/200));
}

TEST_CASE("transactions: atr_entry::has_expired: exactly at boundary is not expired", "[unit]")
{
  // elapsed == ttl (not strictly greater) → not expired
  const std::uint64_t start_ms = 1000ULL;
  const std::uint32_t ttl_ms = 500U;
  const std::uint64_t cas_ns = 1500ULL * 1'000'000ULL; // elapsed == ttl
  const auto entry = make_atr_entry(cas_ns, start_ms, ttl_ms);
  REQUIRE_FALSE(entry.has_expired());
}

// ---------------------------------------------------------------------------
// 4.  staged_mutation::type_as_string — returns std::string_view (zero allocation)
// ---------------------------------------------------------------------------

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
