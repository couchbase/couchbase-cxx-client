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
 * Unit tests for error_class_from_response<lookup_in_response> (CXXCBC-810): a document-level
 * success with absent per-field (subdoc) paths is classified as success, not an error, so a
 * non-transacted document read is decided by inspecting links rather than by a path-not-found
 * error class.
 */

#include "test_helper.hxx"

#include "core/error_context/subdocument_error_context.hxx"
#include "core/operations/document_lookup_in.hxx"
#include "core/protocol/client_opcode.hxx"
#include "core/transactions/error_class.hxx"
#include "core/transactions/internal/utils.hxx"

#include <couchbase/error_codes.hxx>

#include <optional>

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

// A lookup_in whose document level succeeds but whose individual paths fail is NOT an error. When a
// document is not in a transaction the txn.* XATTR paths are absent, so the server returns
// subdoc_multi_path_failure: a document-level success (empty ctx.ec()) with the missing paths
// recorded per field. The generic error_class_from_response classifies this as success (nullopt),
// and the caller decides transaction membership by inspecting the parsed links -- matching the
// reference SDKs and the spec, where a document-level FAIL_PATH_NOT_FOUND "can never trigger" on
// the read path. (A lookup_in specialization that promoted the absent txn.* path to
// FAIL_PATH_NOT_FOUND used to exist here and was removed for exactly this reason.)

TEST_CASE("transactions: error_class_from_response(lookup_in): document-level success with failed "
          "per-field paths is not an error",
          "[unit]")
{
  lookup_in_response resp;
  resp.ctx = make_subdoc_ctx({}, /*first_error_index=*/0);
  resp.fields.push_back(make_field(couchbase::errc::key_value::path_not_found));

  REQUIRE_FALSE(error_class_from_response(resp).has_value());
}
