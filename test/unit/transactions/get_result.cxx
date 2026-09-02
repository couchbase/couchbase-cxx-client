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

#include "core/transactions/transaction_get_result.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>

#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
using couchbase::core::document_id;
using couchbase::core::transactions::transaction_get_result;

auto
row_id() -> document_id
{
  return { "default", "_default", "_default", "key" };
}

void
an_scas_string_populates_the_cas([[maybe_unused]] context& ctx)
{
  const transaction_get_result doc(
    row_id(),
    couchbase::core::utils::json::parse(R"({"scas":"1780000000000000000","doc":{"k":"v"}})"));

  assert_eq(doc.cas().value(), 1780000000000000000ULL, "the CAS decoded from the scas string");
  assert_false(doc.content().data.empty(), "the document body is carried alongside the CAS");
}

void
a_numeric_cas_populates_the_cas([[maybe_unused]] context& ctx)
{
  const transaction_get_result doc(
    row_id(), couchbase::core::utils::json::parse(R"({"cas":42,"doc":{"k":"v"}})"));

  assert_eq(doc.cas().value(), 42ULL, "the CAS decoded from the numeric cas field");
}

void
a_row_carrying_neither_scas_nor_cas_is_rejected([[maybe_unused]] context& ctx)
{
  try {
    [[maybe_unused]] const transaction_get_result doc(
      row_id(), couchbase::core::utils::json::parse(R"({"doc":{"k":"v"}})"));
    fail("a row with no CAS at all is rejected rather than accepted with a zero CAS");
  } catch (const std::system_error& e) {
    assert_eq(e.code(),
              std::error_code{ couchbase::errc::common::decoding_failure },
              "the rejection is reported as a decoding failure");
  }
}

void
a_row_with_an_scas_and_no_doc_is_accepted_with_empty_content([[maybe_unused]] context& ctx)
{
  const transaction_get_result doc(row_id(),
                                   couchbase::core::utils::json::parse(R"({"scas":"123"})"));

  assert_eq(doc.cas().value(), 123ULL, "the CAS decoded from the scas string");
  assert_true(doc.content().data.empty(), "a row with no doc field carries no content");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_scas_string_populates_the_cas), {}, timeout::instant },
      { CASE(a_numeric_cas_populates_the_cas), {}, timeout::instant },
      { CASE(a_row_carrying_neither_scas_nor_cas_is_rejected), {}, timeout::instant },
      { CASE(a_row_with_an_scas_and_no_doc_is_accepted_with_empty_content), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
