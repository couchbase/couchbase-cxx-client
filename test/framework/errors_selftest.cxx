/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// What framework/errors.hxx prints when it fails.
//
// assert_success replaces around 3,000 REQUIRE_SUCCESS call sites, so its failure message is what
// most failures in the migrated suite will look like. These cases pin that message. They need the
// client library for couchbase::error, but no server: every value here is constructed by hand.

#include "framework/errors.hxx"
#include "framework/test_registry.hxx"

#include <couchbase/error_codes.hxx>

#include <string>
#include <system_error>

namespace couchbase::test
{
namespace
{
auto
message_of(void (*fn)()) -> std::string
{
  try {
    fn();
  } catch (const test_assertion_failure& e) {
    return e.what();
  }
  return {};
}

void
a_success_passes([[maybe_unused]] context& ctx)
{
  assert_success(std::error_code{}, "a default error_code is success");
  assert_success(couchbase::error{}, "and so is a default error");
}

void
a_failed_error_code_names_its_category_value_and_message([[maybe_unused]] context& ctx)
{
  const auto message = message_of([]() {
    assert_success(make_error_code(couchbase::errc::key_value::document_not_found));
  });

  assert_contains(message, "couchbase.key_value", "the category is named");
  assert_contains(message,
                  std::to_string(static_cast<int>(couchbase::errc::key_value::document_not_found)),
                  "the value is named");
  assert_contains(message, "document_not_found", "and so is the message");
}

void
a_failed_error_names_its_message_and_cause([[maybe_unused]] context& ctx)
{
  const auto message = message_of([]() {
    const couchbase::error cause{ make_error_code(couchbase::errc::common::unambiguous_timeout),
                                  "the inner reason" };
    const couchbase::error error{
      make_error_code(couchbase::errc::common::request_canceled), "the outer reason", {}, cause
    };
    assert_success(error);
  });

  assert_contains(message, "the outer reason", "the error's own message is reported");
  assert_contains(message, "caused by", "the cause chain is followed");
  assert_contains(message, "the inner reason", "down to the cause's message");
}

void
assert_error_distinguishes_one_failure_from_another([[maybe_unused]] context& ctx)
{
  const auto expected = make_error_code(couchbase::errc::key_value::document_not_found);
  assert_error(expected, expected, "the expected error passes");

  const auto message = message_of([]() {
    assert_error(make_error_code(couchbase::errc::common::unambiguous_timeout),
                 make_error_code(couchbase::errc::key_value::document_not_found));
  });
  assert_contains(message, "unambiguous_timeout", "the failure names what actually happened");
  assert_contains(message, "document_not_found", "and what was expected instead");
}

void
error_codes_format_as_operands([[maybe_unused]] context& ctx)
{
  // assert_eq prints operands only where fmt can format them. Without the specialisation in
  // errors.hxx, a mismatch between two error codes would say "expected equal" and nothing else.
  const auto message = message_of([]() {
    assert_eq(make_error_code(couchbase::errc::common::unambiguous_timeout),
              make_error_code(couchbase::errc::common::request_canceled));
  });
  assert_contains(message, "unambiguous_timeout", "the actual code renders");
  assert_contains(message, "request_canceled", "and so does the expected one");

  assert_eq(fmt::format("{}", std::error_code{}), std::string{ "success" }, "success renders");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_success_passes) },
      { CASE(a_failed_error_code_names_its_category_value_and_message) },
      { CASE(a_failed_error_names_its_message_and_cause) },
      { CASE(assert_error_distinguishes_one_failure_from_another) },
      { CASE(error_codes_format_as_operands) },
    },
  };
}

} // namespace couchbase::test
