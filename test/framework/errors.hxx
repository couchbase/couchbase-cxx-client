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

#pragma once

// Asserting that an operation succeeded.
//
// This is the suite's most common assertion by a wide margin -- REQUIRE_SUCCESS and its
// descendants outnumber every other check in the tree -- so what it prints on failure is what
// most failures look like. It has to name
// the category, the value and the message, and for couchbase::error the context and the cause
// chain, because "expected equal" identifies nothing.
//
// NOT lean: it names couchbase::error, so a test includes it only when it asserts on one.

#include "test_framework.hxx"

#include <couchbase/error.hxx>

#include <string>
#include <string_view>
#include <system_error>

namespace couchbase::test
{

// One spelling for an error code, shared with the one assert_eq prints, so a failure and a message
// built by hand cannot describe the same code differently.
[[nodiscard]] inline auto
describe(const std::error_code& ec) -> std::string
{
  return operand_printer<std::error_code>::to_text(ec);
}

[[nodiscard]] inline auto
describe(const couchbase::error& error) -> std::string
{
  std::string result = describe(error.ec());
  if (!error.message().empty()) {
    result += " - " + error.message();
  }
  if (error.ctx()) {
    result += " | " + error.ctx().to_json();
  }
  if (const auto cause = error.cause(); cause.has_value()) {
    result += " (caused by: " + describe(cause.value()) + ")";
  }
  return result;
}

// So assert_eq and assert_ne on a couchbase::error print operands rather than nothing.
//
// Overloads, not an operand_printer specialisation. The template is declared in
// test_framework.hxx, which every test includes, while this header is included only where a test
// asserts on an error -- so a specialisation here is visible in some translation units of a binary
// and not others, and assert_eq<couchbase::error, ...> then has two definitions in one program.
// That is the same hazard that keeps the std::error_code printer in test_framework.hxx, and it
// applies to every type this header could add: `couchbase::error` reaches a test through
// <couchbase/error.hxx>, with its own operator==, whether or not this header came with it.
//
// An overload is resolved by ordinary lookup at the call, so a case body finds it however late this
// header is included. What it would not reach is a dependent call from a template defined above
// the include, because argument-dependent lookup from couchbase::error searches namespace
// couchbase and not couchbase::test. No test compares errors from a template.
inline void
assert_eq(const couchbase::error& actual,
          const couchbase::error& expected,
          std::string_view message = "expected equal",
          source_location loc = source_location::current())
{
  if (!(actual == expected)) {
    throw test_assertion_failure(detail::at(loc, message) + " (actual: " + describe(actual) +
                                 ", expected: " + describe(expected) + ")");
  }
}

inline void
assert_ne(const couchbase::error& actual,
          const couchbase::error& unexpected,
          std::string_view message = "expected different",
          source_location loc = source_location::current())
{
  if (actual == unexpected) {
    throw test_assertion_failure(detail::at(loc, message) + " (both are: " + describe(actual) +
                                 ")");
  }
}

inline void
assert_success(const std::error_code& ec,
               std::string_view message = "expected success",
               source_location loc = source_location::current())
{
  if (ec) {
    throw test_assertion_failure(detail::at(loc, message) + " (" + describe(ec) + ")");
  }
}

inline void
assert_success(const couchbase::error& error,
               std::string_view message = "expected success",
               source_location loc = source_location::current())
{
  if (error.ec()) {
    throw test_assertion_failure(detail::at(loc, message) + " (" + describe(error) + ")");
  }
}

// The error a case was expecting. Asserting only that *an* error came back is how a test keeps
// passing after the operation starts failing for an entirely different reason.
inline void
assert_error(const std::error_code& ec,
             const std::error_code& expected,
             std::string_view message = "expected a specific error",
             source_location loc = source_location::current())
{
  if (ec != expected) {
    throw test_assertion_failure(detail::at(loc, message) + " (actual: " + describe(ec) +
                                 ", expected: " + describe(expected) + ")");
  }
}

inline void
assert_error(const couchbase::error& error,
             const std::error_code& expected,
             std::string_view message = "expected a specific error",
             source_location loc = source_location::current())
{
  if (error.ec() != expected) {
    throw test_assertion_failure(detail::at(loc, message) + " (actual: " + describe(error) +
                                 ", expected: " + describe(expected) + ")");
  }
}

} // namespace couchbase::test
