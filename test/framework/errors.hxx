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
#include <system_error>

namespace couchbase::test
{

[[nodiscard]] inline auto
describe(const std::error_code& ec) -> std::string
{
  if (!ec) {
    return "success";
  }
  return std::string{ ec.category().name() } + ':' + std::to_string(ec.value()) + " (" +
         ec.message() + ")";
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

// So assert_eq and assert_ne on error values print operands rather than nothing. A class template
// specialisation is found at the point of instantiation, so it works from a header included after
// test_framework.hxx -- which a free function overload would not.
template<>
struct operand_printer<std::error_code> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const std::error_code& ec) -> std::string
  {
    return describe(ec);
  }
};

template<>
struct operand_printer<couchbase::error> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const couchbase::error& error) -> std::string
  {
    return describe(error);
  }
};

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
