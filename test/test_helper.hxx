/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "utils/binary.hxx"
#include "utils/test_context.hxx"
#include "utils/test_data.hxx"

#include <couchbase/error.hxx>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_tostring.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <tao/json/stream.hpp>

#include <system_error>

namespace Catch
{

template<>
struct StringMaker<std::error_code> {
  static auto convert(const std::error_code& ec) -> std::string
  {
    if (!ec) {
      return "success";
    }
    return ec.category().name() + std::string(":") + std::to_string(ec.value()) + " (" +
           ec.message() + ")";
  }
};

template<>
struct StringMaker<couchbase::error> {
  static auto convert(const couchbase::error& e) -> std::string
  {
    std::string result = StringMaker<std::error_code>::convert(e.ec());
    if (!e.message().empty()) {
      result += " - " + e.message();
    }
    if (e.ctx()) {
      result += " | " + e.ctx().to_json();
    }
    if (auto cause = e.cause(); cause.has_value()) {
      result += " (caused by: " + convert(cause.value()) + ")";
    }
    return result;
  }
};

} // namespace Catch

namespace couchbase::test
{

namespace detail
{
inline auto
extract_ec(std::error_code ec) -> std::error_code
{
  return ec;
}
inline auto
extract_ec(const couchbase::error& e) -> std::error_code
{
  return e.ec();
}
} // namespace detail

class IsSuccessErrorCode : public Catch::Matchers::MatcherBase<std::error_code>
{
public:
  auto match(const std::error_code& ec) const -> bool override
  {
    return !ec;
  }

  auto describe() const -> std::string override
  {
    return "is a success error_code";
  }
};

} // namespace couchbase::test

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REQUIRE_SUCCESS(arg)                                                                       \
  REQUIRE_THAT(couchbase::test::detail::extract_ec(arg), couchbase::test::IsSuccessErrorCode())
#define EXPECT_SUCCESS(result)                                                                     \
  do {                                                                                             \
    auto&& couchbase_result_ = (result);                                                           \
    auto couchbase_ec_ = couchbase_result_ ? std::error_code{} : couchbase_result_.error();        \
    REQUIRE_THAT(couchbase_ec_, couchbase::test::IsSuccessErrorCode());                            \
  } while (false)
