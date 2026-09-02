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

#include "framework/test_registry.hxx"

#include "core/utils/binary.hxx"
#include "profile.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/query_options.hxx>

#include <cstddef>
#include <utility>

namespace couchbase::test
{
namespace
{
using couchbase::core::utils::to_binary;

void
query_options_encode_positional_parameters_automatically([[maybe_unused]] context& ctx)
{
  const ::profile john{ "john", "John Doe", 1970 };
  auto options =
    couchbase::query_options{}.positional_parameters("foo", 42, 3.14, false, nullptr, john).build();

  assert_eq(options.positional_parameters.size(), std::size_t{ 6 }, "every parameter is encoded");
  assert_true(options.positional_parameters[0] == to_binary(R"("foo")"), "a string");
  assert_true(options.positional_parameters[1] == to_binary("42"), "an integer");
  assert_true(options.positional_parameters[2] == to_binary("3.14"), "a floating point number");
  assert_true(options.positional_parameters[3] == to_binary("false"), "a boolean");
  assert_true(options.positional_parameters[4] == to_binary("null"), "a null");
  assert_true(options.positional_parameters[5] ==
                to_binary(R"({"birth_year":1970,"full_name":"John Doe","username":"john"})"),
              "a user type with a serializer");
}

void
query_options_encode_named_parameters_automatically([[maybe_unused]] context& ctx)
{
  const ::profile john{ "john", "John Doe", 1970 };
  auto options = couchbase::query_options{}
                   .named_parameters(std::pair{ "str_param", "foo" },
                                     std::pair{ "int_param", 42 },
                                     std::pair{ "real_param", 3.14 },
                                     std::pair{ "bool_param", false },
                                     std::pair{ "null_param", nullptr },
                                     std::pair{ "user_param", john })
                   .build();

  assert_eq(options.named_parameters.size(), std::size_t{ 6 }, "every parameter is encoded");
  assert_true(options.named_parameters.at("str_param") == to_binary(R"("foo")"), "a string");
  assert_true(options.named_parameters.at("int_param") == to_binary("42"), "an integer");
  assert_true(options.named_parameters.at("real_param") == to_binary("3.14"),
              "a floating point number");
  assert_true(options.named_parameters.at("bool_param") == to_binary("false"), "a boolean");
  assert_true(options.named_parameters.at("null_param") == to_binary("null"), "a null");
  assert_true(options.named_parameters.at("user_param") ==
                to_binary(R"({"birth_year":1970,"full_name":"John Doe","username":"john"})"),
              "a user type with a serializer");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(query_options_encode_positional_parameters_automatically) },
      { CASE(query_options_encode_named_parameters_automatically) },
    },
  };
}

} // namespace couchbase::test
