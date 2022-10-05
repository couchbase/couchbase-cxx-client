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

#include "core/utils/binary.hxx"
#include "profile.hxx"
#include "test_helper.hxx"

#include <couchbase/query_options.hxx>

TEST_CASE("unit: query options can encode positional parameters automatically", "[unit]")
{
    profile john{ "john", "John Doe", 1970 };
    auto options = couchbase::query_options{}.positional_parameters("foo", 42, 3.14, false, nullptr, john).build();
    REQUIRE(options.positional_parameters.size() == 6);
    REQUIRE(options.positional_parameters[0] == couchbase::core::utils::to_binary("\"foo\""));
    REQUIRE(options.positional_parameters[1] == couchbase::core::utils::to_binary("42"));
    REQUIRE(options.positional_parameters[2] == couchbase::core::utils::to_binary("3.14"));
    REQUIRE(options.positional_parameters[3] == couchbase::core::utils::to_binary("false"));
    REQUIRE(options.positional_parameters[4] == couchbase::core::utils::to_binary("null"));
    REQUIRE(options.positional_parameters[5] ==
            couchbase::core::utils::to_binary("{\"birth_year\":1970,\"full_name\":\"John Doe\",\"username\":\"john\"}"));
}

TEST_CASE("unit: query options can encode named parameters automatically", "[unit]")
{
    profile john{ "john", "John Doe", 1970 };
    auto options = couchbase::query_options{}
                     .named_parameters(std::pair{ "str_param", "foo" },
                                       std::pair{ "int_param", 42 },
                                       std::pair{ "real_param", 3.14 },
                                       std::pair{ "bool_param", false },
                                       std::pair{ "null_param", nullptr },
                                       std::pair{ "user_param", john })
                     .build();
    REQUIRE(options.named_parameters.size() == 6);
    REQUIRE(options.named_parameters["str_param"] == couchbase::core::utils::to_binary("\"foo\""));
    REQUIRE(options.named_parameters["int_param"] == couchbase::core::utils::to_binary("42"));
    REQUIRE(options.named_parameters["real_param"] == couchbase::core::utils::to_binary("3.14"));
    REQUIRE(options.named_parameters["bool_param"] == couchbase::core::utils::to_binary("false"));
    REQUIRE(options.named_parameters["null_param"] == couchbase::core::utils::to_binary("null"));
    REQUIRE(options.named_parameters["user_param"] ==
            couchbase::core::utils::to_binary("{\"birth_year\":1970,\"full_name\":\"John Doe\",\"username\":\"john\"}"));
}
