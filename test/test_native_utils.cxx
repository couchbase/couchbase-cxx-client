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

#include "test_helper.hxx"

#include <tao/json.hpp>

#include <couchbase/utils/json.hxx>

TEST_CASE("unit: transformer to deduplicate JSON keys", "[unit]")
{
    using Catch::Contains;

    std::string input{ R"({"answer":"wrong","answer":42})" };

    CHECK_THROWS_WITH(tao::json::from_string(input), Contains("duplicate JSON object key \"answer\""));

    auto result = tao::json::from_string<couchbase::utils::json::last_key_wins>(input);
    INFO(tao::json::to_string(result))
    CHECK(result.is_object());
    CHECK(result.find("answer") != nullptr);
    CHECK(result["answer"].is_integer());
    CHECK(result["answer"].as<std::int64_t>() == 42);
}
