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

#include <couchbase/build_version.hxx>
#include <couchbase/errors.hxx>
#include <couchbase/meta/version.hxx>
#include <couchbase/utils/join_strings.hxx>
#include <couchbase/utils/json.hxx>
#include <couchbase/utils/url_codec.hxx>

#include <tao/json.hpp>

TEST_CASE("unit: transformer to deduplicate JSON keys", "[unit]")
{
    using Catch::Contains;

    std::string input{ R"({"answer":"wrong","answer":42})" };

    CHECK_THROWS_WITH(tao::json::from_string(input), Contains("duplicate JSON object key \"answer\""));

    auto result = couchbase::utils::json::parse(input);
    INFO(couchbase::utils::json::generate(result))
    CHECK(result.is_object());
    CHECK(result.find("answer") != nullptr);
    CHECK(result["answer"].is_integer());
    CHECK(result["answer"].as<std::int64_t>() == 42);
}

TEST_CASE("unit: string representation of the error codes", "[unit]")
{
    std::error_code rc = couchbase::error::common_errc::authentication_failure;
    CHECK(rc.category().name() == std::string("couchbase.common"));
    CHECK(rc.value() == 6);
    std::stringstream ss;
    ss << rc;
    CHECK(ss.str() == "couchbase.common:6");
}

TEST_CASE("unit: url path escape", "[unit]")
{
    REQUIRE(couchbase::utils::string_codec::v2::path_escape("a/b") == "a%2Fb");
}

TEST_CASE("unit: join strings", "[unit]")
{
    std::vector<std::string> field_specs{ "testkey:string" };

    REQUIRE(couchbase::utils::join_strings(field_specs, ",") == "testkey:string");

    field_specs.emplace_back("volume:double");
    field_specs.emplace_back("id:integer");
    REQUIRE(couchbase::utils::join_strings(field_specs, ",") == "testkey:string,volume:double,id:integer");
}

TEST_CASE("unit: join strings (fmt version)", "[unit]")
{
    std::vector<std::string> field_specs{ "testkey:string" };

    REQUIRE(couchbase::utils::join_strings_fmt("{}", field_specs, ",") == "testkey:string");

    field_specs.emplace_back("volume:double");
    field_specs.emplace_back("id:integer");
    REQUIRE(couchbase::utils::join_strings_fmt("{}", field_specs, ",") == "testkey:string,volume:double,id:integer");
}

TEST_CASE("unit: user_agent string", "[unit]")
{
    std::string core_version = fmt::format("cxx/{}.{}.{}/{}",
                                           COUCHBASE_CXX_CLIENT_VERSION_MAJOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_MINOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_PATCH,
                                           COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT);

    auto simple_user_agent = couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE");
    REQUIRE(simple_user_agent == fmt::format(R"({{"a":"{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version));
    REQUIRE(simple_user_agent.size() == 53);

    REQUIRE(couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", "couchnode/1.2.3; openssl/1.1.1l") ==
            fmt::format(R"({{"a":"{};couchnode/1.2.3; openssl/1.1.1l","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version));

    std::string long_extra = "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef"
                             "01234567890abcdef01234567890abcdef";
    REQUIRE(long_extra.size() == 272);

    REQUIRE(couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra) ==
            fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra));

    auto trimmed_user_agent = couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra, 250);
    REQUIRE(trimmed_user_agent.size() == 250);
    REQUIRE(250 - simple_user_agent.size() == 197);
    REQUIRE(trimmed_user_agent == fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra.substr(0, 196)));

    auto long_extra_with_non_printable_characters = long_extra.substr(0, 193) + "\n\n";
    trimmed_user_agent = couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra_with_non_printable_characters, 250);
    REQUIRE(trimmed_user_agent.size() == 249);
    REQUIRE(trimmed_user_agent ==
            fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra.substr(0, 193) + "\\n"));

    auto long_and_weird_extra = "hello" + std::string(300, '\n');
    trimmed_user_agent = couchbase::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_and_weird_extra, 250);
    REQUIRE(trimmed_user_agent == simple_user_agent);

    REQUIRE(fmt::format("{}; client/0xDEADBEEF; session/0xCAFEBEBE; {}; hello world", core_version, couchbase::meta::os()) ==
            couchbase::meta::user_agent_for_http("0xDEADBEEF", "0xCAFEBEBE", "hello\nworld"));
}
