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

#include "core/meta/version.hxx"
#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "core/utils/url_codec.hxx"
#include <couchbase/build_version.hxx>

#include <couchbase/error_codes.hxx>

#include "third_party/snappy/snappy.h"

#include <tao/json.hpp>

TEST_CASE("unit: transformer to deduplicate JSON keys", "[unit]")
{
    using Catch::Contains;

    std::string input{ R"({"answer":"wrong","answer":42})" };

    CHECK_THROWS_WITH(tao::json::from_string(input), Contains("duplicate JSON object key \"answer\""));

    auto result = couchbase::core::utils::json::parse(input);
    INFO(couchbase::core::utils::json::generate(result))
    CHECK(result.is_object());
    CHECK(result.find("answer") != nullptr);
    CHECK(result["answer"].is_integer());
    CHECK(result["answer"].as<std::int64_t>() == 42);
}

TEST_CASE("unit: string representation of the error codes", "[unit]")
{
    std::error_code rc = couchbase::errc::common::authentication_failure;
    CHECK(rc.category().name() == std::string("couchbase.common"));
    CHECK(rc.value() == 6);
    std::stringstream ss;
    ss << rc;
    CHECK(ss.str() == "couchbase.common:6");
}

TEST_CASE("unit: url path escape", "[unit]")
{
    REQUIRE(couchbase::core::utils::string_codec::v2::path_escape("a/b") == "a%2Fb");
}

TEST_CASE("unit: join strings", "[unit]")
{
    std::vector<std::string> field_specs{ "testkey:string" };

    REQUIRE(couchbase::core::utils::join_strings(field_specs, ",") == "testkey:string");

    field_specs.emplace_back("volume:double");
    field_specs.emplace_back("id:integer");
    REQUIRE(couchbase::core::utils::join_strings(field_specs, ",") == "testkey:string,volume:double,id:integer");
}

TEST_CASE("unit: join strings (fmt version)", "[unit]")
{
    std::vector<std::string> field_specs{ "testkey:string" };

    REQUIRE(couchbase::core::utils::join_strings_fmt("{}", field_specs, ",") == "testkey:string");

    field_specs.emplace_back("volume:double");
    field_specs.emplace_back("id:integer");
    REQUIRE(couchbase::core::utils::join_strings_fmt("{}", field_specs, ",") == "testkey:string,volume:double,id:integer");
}

TEST_CASE("unit: user_agent string", "[unit]")
{
    std::string core_version = fmt::format("cxx/{}.{}.{}/{}",
                                           COUCHBASE_CXX_CLIENT_VERSION_MAJOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_MINOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_PATCH,
                                           COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT);

    auto simple_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE");
    REQUIRE(simple_user_agent == fmt::format(R"({{"a":"{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version));
    REQUIRE(simple_user_agent.size() == 53);

    REQUIRE(couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", "couchnode/1.2.3; openssl/1.1.1l") ==
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

    REQUIRE(couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra) ==
            fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra));

    auto trimmed_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra, 250);
    REQUIRE(trimmed_user_agent.size() == 250);
    REQUIRE(250 - simple_user_agent.size() == 197);
    REQUIRE(trimmed_user_agent == fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra.substr(0, 196)));

    auto long_extra_with_non_printable_characters = long_extra.substr(0, 193) + "\n\n";
    trimmed_user_agent =
      couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra_with_non_printable_characters, 250);
    REQUIRE(trimmed_user_agent.size() == 249);
    REQUIRE(trimmed_user_agent ==
            fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra.substr(0, 193) + "\\n"));

    auto long_and_weird_extra = "hello" + std::string(300, '\n');
    trimmed_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_and_weird_extra, 250);
    REQUIRE(trimmed_user_agent == simple_user_agent);

    REQUIRE(fmt::format("{}; client/0xDEADBEEF; session/0xCAFEBEBE; {}; hello world", core_version, couchbase::core::meta::os()) ==
            couchbase::core::meta::user_agent_for_http("0xDEADBEEF", "0xCAFEBEBE", "hello\nworld"));
}

TEST_CASE("unit: snappy can decompress Brett's payload", "[unit]")
{
    std::vector payload{
        std::byte{ 0x95 }, std::byte{ 0x02 }, std::byte{ 0xf0 }, std::byte{ 0x4c }, std::byte{ 0x7b }, std::byte{ 0x22 }, std::byte{ 0x63 },
        std::byte{ 0x75 }, std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x6f }, std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x72 },
        std::byte{ 0x49 }, std::byte{ 0x44 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x37 },
        std::byte{ 0x35 }, std::byte{ 0x39 }, std::byte{ 0x30 }, std::byte{ 0x2d }, std::byte{ 0x56 }, std::byte{ 0x48 }, std::byte{ 0x56 },
        std::byte{ 0x45 }, std::byte{ 0x47 }, std::byte{ 0x22 }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x67 },
        std::byte{ 0x65 }, std::byte{ 0x6e }, std::byte{ 0x64 }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x22 }, std::byte{ 0x3a },
        std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x46 }, std::byte{ 0x65 }, std::byte{ 0x6d }, std::byte{ 0x61 }, std::byte{ 0x6c },
        std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x53 }, std::byte{ 0x65 },
        std::byte{ 0x6e }, std::byte{ 0x69 }, std::byte{ 0x6f }, std::byte{ 0x72 }, std::byte{ 0x43 }, std::byte{ 0x69 }, std::byte{ 0x74 },
        std::byte{ 0x69 }, std::byte{ 0x7a }, std::byte{ 0x65 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x20 },
        std::byte{ 0x30 }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x50 }, std::byte{ 0x61 }, std::byte{ 0x72 },
        std::byte{ 0x74 }, std::byte{ 0x6e }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x01 }, std::byte{ 0x41 }, std::byte{ 0x08 },
        std::byte{ 0x59 }, std::byte{ 0x65 }, std::byte{ 0x73 }, std::byte{ 0x01 }, std::byte{ 0x3a }, std::byte{ 0x08 }, std::byte{ 0x44 },
        std::byte{ 0x65 }, std::byte{ 0x70 }, std::byte{ 0x01 }, std::byte{ 0x3c }, std::byte{ 0x08 }, std::byte{ 0x6e }, std::byte{ 0x74 },
        std::byte{ 0x73 }, std::byte{ 0x01 }, std::byte{ 0x15 }, std::byte{ 0x04 }, std::byte{ 0x4e }, std::byte{ 0x6f }, std::byte{ 0x01 },
        std::byte{ 0x14 }, std::byte{ 0x5c }, std::byte{ 0x74 }, std::byte{ 0x65 }, std::byte{ 0x6e }, std::byte{ 0x75 }, std::byte{ 0x72 },
        std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x20 }, std::byte{ 0x31 }, std::byte{ 0x2c }, std::byte{ 0x20 },
        std::byte{ 0x22 }, std::byte{ 0x50 }, std::byte{ 0x68 }, std::byte{ 0x6f }, std::byte{ 0x6e }, std::byte{ 0x65 }, std::byte{ 0x53 },
        std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x76 }, std::byte{ 0x69 }, std::byte{ 0x63 }, std::byte{ 0x01 }, std::byte{ 0x13 },
        std::byte{ 0x0d }, std::byte{ 0x23 }, std::byte{ 0x2c }, std::byte{ 0x4d }, std::byte{ 0x75 }, std::byte{ 0x6c }, std::byte{ 0x74 },
        std::byte{ 0x69 }, std::byte{ 0x70 }, std::byte{ 0x6c }, std::byte{ 0x65 }, std::byte{ 0x4c }, std::byte{ 0x69 }, std::byte{ 0x6e },
        std::byte{ 0x65 }, std::byte{ 0x0d }, std::byte{ 0x3a }, std::byte{ 0x04 }, std::byte{ 0x20 }, std::byte{ 0x70 }, std::byte{ 0x01 },
        std::byte{ 0x2a }, std::byte{ 0x04 }, std::byte{ 0x20 }, std::byte{ 0x73 }, std::byte{ 0x0d }, std::byte{ 0x2b }, std::byte{ 0x28 },
        std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x49 }, std::byte{ 0x6e }, std::byte{ 0x74 }, std::byte{ 0x65 },
        std::byte{ 0x72 }, std::byte{ 0x6e }, std::byte{ 0x65 }, std::byte{ 0x74 }, std::byte{ 0x1d }, std::byte{ 0x3e }, std::byte{ 0x08 },
        std::byte{ 0x44 }, std::byte{ 0x53 }, std::byte{ 0x4c }, std::byte{ 0x01 }, std::byte{ 0x1a }, std::byte{ 0x0c }, std::byte{ 0x4f },
        std::byte{ 0x6e }, std::byte{ 0x6c }, std::byte{ 0x69 }, std::byte{ 0x01 }, std::byte{ 0x56 }, std::byte{ 0x14 }, std::byte{ 0x63 },
        std::byte{ 0x75 }, std::byte{ 0x72 }, std::byte{ 0x69 }, std::byte{ 0x74 }, std::byte{ 0x79 }, std::byte{ 0x19 }, std::byte{ 0x7a },
        std::byte{ 0x09 }, std::byte{ 0x18 }, std::byte{ 0x14 }, std::byte{ 0x42 }, std::byte{ 0x61 }, std::byte{ 0x63 }, std::byte{ 0x6b },
        std::byte{ 0x75 }, std::byte{ 0x70 }, std::byte{ 0x01 }, std::byte{ 0x16 }, std::byte{ 0x15 }, std::byte{ 0xa5 }, std::byte{ 0x01 },
        std::byte{ 0x7e }, std::byte{ 0x44 }, std::byte{ 0x50 }, std::byte{ 0x72 }, std::byte{ 0x6f }, std::byte{ 0x74 }, std::byte{ 0x65 },
        std::byte{ 0x63 }, std::byte{ 0x74 }, std::byte{ 0x69 }, std::byte{ 0x6f }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x3a },
        std::byte{ 0x20 }, std::byte{ 0x22 }, std::byte{ 0x4e }, std::byte{ 0x6f }, std::byte{ 0x22 }, std::byte{ 0x7d }
    };

    std::string uncompressed;
    REQUIRE(snappy::Uncompress(reinterpret_cast<const char*>(payload.data()), payload.size(), &uncompressed));
    REQUIRE(
      uncompressed ==
      R"({"customerID": "7590-VHVEG", "gender": "Female", "SeniorCitizen": 0, "Partner": "Yes", "Dependents": "No", "tenure": 1, "PhoneService": "No", "MultipleLines": "No phone service", "InternetService": "DSL", "OnlineSecurity": "No", "OnlineBackup": "Yes", "DeviceProtection": "No"})");
}
