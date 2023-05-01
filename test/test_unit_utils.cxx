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

#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_exception.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "core/meta/version.hxx"
#include "core/platform/base64.h"
#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "core/utils/movable_function.hxx"
#include "core/utils/url_codec.hxx"

#include <couchbase/build_version.hxx>

#include <couchbase/error_codes.hxx>

#include <tao/json.hpp>

TEST_CASE("unit: transformer to deduplicate JSON keys", "[unit]")
{
    using Catch::Matchers::ContainsSubstring;

    std::string input{ R"({"answer":"wrong","answer":42})" };

    CHECK_THROWS_WITH(tao::json::from_string(input), ContainsSubstring("duplicate JSON object key \"answer\""));

    auto result = couchbase::core::utils::json::parse(input);
    INFO(couchbase::core::utils::json::generate(result));
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
    std::string os_version = fmt::format(";{}/{}", COUCHBASE_CXX_CLIENT_SYSTEM_NAME, COUCHBASE_CXX_CLIENT_SYSTEM_PROCESSOR);
    std::string core_version = fmt::format("cxx/{}.{}.{}/{};{}/{}",
                                           COUCHBASE_CXX_CLIENT_VERSION_MAJOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_MINOR,
                                           COUCHBASE_CXX_CLIENT_VERSION_PATCH,
                                           COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT,
                                           COUCHBASE_CXX_CLIENT_SYSTEM_NAME,
                                           COUCHBASE_CXX_CLIENT_SYSTEM_PROCESSOR);

    auto simple_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE");
    REQUIRE(simple_user_agent == fmt::format(R"({{"a":"{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version));
    REQUIRE(simple_user_agent.size() == 53 + os_version.size());

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
    REQUIRE(250 - simple_user_agent.size() == 197 - os_version.size());
    REQUIRE(trimmed_user_agent ==
            fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version, long_extra.substr(0, 196 - os_version.size())));

    auto long_extra_with_non_printable_characters = long_extra.substr(0, 193 - os_version.size()) + "\n\n";
    trimmed_user_agent =
      couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra_with_non_printable_characters, 250);
    REQUIRE(trimmed_user_agent.size() == 249);
    REQUIRE(trimmed_user_agent == fmt::format(R"({{"a":"{};{}","i":"0xDEADBEEF/0xCAFEBEBE"}})",
                                              core_version,
                                              long_extra.substr(0, 193 - os_version.size()) + "\\n"));

    auto long_and_weird_extra = "hello" + std::string(300, '\n');
    trimmed_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_and_weird_extra, 250);
    REQUIRE(trimmed_user_agent == simple_user_agent);

    REQUIRE(fmt::format("{}; client/0xDEADBEEF; session/0xCAFEBEBE; {}; hello world", core_version, couchbase::core::meta::os()) ==
            couchbase::core::meta::user_agent_for_http("0xDEADBEEF", "0xCAFEBEBE", "hello\nworld"));
}

TEST_CASE("unit: utils::movable_function should be false after moving value out", "[unit]")
{
    auto ptr = std::make_unique<int>(42);
    couchbase::core::utils::movable_function<bool(int)> src_handler = [ptr = std::move(ptr)](int val) {
        return ptr != nullptr && *ptr == val;
    };
    REQUIRE(src_handler);
    REQUIRE(src_handler(42));
    REQUIRE_FALSE(src_handler(43));

    couchbase::core::utils::movable_function<bool(int)> dst_handler = std::move(src_handler);
    REQUIRE(dst_handler);
    REQUIRE(dst_handler(42));
    REQUIRE_FALSE(dst_handler(43));
    REQUIRE_FALSE(src_handler);
}

TEST_CASE("unit: base64", "[unit]")
{
    REQUIRE(couchbase::core::base64::encode(std::vector{ std::byte{ 255 } }, false) == "/w==");
    REQUIRE(couchbase::core::base64::encode(std::vector{ std::byte{ 255 } }, true) == "/w==\n");

    std::array binary{
        std::byte{ 0x00 }, std::byte{ 0x01 }, std::byte{ 0x02 }, std::byte{ 0x03 }, std::byte{ 0x04 }, std::byte{ 0x05 }, std::byte{ 0x06 },
        std::byte{ 0x07 }, std::byte{ 0x08 }, std::byte{ 0x09 }, std::byte{ 0x0a }, std::byte{ 0x0b }, std::byte{ 0x0c }, std::byte{ 0x0d },
        std::byte{ 0x0e }, std::byte{ 0x0f }, std::byte{ 0x10 }, std::byte{ 0x11 }, std::byte{ 0x12 }, std::byte{ 0x13 }, std::byte{ 0x14 },
        std::byte{ 0x15 }, std::byte{ 0x16 }, std::byte{ 0x17 }, std::byte{ 0x18 }, std::byte{ 0x19 }, std::byte{ 0x1a }, std::byte{ 0x1b },
        std::byte{ 0x1c }, std::byte{ 0x1d }, std::byte{ 0x1e }, std::byte{ 0x1f }, std::byte{ 0x20 }, std::byte{ 0x21 }, std::byte{ 0x22 },
        std::byte{ 0x23 }, std::byte{ 0x24 }, std::byte{ 0x25 }, std::byte{ 0x26 }, std::byte{ 0x27 }, std::byte{ 0x28 }, std::byte{ 0x29 },
        std::byte{ 0x2a }, std::byte{ 0x2b }, std::byte{ 0x2c }, std::byte{ 0x2d }, std::byte{ 0x2e }, std::byte{ 0x2f }, std::byte{ 0x30 },
        std::byte{ 0x31 }, std::byte{ 0x32 }, std::byte{ 0x33 }, std::byte{ 0x34 }, std::byte{ 0x35 }, std::byte{ 0x36 }, std::byte{ 0x37 },
        std::byte{ 0x38 }, std::byte{ 0x39 }, std::byte{ 0x3a }, std::byte{ 0x3b }, std::byte{ 0x3c }, std::byte{ 0x3d }, std::byte{ 0x3e },
        std::byte{ 0x3f }, std::byte{ 0x40 }, std::byte{ 0x41 }, std::byte{ 0x42 }, std::byte{ 0x43 }, std::byte{ 0x44 }, std::byte{ 0x45 },
        std::byte{ 0x46 }, std::byte{ 0x47 }, std::byte{ 0x48 }, std::byte{ 0x49 }, std::byte{ 0x4a }, std::byte{ 0x4b }, std::byte{ 0x4c },
        std::byte{ 0x4d }, std::byte{ 0x4e }, std::byte{ 0x4f }, std::byte{ 0x50 }, std::byte{ 0x51 }, std::byte{ 0x52 }, std::byte{ 0x53 },
        std::byte{ 0x54 }, std::byte{ 0x55 }, std::byte{ 0x56 }, std::byte{ 0x57 }, std::byte{ 0x58 }, std::byte{ 0x59 }, std::byte{ 0x5a },
        std::byte{ 0x5b }, std::byte{ 0x5c }, std::byte{ 0x5d }, std::byte{ 0x5e }, std::byte{ 0x5f }, std::byte{ 0x60 }, std::byte{ 0x61 },
        std::byte{ 0x62 }, std::byte{ 0x63 }, std::byte{ 0x64 }, std::byte{ 0x65 }, std::byte{ 0x66 }, std::byte{ 0x67 }, std::byte{ 0x68 },
        std::byte{ 0x69 }, std::byte{ 0x6a }, std::byte{ 0x6b }, std::byte{ 0x6c }, std::byte{ 0x6d }, std::byte{ 0x6e }, std::byte{ 0x6f },
        std::byte{ 0x70 }, std::byte{ 0x71 }, std::byte{ 0x72 }, std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x75 }, std::byte{ 0x76 },
        std::byte{ 0x77 }, std::byte{ 0x78 }, std::byte{ 0x79 }, std::byte{ 0x7a }, std::byte{ 0x7b }, std::byte{ 0x7c }, std::byte{ 0x7d },
        std::byte{ 0x7e }, std::byte{ 0x7f }, std::byte{ 0x80 }, std::byte{ 0x81 }, std::byte{ 0x82 }, std::byte{ 0x83 }, std::byte{ 0x84 },
        std::byte{ 0x85 }, std::byte{ 0x86 }, std::byte{ 0x87 }, std::byte{ 0x88 }, std::byte{ 0x89 }, std::byte{ 0x8a }, std::byte{ 0x8b },
        std::byte{ 0x8c }, std::byte{ 0x8d }, std::byte{ 0x8e }, std::byte{ 0x8f }, std::byte{ 0x90 }, std::byte{ 0x91 }, std::byte{ 0x92 },
        std::byte{ 0x93 }, std::byte{ 0x94 }, std::byte{ 0x95 }, std::byte{ 0x96 }, std::byte{ 0x97 }, std::byte{ 0x98 }, std::byte{ 0x99 },
        std::byte{ 0x9a }, std::byte{ 0x9b }, std::byte{ 0x9c }, std::byte{ 0x9d }, std::byte{ 0x9e }, std::byte{ 0x9f }, std::byte{ 0xa0 },
        std::byte{ 0xa1 }, std::byte{ 0xa2 }, std::byte{ 0xa3 }, std::byte{ 0xa4 }, std::byte{ 0xa5 }, std::byte{ 0xa6 }, std::byte{ 0xa7 },
        std::byte{ 0xa8 }, std::byte{ 0xa9 }, std::byte{ 0xaa }, std::byte{ 0xab }, std::byte{ 0xac }, std::byte{ 0xad }, std::byte{ 0xae },
        std::byte{ 0xaf }, std::byte{ 0xb0 }, std::byte{ 0xb1 }, std::byte{ 0xb2 }, std::byte{ 0xb3 }, std::byte{ 0xb4 }, std::byte{ 0xb5 },
        std::byte{ 0xb6 }, std::byte{ 0xb7 }, std::byte{ 0xb8 }, std::byte{ 0xb9 }, std::byte{ 0xba }, std::byte{ 0xbb }, std::byte{ 0xbc },
        std::byte{ 0xbd }, std::byte{ 0xbe }, std::byte{ 0xbf }, std::byte{ 0xc0 }, std::byte{ 0xc1 }, std::byte{ 0xc2 }, std::byte{ 0xc3 },
        std::byte{ 0xc4 }, std::byte{ 0xc5 }, std::byte{ 0xc6 }, std::byte{ 0xc7 }, std::byte{ 0xc8 }, std::byte{ 0xc9 }, std::byte{ 0xca },
        std::byte{ 0xcb }, std::byte{ 0xcc }, std::byte{ 0xcd }, std::byte{ 0xce }, std::byte{ 0xcf }, std::byte{ 0xd0 }, std::byte{ 0xd1 },
        std::byte{ 0xd2 }, std::byte{ 0xd3 }, std::byte{ 0xd4 }, std::byte{ 0xd5 }, std::byte{ 0xd6 }, std::byte{ 0xd7 }, std::byte{ 0xd8 },
        std::byte{ 0xd9 }, std::byte{ 0xda }, std::byte{ 0xdb }, std::byte{ 0xdc }, std::byte{ 0xdd }, std::byte{ 0xde }, std::byte{ 0xdf },
        std::byte{ 0xe0 }, std::byte{ 0xe1 }, std::byte{ 0xe2 }, std::byte{ 0xe3 }, std::byte{ 0xe4 }, std::byte{ 0xe5 }, std::byte{ 0xe6 },
        std::byte{ 0xe7 }, std::byte{ 0xe8 }, std::byte{ 0xe9 }, std::byte{ 0xea }, std::byte{ 0xeb }, std::byte{ 0xec }, std::byte{ 0xed },
        std::byte{ 0xee }, std::byte{ 0xef }, std::byte{ 0xf0 }, std::byte{ 0xf1 }, std::byte{ 0xf2 }, std::byte{ 0xf3 }, std::byte{ 0xf4 },
        std::byte{ 0xf5 }, std::byte{ 0xf6 }, std::byte{ 0xf7 }, std::byte{ 0xf8 }, std::byte{ 0xf9 }, std::byte{ 0xfa }, std::byte{ 0xfb },
        std::byte{ 0xfc }, std::byte{ 0xfd }, std::byte{ 0xfe }, std::byte{ 0xff }
    };

    std::string base64{ "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+"
                        "P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+"
                        "AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/"
                        "wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w==" };

    std::string base64_pretty{ "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v\nMDEyMzQ1Njc4OTo7PD0+"
                               "P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5f\nYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+"
                               "AgYKDhIWGh4iJiouMjY6P\nkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/"
                               "\nwMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v\n8PHy8/T19vf4+fr7/P3+/w==\n" };

    REQUIRE(couchbase::core::base64::encode(binary, false) == base64);
    REQUIRE(couchbase::core::base64::encode(binary, true) == base64_pretty);
}

namespace couchbase::core::meta
{
std::string
parse_git_describe_output(const std::string& git_describe_output);
}

TEST_CASE("unit: semantic version string", "[unit]")
{
    REQUIRE(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4-16-gfbc9922") == "1.0.0-beta.4+16.fbc9922");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("1.0.0-16-gfbc9922") == "1.0.0+16.fbc9922");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("") == "");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("unknown") == "");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("invalid") == "");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("1.0.0.0.0") == "");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4-0-gfbc9922") == "1.0.0-beta.4");
    REQUIRE(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4") == "1.0.0-beta.4");
}

#if 0
// This test is commented out because, it is not necessary to run it with the suite, but it still useful for debugging.

#include "core/platform/uuid.h"

TEST_CASE("unit: uuid collision", "[unit]")
{
    std::array<std::set<std::string>, 10> uuids{};
    std::vector<std::thread> threads{};
    threads.reserve(10);

    for (std::size_t t = 0; t < 10; ++t) {
        threads.emplace_back([&uuids, t]() {
            for (std::size_t i = 0; i < 1'000'000; ++i) {
                auto uuid = couchbase::core::uuid::to_string(couchbase::core::uuid::random());
                uuids[t].insert(uuid);
            }
        });
    }
    for (std::size_t t = 0; t < 10; ++t) {
        threads[t].join();
        REQUIRE(uuids[t].size() == 1'000'000);
    }

    std::set<std::string> all_uuids{};
    for (std::size_t t = 0; t < 10; ++t) {
        for (const auto& uuid : uuids[t]) {
            REQUIRE(all_uuids.insert(uuid).second);
        }
    }
}
#endif
