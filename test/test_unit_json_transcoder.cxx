/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include <catch2/catch_approx.hpp>

#include <couchbase/codec/default_json_transcoder.hxx>

#include <tao/json.hpp>

using Catch::Approx;

TEST_CASE("unit: default_json_transcoder encodes primitives", "[unit]")
{
    {
        /*
         * echo -n '"hello, world"' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> expected_data{
            std::byte{ 0x22 }, std::byte{ 0x68 }, std::byte{ 0x65 }, std::byte{ 0x6c }, std::byte{ 0x6c },
            std::byte{ 0x6f }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x77 }, std::byte{ 0x6f },
            std::byte{ 0x72 }, std::byte{ 0x6c }, std::byte{ 0x64 }, std::byte{ 0x22 },
        };
        auto encoded = couchbase::codec::default_json_transcoder::encode("hello, world");
        REQUIRE(encoded.data == expected_data);
        REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);
    }

    {
        /*
         * echo -n '3.14' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> expected_data{
            std::byte{ 0x33 },
            std::byte{ 0x2e },
            std::byte{ 0x31 },
            std::byte{ 0x34 },
        };
        auto encoded = couchbase::codec::default_json_transcoder::encode(3.14);
        REQUIRE(encoded.data == expected_data);
        REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);
    }

    {
        /*
         * echo -n 'true' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> expected_data{
            std::byte{ 0x74 },
            std::byte{ 0x72 },
            std::byte{ 0x75 },
            std::byte{ 0x65 },
        };
        auto encoded = couchbase::codec::default_json_transcoder::encode(true);
        REQUIRE(encoded.data == expected_data);
        REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);
    }
}

TEST_CASE("unit: default_json_transcoder decodes primitives", "[unit]")
{
    {
        /*
         * echo -n '"hello, world"' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> encoded_data{
            std::byte{ 0x22 }, std::byte{ 0x68 }, std::byte{ 0x65 }, std::byte{ 0x6c }, std::byte{ 0x6c },
            std::byte{ 0x6f }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x77 }, std::byte{ 0x6f },
            std::byte{ 0x72 }, std::byte{ 0x6c }, std::byte{ 0x64 }, std::byte{ 0x22 },
        };
        auto decoded = couchbase::codec::default_json_transcoder::decode<std::string>(
          { encoded_data, couchbase::codec::codec_flags::json_common_flags });
        REQUIRE(decoded == "hello, world");
    }

    {
        /*
         * echo -n '3.14' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> encoded_data{
            std::byte{ 0x33 },
            std::byte{ 0x2e },
            std::byte{ 0x31 },
            std::byte{ 0x34 },
        };
        auto decoded =
          couchbase::codec::default_json_transcoder::decode<double>({ encoded_data, couchbase::codec::codec_flags::json_common_flags });
        REQUIRE(Approx(decoded) == 3.14);
    }

    {
        /*
         * echo -n 'true' \
         *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
         */
        std::vector<std::byte> encoded_data{
            std::byte{ 0x74 },
            std::byte{ 0x72 },
            std::byte{ 0x75 },
            std::byte{ 0x65 },
        };
        auto decoded =
          couchbase::codec::default_json_transcoder::decode<bool>({ encoded_data, couchbase::codec::codec_flags::json_common_flags });
        REQUIRE(decoded == true);
    }
}

struct profile {
    std::string username{};
    std::string full_name{};
    std::uint32_t birth_year{};
};

template<>
struct tao::json::traits<profile> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const profile& p)
    {
        v = {
            { "username", p.username },
            { "full_name", p.full_name },
            { "birth_year", p.birth_year },
        };
    }

    template<template<typename...> class Traits>
    static profile as(const tao::json::basic_value<Traits>& v)
    {
        profile result;
        const auto& object = v.get_object();
        result.username = object.at("username").template as<std::string>();
        result.full_name = object.at("full_name").template as<std::string>();
        result.birth_year = object.at("birth_year").template as<std::uint32_t>();
        return result;
    }
};

TEST_CASE("unit: default_json_transcoder encodes user data", "[unit]")
{
    profile albert{ "this_guy_again", "Albert Einstein", 1879 };

    /*
     * echo -n '{"birth_year":1879,"full_name":"Albert Einstein", "username":"this_guy_again"}' \
     *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
     */
    std::vector<std::byte> expected_data{
        std::byte{ 0x7b }, std::byte{ 0x22 }, std::byte{ 0x62 }, std::byte{ 0x69 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x68 },
        std::byte{ 0x5f }, std::byte{ 0x79 }, std::byte{ 0x65 }, std::byte{ 0x61 }, std::byte{ 0x72 }, std::byte{ 0x22 }, std::byte{ 0x3a },
        std::byte{ 0x31 }, std::byte{ 0x38 }, std::byte{ 0x37 }, std::byte{ 0x39 }, std::byte{ 0x2c }, std::byte{ 0x22 }, std::byte{ 0x66 },
        std::byte{ 0x75 }, std::byte{ 0x6c }, std::byte{ 0x6c }, std::byte{ 0x5f }, std::byte{ 0x6e }, std::byte{ 0x61 }, std::byte{ 0x6d },
        std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x41 }, std::byte{ 0x6c }, std::byte{ 0x62 },
        std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x20 }, std::byte{ 0x45 }, std::byte{ 0x69 }, std::byte{ 0x6e },
        std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x65 }, std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x2c },
        std::byte{ 0x22 }, std::byte{ 0x75 }, std::byte{ 0x73 }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x6e }, std::byte{ 0x61 },
        std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x74 }, std::byte{ 0x68 },
        std::byte{ 0x69 }, std::byte{ 0x73 }, std::byte{ 0x5f }, std::byte{ 0x67 }, std::byte{ 0x75 }, std::byte{ 0x79 }, std::byte{ 0x5f },
        std::byte{ 0x61 }, std::byte{ 0x67 }, std::byte{ 0x61 }, std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x7d },
    };

    auto encoded = couchbase::codec::default_json_transcoder::encode(albert);
    REQUIRE(encoded.data == expected_data);
    REQUIRE(encoded.flags == couchbase::codec::codec_flags::json_common_flags);
}

TEST_CASE("unit: default_json_transcoder decodes user data", "[unit]")
{
    /*
     * echo -n '{"birth_year":1879,"full_name":"Albert Einstein", "username":"this_guy_again"}' \
     *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
     */
    std::vector<std::byte> encoded_data{
        std::byte{ 0x7b }, std::byte{ 0x22 }, std::byte{ 0x62 }, std::byte{ 0x69 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x68 },
        std::byte{ 0x5f }, std::byte{ 0x79 }, std::byte{ 0x65 }, std::byte{ 0x61 }, std::byte{ 0x72 }, std::byte{ 0x22 }, std::byte{ 0x3a },
        std::byte{ 0x31 }, std::byte{ 0x38 }, std::byte{ 0x37 }, std::byte{ 0x39 }, std::byte{ 0x2c }, std::byte{ 0x22 }, std::byte{ 0x66 },
        std::byte{ 0x75 }, std::byte{ 0x6c }, std::byte{ 0x6c }, std::byte{ 0x5f }, std::byte{ 0x6e }, std::byte{ 0x61 }, std::byte{ 0x6d },
        std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x41 }, std::byte{ 0x6c }, std::byte{ 0x62 },
        std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x20 }, std::byte{ 0x45 }, std::byte{ 0x69 }, std::byte{ 0x6e },
        std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x65 }, std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x2c },
        std::byte{ 0x22 }, std::byte{ 0x75 }, std::byte{ 0x73 }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x6e }, std::byte{ 0x61 },
        std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x74 }, std::byte{ 0x68 },
        std::byte{ 0x69 }, std::byte{ 0x73 }, std::byte{ 0x5f }, std::byte{ 0x67 }, std::byte{ 0x75 }, std::byte{ 0x79 }, std::byte{ 0x5f },
        std::byte{ 0x61 }, std::byte{ 0x67 }, std::byte{ 0x61 }, std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x7d },
    };

    auto decoded =
      couchbase::codec::default_json_transcoder::decode<profile>({ encoded_data, couchbase::codec::codec_flags::json_common_flags });
    REQUIRE(decoded.username == "this_guy_again");
    REQUIRE(decoded.full_name == "Albert Einstein");
    REQUIRE(decoded.birth_year == 1879);

    auto value = couchbase::codec::default_json_transcoder::decode<tao::json::value>(
      { encoded_data, couchbase::codec::codec_flags::json_common_flags });
    REQUIRE(value.at("username").get_string() == "this_guy_again");
    REQUIRE(value.at("full_name").get_string() == "Albert Einstein");
    REQUIRE(value.at("birth_year").get_unsigned() == 1879);
}
