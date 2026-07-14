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

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/lenient_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json.hpp>

#include <cstdint>
#include <string_view>
#include <system_error>
#include <vector>

using Catch::Approx;

namespace
{
auto
to_bytes(std::string_view text) -> std::vector<std::byte>
{
  std::vector<std::byte> out;
  out.reserve(text.size());
  for (const auto c : text) {
    out.push_back(static_cast<std::byte>(c));
  }
  return out;
}
} // namespace

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
    auto decoded = couchbase::codec::default_json_transcoder::decode<double>(
      { encoded_data, couchbase::codec::codec_flags::json_common_flags });
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
    auto decoded = couchbase::codec::default_json_transcoder::decode<bool>(
      { encoded_data, couchbase::codec::codec_flags::json_common_flags });
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
    std::byte{ 0x7b }, std::byte{ 0x22 }, std::byte{ 0x62 }, std::byte{ 0x69 }, std::byte{ 0x72 },
    std::byte{ 0x74 }, std::byte{ 0x68 }, std::byte{ 0x5f }, std::byte{ 0x79 }, std::byte{ 0x65 },
    std::byte{ 0x61 }, std::byte{ 0x72 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x31 },
    std::byte{ 0x38 }, std::byte{ 0x37 }, std::byte{ 0x39 }, std::byte{ 0x2c }, std::byte{ 0x22 },
    std::byte{ 0x66 }, std::byte{ 0x75 }, std::byte{ 0x6c }, std::byte{ 0x6c }, std::byte{ 0x5f },
    std::byte{ 0x6e }, std::byte{ 0x61 }, std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 },
    std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x41 }, std::byte{ 0x6c }, std::byte{ 0x62 },
    std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x20 }, std::byte{ 0x45 },
    std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x65 },
    std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x2c }, std::byte{ 0x22 },
    std::byte{ 0x75 }, std::byte{ 0x73 }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x6e },
    std::byte{ 0x61 }, std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a },
    std::byte{ 0x22 }, std::byte{ 0x74 }, std::byte{ 0x68 }, std::byte{ 0x69 }, std::byte{ 0x73 },
    std::byte{ 0x5f }, std::byte{ 0x67 }, std::byte{ 0x75 }, std::byte{ 0x79 }, std::byte{ 0x5f },
    std::byte{ 0x61 }, std::byte{ 0x67 }, std::byte{ 0x61 }, std::byte{ 0x69 }, std::byte{ 0x6e },
    std::byte{ 0x22 }, std::byte{ 0x7d },
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
    std::byte{ 0x7b }, std::byte{ 0x22 }, std::byte{ 0x62 }, std::byte{ 0x69 }, std::byte{ 0x72 },
    std::byte{ 0x74 }, std::byte{ 0x68 }, std::byte{ 0x5f }, std::byte{ 0x79 }, std::byte{ 0x65 },
    std::byte{ 0x61 }, std::byte{ 0x72 }, std::byte{ 0x22 }, std::byte{ 0x3a }, std::byte{ 0x31 },
    std::byte{ 0x38 }, std::byte{ 0x37 }, std::byte{ 0x39 }, std::byte{ 0x2c }, std::byte{ 0x22 },
    std::byte{ 0x66 }, std::byte{ 0x75 }, std::byte{ 0x6c }, std::byte{ 0x6c }, std::byte{ 0x5f },
    std::byte{ 0x6e }, std::byte{ 0x61 }, std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 },
    std::byte{ 0x3a }, std::byte{ 0x22 }, std::byte{ 0x41 }, std::byte{ 0x6c }, std::byte{ 0x62 },
    std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x74 }, std::byte{ 0x20 }, std::byte{ 0x45 },
    std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x65 },
    std::byte{ 0x69 }, std::byte{ 0x6e }, std::byte{ 0x22 }, std::byte{ 0x2c }, std::byte{ 0x22 },
    std::byte{ 0x75 }, std::byte{ 0x73 }, std::byte{ 0x65 }, std::byte{ 0x72 }, std::byte{ 0x6e },
    std::byte{ 0x61 }, std::byte{ 0x6d }, std::byte{ 0x65 }, std::byte{ 0x22 }, std::byte{ 0x3a },
    std::byte{ 0x22 }, std::byte{ 0x74 }, std::byte{ 0x68 }, std::byte{ 0x69 }, std::byte{ 0x73 },
    std::byte{ 0x5f }, std::byte{ 0x67 }, std::byte{ 0x75 }, std::byte{ 0x79 }, std::byte{ 0x5f },
    std::byte{ 0x61 }, std::byte{ 0x67 }, std::byte{ 0x61 }, std::byte{ 0x69 }, std::byte{ 0x6e },
    std::byte{ 0x22 }, std::byte{ 0x7d },
  };

  auto decoded = couchbase::codec::default_json_transcoder::decode<profile>(
    { encoded_data, couchbase::codec::codec_flags::json_common_flags });
  REQUIRE(decoded.username == "this_guy_again");
  REQUIRE(decoded.full_name == "Albert Einstein");
  REQUIRE(decoded.birth_year == 1879);

  auto value = couchbase::codec::default_json_transcoder::decode<tao::json::value>(
    { encoded_data, couchbase::codec::codec_flags::json_common_flags });
  REQUIRE(value.at("username").get_string() == "this_guy_again");
  REQUIRE(value.at("full_name").get_string() == "Albert Einstein");
  REQUIRE(value.at("birth_year").get_unsigned() == 1879);
}

TEST_CASE("unit: default_lenient_json_transcoder is recognized as a transcoder", "[unit]")
{
  STATIC_REQUIRE(
    couchbase::codec::is_transcoder_v<couchbase::codec::default_lenient_json_transcoder>);
}

TEST_CASE("unit: default_lenient_json_transcoder encodes with JSON common flags", "[unit]")
{
  // Encoding must be identical to the strict transcoder: it tags the payload with JSON common
  // flags. Only the decode side is lenient.
  const profile albert{ "this_guy_again", "Albert Einstein", 1879 };

  auto strict = couchbase::codec::default_json_transcoder::encode(albert);
  auto lenient = couchbase::codec::default_lenient_json_transcoder::encode(albert);

  REQUIRE(lenient.data == strict.data);
  REQUIRE(lenient.flags == couchbase::codec::codec_flags::json_common_flags);
}

TEST_CASE("unit: default_lenient_json_transcoder decodes irrespective of common flags", "[unit]")
{
  const auto data = to_bytes(R"({"answer":42})");

  for (const std::uint32_t flags : {
         couchbase::codec::codec_flags::json_common_flags,
         couchbase::codec::codec_flags::binary_common_flags,
         couchbase::codec::codec_flags::string_common_flags,
         std::uint32_t{ 0 },
       }) {
    auto value =
      couchbase::codec::default_lenient_json_transcoder::decode<tao::json::value>({ data, flags });
    REQUIRE(value.at("answer").get_unsigned() == 42);
  }
}

TEST_CASE("unit: lenient transcoder reads JSON stored with binary flags that strict one rejects",
          "[unit]")
{
  // This is the transactions read-path scenario from the ExtBinarySupport spec: a valid JSON
  // document stored with binary common flags.
  const couchbase::codec::encoded_value binary_flagged{
    to_bytes(R"({"answer":42})"),
    couchbase::codec::codec_flags::binary_common_flags,
  };

  // The strict transcoder (the transactions default before this change) refuses to decode content
  // that is not tagged with JSON common flags.
  REQUIRE_THROWS_AS(
    couchbase::codec::default_json_transcoder::decode<tao::json::value>(binary_flagged),
    std::system_error);

  // The lenient transcoder ignores the flags and decodes the JSON payload, matching the reference
  // SDK's bare-JsonSerializer read behaviour used by the transactions read path.
  auto value =
    couchbase::codec::default_lenient_json_transcoder::decode<tao::json::value>(binary_flagged);
  REQUIRE(value.at("answer").get_unsigned() == 42);
}

TEST_CASE("unit: lenient transcoder decodes user data stored with binary flags", "[unit]")
{
  // Round-trip the way a transaction read would: a document encoded as JSON but re-tagged with
  // binary common flags is still decoded into the user's type.
  const profile albert{ "this_guy_again", "Albert Einstein", 1879 };
  const auto encoded = couchbase::codec::default_lenient_json_transcoder::encode(albert);
  const couchbase::codec::encoded_value binary_flagged{
    encoded.data,
    couchbase::codec::codec_flags::binary_common_flags,
  };

  auto decoded = couchbase::codec::default_lenient_json_transcoder::decode<profile>(binary_flagged);
  REQUIRE(decoded.username == "this_guy_again");
  REQUIRE(decoded.full_name == "Albert Einstein");
  REQUIRE(decoded.birth_year == 1879);
}
