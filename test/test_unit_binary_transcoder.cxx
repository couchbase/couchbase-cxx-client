/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/get_result.hxx>

TEST_CASE("unit: binary_raw_transcoder sets flags", "[unit]")
{
  /*
   * echo -n '"hello, world"' \
   *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
   */
  std::vector<std::byte> data{
    std::byte{ 0x22 }, std::byte{ 0x68 }, std::byte{ 0x65 }, std::byte{ 0x6c }, std::byte{ 0x6c },
    std::byte{ 0x6f }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x77 }, std::byte{ 0x6f },
    std::byte{ 0x72 }, std::byte{ 0x6c }, std::byte{ 0x64 }, std::byte{ 0x22 },
  };

  auto encoded = couchbase::codec::raw_binary_transcoder::encode(data);
  REQUIRE(encoded.data == data);
  REQUIRE(encoded.flags == couchbase::codec::codec_flags::binary_common_flags);

  auto decoded = couchbase::codec::raw_binary_transcoder::decode(encoded);
  REQUIRE(decoded == data);
}

TEST_CASE("unit: binary_raw_transcoder checks flags", "[unit]")
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

  REQUIRE_THROWS_AS(
    [](auto encoded) {
      return couchbase::codec::raw_binary_transcoder::decode(encoded);
    }(encoded),
    std::system_error);
}

TEST_CASE("unit: binary_raw_transcoder works with get result", "[unit]")
{
  std::vector<std::byte> data{
    { std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xef } }
  };
  couchbase::get_result result(
    {}, { data, couchbase::codec::codec_flags::binary_common_flags }, {});

  REQUIRE(result.content_as<couchbase::codec::raw_binary_transcoder>() == data);
  REQUIRE(result.content_as<std::vector<std::byte>, couchbase::codec::raw_binary_transcoder>() ==
          data);
}
