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

#include "framework/test_registry.hxx"

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/get_result.hxx>

#include <cstddef>
#include <system_error>
#include <vector>

namespace couchbase::test
{
namespace
{
/*
 * echo -n '"hello, world"' \
 *  | ruby -e 'puts ARGF.read.chars.map{|c| format("std::byte{0x%02x}", c.ord)}.join(", ")'
 */
auto
hello_world_json() -> std::vector<std::byte>
{
  return {
    std::byte{ 0x22 }, std::byte{ 0x68 }, std::byte{ 0x65 }, std::byte{ 0x6c }, std::byte{ 0x6c },
    std::byte{ 0x6f }, std::byte{ 0x2c }, std::byte{ 0x20 }, std::byte{ 0x77 }, std::byte{ 0x6f },
    std::byte{ 0x72 }, std::byte{ 0x6c }, std::byte{ 0x64 }, std::byte{ 0x22 },
  };
}

void
raw_binary_transcoder_round_trips_bytes_under_binary_flags([[maybe_unused]] context& ctx)
{
  const auto data = hello_world_json();

  auto encoded = couchbase::codec::raw_binary_transcoder::encode(data);
  assert_true(encoded.data == data, "the payload is passed through unaltered");
  assert_eq(encoded.flags,
            couchbase::codec::codec_flags::binary_common_flags,
            "the payload is tagged as binary");

  auto decoded = couchbase::codec::raw_binary_transcoder::decode(encoded);
  assert_true(decoded == data, "the payload survives the round trip");
}

void
raw_binary_transcoder_rejects_content_that_is_not_binary([[maybe_unused]] context& ctx)
{
  auto encoded = couchbase::codec::default_json_transcoder::encode("hello, world");
  assert_true(encoded.data == hello_world_json(), "the JSON encoding of the string");
  assert_eq(encoded.flags,
            couchbase::codec::codec_flags::json_common_flags,
            "the payload is tagged as JSON");

  assert_throws<std::system_error>(
    [&encoded]() {
      return couchbase::codec::raw_binary_transcoder::decode(encoded);
    },
    "content tagged as JSON is refused rather than handed back as bytes");
}

void
get_result_content_as_reaches_the_raw_binary_transcoder([[maybe_unused]] context& ctx)
{
  const std::vector<std::byte> data{
    std::byte{ 0xde },
    std::byte{ 0xad },
    std::byte{ 0xbe },
    std::byte{ 0xef },
  };
  couchbase::get_result result(
    {}, { data, couchbase::codec::codec_flags::binary_common_flags }, {});

  assert_true(result.content_as<couchbase::codec::raw_binary_transcoder>() == data,
              "the transcoder-only overload");
  assert_true(
    result.content_as<std::vector<std::byte>, couchbase::codec::raw_binary_transcoder>() == data,
    "the document-type overload");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(raw_binary_transcoder_round_trips_bytes_under_binary_flags) },
      { CASE(raw_binary_transcoder_rejects_content_that_is_not_binary) },
      { CASE(get_result_content_as_reaches_the_raw_binary_transcoder) },
    },
  };
}

} // namespace couchbase::test
