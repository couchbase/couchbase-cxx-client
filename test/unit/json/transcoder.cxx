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

#include "framework/test_registry.hxx"

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/lenient_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace couchbase::test
{
struct profile {
  std::string username{};
  std::string full_name{};
  std::uint32_t birth_year{};
};
} // namespace couchbase::test

template<>
struct tao::json::traits<couchbase::test::profile> {
  template<template<typename...> class Traits>
  static void assign(tao::json::basic_value<Traits>& v, const couchbase::test::profile& p)
  {
    v = {
      { "username", p.username },
      { "full_name", p.full_name },
      { "birth_year", p.birth_year },
    };
  }

  template<template<typename...> class Traits>
  static couchbase::test::profile as(const tao::json::basic_value<Traits>& v)
  {
    couchbase::test::profile result;
    const auto& object = v.get_object();
    result.username = object.at("username").template as<std::string>();
    result.full_name = object.at("full_name").template as<std::string>();
    result.birth_year = object.at("birth_year").template as<std::uint32_t>();
    return result;
  }
};

namespace couchbase::test
{
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

// The object the profile traits produce: tao::json orders an object's keys, so the field order
// here is the encoder's contract rather than the order the traits list them in.
auto
albert_json() -> std::vector<std::byte>
{
  return to_bytes(
    R"({"birth_year":1879,"full_name":"Albert Einstein","username":"this_guy_again"})");
}

auto
albert() -> profile
{
  return { "this_guy_again", "Albert Einstein", 1879 };
}

void
default_json_transcoder_encodes_primitives([[maybe_unused]] context& ctx)
{
  {
    auto encoded = couchbase::codec::default_json_transcoder::encode("hello, world");
    assert_true(encoded.data == to_bytes(R"("hello, world")"), "a string encodes as JSON text");
    assert_eq(encoded.flags,
              couchbase::codec::codec_flags::json_common_flags,
              "a string is tagged as JSON");
  }

  {
    auto encoded = couchbase::codec::default_json_transcoder::encode(3.14);
    assert_true(encoded.data == to_bytes("3.14"), "a double encodes as a JSON number");
    assert_eq(encoded.flags,
              couchbase::codec::codec_flags::json_common_flags,
              "a double is tagged as JSON");
  }

  {
    auto encoded = couchbase::codec::default_json_transcoder::encode(true);
    assert_true(encoded.data == to_bytes("true"), "a bool encodes as a JSON literal");
    assert_eq(
      encoded.flags, couchbase::codec::codec_flags::json_common_flags, "a bool is tagged as JSON");
  }
}

void
default_json_transcoder_decodes_primitives([[maybe_unused]] context& ctx)
{
  {
    auto decoded = couchbase::codec::default_json_transcoder::decode<std::string>(
      { to_bytes(R"("hello, world")"), couchbase::codec::codec_flags::json_common_flags });
    assert_eq(decoded, std::string{ "hello, world" }, "JSON text decodes into a string");
  }

  {
    auto decoded = couchbase::codec::default_json_transcoder::decode<double>(
      { to_bytes("3.14"), couchbase::codec::codec_flags::json_common_flags });
    // A correctly rounded parse of this text is the literal itself, so the tolerance covers
    // rounding and nothing else.
    assert_near(decoded, 3.14, 1e-12, "a JSON number decodes into a double");
  }

  {
    auto decoded = couchbase::codec::default_json_transcoder::decode<bool>(
      { to_bytes("true"), couchbase::codec::codec_flags::json_common_flags });
    assert_true(decoded, "a JSON literal decodes into a bool");
  }
}

void
default_json_transcoder_encodes_user_data([[maybe_unused]] context& ctx)
{
  auto encoded = couchbase::codec::default_json_transcoder::encode(albert());

  assert_true(encoded.data == albert_json(), "the user type encodes through its json traits");
  assert_eq(encoded.flags,
            couchbase::codec::codec_flags::json_common_flags,
            "the document is tagged as JSON");
}

void
default_json_transcoder_decodes_user_data([[maybe_unused]] context& ctx)
{
  const couchbase::codec::encoded_value stored{
    albert_json(),
    couchbase::codec::codec_flags::json_common_flags,
  };

  auto decoded = couchbase::codec::default_json_transcoder::decode<profile>(stored);
  assert_eq(decoded.username, std::string{ "this_guy_again" }, "the username field");
  assert_eq(decoded.full_name, std::string{ "Albert Einstein" }, "the full name field");
  assert_eq(decoded.birth_year, std::uint32_t{ 1879 }, "the birth year field");

  auto value = couchbase::codec::default_json_transcoder::decode<tao::json::value>(stored);
  assert_eq(value.at("username").get_string(), std::string{ "this_guy_again" }, "the username key");
  assert_eq(
    value.at("full_name").get_string(), std::string{ "Albert Einstein" }, "the full name key");
  assert_eq(value.at("birth_year").get_unsigned(), std::uint64_t{ 1879 }, "the birth year key");
}

void
default_lenient_json_transcoder_is_a_transcoder([[maybe_unused]] context& ctx)
{
  static_assert(
    couchbase::codec::is_transcoder_v<couchbase::codec::default_lenient_json_transcoder>,
    "the lenient transcoder satisfies the transcoder requirements");
}

void
the_lenient_transcoder_encodes_as_the_strict_one_does([[maybe_unused]] context& ctx)
{
  // Only the decode side is lenient: an encoded document is still tagged as JSON, so a reader
  // holding the strict transcoder can read back what the lenient one wrote.
  auto strict = couchbase::codec::default_json_transcoder::encode(albert());
  auto lenient = couchbase::codec::default_lenient_json_transcoder::encode(albert());

  assert_true(lenient.data == strict.data, "the same bytes");
  assert_eq(lenient.flags,
            couchbase::codec::codec_flags::json_common_flags,
            "the document is tagged as JSON");
}

void
the_lenient_transcoder_decodes_under_any_common_flags([[maybe_unused]] context& ctx)
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
    assert_eq(value.at("answer").get_unsigned(),
              std::uint64_t{ 42 },
              "the JSON payload is decoded whatever the flags claim");
  }
}

void
json_stored_with_binary_flags_is_read_only_by_the_lenient_transcoder([[maybe_unused]] context& ctx)
{
  // The transactions read path: a valid JSON document stored with binary common flags, which the
  // reference SDK's bare JsonSerializer reads and the strict transcoder refuses.
  const couchbase::codec::encoded_value binary_flagged{
    to_bytes(R"({"answer":42})"),
    couchbase::codec::codec_flags::binary_common_flags,
  };

  assert_throws<std::system_error>(
    [&binary_flagged]() {
      return couchbase::codec::default_json_transcoder::decode<tao::json::value>(binary_flagged);
    },
    "the strict transcoder refuses content not tagged as JSON");

  auto value =
    couchbase::codec::default_lenient_json_transcoder::decode<tao::json::value>(binary_flagged);
  assert_eq(value.at("answer").get_unsigned(),
            std::uint64_t{ 42 },
            "the lenient transcoder ignores the flags and decodes the payload");
}

void
the_lenient_transcoder_decodes_user_data_stored_with_binary_flags([[maybe_unused]] context& ctx)
{
  const auto encoded = couchbase::codec::default_lenient_json_transcoder::encode(albert());
  const couchbase::codec::encoded_value binary_flagged{
    encoded.data,
    couchbase::codec::codec_flags::binary_common_flags,
  };

  auto decoded = couchbase::codec::default_lenient_json_transcoder::decode<profile>(binary_flagged);
  assert_eq(decoded.username, std::string{ "this_guy_again" }, "the username field");
  assert_eq(decoded.full_name, std::string{ "Albert Einstein" }, "the full name field");
  assert_eq(decoded.birth_year, std::uint32_t{ 1879 }, "the birth year field");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(default_json_transcoder_encodes_primitives) },
      { CASE(default_json_transcoder_decodes_primitives) },
      { CASE(default_json_transcoder_encodes_user_data) },
      { CASE(default_json_transcoder_decodes_user_data) },
      { CASE(default_lenient_json_transcoder_is_a_transcoder) },
      { CASE(the_lenient_transcoder_encodes_as_the_strict_one_does) },
      { CASE(the_lenient_transcoder_decodes_under_any_common_flags) },
      { CASE(json_stored_with_binary_flags_is_read_only_by_the_lenient_transcoder) },
      { CASE(the_lenient_transcoder_decodes_user_data_stored_with_binary_flags) },
    },
  };
}

} // namespace couchbase::test
