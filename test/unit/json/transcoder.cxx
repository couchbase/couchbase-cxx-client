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

#include "core/utils/json.hxx"

#include <couchbase/codec/codec_flags.hxx>
#include <couchbase/codec/default_json_transcoder.hxx>
#include <couchbase/codec/lenient_json_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace
{
thread_local volatile long g_alloc_count{ 0 };
} // namespace

#ifndef COUCHBASE_CXX_CLIENT_BUILD_SANITIZED
auto
operator new(std::size_t sz) -> void*
{
  g_alloc_count = g_alloc_count + 1;
  if (void* p = std::malloc(sz == 0 ? 1 : sz)) {
    return p;
  }
  throw std::bad_alloc{};
}

void
operator delete(void* p) noexcept
{
  std::free(p);
}

void
operator delete(void* p, std::size_t /* sz */) noexcept
{
  std::free(p);
}
#endif

namespace couchbase::test
{
namespace
{
// The allocation budgets read g_alloc_count, which only the replacement operator new above
// increments. A sanitized build compiles the replacement out, and under valgrind's memcheck the
// client library's allocations bypass it while this file's own still reach it. Either way the
// counter stays at zero for the code under test and every budget is met, so a case with a budget
// runs only where an allocation made inside the library is counted. MSVC debug iterators add a
// proxy allocation to every container, which these budgets would count, so they do not run there
// either.
class needs_counted_allocations : public requirement
{
public:
  [[nodiscard]] auto describe() const -> std::string override
  {
    return "exact counts of the client library's allocations";
  }

  [[nodiscard]] auto check([[maybe_unused]] context& ctx) const -> check_result override
  {
#if defined(_ITERATOR_DEBUG_LEVEL) && _ITERATOR_DEBUG_LEVEL != 0
    return check_result::missing("debug iterators allocate a proxy for every container");
#else
    const tao::json::value document{ { "probe", 1 } };
    g_alloc_count = 0;
    // A non-empty object is generated into a reserved buffer: at least one allocation.
    const auto generated = couchbase::core::utils::json::generate_binary(document);
    if (generated.empty() || g_alloc_count == 0) {
      return check_result::missing("allocations in the client library are not counted");
    }
    return check_result::ok();
#endif
  }
};
} // namespace

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

// Every escape is a write of its own: a control character through write(tao::binary_view), a
// newline through write(std::string_view). Each document escapes through one of them only.
auto
escape_heavy_documents() -> std::vector<tao::json::value>
{
  return {
    tao::json::value{ { "controls", std::string(2048, '\x01') } },
    tao::json::value{ { "newlines", std::string(2048, '\n') } },
  };
}

void
generate_binary_matches_generate_across_escaped_and_multi_field_documents(
  [[maybe_unused]] context& ctx)
{
  const auto small_binary = couchbase::core::utils::json::generate_binary(albert());
  assert_true(small_binary == albert_json(), "generate_binary produces the expected JSON bytes");

  for (const auto& escaped : escape_heavy_documents()) {
    assert_true(couchbase::core::utils::json::generate_binary(escaped) ==
                  to_bytes(couchbase::core::utils::json::generate(escaped)),
                "an escape-heavy document matches generate byte-for-byte");
  }

  tao::json::value doc = tao::json::empty_object;
  for (int i = 0; i < 20; ++i) {
    doc["field_" + std::to_string(i)] = "line1\n\t\"quoted_" + std::to_string(i) +
                                        "\"\\backslash\r\n\x01"
                                        "end";
  }
  doc["flag_true"] = true;
  doc["flag_false"] = false;
  doc["null_val"] = tao::json::null;
  assert_true(couchbase::core::utils::json::generate_binary(doc) ==
                to_bytes(couchbase::core::utils::json::generate(doc)),
              "generate_binary matches generate byte-for-byte across multi-field escaped JSON");
}

void
generate_binary_grows_geometrically_through_escape_heavy_documents([[maybe_unused]] context& ctx)
{
  // A reserve of the exact size in either write() reallocates on nearly every write of its document
  // and exceeds the bound, while geometric growth stays within it.
  for (const auto& doc : escape_heavy_documents()) {
    g_alloc_count = 0;
    const auto as_binary = couchbase::core::utils::json::generate_binary(doc);
    const long allocs = g_alloc_count;
    assert_true(!as_binary.empty(), "the document is generated");
    assert_true(allocs <= 12L, "generate_binary grows geometrically rather than per write");
  }
}

void
generate_binary_retains_at_most_the_allowed_multiple_of_its_size([[maybe_unused]] context& ctx)
{
  // The contract: a result keeps at most four times its size in capacity. The implementation names
  // the factor, and the static_assert stops it from drifting away from the contract.
  static_assert(couchbase::core::utils::json::generate_binary_max_capacity_factor == 4);
  constexpr std::size_t max_capacity_factor{ 4 };

  // The results are built in place: a copy would have a capacity equal to its size.
  for (const auto& generated : {
         couchbase::core::utils::json::generate_binary(albert()),
         couchbase::core::utils::json::generate_binary(tao::json::value{ { "x", 1 } }),
         couchbase::core::utils::json::generate_binary(tao::json::value(true)),
         couchbase::core::utils::json::generate_binary(tao::json::empty_object),
         couchbase::core::utils::json::generate_binary(tao::json::empty_array),
       }) {
    assert_true(generated.capacity() <= max_capacity_factor * generated.size(),
                "a generated document retains at most four times its size");
  }
}

void
generate_binary_allocates_for_a_scalar_only_what_its_bytes_need([[maybe_unused]] context& ctx)
{
  // A scalar gets no initial container buffer. Reserving one and shrinking it afterwards leaves
  // the capacity small but costs an allocation that a vector of the same bytes does not.
  const tao::json::value scalar(true);
  g_alloc_count = 0;
  const auto generated = couchbase::core::utils::json::generate_binary(scalar);
  const long generated_allocs = g_alloc_count;
  g_alloc_count = 0;
  const std::vector<std::byte> direct(generated.begin(), generated.end());
  const long direct_allocs = g_alloc_count;
  assert_eq(generated_allocs,
            direct_allocs,
            "a scalar is generated with as many allocations as a vector of its bytes");
  assert_true(direct == generated, "the vector holds the generated bytes");
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
      { CASE(generate_binary_matches_generate_across_escaped_and_multi_field_documents) },
      { CASE(generate_binary_grows_geometrically_through_escape_heavy_documents),
        { std::make_shared<const needs_counted_allocations>() } },
      { CASE(generate_binary_retains_at_most_the_allowed_multiple_of_its_size) },
      { CASE(generate_binary_allocates_for_a_scalar_only_what_its_bytes_need),
        { std::make_shared<const needs_counted_allocations>() } },
    },
  };
}

} // namespace couchbase::test
