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

#include "framework/test_registry.hxx"

#include "core/meta/version.hxx"
#include "core/platform/base64.h"
#include "core/utils/concurrent_fixed_priority_queue.hxx"
#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "core/utils/movable_function.hxx"
#include "core/utils/url_codec.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/build_version.hxx>
#include <couchbase/error_codes.hxx>

#include "include_ssl/crypto.h"

#include <spdlog/fmt/fmt.h>
#include <tao/json.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace couchbase::core::meta
{
std::string
parse_git_describe_output(const std::string& git_describe_output);
}

namespace couchbase::test
{
namespace
{
constexpr const char* ssl_lib_id =
#if defined(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
  "bssl"
#else
  "ssl"
#endif
  ;

auto
with_build(std::string_view input)
{
  // fmt::runtime, because the pattern arrives as a value: fmt::format's checked overload is
  // consteval from C++20 on and would reject it there, while accepting it under C++17.
  return fmt::format(fmt::runtime(input), fmt::arg("build", COUCHBASE_CXX_CLIENT_VERSION_BUILD));
}

void
a_duplicate_json_key_is_resolved_rather_than_rejected([[maybe_unused]] context& ctx)
{
  std::string input{ R"({"answer":"wrong","answer":42})" };

  // The bare parser refuses the document; the SDK's own parser installs a transformer that keeps
  // the last value for a repeated key.
  assert_throws_with(
    "duplicate JSON object key \"answer\"",
    [&input]() {
      static_cast<void>(tao::json::from_string(input));
    },
    "the untransformed parser names the duplicate key");

  auto result = couchbase::core::utils::json::parse(input);
  assert_true(result.is_object(), "the document parses as an object");
  assert_true(result.find("answer") != nullptr, "the repeated key is present once");
  assert_true(result["answer"].is_integer(), "the last value wins, and it is the integer");
  assert_eq(result["answer"].as<std::int64_t>(), std::int64_t{ 42 }, "the last value");
}

void
an_error_code_renders_as_its_category_and_value([[maybe_unused]] context& ctx)
{
  std::error_code rc = couchbase::errc::common::authentication_failure;
  assert_eq(std::string{ rc.category().name() },
            std::string{ "couchbase.common" },
            "the category the code belongs to");
  assert_eq(rc.value(), 6, "the code's value within its category");

  std::stringstream ss;
  ss << rc;
  assert_eq(ss.str(), std::string{ "couchbase.common:6" }, "the streamed rendering");
}

void
a_path_separator_is_percent_escaped([[maybe_unused]] context& ctx)
{
  assert_eq(couchbase::core::utils::string_codec::v2::path_escape("a/b"),
            std::string{ "a%2Fb" },
            "a slash inside a path segment does not stay a separator");
}

void
join_strings_separates_every_element([[maybe_unused]] context& ctx)
{
  std::vector<std::string> field_specs{ "testkey:string" };
  assert_eq(couchbase::core::utils::join_strings(field_specs, ","),
            std::string{ "testkey:string" },
            "a single element carries no separator");

  field_specs.emplace_back("volume:double");
  field_specs.emplace_back("id:integer");
  assert_eq(couchbase::core::utils::join_strings(field_specs, ","),
            std::string{ "testkey:string,volume:double,id:integer" },
            "the separator goes between elements, not around them");
}

void
join_strings_fmt_separates_every_element([[maybe_unused]] context& ctx)
{
  std::vector<std::string> field_specs{ "testkey:string" };
  assert_eq(couchbase::core::utils::join_strings_fmt(field_specs, ","),
            std::string{ "testkey:string" },
            "a single element carries no separator");

  field_specs.emplace_back("volume:double");
  field_specs.emplace_back("id:integer");
  assert_eq(couchbase::core::utils::join_strings_fmt(field_specs, ","),
            std::string{ "testkey:string,volume:double,id:integer" },
            "the separator goes between elements, not around them");
}

void
the_user_agent_trims_and_escapes_the_extra_it_is_given([[maybe_unused]] context& ctx)
{
  std::string core_version = fmt::format("cxx/{} ({}/{};{}/0x{:x})",
                                         couchbase::core::meta::sdk_semver(),
                                         COUCHBASE_CXX_CLIENT_SYSTEM_NAME,
                                         COUCHBASE_CXX_CLIENT_SYSTEM_PROCESSOR,
                                         ssl_lib_id,
                                         OpenSSL_version_num());

  auto simple_user_agent = couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE");
  assert_eq(simple_user_agent,
            fmt::format(R"({{"a":"{}","i":"0xDEADBEEF/0xCAFEBEBE"}})", core_version),
            "the agent with no extra");

  auto core_version_prefix = core_version.substr(0, core_version.size() - 1);
  assert_eq(
    couchbase::core::meta::user_agent_for_mcbp(
      "0xDEADBEEF", "0xCAFEBEBE", "couchnode/1.2.3; openssl/1.1.1l"),
    fmt::format(R"!({{"a":"{};couchnode/1.2.3; openssl/1.1.1l)","i":"0xDEADBEEF/0xCAFEBEBE"}})!",
                core_version_prefix),
    "a wrapper's extra is appended inside the version's parentheses");

  std::string long_extra = "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef"
                           "01234567890abcdef01234567890abcdef";
  assert_eq(long_extra.size(), std::size_t{ 272 }, "the extra used below outgrows every budget");

  assert_eq(couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra),
            fmt::format(
              R"!({{"a":"{};{})","i":"0xDEADBEEF/0xCAFEBEBE"}})!", core_version_prefix, long_extra),
            "with no budget the extra is carried whole");

  auto trimmed_user_agent =
    couchbase::core::meta::user_agent_for_mcbp("0xDEADBEEF", "0xCAFEBEBE", long_extra, 250);
  assert_eq(trimmed_user_agent.size(), std::size_t{ 250 }, "the budget is filled exactly");
  assert_eq(trimmed_user_agent,
            fmt::format(R"!({{"a":"{};{})","i":"0xDEADBEEF/0xCAFEBEBE"}})!",
                        core_version_prefix,
                        long_extra.substr(0, 250 - simple_user_agent.size() - 1 /* ';' */)),
            "the extra is cut, and nothing else is");

  auto long_extra_with_non_printable_characters =
    long_extra.substr(0, 250 - simple_user_agent.size() - 4 /* ';' and room for 1.5 of '\n\n' */) +
    "\n\n";
  trimmed_user_agent = couchbase::core::meta::user_agent_for_mcbp(
    "0xDEADBEEF", "0xCAFEBEBE", long_extra_with_non_printable_characters, 250);
  assert_eq(
    trimmed_user_agent.size(), std::size_t{ 249 }, "an escape that would not fit is dropped whole");
  assert_eq(
    trimmed_user_agent,
    fmt::format(
      R"!({{"a":"{};{})","i":"0xDEADBEEF/0xCAFEBEBE"}})!",
      core_version_prefix,
      long_extra.substr(0, 250 - simple_user_agent.size() - 4 /* ';' and room for '\n' */) + "\\n"),
    "a newline reaches the agent escaped rather than raw");

  auto long_and_weird_extra = "hello" + std::string(300, '\n');
  trimmed_user_agent = couchbase::core::meta::user_agent_for_mcbp(
    "0xDEADBEEF", "0xCAFEBEBE", long_and_weird_extra, 250);
  assert_eq(trimmed_user_agent,
            simple_user_agent,
            "an extra whose escaped form cannot fit is dropped entirely");

  assert_eq(couchbase::core::meta::user_agent_for_http("0xDEADBEEF", "0xCAFEBEBE", "hello\nworld"),
            fmt::format("{};client/0xDEADBEEF;session/0xCAFEBEBE;{};hello world)",
                        core_version_prefix,
                        couchbase::core::meta::os()),
            "the HTTP agent names the client and session, and folds the newline to a space");
}

void
build_info_reports_whether_couchbase2_is_supported([[maybe_unused]] context& ctx)
{
  const auto info = couchbase::core::meta::sdk_build_info();
  const auto entry = info.find("couchbase2");
  assert_true(entry != info.end(), "the build info names couchbase2");

  // Asserted against the macro rather than against a fixed value, so the case holds in either
  // configuration and still fails if the entry stops tracking how the library was built.
#if defined(COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2)
  assert_eq(entry->second, std::string{ "true" }, "the entry tracks how the library was built");
#else
  assert_eq(entry->second, std::string{ "false" }, "the entry tracks how the library was built");
#endif

  // The JSON rendering keeps its own list of which keys are booleans, so the raw entry above says
  // nothing about it: assert the rendered type as well as the value, or dropping the key from that
  // list would silently turn it back into a string.
  // Through a string_view: parse() also has a json_string overload, and std::string converts to
  // both.
  const auto build_info_json = couchbase::core::meta::sdk_build_info_json();
  const auto rendered = couchbase::core::utils::json::parse(std::string_view{ build_info_json });
  const auto* couchbase2 = rendered.find("couchbase2");
  assert_true(couchbase2 != nullptr, "the rendered build info names couchbase2");
  assert_true(couchbase2->is_boolean(), "the key renders as a JSON boolean, not a string");
  assert_eq(couchbase2->get_boolean(),
            entry->second == "true",
            "the rendered value agrees with the raw entry");
}

void
a_moved_from_movable_function_is_empty([[maybe_unused]] context& ctx)
{
  auto ptr = std::make_unique<int>(42);
  couchbase::core::utils::movable_function<bool(int)> src_handler = [ptr =
                                                                       std::move(ptr)](int val) {
    return ptr != nullptr && *ptr == val;
  };
  assert_true(static_cast<bool>(src_handler), "a constructed function holds a target");
  assert_true(src_handler(42), "the captured value reaches the call");
  assert_false(src_handler(43), "the captured value is compared, not ignored");

  couchbase::core::utils::movable_function<bool(int)> dst_handler = std::move(src_handler);
  assert_true(static_cast<bool>(dst_handler), "the target moves across");
  assert_true(dst_handler(42), "the captured value moves with it");
  assert_false(dst_handler(43), "the captured value is still compared");
  // A moved-from movable_function is required to be empty; reading it here is
  // that specification, not a use-after-move.
  // cppcheck-suppress accessMoved
  assert_false(static_cast<bool>(src_handler), "the moved-from function holds nothing");
}

void
base64_encodes_with_and_without_line_breaks([[maybe_unused]] context& ctx)
{
  assert_eq(couchbase::core::base64::encode(std::vector{ std::byte{ 255 } }, false),
            std::string{ "/w==" },
            "a single byte is padded to a full quantum");
  assert_eq(couchbase::core::base64::encode(std::vector{ std::byte{ 255 } }, true),
            std::string{ "/w==\n" },
            "the pretty form ends with a line break");

  std::array binary{
    std::byte{ 0x00 }, std::byte{ 0x01 }, std::byte{ 0x02 }, std::byte{ 0x03 }, std::byte{ 0x04 },
    std::byte{ 0x05 }, std::byte{ 0x06 }, std::byte{ 0x07 }, std::byte{ 0x08 }, std::byte{ 0x09 },
    std::byte{ 0x0a }, std::byte{ 0x0b }, std::byte{ 0x0c }, std::byte{ 0x0d }, std::byte{ 0x0e },
    std::byte{ 0x0f }, std::byte{ 0x10 }, std::byte{ 0x11 }, std::byte{ 0x12 }, std::byte{ 0x13 },
    std::byte{ 0x14 }, std::byte{ 0x15 }, std::byte{ 0x16 }, std::byte{ 0x17 }, std::byte{ 0x18 },
    std::byte{ 0x19 }, std::byte{ 0x1a }, std::byte{ 0x1b }, std::byte{ 0x1c }, std::byte{ 0x1d },
    std::byte{ 0x1e }, std::byte{ 0x1f }, std::byte{ 0x20 }, std::byte{ 0x21 }, std::byte{ 0x22 },
    std::byte{ 0x23 }, std::byte{ 0x24 }, std::byte{ 0x25 }, std::byte{ 0x26 }, std::byte{ 0x27 },
    std::byte{ 0x28 }, std::byte{ 0x29 }, std::byte{ 0x2a }, std::byte{ 0x2b }, std::byte{ 0x2c },
    std::byte{ 0x2d }, std::byte{ 0x2e }, std::byte{ 0x2f }, std::byte{ 0x30 }, std::byte{ 0x31 },
    std::byte{ 0x32 }, std::byte{ 0x33 }, std::byte{ 0x34 }, std::byte{ 0x35 }, std::byte{ 0x36 },
    std::byte{ 0x37 }, std::byte{ 0x38 }, std::byte{ 0x39 }, std::byte{ 0x3a }, std::byte{ 0x3b },
    std::byte{ 0x3c }, std::byte{ 0x3d }, std::byte{ 0x3e }, std::byte{ 0x3f }, std::byte{ 0x40 },
    std::byte{ 0x41 }, std::byte{ 0x42 }, std::byte{ 0x43 }, std::byte{ 0x44 }, std::byte{ 0x45 },
    std::byte{ 0x46 }, std::byte{ 0x47 }, std::byte{ 0x48 }, std::byte{ 0x49 }, std::byte{ 0x4a },
    std::byte{ 0x4b }, std::byte{ 0x4c }, std::byte{ 0x4d }, std::byte{ 0x4e }, std::byte{ 0x4f },
    std::byte{ 0x50 }, std::byte{ 0x51 }, std::byte{ 0x52 }, std::byte{ 0x53 }, std::byte{ 0x54 },
    std::byte{ 0x55 }, std::byte{ 0x56 }, std::byte{ 0x57 }, std::byte{ 0x58 }, std::byte{ 0x59 },
    std::byte{ 0x5a }, std::byte{ 0x5b }, std::byte{ 0x5c }, std::byte{ 0x5d }, std::byte{ 0x5e },
    std::byte{ 0x5f }, std::byte{ 0x60 }, std::byte{ 0x61 }, std::byte{ 0x62 }, std::byte{ 0x63 },
    std::byte{ 0x64 }, std::byte{ 0x65 }, std::byte{ 0x66 }, std::byte{ 0x67 }, std::byte{ 0x68 },
    std::byte{ 0x69 }, std::byte{ 0x6a }, std::byte{ 0x6b }, std::byte{ 0x6c }, std::byte{ 0x6d },
    std::byte{ 0x6e }, std::byte{ 0x6f }, std::byte{ 0x70 }, std::byte{ 0x71 }, std::byte{ 0x72 },
    std::byte{ 0x73 }, std::byte{ 0x74 }, std::byte{ 0x75 }, std::byte{ 0x76 }, std::byte{ 0x77 },
    std::byte{ 0x78 }, std::byte{ 0x79 }, std::byte{ 0x7a }, std::byte{ 0x7b }, std::byte{ 0x7c },
    std::byte{ 0x7d }, std::byte{ 0x7e }, std::byte{ 0x7f }, std::byte{ 0x80 }, std::byte{ 0x81 },
    std::byte{ 0x82 }, std::byte{ 0x83 }, std::byte{ 0x84 }, std::byte{ 0x85 }, std::byte{ 0x86 },
    std::byte{ 0x87 }, std::byte{ 0x88 }, std::byte{ 0x89 }, std::byte{ 0x8a }, std::byte{ 0x8b },
    std::byte{ 0x8c }, std::byte{ 0x8d }, std::byte{ 0x8e }, std::byte{ 0x8f }, std::byte{ 0x90 },
    std::byte{ 0x91 }, std::byte{ 0x92 }, std::byte{ 0x93 }, std::byte{ 0x94 }, std::byte{ 0x95 },
    std::byte{ 0x96 }, std::byte{ 0x97 }, std::byte{ 0x98 }, std::byte{ 0x99 }, std::byte{ 0x9a },
    std::byte{ 0x9b }, std::byte{ 0x9c }, std::byte{ 0x9d }, std::byte{ 0x9e }, std::byte{ 0x9f },
    std::byte{ 0xa0 }, std::byte{ 0xa1 }, std::byte{ 0xa2 }, std::byte{ 0xa3 }, std::byte{ 0xa4 },
    std::byte{ 0xa5 }, std::byte{ 0xa6 }, std::byte{ 0xa7 }, std::byte{ 0xa8 }, std::byte{ 0xa9 },
    std::byte{ 0xaa }, std::byte{ 0xab }, std::byte{ 0xac }, std::byte{ 0xad }, std::byte{ 0xae },
    std::byte{ 0xaf }, std::byte{ 0xb0 }, std::byte{ 0xb1 }, std::byte{ 0xb2 }, std::byte{ 0xb3 },
    std::byte{ 0xb4 }, std::byte{ 0xb5 }, std::byte{ 0xb6 }, std::byte{ 0xb7 }, std::byte{ 0xb8 },
    std::byte{ 0xb9 }, std::byte{ 0xba }, std::byte{ 0xbb }, std::byte{ 0xbc }, std::byte{ 0xbd },
    std::byte{ 0xbe }, std::byte{ 0xbf }, std::byte{ 0xc0 }, std::byte{ 0xc1 }, std::byte{ 0xc2 },
    std::byte{ 0xc3 }, std::byte{ 0xc4 }, std::byte{ 0xc5 }, std::byte{ 0xc6 }, std::byte{ 0xc7 },
    std::byte{ 0xc8 }, std::byte{ 0xc9 }, std::byte{ 0xca }, std::byte{ 0xcb }, std::byte{ 0xcc },
    std::byte{ 0xcd }, std::byte{ 0xce }, std::byte{ 0xcf }, std::byte{ 0xd0 }, std::byte{ 0xd1 },
    std::byte{ 0xd2 }, std::byte{ 0xd3 }, std::byte{ 0xd4 }, std::byte{ 0xd5 }, std::byte{ 0xd6 },
    std::byte{ 0xd7 }, std::byte{ 0xd8 }, std::byte{ 0xd9 }, std::byte{ 0xda }, std::byte{ 0xdb },
    std::byte{ 0xdc }, std::byte{ 0xdd }, std::byte{ 0xde }, std::byte{ 0xdf }, std::byte{ 0xe0 },
    std::byte{ 0xe1 }, std::byte{ 0xe2 }, std::byte{ 0xe3 }, std::byte{ 0xe4 }, std::byte{ 0xe5 },
    std::byte{ 0xe6 }, std::byte{ 0xe7 }, std::byte{ 0xe8 }, std::byte{ 0xe9 }, std::byte{ 0xea },
    std::byte{ 0xeb }, std::byte{ 0xec }, std::byte{ 0xed }, std::byte{ 0xee }, std::byte{ 0xef },
    std::byte{ 0xf0 }, std::byte{ 0xf1 }, std::byte{ 0xf2 }, std::byte{ 0xf3 }, std::byte{ 0xf4 },
    std::byte{ 0xf5 }, std::byte{ 0xf6 }, std::byte{ 0xf7 }, std::byte{ 0xf8 }, std::byte{ 0xf9 },
    std::byte{ 0xfa }, std::byte{ 0xfb }, std::byte{ 0xfc }, std::byte{ 0xfd }, std::byte{ 0xfe },
    std::byte{ 0xff }
  };

  std::string base64{
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+"
    "P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+"
    "AgYKDhIWGh4iJiouMjY6PkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/"
    "wMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v8PHy8/T19vf4+fr7/P3+/w=="
  };

  std::string base64_pretty{
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v\nMDEyMzQ1Njc4OTo7PD0+"
    "P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5f\nYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+"
    "AgYKDhIWGh4iJiouMjY6P\nkJGSk5SVlpeYmZqbnJ2en6ChoqOkpaanqKmqq6ytrq+wsbKztLW2t7i5uru8vb6/"
    "\nwMHCw8TFxsfIycrLzM3Oz9DR0tPU1dbX2Nna29zd3t/g4eLj5OXm5+jp6uvs7e7v\n8PHy8/T19vf4+fr7/P3+/w==\n"
  };

  assert_eq(couchbase::core::base64::encode(binary, false), base64, "every byte value round-trips");
  assert_eq(couchbase::core::base64::encode(binary, true),
            base64_pretty,
            "the pretty form breaks the line every 64 characters");
}

void
git_describe_output_becomes_a_semantic_version([[maybe_unused]] context& ctx)
{
  assert_eq(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4-16-gfbc9922"),
            with_build("1.0.0-beta.4+16.{build}.fbc9922"),
            "a pre-release with commits since the tag");
  assert_eq(couchbase::core::meta::parse_git_describe_output("1.0.0-16-gfbc9922"),
            with_build("1.0.0+16.{build}.fbc9922"),
            "a release with commits since the tag");
  assert_eq(couchbase::core::meta::parse_git_describe_output(""),
            std::string{},
            "no description yields no version");
  assert_eq(couchbase::core::meta::parse_git_describe_output("unknown"),
            std::string{},
            "a description git could not produce yields no version");
  assert_eq(couchbase::core::meta::parse_git_describe_output("invalid"),
            std::string{},
            "a description that is not a version yields no version");
  assert_eq(couchbase::core::meta::parse_git_describe_output("1.0.0.0.0"),
            std::string{},
            "a four-component version is not semantic");
  assert_eq(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4-0-gfbc9922"),
            with_build("1.0.0-beta.4"),
            "no commits since the tag leaves the tag alone");
  assert_eq(couchbase::core::meta::parse_git_describe_output("1.0.0-beta.4"),
            std::string{ "1.0.0-beta.4" },
            "a bare tag is already a semantic version");
}

void
a_priority_queue_below_capacity_drops_nothing([[maybe_unused]] context& ctx)
{
  auto queue = couchbase::core::utils::concurrent_fixed_priority_queue<int>(3);
  assert_true(queue.empty(), "a new queue holds nothing");

  queue.emplace(1);
  queue.emplace(2);

  auto [data, dropped] = queue.steal_data();
  assert_eq(dropped, std::size_t{ 0 }, "nothing was dropped");
  assert_eq(data.size(), std::size_t{ 2 }, "every item was kept");
  assert_eq(data.top(), 2, "the highest priority comes first");
  data.pop();
  assert_eq(data.top(), 1, "the next highest priority follows");
  data.pop();

  assert_true(queue.empty(), "stealing the data empties the queue");
}

void
a_priority_queue_at_capacity_drops_nothing([[maybe_unused]] context& ctx)
{
  auto queue = couchbase::core::utils::concurrent_fixed_priority_queue<int>(3);
  assert_true(queue.empty(), "a new queue holds nothing");

  queue.emplace(10);
  queue.emplace(1);
  queue.emplace(2);

  auto [data, dropped] = queue.steal_data();
  assert_eq(dropped, std::size_t{ 0 }, "nothing was dropped");
  assert_eq(data.size(), std::size_t{ 3 }, "every item was kept");
  assert_eq(data.top(), 10, "the highest priority comes first");
  data.pop();
  assert_eq(data.top(), 2, "the next highest priority follows");
  data.pop();
  assert_eq(data.top(), 1, "the lowest priority comes last");
  data.pop();

  assert_true(queue.empty(), "stealing the data empties the queue");
}

void
a_priority_queue_over_capacity_drops_the_lowest_priorities([[maybe_unused]] context& ctx)
{
  auto queue = couchbase::core::utils::concurrent_fixed_priority_queue<int>(3);
  assert_true(queue.empty(), "a new queue holds nothing");

  queue.emplace(2);
  queue.emplace(10);
  queue.emplace(1);
  queue.emplace(20);
  queue.emplace(5);

  auto [data, dropped] = queue.steal_data();
  assert_eq(dropped, std::size_t{ 2 }, "the surplus is counted, not silently lost");
  assert_eq(data.size(), std::size_t{ 3 }, "the queue never exceeds its capacity");
  assert_eq(data.top(), 20, "the highest priority survives");
  data.pop();
  assert_eq(data.top(), 10, "the next highest survives");
  data.pop();
  assert_eq(data.top(), 5, "the lowest surviving priority still beats what was dropped");
  data.pop();

  assert_true(queue.empty(), "stealing the data empties the queue");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_duplicate_json_key_is_resolved_rather_than_rejected), {}, timeout::instant },
      { CASE(an_error_code_renders_as_its_category_and_value), {}, timeout::instant },
      { CASE(a_path_separator_is_percent_escaped), {}, timeout::instant },
      { CASE(join_strings_separates_every_element), {}, timeout::instant },
      { CASE(join_strings_fmt_separates_every_element), {}, timeout::instant },
      { CASE(the_user_agent_trims_and_escapes_the_extra_it_is_given), {}, timeout::instant },
      { CASE(build_info_reports_whether_couchbase2_is_supported), {}, timeout::instant },
      { CASE(a_moved_from_movable_function_is_empty), {}, timeout::instant },
      { CASE(base64_encodes_with_and_without_line_breaks), {}, timeout::instant },
      { CASE(git_describe_output_becomes_a_semantic_version), {}, timeout::instant },
      { CASE(a_priority_queue_below_capacity_drops_nothing), {}, timeout::instant },
      { CASE(a_priority_queue_at_capacity_drops_nothing), {}, timeout::instant },
      { CASE(a_priority_queue_over_capacity_drops_the_lowest_priorities), {}, timeout::instant },
    },
  };
}

} // namespace couchbase::test
