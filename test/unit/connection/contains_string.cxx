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

#include "core/utils/contains_string.hxx"

#include <string>

namespace couchbase::test
{
namespace
{
using couchbase::core::utils::contains_string;

void
an_empty_substring_always_matches([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("", ""), "in an empty input");
  assert_true(contains_string("hello", ""), "in a non-empty input");
  assert_true(contains_string("", "", true), "in an empty input, ignoring case");
  assert_true(contains_string("hello", "", true), "in a non-empty input, ignoring case");
}

void
an_empty_input_cannot_contain_a_non_empty_substring([[maybe_unused]] context& ctx)
{
  assert_false(contains_string("", "x"), "case-sensitive");
  assert_false(contains_string("", "x", true), "ignoring case");
}

void
a_substring_longer_than_the_input_never_matches([[maybe_unused]] context& ctx)
{
  assert_false(contains_string("ab", "abc"), "case-sensitive");
  assert_false(contains_string("ab", "abc", true), "ignoring case");
}

void
an_exact_substring_is_found([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Index does not exist", "Index"), "at the start");
  assert_true(contains_string("Index does not exist", "does not"), "in the middle");
  assert_true(contains_string("Index does not exist", "exist"), "at the end");
}

void
differing_case_does_not_match_by_default([[maybe_unused]] context& ctx)
{
  assert_false(contains_string("Index does not exist", "index"), "a lowercased substring");
  assert_false(contains_string("Index does not exist", "INDEX"), "an uppercased substring");
  assert_false(contains_string("Index does not exist", "EXIST"),
               "an uppercased trailing substring");
}

void
a_substring_equal_to_the_input_matches([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Bucket Not Found", "Bucket Not Found"), "the whole input");
}

void
a_substring_at_the_start_or_the_end_is_found([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Bucket Not Found", "Bucket"), "at the start");
  assert_true(contains_string("Bucket Not Found", "Found"), "at the end");
}

void
a_missing_substring_is_not_found([[maybe_unused]] context& ctx)
{
  assert_false(contains_string("Bucket Not Found", "Scope"), "absent from the input");
}

void
case_insensitive_matching_ignores_case_on_both_sides([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Index does not exist", "index", true), "a lowercased substring");
  assert_true(contains_string("Index does not exist", "INDEX", true), "an uppercased substring");
  assert_true(contains_string("INDEX DOES NOT EXIST", "index", true), "an uppercased input");
  assert_true(contains_string("index does not exist", "INDEX", true), "a lowercased input");
  assert_true(contains_string("iNdEx DoEs NoT eXiSt", "InDeX", true), "mixed case on both sides");
}

void
non_letter_characters_are_compared_exactly([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Error: 404", "error: 404", true), "digits and punctuation match");
  assert_false(contains_string("Error: 404", "error: 500", true), "differing digits do not match");
}

void
only_ascii_letters_are_folded([[maybe_unused]] context& ctx)
{
  // Bytes outside the ASCII letter range are compared byte-for-byte; no locale machinery is
  // involved.
  assert_true(contains_string("café", "CAF", true), "the ASCII prefix folds");
  assert_true(contains_string("café", "café"), "the non-ASCII bytes match themselves");
  assert_false(contains_string("café", "CAFE", true), "a non-ASCII byte does not fold to E");
}

// The substring checks query_response_parsing.cxx applies to query status code 5000
// ("Internal Error") to tell index_exists, index_not_found and bucket_not_found apart.
void
the_index_already_exists_pattern_is_matched([[maybe_unused]] context& ctx)
{
  const std::string msg = "GSI Index idx1 already exists.";
  assert_true(contains_string(msg, "index", true), "the index token");
  assert_true(contains_string(msg, "already exist", true), "the already-exists token");
}

void
the_index_not_found_pattern_is_matched([[maybe_unused]] context& ctx)
{
  const std::string msg = "GSI index idx1 Not Found.";
  assert_true(contains_string(msg, "index", true), "the index token");
  assert_true(contains_string(msg, "not found", true), "the not-found token");
}

void
the_index_does_not_exist_pattern_is_matched_case_sensitively([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Index does not exist", "Index does not exist"), "the exact message");
}

void
the_bucket_not_found_pattern_is_matched_case_sensitively([[maybe_unused]] context& ctx)
{
  assert_true(contains_string("Bucket Not Found", "Bucket Not Found"), "the exact message");
  assert_false(contains_string("bucket not found", "Bucket Not Found"),
               "a lowercased message is not the same pattern");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_empty_substring_always_matches) },
      { CASE(an_empty_input_cannot_contain_a_non_empty_substring) },
      { CASE(a_substring_longer_than_the_input_never_matches) },
      { CASE(an_exact_substring_is_found) },
      { CASE(differing_case_does_not_match_by_default) },
      { CASE(a_substring_equal_to_the_input_matches) },
      { CASE(a_substring_at_the_start_or_the_end_is_found) },
      { CASE(a_missing_substring_is_not_found) },
      { CASE(case_insensitive_matching_ignores_case_on_both_sides) },
      { CASE(non_letter_characters_are_compared_exactly) },
      { CASE(only_ascii_letters_are_folded) },
      { CASE(the_index_already_exists_pattern_is_matched) },
      { CASE(the_index_not_found_pattern_is_matched) },
      { CASE(the_index_does_not_exist_pattern_is_matched_case_sensitively) },
      { CASE(the_bucket_not_found_pattern_is_matched_case_sensitively) },
    },
  };
}

} // namespace couchbase::test
