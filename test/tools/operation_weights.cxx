/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "test/framework/test_framework.hxx"

#include "tools/operation_weights.hxx"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
void
terms_are_read_in_order([[maybe_unused]] context& ctx)
{
  const auto weights = cbc::parse_operation_weights("1:2:3:4:5:6");
  assert_eq(weights.gets, std::size_t{ 1 });
  assert_eq(weights.replaces, std::size_t{ 2 });
  assert_eq(weights.deletes, std::size_t{ 3 });
  assert_eq(weights.inserts, std::size_t{ 4 });
  assert_eq(weights.queries, std::size_t{ 5 });
  assert_eq(weights.upserts, std::size_t{ 6 });
  assert_eq(weights.to_string(), std::string{ "1:2:3:4:5:6" });
  // The all-zero check and the operation draw read to_vector().
  assert_eq(weights.to_vector(), std::vector<std::size_t>{ 1, 2, 3, 4, 5, 6 });
}

void
a_five_term_ratio_has_no_upserts([[maybe_unused]] context& ctx)
{
  const auto weights = cbc::parse_operation_weights("1:1:0:0:0");
  assert_eq(weights.upserts, std::size_t{ 0 });
  assert_eq(weights.to_string(), std::string{ "1:1:0:0:0:0" });
}

void
a_missing_or_malformed_term_keeps_its_default([[maybe_unused]] context& ctx)
{
  const auto weights = cbc::parse_operation_weights("5:x");
  assert_eq(weights.to_string(), std::string{ "5:1:0:0:0:0" });
  // A sign must not wrap to the largest unsigned weight.
  assert_eq(cbc::parse_operation_weights("-1:5").to_string(), std::string{ "1:5:0:0:0:0" });
  assert_eq(cbc::parse_operation_weights("G=1:0:-1").to_string(), std::string{ "1:0:0:0:0:0" });
  assert_eq(cbc::parse_operation_weights("9x:2").to_string(), std::string{ "9:2:0:0:0:0" });
}

void
named_terms_leave_the_others_at_zero([[maybe_unused]] context& ctx)
{
  // Each letter sets its own slot.
  assert_eq(cbc::parse_operation_weights("G=1").to_string(), std::string{ "1:0:0:0:0:0" });
  assert_eq(cbc::parse_operation_weights("R=1").to_string(), std::string{ "0:1:0:0:0:0" });
  assert_eq(cbc::parse_operation_weights("D=1").to_string(), std::string{ "0:0:1:0:0:0" });
  assert_eq(cbc::parse_operation_weights("I=1").to_string(), std::string{ "0:0:0:1:0:0" });
  assert_eq(cbc::parse_operation_weights("Q=1").to_string(), std::string{ "0:0:0:0:1:0" });
  assert_eq(cbc::parse_operation_weights("U=1").to_string(), std::string{ "0:0:0:0:0:1" });
  for (const char* input : { "G=1:U=1", "g=1:u=1", "G=1::U=1", "U=1:G=1" }) {
    assert_eq(cbc::parse_operation_weights(input).to_string(), std::string{ "1:0:0:0:0:1" }, input);
  }
}

void
a_positional_term_follows_the_previous_term([[maybe_unused]] context& ctx)
{
  assert_eq(cbc::parse_operation_weights("G=1:0:0:0:0:U=1").to_string(),
            std::string{ "1:0:0:0:0:1" });
  assert_eq(cbc::parse_operation_weights("R=2:5").to_string(), std::string{ "0:2:5:0:0:0" });
  assert_eq(cbc::parse_operation_weights("R=2::5").to_string(), std::string{ "0:2:0:5:0:0" });
  assert_eq(cbc::parse_operation_weights("U=1:5").to_string(), std::string{ "0:0:0:0:0:1" });
  // A named term after the first still clears every weight it does not set.
  assert_eq(cbc::parse_operation_weights("1:U=1").to_string(), std::string{ "1:0:0:0:0:1" });
  // An empty term must not clear a weight a named term already set.
  assert_eq(cbc::parse_operation_weights("D=4:R=2::5").to_string(), std::string{ "0:2:4:5:0:0" });
}

void
a_bad_named_term_is_rejected([[maybe_unused]] context& ctx)
{
  for (const char* input :
       { "X=1", "G=abc", "G=", "GR=1", "G=1x", "U=1:5:X=1", "G=18446744073709551616:U=1" }) {
    assert_throws<std::invalid_argument>(
      [input] {
        [[maybe_unused]] const auto weights = cbc::parse_operation_weights(input);
      },
      input);
  }
}

void
a_keyset_rejects_deletes_and_inserts([[maybe_unused]] context& ctx)
{
  assert_true(cbc::keeps_every_key(cbc::parse_operation_weights("3:1:0:0:1")),
              "gets, replaces and queries keep every key");
  assert_false(cbc::keeps_every_key(cbc::parse_operation_weights("1:1:1:0:0")),
               "a delete weight removes keys");
  assert_false(cbc::keeps_every_key(cbc::parse_operation_weights("1:1:0:1:0")),
               "an insert weight adds keys");
  assert_true(cbc::keeps_every_key(cbc::parse_operation_weights("0:0:0:0:0:1")),
              "an upsert writes a key the keyset already has");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_operation_weights",
    {
      { "terms_are_read_in_order", terms_are_read_in_order, {}, timeout::instant },
      { "a_five_term_ratio_has_no_upserts",
        a_five_term_ratio_has_no_upserts,
        {},
        timeout::instant },
      { "a_missing_or_malformed_term_keeps_its_default",
        a_missing_or_malformed_term_keeps_its_default,
        {},
        timeout::instant },
      { "named_terms_leave_the_others_at_zero",
        named_terms_leave_the_others_at_zero,
        {},
        timeout::instant },
      { "a_positional_term_follows_the_previous_term",
        a_positional_term_follows_the_previous_term,
        {},
        timeout::instant },
      { "a_bad_named_term_is_rejected", a_bad_named_term_is_rejected, {}, timeout::instant },
      { "a_keyset_rejects_deletes_and_inserts",
        a_keyset_rejects_deletes_and_inserts,
        {},
        timeout::instant },
    },
  };
}
} // namespace couchbase::test
