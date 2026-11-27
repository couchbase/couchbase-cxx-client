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
#include <string>

namespace couchbase::test
{
namespace
{
void
terms_are_read_in_order([[maybe_unused]] context& ctx)
{
  const auto weights = cbc::parse_operation_weights("1:2:3:4:5");
  assert_eq(weights.gets, std::size_t{ 1 });
  assert_eq(weights.replaces, std::size_t{ 2 });
  assert_eq(weights.deletes, std::size_t{ 3 });
  assert_eq(weights.inserts, std::size_t{ 4 });
  assert_eq(weights.queries, std::size_t{ 5 });
  assert_eq(weights.to_string(), std::string{ "1:2:3:4:5" });
}

void
a_missing_or_malformed_term_keeps_its_default([[maybe_unused]] context& ctx)
{
  const auto weights = cbc::parse_operation_weights("5:x");
  assert_eq(weights.to_string(), std::string{ "5:1:0:0:0" });
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
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_operation_weights",
    {
      { "terms_are_read_in_order", terms_are_read_in_order, {}, timeout::instant },
      { "a_missing_or_malformed_term_keeps_its_default",
        a_missing_or_malformed_term_keeps_its_default,
        {},
        timeout::instant },
      { "a_keyset_rejects_deletes_and_inserts",
        a_keyset_rejects_deletes_and_inserts,
        {},
        timeout::instant },
    },
  };
}
} // namespace couchbase::test
