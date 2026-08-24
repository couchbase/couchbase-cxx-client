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

#include "core/utils/json.hxx"
#include "tools/document_body_generator.hxx"

#include <cstddef>
#include <set>
#include <string>
#include <vector>

namespace couchbase::test
{
namespace
{
constexpr std::size_t document_size{ 256 };
constexpr std::size_t sample{ 500 };
constexpr std::uint64_t seed{ 1234 };

auto
collect(cbc::document_body_generator& generator, std::size_t count)
  -> std::vector<std::vector<std::byte>>
{
  std::vector<std::vector<std::byte>> bodies;
  bodies.reserve(count);
  for (std::size_t i = 0; i < count; ++i) {
    bodies.push_back(generator.next());
  }
  return bodies;
}

auto
distinct(const std::vector<std::vector<std::byte>>& bodies) -> std::size_t
{
  return std::set<std::vector<std::byte>>{ bodies.begin(), bodies.end() }.size();
}

void
random_document_bodies_are_distinct()
{
  // The defect this guards: a body generated once and reused makes every
  // document identical, and the storage engine then compresses a block of them
  // to a fraction of its size however random a single body looks.
  for (auto format : { cbc::body_format::json, cbc::body_format::binary }) {
    cbc::document_body_generator generator{
      cbc::body_fill::random, format, document_size, 0, seed,
    };

    assert_eq(distinct(collect(generator, sample)), sample, "bodies repeated within one worker");
  }
}

void
a_constant_fill_repeats_one_body()
{
  cbc::document_body_generator generator{
    cbc::body_fill::constant, cbc::body_format::binary, document_size, 0, seed,
  };

  assert_eq(distinct(collect(generator, sample)), std::size_t{ 1 });
}

void
a_binary_body_is_exactly_the_requested_size()
{
  for (auto fill : { cbc::body_fill::constant, cbc::body_fill::random }) {
    cbc::document_body_generator generator{
      fill, cbc::body_format::binary, document_size, 0, seed,
    };

    for (const auto& body : collect(generator, 16)) {
      assert_eq(body.size(), document_size);
    }
  }
}

void
a_random_json_body_parses_and_keeps_its_requested_length()
{
  cbc::document_body_generator generator{
    cbc::body_fill::random, cbc::body_format::json, document_size, 0, seed,
  };

  for (const auto& body : collect(generator, 16)) {
    auto parsed = couchbase::core::utils::json::parse_binary(body);
    assert_eq(parsed["size"].get_unsigned(), static_cast<std::uint64_t>(document_size));
    const auto& text = parsed["text"].get_string();
    assert_eq(text.size(), document_size);
    assert_eq(
      text.find_first_not_of("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"),
      std::string::npos,
      "a JSON body carried a character outside the JSON-safe alphabet");
  }
}

void
a_seed_reproduces_a_dataset_and_a_different_seed_does_not()
{
  const auto make = [](std::uint64_t value) {
    cbc::document_body_generator generator{
      cbc::body_fill::random, cbc::body_format::binary, document_size, 0, value,
    };
    return collect(generator, 32);
  };

  assert_true(make(7) == make(7), "the same seed produced a different sequence");

  // Worker threads offset a shared seed by their index, so neighbouring seeds
  // are the case that must not collide.
  assert_false(make(7) == make(8), "neighbouring seeds produced the same sequence");
}

void
a_pool_holds_distinct_bodies_and_cycles_through_them()
{
  constexpr std::size_t pooled{ 8 };

  cbc::document_body_generator generator{
    cbc::body_fill::random, cbc::body_format::binary, document_size, document_size * pooled, seed,
  };
  assert_eq(generator.pooled_documents(), pooled);

  const auto first = collect(generator, pooled);
  assert_eq(distinct(first), pooled, "the pool holds duplicate bodies");

  // Cycled, not sampled: a pool drawn from at random repeats bodies inside a
  // single storage block long before it is exhausted.
  assert_true(collect(generator, pooled) == first, "the pool did not cycle in order");
}

void
a_zero_document_size_yields_the_predefined_document()
{
  const auto predefined = couchbase::core::utils::to_binary(R"({"type":"fake_profile"})");

  cbc::document_body_generator generator{
    cbc::body_fill::random, cbc::body_format::json, 0, 0, seed, predefined,
  };

  for (const auto& body : collect(generator, 4)) {
    assert_true(body == predefined, "a zero size did not yield the predefined document");
  }
}
} // namespace

auto
tests() -> test_suite
{
  return {
    "tools_document_body_generator",
    {
      { "random_document_bodies_are_distinct", random_document_bodies_are_distinct, timeout::fast },
      { "a_constant_fill_repeats_one_body", a_constant_fill_repeats_one_body, timeout::instant },
      { "a_binary_body_is_exactly_the_requested_size",
        a_binary_body_is_exactly_the_requested_size,
        timeout::instant },
      { "a_random_json_body_parses_and_keeps_its_requested_length",
        a_random_json_body_parses_and_keeps_its_requested_length,
        timeout::instant },
      { "a_seed_reproduces_a_dataset_and_a_different_seed_does_not",
        a_seed_reproduces_a_dataset_and_a_different_seed_does_not,
        timeout::instant },
      { "a_pool_holds_distinct_bodies_and_cycles_through_them",
        a_pool_holds_distinct_bodies_and_cycles_through_them,
        timeout::instant },
      { "a_zero_document_size_yields_the_predefined_document",
        a_zero_document_size_yields_the_predefined_document,
        timeout::instant },
    },
  };
}
} // namespace couchbase::test
