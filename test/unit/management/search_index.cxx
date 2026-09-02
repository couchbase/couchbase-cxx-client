/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "utils/test_data.hxx"

#include "core/management/search_index.hxx"

namespace couchbase::test
{
namespace
{
auto
index_with_params(const std::string& file) -> couchbase::core::management::search::index
{
  couchbase::core::management::search::index search_index{};
  search_index.params_json = ::test::utils::read_test_data(file);
  return search_index;
}

void
an_index_with_a_vector_field_is_a_vector_index([[maybe_unused]] context& ctx)
{
  assert_true(index_with_params("sample_vector_index_params.json").is_vector_index(),
              "a vector field at the top level of the mapping");
}

void
a_vector_field_under_a_nested_property_is_found([[maybe_unused]] context& ctx)
{
  assert_true(
    index_with_params("sample_vector_index_with_nested_properties_params.json").is_vector_index(),
    "a vector field reached only by descending into a property");
}

void
an_index_without_a_vector_field_is_not_a_vector_index([[maybe_unused]] context& ctx)
{
  assert_false(index_with_params("travel_sample_index_params.json").is_vector_index(),
               "an ordinary full-text index");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(an_index_with_a_vector_field_is_a_vector_index) },
      { CASE(a_vector_field_under_a_nested_property_is_found) },
      { CASE(an_index_without_a_vector_field_is_not_a_vector_index) },
    },
  };
}

} // namespace couchbase::test
