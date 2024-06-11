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

#include "test_helper_integration.hxx"

#include "core/management/search_index.hxx"

TEST_CASE("unit: can determine if an index is a vector index")
{
  SECTION("vector index")
  {
    couchbase::core::management::search::index search_index{};
    search_index.params_json = test::utils::read_test_data("sample_vector_index_params.json");

    REQUIRE(search_index.is_vector_index());
  }

  SECTION("vector index with nested properties")
  {

    couchbase::core::management::search::index search_index{};
    search_index.params_json =
      test::utils::read_test_data("sample_vector_index_with_nested_properties_params.json");

    REQUIRE(search_index.is_vector_index());
  }

  SECTION("non-vector index")
  {

    couchbase::core::management::search::index search_index{};
    search_index.params_json = test::utils::read_test_data("travel_sample_index_params.json");

    REQUIRE_FALSE(search_index.is_vector_index());
  }
}
