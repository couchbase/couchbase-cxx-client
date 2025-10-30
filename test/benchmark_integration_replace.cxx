/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2025 Couchbase, Inc.
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

#include "benchmark_helper_integration.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>

#include <tao/json/value.hpp>

TEST_CASE("benchmark: replace a document", "[benchmark]")
{
  test::utils::integration_test_guard integration;

  auto cluster = integration.public_cluster();
  auto collection = cluster.bucket(integration.ctx.bucket).default_collection();

  const tao::json::value initial_value = {
    { "a", 1.0 },
    { "b", 2.0 },
  };

  const tao::json::value new_value = {
    { "a", 3.0 },
    { "b", 4.0 },
  };

  auto key = test::utils::uniq_id("foo");
  {
    const auto [err, _] = collection.upsert(key, initial_value).get();
    REQUIRE_SUCCESS(err.ec());
  }

  BENCHMARK("replace with Public API")
  {
    const auto [err, _] = collection.replace(key, new_value).get();
    REQUIRE_SUCCESS(err.ec());
  };
}
