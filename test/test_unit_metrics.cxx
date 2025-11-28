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

#include "test_helper.hxx"

#include "core/metrics/meter_wrapper.hxx"
#include "core/tracing/constants.hxx"

#include <couchbase/error_codes.hxx>
#include <spdlog/fmt/bundled/printf.h>

TEST_CASE("unit: metric attributes encoding", "[unit]")
{
  SECTION("all attributes set")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      "test-bucket",
      "test-scope",
      "test-collection",
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 9);
    REQUIRE(tags.at("couchbase.service") == "kv");
    REQUIRE(tags.at("db.operation.name") == "get");
    REQUIRE(tags.at("db.namespace") == "test-bucket");
    REQUIRE(tags.at("couchbase.scope.name") == "test-scope");
    REQUIRE(tags.at("couchbase.collection.name") == "test-collection");
    REQUIRE(tags.at("error.type") == "DocumentNotFound");
    REQUIRE(tags.at("couchbase.cluster.name") == "test-cluster");
    REQUIRE(tags.at("couchbase.cluster.uuid") == "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251");
    REQUIRE(tags.at("db.system.name") == "couchbase");
  }

  SECTION("successful operation")
  {
    couchbase::core::metrics::metric_attributes attrs{ couchbase::core::tracing::service::key_value,
                                                       "get",
                                                       {},
                                                       "test-bucket",
                                                       "test-scope",
                                                       "test-collection",
                                                       { "test-cluster",
                                                         "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" } };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 8);
    REQUIRE(tags.find("error.type") == tags.end());
  }

  SECTION("cluster labels missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      {},
      "test-bucket",
      "test-scope",
      "test-collection",
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 6);
    REQUIRE(tags.find("couchbase.cluster.uuid") == tags.end());
    REQUIRE(tags.find("couchbase.cluster.name") == tags.end());
  }

  SECTION("bucket/scope/collection names missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::tracing::service::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      {},
      {},
      {},
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 6);
    REQUIRE(tags.find("db.namespace") == tags.end());
    REQUIRE(tags.find("couchbase.scope.name") == tags.end());
    REQUIRE(tags.find("couchbase.collection.name") == tags.end());
  }
}
