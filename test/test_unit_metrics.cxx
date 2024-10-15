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

#include <couchbase/error_codes.hxx>

TEST_CASE("unit: metric attributes encoding", "[unit]")
{
  SECTION("all attributes set")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::service_type::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      "test-bucket",
      "test-scope",
      "test-collection",
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 8);
    REQUIRE(tags.at("db.couchbase.service") == "kv");
    REQUIRE(tags.at("db.operation") == "get");
    REQUIRE(tags.at("db.name") == "test-bucket");
    REQUIRE(tags.at("db.couchbase.scope") == "test-scope");
    REQUIRE(tags.at("db.couchbase.collection") == "test-collection");
    REQUIRE(tags.at("outcome") == "DocumentNotFound");
    REQUIRE(tags.at("db.couchbase.cluster_name") == "test-cluster");
    REQUIRE(tags.at("db.couchbase.cluster_uuid") == "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251");
  }

  SECTION("successful operation")
  {
    couchbase::core::metrics::metric_attributes attrs{ couchbase::core::service_type::key_value,
                                                       "get",
                                                       {},
                                                       "test-bucket",
                                                       "test-scope",
                                                       "test-collection",
                                                       { "test-cluster",
                                                         "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" } };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 8);
    REQUIRE(tags.at("outcome") == "Success");
  }

  SECTION("cluster labels missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::service_type::key_value,
      "get",
      {},
      "test-bucket",
      "test-scope",
      "test-collection",
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 6);
    REQUIRE(tags.find("db.couchbase.cluster_uuid") == tags.end());
    REQUIRE(tags.find("db.couchbase.cluster_name") == tags.end());
  }

  SECTION("bucket/scope/collection names missing")
  {
    couchbase::core::metrics::metric_attributes attrs{
      couchbase::core::service_type::key_value,
      "get",
      couchbase::errc::key_value::document_not_found,
      {},
      {},
      {},
      { "test-cluster", "d476fe9c-1f66-4bf4-9c2b-9ee866fc5251" }
    };

    auto tags = attrs.encode();

    REQUIRE(tags.size() == 5);
    REQUIRE(tags.find("db.name") == tags.end());
    REQUIRE(tags.find("db.couchbase.scope") == tags.end());
    REQUIRE(tags.find("db.couchbase.collection") == tags.end());
  }
}
