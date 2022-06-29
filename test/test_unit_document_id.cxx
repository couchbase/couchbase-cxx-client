/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <couchbase/api/document_id.hxx>

TEST_CASE("unit: document_id uses default collection", "[unit]")
{
    couchbase::api::document_id id("travel-sample", "airport_42");

    REQUIRE(id.bucket() == "travel-sample");
    REQUIRE(id.scope() == "_default");
    REQUIRE(id.collection() == "_default");
    REQUIRE(id.key() == "airport_42");

    REQUIRE(id.scope() == couchbase::api::document_id::default_scope);
    REQUIRE(id.collection() == couchbase::api::document_id::default_collection);
}

TEST_CASE("unit: document_id can leave scope and collection empty for old servers", "[unit]")
{
    couchbase::api::document_id id("travel-sample", "airport_42", false);

    REQUIRE(id.bucket() == "travel-sample");
    REQUIRE(id.scope().empty());
    REQUIRE(id.collection().empty());
    REQUIRE(id.key() == "airport_42");
}

TEST_CASE("unit: document_id can use custom collection", "[unit]")
{
    couchbase::api::document_id id("travel-sample", "myapp_production", "airports", "airport_42");

    REQUIRE(id.bucket() == "travel-sample");
    REQUIRE(id.scope() == "myapp_production");
    REQUIRE(id.collection() == "airports");
    REQUIRE(id.key() == "airport_42");
}

TEST_CASE("unit: document_id validates collection and scope", "[unit]")
{
    using Catch::Contains;

    CHECK_THROWS_WITH(couchbase::api::document_id("travel-sample", "invalid?scope", "airports", "airport_42"),
                      Contains("invalid scope_name"));
    CHECK_THROWS_WITH(couchbase::api::document_id("travel-sample", "myapp_production", "invalid?collection", "airport_42"),
                      Contains("invalid collection_name"));
}
