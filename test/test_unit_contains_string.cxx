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

#include "test_helper.hxx"

#include "core/utils/contains_string.hxx"

TEST_CASE("unit: contains_string empty inputs", "[unit]")
{
  using couchbase::core::utils::contains_string;

  SECTION("empty substring always matches")
  {
    CHECK(contains_string("", ""));
    CHECK(contains_string("hello", ""));
    CHECK(contains_string("", "", true));
    CHECK(contains_string("hello", "", true));
  }

  SECTION("empty input cannot contain non-empty substring")
  {
    CHECK_FALSE(contains_string("", "x"));
    CHECK_FALSE(contains_string("", "x", true));
  }

  SECTION("substring longer than input never matches")
  {
    CHECK_FALSE(contains_string("ab", "abc"));
    CHECK_FALSE(contains_string("ab", "abc", true));
  }
}

TEST_CASE("unit: contains_string case-sensitive (default)", "[unit]")
{
  using couchbase::core::utils::contains_string;

  SECTION("exact substring is found")
  {
    CHECK(contains_string("Index does not exist", "Index"));
    CHECK(contains_string("Index does not exist", "does not"));
    CHECK(contains_string("Index does not exist", "exist"));
  }

  SECTION("differing case does not match")
  {
    CHECK_FALSE(contains_string("Index does not exist", "index"));
    CHECK_FALSE(contains_string("Index does not exist", "INDEX"));
    CHECK_FALSE(contains_string("Index does not exist", "EXIST"));
  }

  SECTION("substring equal to input matches")
  {
    CHECK(contains_string("Bucket Not Found", "Bucket Not Found"));
  }

  SECTION("substring at start and end")
  {
    CHECK(contains_string("Bucket Not Found", "Bucket"));
    CHECK(contains_string("Bucket Not Found", "Found"));
  }

  SECTION("missing substring is not found")
  {
    CHECK_FALSE(contains_string("Bucket Not Found", "Scope"));
  }
}

TEST_CASE("unit: contains_string case-insensitive", "[unit]")
{
  using couchbase::core::utils::contains_string;

  SECTION("matches regardless of case in input or substring")
  {
    CHECK(contains_string("Index does not exist", "index", true));
    CHECK(contains_string("Index does not exist", "INDEX", true));
    CHECK(contains_string("INDEX DOES NOT EXIST", "index", true));
    CHECK(contains_string("index does not exist", "INDEX", true));
    CHECK(contains_string("iNdEx DoEs NoT eXiSt", "InDeX", true));
  }

  SECTION("non-letter characters are compared exactly")
  {
    CHECK(contains_string("Error: 404", "error: 404", true));
    CHECK_FALSE(contains_string("Error: 404", "error: 500", true));
  }

  SECTION("only ASCII A-Z / a-z are folded")
  {
    // Bytes outside the ASCII letter range are compared byte-for-byte; no
    // locale machinery is involved.
    CHECK(contains_string("café", "CAF", true));
    CHECK(contains_string("café", "café"));
    CHECK_FALSE(contains_string("café", "CAFE", true));
  }
}

TEST_CASE("unit: contains_string query error mapping patterns", "[unit]")
{
  // These are the substring checks that replaced the std::regex_search calls
  // in document_query.cxx for status code 5000 ("Internal Error").
  using couchbase::core::utils::contains_string;

  SECTION("index already exists pattern")
  {
    const std::string msg = "GSI Index idx1 already exists.";
    CHECK(contains_string(msg, "index", true));
    CHECK(contains_string(msg, "already exist", true));
  }

  SECTION("index not found pattern")
  {
    const std::string msg = "GSI index idx1 Not Found.";
    CHECK(contains_string(msg, "index", true));
    CHECK(contains_string(msg, "not found", true));
  }

  SECTION("Index does not exist is matched case-sensitively")
  {
    CHECK(contains_string("Index does not exist", "Index does not exist"));
  }

  SECTION("Bucket Not Found is matched case-sensitively")
  {
    CHECK(contains_string("Bucket Not Found", "Bucket Not Found"));
    CHECK_FALSE(contains_string("bucket not found", "Bucket Not Found"));
  }
}
