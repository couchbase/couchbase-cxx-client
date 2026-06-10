/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include "core/io/configuration_belongs_to_session.hxx"

#include <optional>
#include <string>

// The argument order is (config_bucket, session_bucket). These named constants
// make each call site below read in terms of *what kind* of config arrives on
// *what kind* of session, rather than as a pair of anonymous optionals.
namespace
{
const std::optional<std::string> cluster_level{}; // std::nullopt => GCCCP / no bucket
} // namespace

TEST_CASE("unit: configuration_belongs_to_session cluster-level session", "[unit]")
{
  using couchbase::core::io::configuration_belongs_to_session;

  // A cluster-level (GCCCP) session is bound to no bucket. It accepts only
  // cluster-level maps, and must ignore any bucket-scoped map that happens to
  // arrive on it.
  SECTION("accepts a cluster-level config")
  {
    CHECK(configuration_belongs_to_session(cluster_level, cluster_level));
  }

  SECTION("ignores a bucket-scoped config")
  {
    CHECK_FALSE(configuration_belongs_to_session("default", cluster_level));
    CHECK_FALSE(configuration_belongs_to_session("travel-sample", cluster_level));
  }
}

TEST_CASE("unit: configuration_belongs_to_session bucket-level session", "[unit]")
{
  using couchbase::core::io::configuration_belongs_to_session;

  // A session bound to a bucket accepts only maps for that exact bucket.
  SECTION("accepts a config for its own bucket")
  {
    CHECK(configuration_belongs_to_session("default", "default"));
    CHECK(configuration_belongs_to_session("travel-sample", "travel-sample"));
  }

  SECTION("ignores a config for a different bucket")
  {
    CHECK_FALSE(configuration_belongs_to_session("travel-sample", "default"));
    CHECK_FALSE(configuration_belongs_to_session("default", "travel-sample"));
  }

  SECTION("ignores a cluster-level config")
  {
    // A bucket-bound session must not be steered by a bucket-less (GCCCP) map.
    CHECK_FALSE(configuration_belongs_to_session(cluster_level, "default"));
  }
}

TEST_CASE("unit: configuration_belongs_to_session bucket-name matching is exact", "[unit]")
{
  using couchbase::core::io::configuration_belongs_to_session;

  // Bucket identity is compared by exact string equality: no case-folding, no
  // trimming, no prefix matching. A near-miss is a different bucket.
  SECTION("case-sensitive")
  {
    CHECK_FALSE(configuration_belongs_to_session("Default", "default"));
    CHECK_FALSE(configuration_belongs_to_session("DEFAULT", "default"));
  }

  SECTION("no prefix or substring matching")
  {
    CHECK_FALSE(configuration_belongs_to_session("default2", "default"));
    CHECK_FALSE(configuration_belongs_to_session("default", "default2"));
  }

  SECTION("whitespace is significant")
  {
    CHECK_FALSE(configuration_belongs_to_session("default ", "default"));
  }

  SECTION("empty bucket name is just another name and matches itself")
  {
    // An engaged-but-empty optional is distinct from std::nullopt: both sides
    // name "a bucket", and that bucket happens to be the empty string, so they
    // match. (This is a degenerate input, exercised here only to pin behaviour.)
    CHECK(configuration_belongs_to_session(std::string{}, std::string{}));
    CHECK_FALSE(configuration_belongs_to_session(std::string{}, cluster_level));
    CHECK_FALSE(configuration_belongs_to_session(cluster_level, std::string{}));
  }
}
