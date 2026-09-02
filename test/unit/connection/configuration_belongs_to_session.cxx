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

#include "framework/test_registry.hxx"

#include "core/io/configuration_belongs_to_session.hxx"

#include <optional>
#include <string>

namespace couchbase::test
{
namespace
{
using couchbase::core::io::configuration_belongs_to_session;

// The argument order is (config_bucket, session_bucket). This named constant makes each call site
// below read in terms of *what kind* of config arrives on *what kind* of session, rather than as a
// pair of anonymous optionals. A disengaged optional is a cluster-level (GCCCP) map or session,
// bound to no bucket.
const std::optional<std::string> cluster_level{};

void
a_cluster_level_session_accepts_a_cluster_level_config([[maybe_unused]] context& ctx)
{
  assert_true(configuration_belongs_to_session(cluster_level, cluster_level),
              "a bucket-less map belongs to a bucket-less session");
}

void
a_cluster_level_session_ignores_a_bucket_scoped_config([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session("default", cluster_level), "the default bucket");
  assert_false(configuration_belongs_to_session("travel-sample", cluster_level), "another bucket");
}

void
a_bucket_session_accepts_a_config_for_its_own_bucket([[maybe_unused]] context& ctx)
{
  assert_true(configuration_belongs_to_session("default", "default"), "the default bucket");
  assert_true(configuration_belongs_to_session("travel-sample", "travel-sample"),
              "a bucket whose name carries a hyphen");
}

void
a_bucket_session_ignores_a_config_for_another_bucket([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session("travel-sample", "default"), "a foreign map");
  assert_false(configuration_belongs_to_session("default", "travel-sample"), "a foreign map");
}

void
a_bucket_session_ignores_a_cluster_level_config([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session(cluster_level, "default"),
               "a bucket-bound session is not steered by a bucket-less map");
}

void
bucket_names_are_compared_case_sensitively([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session("Default", "default"), "a capitalised name");
  assert_false(configuration_belongs_to_session("DEFAULT", "default"), "an uppercased name");
}

void
bucket_names_are_not_prefix_matched([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session("default2", "default"), "a longer config name");
  assert_false(configuration_belongs_to_session("default", "default2"), "a longer session name");
}

void
whitespace_in_a_bucket_name_is_significant([[maybe_unused]] context& ctx)
{
  assert_false(configuration_belongs_to_session("default ", "default"), "a trailing space");
}

void
an_empty_bucket_name_is_a_name_and_matches_itself([[maybe_unused]] context& ctx)
{
  // An engaged-but-empty optional is distinct from std::nullopt: both sides name "a bucket", and
  // that bucket happens to be the empty string, so they match.
  assert_true(configuration_belongs_to_session(std::string{}, std::string{}),
              "two empty names are the same bucket");
  assert_false(configuration_belongs_to_session(std::string{}, cluster_level),
               "an empty name is not the absence of a name");
  assert_false(configuration_belongs_to_session(cluster_level, std::string{}),
               "the absence of a name is not an empty name");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_cluster_level_session_accepts_a_cluster_level_config) },
      { CASE(a_cluster_level_session_ignores_a_bucket_scoped_config) },
      { CASE(a_bucket_session_accepts_a_config_for_its_own_bucket) },
      { CASE(a_bucket_session_ignores_a_config_for_another_bucket) },
      { CASE(a_bucket_session_ignores_a_cluster_level_config) },
      { CASE(bucket_names_are_compared_case_sensitively) },
      { CASE(bucket_names_are_not_prefix_matched) },
      { CASE(whitespace_in_a_bucket_name_is_significant) },
      { CASE(an_empty_bucket_name_is_a_name_and_matches_itself) },
    },
  };
}

} // namespace couchbase::test
