/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
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

// The cluster-backed probes, against a real cluster.
//
// framework/selftest.cxx drives the runner with stand-in backends, which pins the semantics but
// says nothing about whether cluster_probes.cxx can actually answer. These cases are the other
// half: they require a cluster, so they skip where there is none and exercise the real connection
// where there is one. They are also what makes the three outcomes visible from the outside --
// unset connection string skips them, an unreachable one fails them.

#include "framework/test_runner.hxx"

#include <cstddef>
#include <string>

namespace couchbase::test
{
namespace
{
void
the_version_probe_reports_a_plausible_release(context& ctx)
{
  const auto version = ctx.server_version();
  // 4.x predates every supported release and 99 is not a version anybody ships; between them the
  // check catches a probe that answered with a default-constructed value.
  assert_true(version.major >= 4,
              fmt::format("major version {} is too old to be real", version.major));
  assert_true(version.major < 99, fmt::format("major version {} is not a release", version.major));
  assert_true(!version.to_string().empty(), "the version renders");
}

void
the_kv_service_is_always_present(context& ctx)
{
  // Every Couchbase cluster runs KV. A probe that cannot see it is broken rather than reporting a
  // cluster without data nodes.
  assert_true(ctx.has_service("kv"), "the cluster reports a kv node");
  assert_false(ctx.has_service("no_such_service"), "and does not invent one that does not exist");
}

void
the_topology_probes_answer_with_counts(context& ctx)
{
  assert_true(ctx.number_of_nodes() >= 1, "the cluster has at least one node");
  // A default bucket may legitimately have no replicas, so the assertion is on the probe answering
  // at all rather than on a particular number.
  const auto replicas = ctx.number_of_replicas();
  assert_true(replicas <= 3, fmt::format("{} replicas is beyond what a bucket can hold", replicas));
  // Deliberately not "at least one": server groups are a rack-awareness feature, and a cluster
  // that was never given one reports none. What is always true is that the probe answers and every
  // name it returns is a name.
  for (const auto& group : ctx.server_groups()) {
    assert_false(group.empty(), "a reported server group has a name");
  }
}

void
the_storage_backend_is_one_the_server_names(context& ctx)
{
  // Gated on 7.1: before it the server reports no storage backend at all, and the probe correctly
  // answers "unknown" -- which is not a mapping error to catch, so there is nothing to assert.
  const auto backend = ctx.storage_backend();
  assert_true(backend == "couchstore" || backend == "magma",
              fmt::format("\"{}\" is not a storage backend the server reports", backend));
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "framework_cluster_probes",
    {
      { "the_version_probe_reports_a_plausible_release",
        the_version_probe_reports_a_plausible_release,
        { needs::real_cluster(), needs::service("kv") },
        timeout::integration },
      // No needs::service("kv") here, deliberately: the gate would call the same cached probe the
      // body asserts on, so a probe wrongly answering false would skip this case instead of failing
      // it -- and the one case whose job is to catch that would be the one that never runs.
      { "the_kv_service_is_always_present",
        the_kv_service_is_always_present,
        { needs::real_cluster() },
        timeout::integration },
      { "the_topology_probes_answer_with_counts",
        the_topology_probes_answer_with_counts,
        { needs::real_cluster(), needs::service("kv") },
        timeout::integration },
      { "the_storage_backend_is_one_the_server_names",
        the_storage_backend_is_one_the_server_names,
        { needs::real_cluster(), needs::service("kv"), needs::cluster_version(v7_1) },
        timeout::integration },
    },
  };
}

} // namespace couchbase::test
