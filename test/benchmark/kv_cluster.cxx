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

#include "bench_registry.hxx"
#include "kv_cases.hxx"

namespace couchbase::test::bench
{
namespace
{
// Caches server version and node count for the result JSON before kv_upsert_get closes the probe.
// Queried separately so one failure leaves the other cached; missing values serialize as null.
void
probe(state& st)
{
  try {
    static_cast<void>(st.ctx().server_version());
  } catch (const probe_failure&) { // NOLINT(bugprone-empty-catch)
  }
  try {
    static_cast<void>(st.ctx().number_of_nodes());
  } catch (const probe_failure&) { // NOLINT(bugprone-empty-catch)
  }
}

void
measure(state& st, bool write)
{
  probe(st);
  const auto& config = st.ctx().config();
  for (const auto p : { kv::profile::sdk_default, kv::profile::bare }) {
    kv::connect_and_measure(st,
                            "cluster",
                            p,
                            config.connection_string,
                            config.username,
                            config.password,
                            config.bucket,
                            write);
  }
}

void
kv_get(state& st)
{
  measure(st, false);
}

void
kv_upsert(state& st)
{
  measure(st, true);
}

void
kv_upsert_get(state& st)
{
  probe(st);
  const auto& config = st.ctx().config();
  kv::measure_upsert_get(
    st, "cluster", config.connection_string, config.username, config.password, config.bucket);
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name,
           { { CASE(kv_get), { needs::real_cluster() }, kv::get_description },
             { CASE(kv_upsert), { needs::real_cluster() }, kv::upsert_description },
             { CASE(kv_upsert_get), { needs::real_cluster() }, kv::upsert_get_description } } };
}
} // namespace couchbase::test::bench
