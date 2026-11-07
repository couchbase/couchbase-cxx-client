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
// Opens the probe backend before anything is timed, so the result JSON carries the server version
// and node count.
void
probe(state& st)
{
  try {
    static_cast<void>(st.ctx().server_version());
  } catch (const probe_failure&) {
    return; // The result JSON writes what the probe cannot answer as null.
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
           { { CASE(kv_get), { needs::real_cluster() } },
             { CASE(kv_upsert), { needs::real_cluster() } },
             { CASE(kv_upsert_get), { needs::real_cluster() } } } };
}
} // namespace couchbase::test::bench
