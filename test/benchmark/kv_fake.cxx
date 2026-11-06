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
#include "fake_server.hxx"
#include "kv_cases.hxx"

namespace couchbase::test::bench
{
namespace
{
void
measure(state& st, bool write)
{
  for (const auto p : { kv::profile::sdk_default, kv::profile::bare }) {
    fake_server server{};
    kv::connect_and_measure(st,
                            "fake",
                            p,
                            server.connection_string(),
                            server.username(),
                            server.password(),
                            "default",
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
  fake_server server{};
  kv::measure_upsert_get(
    st, "fake", server.connection_string(), server.username(), server.password(), "default");
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name, { { CASE(kv_get) }, { CASE(kv_upsert) }, { CASE(kv_upsert_get) } } };
}
} // namespace couchbase::test::bench
