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

#include "core/topology/configuration.hxx"
#include "core/topology/configuration_json.hxx"
#include "core/utils/crc32.hxx"

#include <tao/json.hpp>

#include <cstddef>
#include <string>
#include <vector>

namespace couchbase::test::bench
{
namespace
{
constexpr std::size_t key_count = 1000;
constexpr std::size_t vbucket_count = 1024;

auto
make_keys() -> std::vector<std::string>
{
  std::vector<std::string> keys;
  keys.reserve(key_count);
  for (std::size_t i = 0; i < key_count; ++i) {
    keys.emplace_back("bench::" + std::to_string(i));
  }
  return keys;
}

auto
make_configuration() -> core::topology::configuration
{
  tao::json::value rows = tao::json::empty_array;
  for (std::size_t i = 0; i < vbucket_count; ++i) {
    rows.push_back(tao::json::value::array({ 0 }));
  }
  const tao::json::value config{
    { "rev", 1 },
    { "name", "default" },
    { "nodeLocator", "vbucket" },
    { "nodesExt",
      tao::json::value::array({ { { "thisNode", true },
                                  { "hostname", "127.0.0.1" },
                                  { "services", { { "kv", 11210 } } } } }) },
    { "vBucketServerMap",
      { { "numReplicas", 0 },
        { "serverList", tao::json::value::array({ "127.0.0.1:11210" }) },
        { "vBucketMap", rows } } },
  };
  return config.as<core::topology::configuration>();
}

void
vbucket_hash(state& st)
{
  const auto keys = make_keys();
  const auto config = make_configuration();

  std::size_t i = 0;
  st.batched("crc32_t1", 1, [&] {
    const auto& key = keys[i++ % key_count];
    do_not_optimize(core::utils::hash_crc32(key.data(), key.size()));
  });

  i = 0;
  st.batched("map_key_t1", 1, [&] {
    const auto& key = keys[i++ % key_count];
    do_not_optimize(config.map_key(key, 0));
  });

  // The configuration is shared by every thread, as a bucket's current configuration is.
  st.batched("map_key_t8", 8, [&] {
    thread_local std::size_t index = 0;
    const auto& key = keys[index++ % key_count];
    do_not_optimize(config.map_key(key, 0));
  });
}
} // namespace

auto
benchmarks() -> bench_suite
{
  return { suite_name, { { CASE(vbucket_hash) } } };
}
} // namespace couchbase::test::bench
