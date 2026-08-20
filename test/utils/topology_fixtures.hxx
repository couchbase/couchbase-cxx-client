/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#pragma once

#include "core/topology/configuration.hxx"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace test::utils
{
using vbucket_map = couchbase::core::topology::configuration::vbucket_map;

/**
 * Builds a topology configuration around a literal vbucket map. The number of
 * replicas is set independently of the map, because the server publishes the
 * two separately and code under test must cope with them disagreeing.
 */
inline auto
config_with_vbmap(std::optional<vbucket_map> vbmap,
                  std::optional<std::uint32_t> num_replicas,
                  std::size_t number_of_nodes = 0) -> couchbase::core::topology::configuration
{
  couchbase::core::topology::configuration config{};
  config.vbmap = std::move(vbmap);
  config.num_replicas = num_replicas;
  for (std::size_t index = 0; index < number_of_nodes; ++index) {
    couchbase::core::topology::configuration::node node{};
    node.index = index;
    config.nodes.emplace_back(node);
  }
  return config;
}
} // namespace test::utils
