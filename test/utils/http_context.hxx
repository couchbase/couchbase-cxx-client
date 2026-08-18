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

#pragma once

#include "core/cluster_options.hxx"
#include "core/io/http_context.hxx"
#include "core/io/query_cache.hxx"
#include "core/topology/configuration.hxx"

#include <cstdint>
#include <string>

namespace couchbase::test
{
/**
 * Builds a minimal http_context for unit-testing request encoders that do not consult the cluster
 * configuration.
 *
 * http_context holds the configuration, options and query cache by reference, so those have to
 * outlive the returned object; the function-local statics give them static storage duration.
 * hostname and port are held by value, so plain locals are fine.
 */
inline auto
make_http_context() -> couchbase::core::http_context
{
  static couchbase::core::topology::configuration config{};
  static couchbase::core::query_cache query_cache{};
  static couchbase::core::cluster_options cluster_options{};
  std::string hostname{};
  std::uint16_t port{};
  std::string canonical_hostname{};
  std::uint16_t canonical_port{};
  return couchbase::core::http_context{
    config, cluster_options, query_cache, hostname, port, canonical_hostname, canonical_port,
  };
}
} // namespace couchbase::test
