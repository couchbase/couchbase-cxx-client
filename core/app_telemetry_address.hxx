/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2024-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include <string>
#include <vector>

namespace couchbase::core
{
namespace topology
{
struct configuration;
} // namespace topology

struct app_telemetry_address {
  std::string hostname;
  std::string service;
  std::string path;
  std::string host_uuid;
};

auto
get_app_telemetry_addresses(const topology::configuration& config,
                            bool use_tls,
                            const std::string& network) -> std::vector<app_telemetry_address>;
} // namespace couchbase::core
