/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright 2022-Present Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 * ANY KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#pragma once

#include "utils/movable_function.hxx"

#include <chrono>
#include <map>
#include <string>
#include <system_error>

namespace couchbase
{
class retry_strategy;
namespace tracing
{
class request_span;
} // namespace tracing
} // namespace couchbase

namespace couchbase::core
{
class stats_options
{
  public:
    std::string key{};
    std::shared_ptr<couchbase::retry_strategy> retry_strategy{};
    std::chrono::milliseconds timeout{};
    std::shared_ptr<couchbase::tracing::request_span> parent_span{};

    struct {
        std::string user{};
    } internal{};
};

struct single_server_stats {
    std::error_code error{};
    std::map<std::string, std::string> stats{};
};

class stats_result
{
  public:
    std::map<std::string, single_server_stats> servers{};
};

using stats_callback = utils::movable_function<void(stats_result result, std::error_code ec)>;
} // namespace couchbase::core
