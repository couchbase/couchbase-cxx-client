/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "agent_config.hxx"

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <string>

namespace couchbase::core::columnar
{
auto
agent_config::to_string() const -> std::string
{
  return fmt::format(R"(#<columnar_agent_config:{} shim={}, user_agent="{}", timeouts={}>)",
                     static_cast<const void*>(this),
                     shim.to_string(),
                     user_agent,
                     timeouts.to_string());
}

auto
timeout_config::to_string() const -> std::string
{
  return fmt::format(
    R"(#<timeout_config:{} connect_timeout={}, dispatch_timeout={}, query_timeout={}, management_timeout={}>)",
    static_cast<const void*>(this),
    connect_timeout,
    dispatch_timeout,
    query_timeout,
    management_timeout);
}
} // namespace couchbase::core::columnar
