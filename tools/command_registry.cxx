/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "command_registry.hxx"

#include "analytics.hxx"
#include "get.hxx"
#include "pillowfight.hxx"
#include "query.hxx"
#include "version.hxx"

namespace cbc
{
command_registry::command_registry()
  : commands_{
      { "version", std::make_shared<cbc::version>() },
      { "get", std::make_shared<cbc::get>() },
      { "query", std::make_shared<cbc::query>() },
      { "analytics", std::make_shared<cbc::analytics>() },
      { "pillowfight", std::make_shared<cbc::pillowfight>() },
  }
{
}

auto
command_registry::get(const std::string& name) -> std::shared_ptr<command>
{
    if (auto cmd = commands_.find(name); cmd != commands_.end()) {
        return cmd->second;
    }
    return nullptr;
}
} // namespace cbc
