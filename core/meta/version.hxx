/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include <map>
#include <string>

#include "features.hxx"

namespace couchbase::core::meta
{
const std::string&
sdk_id();

const std::string&
sdk_semver();

const std::string&
sdk_version();

const std::string&
sdk_version_short();

std::map<std::string, std::string>
sdk_build_info();

std::string
sdk_build_info_short();

std::string
sdk_build_info_json();

const std::string&
os();

std::string
user_agent_for_http(const std::string& client_id, const std::string& session_id, const std::string& extra = "");

std::string
user_agent_for_mcbp(const std::string& client_id, const std::string& session_id, const std::string& extra = "", std::size_t max_length = 0);
} // namespace couchbase::core::meta
