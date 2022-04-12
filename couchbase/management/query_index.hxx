/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <optional>
#include <string>
#include <vector>

namespace couchbase::management::query
{
struct index {
    bool is_primary{ false };
    std::string id;
    std::string name;
    std::string state;
    std::string datastore_id;
    std::string keyspace_id;
    std::string namespace_id;
    std::string collection_name;
    std::string type;
    std::vector<std::string> index_key{};
    std::optional<std::string> partition{};
    std::optional<std::string> condition{};
    std::optional<std::string> bucket_id{};
    std::optional<std::string> scope_id{};
};

} // namespace couchbase::management::query
