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

namespace couchbase
{
struct collection_identifier {
    static inline auto default_scope = "_default";
    static inline auto default_collection = "_default";

    std::string bucket{};
    std::optional<std::string> scope{};
    std::optional<std::string> collection{};

    static collection_identifier from_default(const std::string& bucket)
    {
        return collection_identifier{ bucket, default_scope, default_collection };
    }
};
} // namespace couchbase
