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

#include <vector>

#include <couchbase/topology/collections_manifest.hxx>

#include <spdlog/fmt/fmt.h>

template<>
struct fmt::formatter<couchbase::topology::collections_manifest> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::topology::collections_manifest& manifest, FormatContext& ctx)
    {
        std::vector<std::string> collections;
        for (const auto& scope : manifest.scopes) {
            for (const auto& collection : scope.collections) {
                collections.emplace_back(fmt::format("{}.{}={}", scope.name, collection.name, collection.uid));
            }
        }

        format_to(ctx.out(),
                  R"(#<manifest:{} uid={}, collections({})=[{}]>)",
                  couchbase::uuid::to_string(manifest.id),
                  manifest.uid,
                  collections.size(),
                  fmt::join(collections, ", "));
        return formatter<std::string>::format("", ctx);
    }
};
