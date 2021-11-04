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

#include <couchbase/diagnostics.hxx>

#include <spdlog/fmt/fmt.h>

template<>
struct fmt::formatter<couchbase::diag::cluster_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::cluster_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::cluster_state::online:
                name = "online";
                break;

            case couchbase::diag::cluster_state::degraded:
                name = "degraded";
                break;

            case couchbase::diag::cluster_state::offline:
                name = "offline";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::diag::endpoint_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::endpoint_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::endpoint_state::disconnected:
                name = "disconnected";
                break;

            case couchbase::diag::endpoint_state::connecting:
                name = "connecting";
                break;

            case couchbase::diag::endpoint_state::connected:
                name = "connected";
                break;

            case couchbase::diag::endpoint_state::disconnecting:
                name = "disconnecting";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};

template<>
struct fmt::formatter<couchbase::diag::ping_state> : formatter<std::string_view> {
    template<typename FormatContext>
    auto format(couchbase::diag::ping_state state, FormatContext& ctx)
    {
        string_view name = "unknown";
        switch (state) {
            case couchbase::diag::ping_state::ok:
                name = "ok";
                break;

            case couchbase::diag::ping_state::timeout:
                name = "timeout";
                break;

            case couchbase::diag::ping_state::error:
                name = "error";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
