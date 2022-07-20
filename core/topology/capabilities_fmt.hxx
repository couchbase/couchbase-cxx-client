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

#include "capabilities.hxx"

#include <fmt/core.h>

template<>
struct fmt::formatter<couchbase::core::bucket_capability> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(couchbase::core::bucket_capability type, FormatContext& ctx) const
    {
        string_view name = "unknown";
        switch (type) {
            case couchbase::core::bucket_capability::couchapi:
                name = "couchapi";
                break;
            case couchbase::core::bucket_capability::xattr:
                name = "xattr";
                break;
            case couchbase::core::bucket_capability::dcp:
                name = "dcp";
                break;
            case couchbase::core::bucket_capability::cbhello:
                name = "cbhello";
                break;
            case couchbase::core::bucket_capability::touch:
                name = "touch";
                break;
            case couchbase::core::bucket_capability::cccp:
                name = "cccp";
                break;
            case couchbase::core::bucket_capability::xdcr_checkpointing:
                name = "xdcr_checkpointing";
                break;
            case couchbase::core::bucket_capability::nodes_ext:
                name = "nodes_ext";
                break;
            case couchbase::core::bucket_capability::collections:
                name = "collections";
                break;
            case couchbase::core::bucket_capability::durable_write:
                name = "durable_write";
                break;
            case couchbase::core::bucket_capability::tombstoned_user_xattrs:
                name = "tombstoned_user_xattrs";
                break;
        }
        return format_to(ctx.out(), "{}", name);
    }
};

template<>
struct fmt::formatter<couchbase::core::cluster_capability> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(couchbase::core::cluster_capability type, FormatContext& ctx) const
    {
        string_view name = "unknown";
        switch (type) {
            case couchbase::core::cluster_capability::n1ql_cost_based_optimizer:
                name = "n1ql_cost_based_optimizer";
                break;
            case couchbase::core::cluster_capability::n1ql_index_advisor:
                name = "n1ql_index_advisor";
                break;
            case couchbase::core::cluster_capability::n1ql_javascript_functions:
                name = "n1ql_javascript_functions";
                break;
            case couchbase::core::cluster_capability::n1ql_inline_functions:
                name = "n1ql_inline_functions";
                break;
            case couchbase::core::cluster_capability::n1ql_enhanced_prepared_statements:
                name = "n1ql_enhanced_prepared_statements";
                break;
        }
        return format_to(ctx.out(), "{}", name);
    }
};
