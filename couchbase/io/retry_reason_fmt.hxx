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

#include <couchbase/io/retry_reason.hxx>

#include <fmt/core.h>

template<>
struct fmt::formatter<couchbase::io::retry_reason> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(couchbase::io::retry_reason reason, FormatContext& ctx) const
    {
        string_view name = "unknown";
        switch (reason) {
            case couchbase::io::retry_reason::do_not_retry:
                name = "do_not_retry";
                break;
            case couchbase::io::retry_reason::unknown:
                name = "unknown";
                break;
            case couchbase::io::retry_reason::socket_not_available:
                name = "socket_not_available";
                break;
            case couchbase::io::retry_reason::service_not_available:
                name = "service_not_available";
                break;
            case couchbase::io::retry_reason::node_not_available:
                name = "node_not_available";
                break;
            case couchbase::io::retry_reason::kv_not_my_vbucket:
                name = "kv_not_my_vbucket";
                break;
            case couchbase::io::retry_reason::kv_collection_outdated:
                name = "kv_collection_outdated";
                break;
            case couchbase::io::retry_reason::kv_error_map_retry_indicated:
                name = "kv_error_map_retry_indicated";
                break;
            case couchbase::io::retry_reason::kv_locked:
                name = "kv_locked";
                break;
            case couchbase::io::retry_reason::kv_temporary_failure:
                name = "kv_temporary_failure";
                break;
            case couchbase::io::retry_reason::kv_sync_write_in_progress:
                name = "kv_sync_write_in_progress";
                break;
            case couchbase::io::retry_reason::kv_sync_write_re_commit_in_progress:
                name = "kv_sync_write_re_commit_in_progress";
                break;
            case couchbase::io::retry_reason::service_response_code_indicated:
                name = "service_response_code_indicated";
                break;
            case couchbase::io::retry_reason::socket_closed_while_in_flight:
                name = "socket_closed_while_in_flight";
                break;
            case couchbase::io::retry_reason::circuit_breaker_open:
                name = "circuit_breaker_open";
                break;
            case couchbase::io::retry_reason::query_prepared_statement_failure:
                name = "query_prepared_statement_failure";
                break;
            case couchbase::io::retry_reason::query_index_not_found:
                name = "query_index_not_found";
                break;
            case couchbase::io::retry_reason::analytics_temporary_failure:
                name = "analytics_temporary_failure";
                break;
            case couchbase::io::retry_reason::search_too_many_requests:
                name = "search_too_many_requests";
                break;
            case couchbase::io::retry_reason::views_temporary_failure:
                name = "views_temporary_failure";
                break;
            case couchbase::io::retry_reason::views_no_active_partition:
                name = "views_no_active_partition";
                break;
        }
        return format_to(ctx.out(), "{}", name);
    }
};
