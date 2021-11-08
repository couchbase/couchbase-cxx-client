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

namespace couchbase::io
{
enum class retry_reason {
    /**
     * default value, e.g. when we don't need to retry
     */
    do_not_retry,

    /**
     * All unexpected/unknown retry errors must not be retried to avoid accidental data loss and non-deterministic behavior.
     */
    unknown,

    /**
     * The socket is not available into which the operation should’ve been written.
     */
    socket_not_available,

    /**
     * The service on a node (i.e. kv, query) is not available.
     */
    service_not_available,

    /**
     * The node where the operation is supposed to be dispatched to is not available.
     */
    node_not_available,

    /**
     * A not my vbucket response has been received.
     */
    kv_not_my_vbucket,

    /**
     * A KV response has been received which signals an outdated collection.
     */
    kv_collection_outdated,

    /**
     * An unknown response was returned and the consulted KV error map indicated a retry.
     */
    kv_error_map_retry_indicated,

    kv_locked,

    kv_temporary_failure,

    kv_sync_write_in_progress,

    kv_sync_write_re_commit_in_progress,

    service_response_code_indicated,

    /**
     * While an operation was in-flight, the underlying socket has been closed.
     */
    socket_closed_while_in_flight,

    /**
     * The circuit breaker is open for the given socket/endpoint and as a result the operation is not sent into it.
     */
    circuit_breaker_open,

    query_prepared_statement_failure,

    query_index_not_found,

    analytics_temporary_failure,

    search_too_many_requests,

    views_temporary_failure,

    views_no_active_partition,
};

constexpr bool
allows_non_idempotent_retry(retry_reason reason)
{
    switch (reason) {
        case retry_reason::socket_not_available:
        case retry_reason::service_not_available:
        case retry_reason::node_not_available:
        case retry_reason::kv_not_my_vbucket:
        case retry_reason::kv_collection_outdated:
        case retry_reason::kv_error_map_retry_indicated:
        case retry_reason::kv_locked:
        case retry_reason::kv_temporary_failure:
        case retry_reason::kv_sync_write_in_progress:
        case retry_reason::kv_sync_write_re_commit_in_progress:
        case retry_reason::service_response_code_indicated:
        case retry_reason::circuit_breaker_open:
        case retry_reason::query_prepared_statement_failure:
        case retry_reason::query_index_not_found:
        case retry_reason::analytics_temporary_failure:
        case retry_reason::search_too_many_requests:
        case retry_reason::views_temporary_failure:
        case retry_reason::views_no_active_partition:
            return true;
        case retry_reason::do_not_retry:
        case retry_reason::socket_closed_while_in_flight:
        case retry_reason::unknown:
            return false;
    }
    return false;
}

constexpr bool
always_retry(retry_reason reason)
{
    switch (reason) {
        case retry_reason::kv_not_my_vbucket:
        case retry_reason::kv_collection_outdated:
        case retry_reason::views_no_active_partition:
            return true;
        case retry_reason::do_not_retry:
        case retry_reason::socket_not_available:
        case retry_reason::service_not_available:
        case retry_reason::node_not_available:
        case retry_reason::kv_error_map_retry_indicated:
        case retry_reason::kv_locked:
        case retry_reason::kv_temporary_failure:
        case retry_reason::kv_sync_write_in_progress:
        case retry_reason::kv_sync_write_re_commit_in_progress:
        case retry_reason::service_response_code_indicated:
        case retry_reason::socket_closed_while_in_flight:
        case retry_reason::circuit_breaker_open:
        case retry_reason::query_prepared_statement_failure:
        case retry_reason::query_index_not_found:
        case retry_reason::analytics_temporary_failure:
        case retry_reason::search_too_many_requests:
        case retry_reason::views_temporary_failure:
        case retry_reason::unknown:
            return false;
    }
    return false;
}

} // namespace couchbase::io
