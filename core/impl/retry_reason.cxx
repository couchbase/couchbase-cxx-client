/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2022-Present Couchbase, Inc.
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

#include <couchbase/retry_reason.hxx>

namespace couchbase
{
bool
allows_non_idempotent_retry(retry_reason reason)
{
    switch (reason) {
        case retry_reason::socket_not_available:
        case retry_reason::service_not_available:
        case retry_reason::node_not_available:
        case retry_reason::key_value_not_my_vbucket:
        case retry_reason::key_value_collection_outdated:
        case retry_reason::key_value_error_map_retry_indicated:
        case retry_reason::key_value_locked:
        case retry_reason::key_value_temporary_failure:
        case retry_reason::key_value_sync_write_in_progress:
        case retry_reason::key_value_sync_write_re_commit_in_progress:
        case retry_reason::service_response_code_indicated:
        case retry_reason::circuit_breaker_open:
        case retry_reason::query_index_not_found:
        case retry_reason::query_prepared_statement_failure:
        case retry_reason::analytics_temporary_failure:
        case retry_reason::search_too_many_requests:
        case retry_reason::views_temporary_failure:
        case retry_reason::views_no_active_partition:
            return true;
        case retry_reason::unknown:
        case retry_reason::socket_closed_while_in_flight:
        case retry_reason::do_not_retry:
            return false;
    }
    return false;
}

bool
always_retry(retry_reason reason)
{
    switch (reason) {
        case retry_reason::unknown:
        case retry_reason::socket_not_available:
        case retry_reason::service_not_available:
        case retry_reason::node_not_available:
        case retry_reason::key_value_error_map_retry_indicated:
        case retry_reason::key_value_locked:
        case retry_reason::key_value_temporary_failure:
        case retry_reason::key_value_sync_write_in_progress:
        case retry_reason::key_value_sync_write_re_commit_in_progress:
        case retry_reason::service_response_code_indicated:
        case retry_reason::socket_closed_while_in_flight:
        case retry_reason::circuit_breaker_open:
        case retry_reason::query_index_not_found:
        case retry_reason::query_prepared_statement_failure:
        case retry_reason::analytics_temporary_failure:
        case retry_reason::search_too_many_requests:
        case retry_reason::do_not_retry:
        case retry_reason::views_temporary_failure:
            return false;

        case retry_reason::key_value_not_my_vbucket:
        case retry_reason::key_value_collection_outdated:
        case retry_reason::views_no_active_partition:
            return true;
    }
    return false;
}

} // namespace couchbase
