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

#include <set>

namespace couchbase::core
{
enum class bucket_capability {
    couchapi,
    xattr,
    dcp,
    cbhello,
    touch,
    cccp,
    xdcr_checkpointing,
    nodes_ext,
    collections,
    durable_write,
    tombstoned_user_xattrs,
    range_scan,
    non_deduped_history,
    subdoc_replace_body_with_xattr,
    subdoc_document_macro_support,
    subdoc_revive_document,
    dcp_ignore_purged_tombstones,
    preserve_expiry,
    query_system_collection,
    mobile_system_collection,
    subdoc_replica_read,
};

enum class cluster_capability {
    n1ql_cost_based_optimizer,
    n1ql_index_advisor,
    n1ql_javascript_functions,
    n1ql_inline_functions,
    n1ql_enhanced_prepared_statements,
    n1ql_read_from_replica,
    search_vector_search,
    search_scoped_search_index,
};

struct configuration_capabilities {
    std::set<bucket_capability> bucket{};
    std::set<cluster_capability> cluster{};

    [[nodiscard]] bool has_cluster_capability(cluster_capability cap) const
    {
        return cluster.find(cap) != cluster.end();
    }

    [[nodiscard]] bool has_bucket_capability(bucket_capability cap) const
    {
        return bucket.find(cap) != bucket.end();
    }

    [[nodiscard]] bool supports_enhanced_prepared_statements() const
    {
        return has_cluster_capability(cluster_capability::n1ql_enhanced_prepared_statements);
    }

    [[nodiscard]] bool supports_read_from_replica() const
    {
        return has_cluster_capability(cluster_capability::n1ql_read_from_replica);
    }

    [[nodiscard]] bool supports_range_scan() const
    {
        return has_bucket_capability(bucket_capability::range_scan);
    }

    [[nodiscard]] bool ephemeral() const
    {
        // Use bucket capabilities to identify if couchapi is missing (then its ephemeral). If its null then
        // we are running an old version of couchbase which doesn't have ephemeral buckets at all.
        return has_bucket_capability(bucket_capability::couchapi);
    }

    [[nodiscard]] bool supports_subdoc_read_replica() const
    {
        return has_bucket_capability(bucket_capability::subdoc_replica_read);
    }

    [[nodiscard]] bool supports_non_deduped_history() const
    {
        return has_bucket_capability(bucket_capability::non_deduped_history);
    }

    [[nodiscard]] bool supports_scoped_search_indexes() const
    {
        return has_cluster_capability(cluster_capability::search_scoped_search_index);
    }

    [[nodiscard]] bool supports_vector_search() const
    {
        return has_cluster_capability(cluster_capability::search_vector_search);
    }
};

} // namespace couchbase::core
