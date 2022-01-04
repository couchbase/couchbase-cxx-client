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

#include <couchbase/protocol/durability_level.hxx>

namespace couchbase::operations::management
{
struct bucket_settings {
    enum class bucket_type { unknown, couchbase, memcached, ephemeral };
    enum class compression_mode { unknown, off, active, passive };
    enum class eviction_policy {
        unknown,

        /**
         * During ejection, everything (including key, metadata, and value) will be ejected.
         *
         * Full Ejection reduces the memory overhead requirement, at the cost of performance.
         *
         * This value is only valid for buckets of type COUCHBASE.
         */
        full,

        /**
         * During ejection, only the value will be ejected (key and metadata will remain in memory).
         *
         * Value Ejection needs more system memory, but provides better performance than Full Ejection.
         *
         * This value is only valid for buckets of type COUCHBASE.
         */
        value_only,

        /**
         * Couchbase Server keeps all data until explicitly deleted, but will reject
         * any new data if you reach the quota (dedicated memory) you set for your bucket.
         *
         * This value is only valid for buckets of type EPHEMERAL.
         */
        no_eviction,

        /**
         * When the memory quota is reached, Couchbase Server ejects data that has not been used recently.
         *
         * This value is only valid for buckets of type EPHEMERAL.
         */
        not_recently_used,
    };
    enum class conflict_resolution_type {
        unknown,
        /**
         * Use timestamp conflict resolution.
         *
         * Timestamp-based conflict resolution (often referred to as Last Write Wins, or LWW) uses the document
         * timestamp (stored in the CAS) to resolve conflicts. The timestamps associated with the most recent
         * updates of source and target documents are compared. The document whose update has the more recent
         * timestamp prevails.
         */
        timestamp,

        /**
         * Use sequence number conflict resolution
         *
         * Conflicts can be resolved by referring to documents' sequence numbers. Sequence numbers are maintained
         * per document, and are incremented on every document-update. The sequence numbers of source and
         * target documents are compared; and the document with the higher sequence number prevails.
         */
        sequence_number,

        /**
         * VOLATILE: This API is subject to change at any time.
         *
         * In Couchbase Server 7.1, this feature is only available in "developer-preview" mode. See the UI XDCR settings.
         */
        custom,
    };
    enum class storage_backend_type { unknown, couchstore, magma };
    struct node {
        std::string hostname;
        std::string status;
        std::string version;
        std::vector<std::string> services;
        std::map<std::string, std::uint16_t> ports;
    };

    std::string name;
    std::string uuid;
    bucket_type bucket_type{ bucket_type::unknown };
    std::uint64_t ram_quota_mb{ 100 };
    std::uint32_t max_expiry{ 0 };
    compression_mode compression_mode{ compression_mode::unknown };
    std::optional<protocol::durability_level> minimum_durability_level{};
    std::uint32_t num_replicas{ 1 };
    bool replica_indexes{ false };
    bool flush_enabled{ false };
    eviction_policy eviction_policy{ eviction_policy::unknown };
    conflict_resolution_type conflict_resolution_type{ conflict_resolution_type::unknown };

    /**
     * UNCOMMITTED: This API may change in the future
     */
    storage_backend_type storage_backend{ storage_backend_type::unknown };

    /**
     * UNCOMMITTED: read-only attribute
     */
    std::vector<std::string> capabilities{};

    /**
     * UNCOMMITTED: read-only attribute
     */
    std::vector<node> nodes{};
};
} // namespace couchbase::operations::management
