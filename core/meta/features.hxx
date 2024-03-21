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

#pragma once

/*
 * This header defines macros for the various features to help
 * users adopt early features or use conditional compilation to avoid
 * unnecessary or untested code.
 *
 * Feel free to update this header with more macros.
 */

/**
 * couchbase::core::meta::sdk_version() function is available
 */
#define COUCHBASE_CXX_CLIENT_HAS_SDK_SEMVER 1

/**
 * couchbase::core::cluster_options and couchbase::security_options support
 * passing TLS trust certificate by value
 */
#define COUCHBASE_CXX_CLIENT_CAN_PASS_TLS_TRUST_CERTIFICATE_BY_VALUE 1

/**
 *  Range scan is available in the core
 *  couchbase::core::range_scan_orchestrator and relevant options in the core API
 */
#define COUCHBASE_CXX_CLIENT_CORE_HAS_RANGE_SCAN 1

/**
 * Query with reads from replica is available:
 *  - use_replica field in couchbase::core::operations::query_request
 *  - couchbase::query_options::use_replica()
 */
#define COUCHBASE_CXX_CLIENT_QUERY_READ_FROM_REPLICA 1

/**
 * Subdoc read from replica is available in the core
 * couchbase::core::lookup_in_replica support
 */
#define COUCHBASE_CXX_CLIENT_CORE_HAS_SUBDOC_READ_REPLICA 1

/**
 * Collection management is accessible from the public API
 * couchbase::bucket::collections() support
 */
#define COUCHBASE_CXX_CLIENT_HAS_PUBLIC_COLLECTION_MANAGEMENT 1

/**
 * Support for bucket no-deduplication feature in bucket and collection management
 */
#define COUCHBASE_CXX_CLIENT_HAS_BUCKET_NO_DEDUP 1

/**
 * Collection query index management is available in the public API
 * couchbase::collection::query_indexes() support
 */
#define COUCHBASE_CXX_CLIENT_HAS_COLLECTION_QUERY_INDEX_MANAGEMENT 1

#define COUCHBASE_CXX_CLIENT_TRANSACTIONS_EXT_PARALLEL_UNSTAGING 1

/**
 * core cluster implementation has been hidden and not accessible through the public API
 */
#define COUCHBASE_CXX_CLIENT_HAS_CORE_CLUSTER_HIDDEN 1

/**
 * expiration_time has been renamed to timeout in transactions_options and transactions_config
 * kv_timeout removed from transactions_options and transactions_config
 */
#define COUCHBASE_CXX_CLIENT_TRANSACTIONS_OPTIONS_HAVE_TIMEOUT 1

/**
 * Search index management is accessible from the public API
 * couchbase::cluster::search_indexes() support
 */
#define COUCHBASE_CXX_CLIENT_HAS_PUBLIC_SEARCH_INDEX_MANAGEMENT 1

/**
 * FTS is accessible from the public API
 * couchbase::cluster::search_query() and couchbase::scope::search_query() support
 */
#define COUCHBASE_CXX_CLIENT_HAS_PUBLIC_SEARCH 1

/**
 * The document not locked (couchbase::errc::key_value::document_not_locked) error code is supported
 */
#define COUCHBASE_CXX_CLIENT_HAS_ERRC_DOCUMENT_NOT_LOCKED 1

/**
 * Vector search is supported via couchbase::cluster::search() or couchbase::scope::search()
 */
#define COUCHBASE_CXX_CLIENT_HAS_VECTOR_SEARCH 1

/**
 * Scope level search index management is supported via couchbase::scope::search_indexes()
 */
#define COUCHBASE_CXX_CLIENT_HAS_SCOPE_SEARCH_INDEX_MANAGEMENT 1

/**
 * Support for couchbase::codec::raw_json_transcoder
 */
#define COUCHBASE_CXX_CLIENT_HAS_RAW_JSON_TRANSCODER 1

/**
 * Support for couchbase::codec::raw_string_transcoder
 */
#define COUCHBASE_CXX_CLIENT_HAS_RAW_STRING_TRANSCODER 1

/**
 * Transaction's transaction_operation_failed has a public getter for its final_error
 */
#define COUCHBASE_CXX_CLIENT_TRANSACTIONS_CAN_FETCH_TO_RAISE 1

/**
 * Hooks in the transactions core are asynchronous (they have a callback parameter)
 */
#define COUCHBASE_CXX_CLIENT_TRANSACTIONS_CORE_ASYNC_HOOKS

/**
 * Range scan is accessible from the public API
 * couchbase::collection::scan()
 */
#define COUCHBASE_CXX_CLIENT_HAS_PUBLIC_RANGE_SCAN 1
