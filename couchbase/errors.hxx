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

#include <system_error>

namespace couchbase::error
{
enum class common_errc {
    /// A request is cancelled and cannot be resolved in a non-ambiguous way. Most likely the request is in-flight on the socket and the
    /// socket gets closed.
    request_canceled = 2,

    /// It is unambiguously determined that the error was caused because of invalid arguments from the user.
    /// Usually only thrown directly when doing request arg validation
    invalid_argument = 3,

    /// It can be determined from the config unambiguously that a given service is not available. I.e. no query node in the config, or a
    /// memcached bucket is accessed and views or n1ql queries should be performed
    service_not_available = 4,

    /// Query: Error range 5xxx
    /// Analytics: Error range 25xxx
    /// KV: error code ERR_INTERNAL (0x84)
    /// Search: HTTP 500
    internal_server_failure = 5,

    /// Query: Error range 10xxx
    /// Analytics: Error range 20xxx
    /// View: HTTP status 401
    /// KV: error code ERR_ACCESS (0x24), ERR_AUTH_ERROR (0x20), AUTH_STALE (0x1f)
    /// Search: HTTP status 401, 403
    authentication_failure = 6,

    /// Analytics: Errors: 23000, 23003
    /// KV: Error code ERR_TMPFAIL (0x86), ERR_BUSY (0x85) ERR_OUT_OF_MEMORY (0x82), ERR_NOT_INITIALIZED (0x25)
    temporary_failure = 7,

    /// Query: code 3000
    /// Analytics: codes 24000
    parsing_failure = 8,

    /// KV: ERR_EXISTS (0x02) when replace or remove with cas
    /// Query: code 12009
    cas_mismatch = 9,

    /// A request is made but the current bucket is not found
    bucket_not_found = 10,

    /// A request is made but the current collection (including scope) is not found
    collection_not_found = 11,

    /// KV: 0x81 (unknown command), 0x83 (not supported)
    unsupported_operation = 12,

    /// A timeout occurs and we aren't sure if the underlying operation has completed.  This normally occurs because we sent the request to
    /// the server successfully, but timed out waiting for the response.  Note that idempotent operations should never return this, as they
    /// do not have ambiguity.
    ambiguous_timeout = 13,

    /// A timeout occurs and we are confident that the operation could not have succeeded.  This normally would occur because we received
    /// confident failures from the server, or never managed to successfully dispatch the operation.
    unambiguous_timeout = 14,

    /// A feature which is not available was used.
    feature_not_available = 15,

    /// A management API attempts to target a scope which does not exist.
    scope_not_found = 16,

    /// Query: Codes 12004, 12016 (warning: regex ahead!) Codes 5000 AND message contains index .+ not found
    /// Analytics: Raised When 24047
    /// Search: Http status code 400 AND text contains “index not found”
    index_not_found = 17,

    /// Query:
    /// Note: the uppercase index for 5000 is not a mistake (also only match on exist not exists because there is a typo
    ///   somewhere in query engine which might either print exist or exists depending on the codepath)
    /// Code 5000 AND message contains Index .+ already exist
    /// Code 4300 AND message contains index .+ already exist
    ///
    /// Analytics: Raised When 24048
    index_exists = 18,

    /// Raised when encoding of a user object failed while trying to write it to the cluster
    encoding_failure = 19,

    /// Raised when decoding of the data into the user object failed
    decoding_failure = 20,

    /**
     * Raised when a service decides that the caller must be rate limited due to exceeding a rate threshold of some sort.
     *
     * * KeyValue
     *   0x30 RateLimitedNetworkIngress -> NetworkIngressRateLimitReached
     *   0x31 RateLimitedNetworkEgress -> NetworkEgressRateLimitReached
     *   0x32 RateLimitedMaxConnections -> MaximumConnectionsReached
     *   0x33 RateLimitedMaxCommands -> RequestRateLimitReached
     *
     * * Cluster Manager (body check tbd)
     *   HTTP 429, Body contains "Limit(s) exceeded [num_concurrent_requests]" -> ConcurrentRequestLimitReached
     *   HTTP 429, Body contains "Limit(s) exceeded [ingress]" -> NetworkIngressRateLimitReached
     *   HTTP 429, Body contains "Limit(s) exceeded [egress]" -> NetworkEgressRateLimitReached
     *   Note: when multiple user limits are exceeded the array would contain all the limits exceeded, as "Limit(s) exceeded
     *   [num_concurrent_requests,egress]"
     *
     * * Query
     *   Code 1191, Message E_SERVICE_USER_REQUEST_EXCEEDED -> RequestRateLimitReached
     *   Code 1192, Message E_SERVICE_USER_REQUEST_RATE_EXCEEDED -> ConcurrentRequestLimitReached
     *   Code 1193, Message E_SERVICE_USER_REQUEST_SIZE_EXCEEDED -> NetworkIngressRateLimitReached
     *   Code 1194, Message E_SERVICE_USER_RESULT_SIZE_EXCEEDED -> NetworkEgressRateLimitReached
     *
     * * Search
     *   HTTP 429, {"status": "fail", "error": "num_concurrent_requests, value >= limit"} -> ConcurrentRequestLimitReached
     *   HTTP 429, {"status": "fail", "error": "num_queries_per_min, value >= limit"}: -> RequestRateLimitReached
     *   HTTP 429, {"status": "fail", "error": "ingress_mib_per_min >= limit"} -> NetworkIngressRateLimitReached
     *   HTTP 429, {"status": "fail", "error": "egress_mib_per_min >= limit"} -> NetworkEgressRateLimitReached
     *
     * * Analytics
     *   Not applicable at the moment.
     *
     * * Views
     *   Not applicable.
     */
    rate_limited = 21,

    /**
     * Raised when a service decides that the caller must be limited due to exceeding a quota threshold of some sort.
     *
     * * KeyValue
     *   0x34 ScopeSizeLimitExceeded
     *
     * * Cluster Manager
     *   HTTP 429, Body contains "Maximum number of collections has been reached for scope "<scope_name>""
     *
     * * Query
     *   Code 5000, Body contains "Limit for number of indexes that can be created per scope has been reached. Limit : value"
     *
     * * Search
     *   HTTP 400 (Bad request), {"status": "fail", "error": "rest_create_index: error creating index: {indexName}, err: manager_api:
     *                           CreateIndex, Prepare failed, err: num_fts_indexes (active + pending) >= limit"}
     * * Analytics
     *   Not applicable at the moment
     *
     * * Views
     *   Not applicable
     */
    quota_limited = 22,
};

/// Errors for related to KeyValue service (kv_engine)
enum class key_value_errc {
    /// The document requested was not found on the server.
    /// KV Code 0x01
    document_not_found = 101,

    /// In get_any_replica, the get_all_replicas returns an empty stream because all the individual errors are dropped (i.e. all returned a
    /// document_not_found)
    document_irretrievable = 102,

    /// The document requested was locked.
    /// KV Code 0x09
    document_locked = 103,

    /// The value that was sent was too large to store (typically > 20MB)
    /// KV Code 0x03
    value_too_large = 104,

    /// An operation which relies on the document not existing fails because the document existed.
    /// KV Code 0x02
    document_exists = 105,

    /// The specified durability level is invalid.
    /// KV Code 0xa0
    durability_level_not_available = 107,

    /// The specified durability requirements are not currently possible (for example, there are an insufficient number of replicas online).
    /// KV Code 0xa1
    durability_impossible = 108,

    /// A sync-write has not completed in the specified time and has an ambiguous result - it may have succeeded or failed, but the final
    /// result is not yet known.
    /// A SEQNO OBSERVE operation is performed and the vbucket UUID changes during polling.
    /// KV Code 0xa3
    durability_ambiguous = 109,

    /// A durable write is attempted against a key which already has a pending durable write.
    /// KV Code 0xa2
    durable_write_in_progress = 110,

    /// The server is currently working to synchronize all replicas for previously performed durable operations (typically occurs after a
    /// rebalance).
    /// KV Code 0xa4
    durable_write_re_commit_in_progress = 111,

    /// The path provided for a sub-document operation was not found.
    /// KV Code 0xc0
    path_not_found = 113,

    /// The path provided for a sub-document operation did not match the actual structure of the document.
    /// KV Code 0xc1
    path_mismatch = 114,

    /// The path provided for a sub-document operation was not syntactically correct.
    /// KV Code 0xc2
    path_invalid = 115,

    /// The path provided for a sub-document operation is too long, or contains too many independent components.
    /// KV Code 0xc3
    path_too_big = 116,

    /// The document contains too many levels to parse.
    /// KV Code 0xc4
    path_too_deep = 117,

    /// The value provided, if inserted into the document, would cause the document to become too deep for the server to accept.
    /// KV Code 0xca
    value_too_deep = 118,

    /// The value provided for a sub-document operation would invalidate the JSON structure of the document if inserted as requested.
    /// KV Code 0xc5
    value_invalid = 119,

    /// A sub-document operation is performed on a non-JSON document.
    /// KV Code 0xc6
    document_not_json = 120,

    /// The existing number is outside the valid range for arithmetic operations.
    /// KV Code 0xc7
    number_too_big = 121,

    /// The delta value specified for an operation is too large.
    /// KV Code 0xc8
    delta_invalid = 122,

    /// A sub-document operation which relies on a path not existing encountered a path which exists.
    /// KV Code 0xc9
    path_exists = 123,

    /// A macro was used which the server did not understand.
    /// KV Code: 0xd0
    xattr_unknown_macro = 124,

    /// A sub-document operation attempts to access multiple xattrs in one operation.
    /// KV Code: 0xcf
    xattr_invalid_key_combo = 126,

    /// A sub-document operation attempts to access an unknown virtual attribute.
    /// KV Code: 0xd1
    xattr_unknown_virtual_attribute = 127,

    /// A sub-document operation attempts to modify a virtual attribute.
    /// KV Code: 0xd2
    xattr_cannot_modify_virtual_attribute = 128,

    /// The user does not have permission to access the attribute. Occurs when the user attempts to read or write a system attribute (name
    /// starts with underscore) but does not have the SystemXattrRead / SystemXattrWrite permission.
    /// KV Code: 0x24
    xattr_no_access = 130,

    /// Only deleted document could be revived
    /// KV Code: 0xd6
    cannot_revive_living_document = 131,
};

/// Errors related to Query service (N1QL)
enum class query_errc {
    /// Raised When code range 4xxx other than those explicitly covered
    planning_failure = 201,

    /// Raised When code range 12xxx and 14xxx (other than 12004 and 12016)
    index_failure = 202,

    /// Raised When codes 4040, 4050, 4060, 4070, 4080, 4090
    prepared_statement_failure = 203,

    /// Raised when code 12009 AND message does not contain CAS mismatch
    dml_failure = 204,
};

/// Errors related to Analytics service (CBAS)
enum class analytics_errc {
    /// Error range 24xxx (excluded are specific codes in the errors below)
    compilation_failure = 301,

    /// Error code 23007
    job_queue_full = 302,

    /// Error codes 24044, 24045, 24025
    dataset_not_found = 303,

    /// Error code 24034
    dataverse_not_found = 304,

    /// Raised When 24040
    dataset_exists = 305,

    /// Raised When 24039
    dataverse_exists = 306,

    /// Raised When 24006
    link_not_found = 307,

    /// Raised When 24055
    link_exists = 308,
};

/// Errors related to Search service (CBFT)
enum class search_errc {
    index_not_ready = 401,

    consistency_mismatch = 402,
};

/// Errors related to Views service (CAPI)
enum class view_errc {
    /// Http status code 404
    /// Reason or error contains "not_found"
    view_not_found = 501,

    /// Raised on the Management APIs only when
    /// * Getting a design document
    /// * Dropping a design document
    /// * And the server returns 404
    design_document_not_found = 502,
};

/// Errors related to management service (ns_server)
enum class management_errc {
    /// Raised from the collection management API
    collection_exists = 601,

    /// Raised from the collection management API
    scope_exists = 602,

    /// Raised from the user management API
    user_not_found = 603,

    /// Raised from the user management API
    group_not_found = 604,

    /// Raised from the bucket management API
    bucket_exists = 605,

    /// Raised from the user management API
    user_exists = 606,

    /// Raised from the bucket management API
    bucket_not_flushable = 607,

    /// Occurs if the function is not found
    /// name is "ERR_APP_NOT_FOUND_TS"
    eventing_function_not_found = 608,

    /// Occurs if the function is not deployed, but the action expects it to
    /// name is "ERR_APP_NOT_DEPLOYED"
    eventing_function_not_deployed = 609,

    /// Occurs when the compilation of the function code failed
    /// name is "ERR_HANDLER_COMPILATION"
    eventing_function_compilation_failure = 610,

    /// Occurs when source and metadata keyspaces are the same.
    /// name is "ERR_SRC_MB_SAME"
    eventing_function_identical_keyspace = 611,

    /// Occurs when a function is deployed but not “fully” bootstrapped
    /// name is "ERR_APP_NOT_BOOTSTRAPPED"
    eventing_function_not_bootstrapped = 612,

    /// Occurs when a function is deployed but the action does not expect it to
    /// name is "ERR_APP_NOT_UNDEPLOYED"
    eventing_function_deployed = 613,

    /// Occurs when a function is paused but the action does not expect it to
    /// name is "ERR_APP_PAUSED"
    eventing_function_paused = 614,
};

/// Field-Level Encryption Error Definitions
enum class field_level_encryption_errc {
    /// Generic cryptography failure.
    generic_cryptography_failure = 700,

    /// Raised by CryptoManager.encrypt() when encryption fails for any reason. Should have one of the other Field-Level Encryption errors
    /// as a cause.
    encryption_failure = 701,

    /// Raised by CryptoManager.decrypt() when decryption fails for any reason. Should have one of the other Field-Level Encryption errors
    /// as a cause.
    decryption_failure = 702,

    /// Raised when a crypto operation fails because a required key is missing.
    crypto_key_not_found = 703,

    /// Raised by an encrypter or decrypter when the key does not meet expectations (for example, if the key is the wrong size).
    invalid_crypto_key = 704,

    /// Raised when a message cannot be decrypted because there is no decrypter registered for the algorithm.
    decrypter_not_found = 705,

    /// Raised when a message cannot be encrypted because there is no encrypter registered under the requested alias.
    encrypter_not_found = 706,

    /// Raised when decryption fails due to malformed input, integrity check failure, etc.
    invalid_ciphertext = 707
};

/// Errors related to networking IO
enum class network_errc {
    /// Unable to resolve node address
    resolve_failure = 1001,

    /// No hosts left to connect
    no_endpoints_left = 1002,

    /// Failed to complete protocol handshake
    handshake_failure = 1003,

    /// Unexpected protocol state or input
    protocol_error = 1004,

    /// Configuration is not available for some reason
    configuration_not_available = 1005,

    /**
     * The cluster object has been explicitly closed, no requests allowed
     */
    cluster_closed = 1006,
};

enum class streaming_json_lexer_errc {
    garbage_trailing = 1101,
    special_expected = 1102,
    special_incomplete = 1103,
    stray_token = 1104,
    missing_token = 1105,
    cannot_insert = 1106,
    escape_outside_string = 1107,
    key_outside_object = 1108,
    string_outside_container = 1109,
    found_null_byte = 1110,
    levels_exceeded = 1111,
    bracket_mismatch = 1112,
    object_key_expected = 1113,
    weird_whitespace = 1114,
    unicode_escape_is_too_short = 1115,
    escape_invalid = 1116,
    trailing_comma = 1117,
    invalid_number = 1118,
    value_expected = 1119,
    percent_bad_hex = 1120,
    json_pointer_bad_path = 1121,
    json_pointer_duplicated_slash = 1122,
    json_pointer_missing_root = 1123,
    not_enough_memory = 1124,
    invalid_codepoint = 1125,
    generic = 1126,
    root_is_not_an_object = 1127,
    root_does_not_match_json_pointer = 1128,
};

namespace detail
{
struct common_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.common";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (common_errc(ev)) {
            case common_errc::unambiguous_timeout:
                return "unambiguous_timeout";
            case common_errc::ambiguous_timeout:
                return "ambiguous_timeout";
            case common_errc::request_canceled:
                return "request_canceled";
            case common_errc::invalid_argument:
                return "invalid_argument";
            case common_errc::service_not_available:
                return "service_not_available";
            case common_errc::internal_server_failure:
                return "internal_server_failure";
            case common_errc::authentication_failure:
                return "authentication_failure";
            case common_errc::temporary_failure:
                return "temporary_failure";
            case common_errc::parsing_failure:
                return "parsing_failure";
            case common_errc::cas_mismatch:
                return "cas_mismatch";
            case common_errc::bucket_not_found:
                return "bucket_not_found";
            case common_errc::scope_not_found:
                return "scope_not_found";
            case common_errc::collection_not_found:
                return "collection_not_found";
            case common_errc::unsupported_operation:
                return "unsupported_operation";
            case common_errc::feature_not_available:
                return "feature_not_available";
            case common_errc::encoding_failure:
                return "encoding_failure";
            case common_errc::decoding_failure:
                return "decoding_failure";
            case common_errc::index_not_found:
                return "index_not_found";
            case common_errc::index_exists:
                return "index_exists";
            case common_errc::rate_limited:
                return "rate_limited";
            case common_errc::quota_limited:
                return "quota_limited";
        }
        return "FIXME: unknown error code common (recompile with newer library)";
    }
};

inline const std::error_category&
get_common_category()
{
    static detail::common_error_category instance;
    return instance;
}

struct key_value_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.key_value";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (key_value_errc(ev)) {
            case key_value_errc::document_not_found:
                return "document_not_found";
            case key_value_errc::document_irretrievable:
                return "document_irretrievable";
            case key_value_errc::document_locked:
                return "document_locked";
            case key_value_errc::value_too_large:
                return "value_too_large";
            case key_value_errc::document_exists:
                return "document_exists";
            case key_value_errc::durability_level_not_available:
                return "durability_level_not_available";
            case key_value_errc::durability_impossible:
                return "durability_impossible";
            case key_value_errc::durability_ambiguous:
                return "durability_ambiguous";
            case key_value_errc::durable_write_in_progress:
                return "durable_write_in_progress";
            case key_value_errc::durable_write_re_commit_in_progress:
                return "durable_write_re_commit_in_progress";
            case key_value_errc::path_not_found:
                return "path_not_found";
            case key_value_errc::path_mismatch:
                return "path_mismatch";
            case key_value_errc::path_invalid:
                return "path_invalid";
            case key_value_errc::path_too_big:
                return "path_too_big";
            case key_value_errc::path_too_deep:
                return "path_too_deep";
            case key_value_errc::value_too_deep:
                return "value_too_deep";
            case key_value_errc::value_invalid:
                return "value_invalid";
            case key_value_errc::document_not_json:
                return "document_not_json";
            case key_value_errc::number_too_big:
                return "number_too_big";
            case key_value_errc::delta_invalid:
                return "delta_invalid";
            case key_value_errc::path_exists:
                return "path_exists";
            case key_value_errc::xattr_unknown_macro:
                return "xattr_unknown_macro";
            case key_value_errc::xattr_invalid_key_combo:
                return "xattr_invalid_key_combo";
            case key_value_errc::xattr_unknown_virtual_attribute:
                return "xattr_unknown_virtual_attribute";
            case key_value_errc::xattr_cannot_modify_virtual_attribute:
                return "xattr_cannot_modify_virtual_attribute";
            case key_value_errc::cannot_revive_living_document:
                return "cannot_revive_living_document";
            case key_value_errc::xattr_no_access:
                return "xattr_no_access";
        }
        return "FIXME: unknown error code key_value (recompile with newer library)";
    }
};
inline const std::error_category&
get_key_value_category()
{
    static detail::key_value_error_category instance;
    return instance;
}

struct query_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.query";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (query_errc(ev)) {
            case query_errc::planning_failure:
                return "planning_failure";
            case query_errc::index_failure:
                return "index_failure";
            case query_errc::prepared_statement_failure:
                return "prepared_statement_failure";
            case query_errc::dml_failure:
                return "dml_failure";
        }
        return "FIXME: unknown error code in query category (recompile with newer library)";
    }
};

inline const std::error_category&
get_query_category()
{
    static detail::query_error_category instance;
    return instance;
}

struct search_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.search";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (search_errc(ev)) {
            case search_errc::index_not_ready:
                return "index_not_ready";
            case search_errc::consistency_mismatch:
                return "consistency_mismatch";
        }
        return "FIXME: unknown error code in search category (recompile with newer library)";
    }
};

inline const std::error_category&
get_search_category()
{
    static detail::search_error_category instance;
    return instance;
}

struct view_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.view";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (view_errc(ev)) {
            case view_errc::view_not_found:
                return "view_not_found";
            case view_errc::design_document_not_found:
                return "design_document_not_found";
        }
        return "FIXME: unknown error code in view category (recompile with newer library)";
    }
};

inline const std::error_category&
get_view_category()
{
    static detail::view_error_category instance;
    return instance;
}

struct analytics_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.analytics";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (analytics_errc(ev)) {
            case analytics_errc::compilation_failure:
                return "compilation_failure";
            case analytics_errc::job_queue_full:
                return "job_queue_full";
            case analytics_errc::dataset_not_found:
                return "dataset_not_found";
            case analytics_errc::dataverse_not_found:
                return "dataverse_not_found";
            case analytics_errc::dataset_exists:
                return "dataset_exists";
            case analytics_errc::dataverse_exists:
                return "dataverse_exists";
            case analytics_errc::link_not_found:
                return "link_not_found";
            case analytics_errc::link_exists:
                return "link_exists";
        }
        return "FIXME: unknown error code in analytics category (recompile with newer library)";
    }
};

inline const std::error_category&
get_analytics_category()
{
    static detail::analytics_error_category instance;
    return instance;
}

struct management_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.management";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (management_errc(ev)) {
            case management_errc::collection_exists:
                return "collection_exists";
            case management_errc::scope_exists:
                return "scope_exists";
            case management_errc::user_not_found:
                return "user_not_found";
            case management_errc::group_not_found:
                return "group_not_found";
            case management_errc::user_exists:
                return "user_exists";
            case management_errc::bucket_exists:
                return "bucket_exists";
            case management_errc::bucket_not_flushable:
                return "bucket_not_flushable";
            case management_errc::eventing_function_not_found:
                return "eventing_function_not_found";
            case management_errc::eventing_function_not_deployed:
                return "eventing_function_not_deployed";
            case management_errc::eventing_function_compilation_failure:
                return "eventing_function_compilation_failure";
            case management_errc::eventing_function_identical_keyspace:
                return "eventing_function_identical_keyspace";
            case management_errc::eventing_function_not_bootstrapped:
                return "eventing_function_not_bootstrapped";
            case management_errc::eventing_function_deployed:
                return "eventing_function_deployed";
            case management_errc::eventing_function_paused:
                return "eventing_function_paused";
        }
        return "FIXME: unknown error code in management category (recompile with newer library)";
    }
};

inline const std::error_category&
get_management_category()
{
    static detail::management_error_category instance;
    return instance;
}

struct network_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.network";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (network_errc(ev)) {
            case network_errc::resolve_failure:
                return "resolve_failure";
            case network_errc::no_endpoints_left:
                return "no_endpoints_left";
            case network_errc::handshake_failure:
                return "handshake_failure";
            case network_errc::protocol_error:
                return "protocol_error";
            case network_errc::configuration_not_available:
                return "configuration_not_available";
            case network_errc::cluster_closed:
                return "cluster_closed";
        }
        return "FIXME: unknown error code in network category (recompile with newer library)";
    }
};

inline const std::error_category&
get_network_category()
{
    static detail::network_error_category instance;
    return instance;
}

struct field_level_encryption_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.field_level_encryption";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (field_level_encryption_errc(ev)) {
            case field_level_encryption_errc::generic_cryptography_failure:
                return "generic_cryptography_failure";
            case field_level_encryption_errc::encryption_failure:
                return "encryption_failure";
            case field_level_encryption_errc::decryption_failure:
                return "decryption_failure";
            case field_level_encryption_errc::crypto_key_not_found:
                return "crypto_key_not_found";
            case field_level_encryption_errc::invalid_crypto_key:
                return "invalid_crypto_key";
            case field_level_encryption_errc::decrypter_not_found:
                return "decrypter_not_found";
            case field_level_encryption_errc::encrypter_not_found:
                return "encrypter_not_found";
            case field_level_encryption_errc::invalid_ciphertext:
                return "invalid_ciphertext";
        }
        return "FIXME: unknown error code in field level encryption category (recompile with newer library)";
    }
};

inline const std::error_category&
get_field_level_encryption_category()
{
    static detail::field_level_encryption_error_category instance;
    return instance;
}

struct streaming_json_lexer_error_category : std::error_category {
    [[nodiscard]] const char* name() const noexcept override
    {
        return "couchbase.streaming_json_lexer";
    }

    [[nodiscard]] std::string message(int ev) const noexcept override
    {
        switch (streaming_json_lexer_errc(ev)) {
            case streaming_json_lexer_errc::garbage_trailing:
                return "garbage_trailing";
            case streaming_json_lexer_errc::special_expected:
                return "special_expected";
            case streaming_json_lexer_errc::special_incomplete:
                return "special_incomplete";
            case streaming_json_lexer_errc::stray_token:
                return "stray_token";
            case streaming_json_lexer_errc::missing_token:
                return "missing_token";
            case streaming_json_lexer_errc::cannot_insert:
                return "cannot_insert";
            case streaming_json_lexer_errc::escape_outside_string:
                return "escape_outside_string";
            case streaming_json_lexer_errc::key_outside_object:
                return "key_outside_object";
            case streaming_json_lexer_errc::string_outside_container:
                return "string_outside_container";
            case streaming_json_lexer_errc::found_null_byte:
                return "found_null_byte";
            case streaming_json_lexer_errc::levels_exceeded:
                return "levels_exceeded";
            case streaming_json_lexer_errc::bracket_mismatch:
                return "bracket_mismatch";
            case streaming_json_lexer_errc::object_key_expected:
                return "object_key_expected";
            case streaming_json_lexer_errc::weird_whitespace:
                return "weird_whitespace";
            case streaming_json_lexer_errc::unicode_escape_is_too_short:
                return "unicode_escape_is_too_short";
            case streaming_json_lexer_errc::escape_invalid:
                return "escape_invalid";
            case streaming_json_lexer_errc::trailing_comma:
                return "trailing_comma";
            case streaming_json_lexer_errc::invalid_number:
                return "invalid_number";
            case streaming_json_lexer_errc::value_expected:
                return "value_expected";
            case streaming_json_lexer_errc::percent_bad_hex:
                return "percent_bad_hex";
            case streaming_json_lexer_errc::json_pointer_bad_path:
                return "json_pointer_bad_path";
            case streaming_json_lexer_errc::json_pointer_duplicated_slash:
                return "json_pointer_duplicated_slash";
            case streaming_json_lexer_errc::json_pointer_missing_root:
                return "json_pointer_missing_root";
            case streaming_json_lexer_errc::not_enough_memory:
                return "not_enough_memory";
            case streaming_json_lexer_errc::invalid_codepoint:
                return "invalid_codepoint";
            case streaming_json_lexer_errc::generic:
                return "streaming json lexer generic error";
            case streaming_json_lexer_errc::root_is_not_an_object:
                return "root_is_not_an_object";
            case streaming_json_lexer_errc::root_does_not_match_json_pointer:
                return "root_does_not_match_json_pointer";
        }
        return "FIXME: unknown error code in streaming json lexer category (recompile with newer library)";
    }
};

inline const std::error_category&
get_streaming_json_lexer_category()
{
    static detail::streaming_json_lexer_error_category instance;
    return instance;
}

} // namespace detail

} // namespace couchbase::error

namespace std
{
template<>
struct is_error_code_enum<couchbase::error::common_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::key_value_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::query_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::search_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::view_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::analytics_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::management_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::network_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::field_level_encryption_errc> : true_type {
};

template<>
struct is_error_code_enum<couchbase::error::streaming_json_lexer_errc> : true_type {
};
} // namespace std

namespace couchbase::error
{
inline std::error_code
make_error_code(couchbase::error::common_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_common_category() };
}

inline std::error_code
make_error_code(couchbase::error::key_value_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_key_value_category() };
}

inline std::error_code
make_error_code(couchbase::error::query_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_query_category() };
}

inline std::error_code
make_error_code(couchbase::error::search_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_search_category() };
}

inline std::error_code
make_error_code(couchbase::error::view_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_view_category() };
}

inline std::error_code
make_error_code(couchbase::error::analytics_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_analytics_category() };
}

inline std::error_code
make_error_code(couchbase::error::management_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_management_category() };
}

inline std::error_code
make_error_code(couchbase::error::network_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_network_category() };
}

inline std::error_code
make_error_code(couchbase::error::field_level_encryption_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_field_level_encryption_category() };
}

inline std::error_code
make_error_code(couchbase::error::streaming_json_lexer_errc e)
{
    return { static_cast<int>(e), couchbase::error::detail::get_streaming_json_lexer_category() };
}

} // namespace couchbase::error
