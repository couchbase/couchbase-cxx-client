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

#include <couchbase/platform/uuid.h>

#include <couchbase/io/http_context.hxx>
#include <couchbase/io/http_message.hxx>
#include <couchbase/timeout_defaults.hxx>
#include <couchbase/json_string.hxx>

#include <couchbase/protocol/mutation_token.hxx>

#include <couchbase/error_context/query.hxx>

namespace couchbase::operations
{
struct query_response_payload {
    struct query_metrics {
        std::string elapsed_time;
        std::string execution_time;
        std::uint64_t result_count;
        std::uint64_t result_size;
        std::optional<std::uint64_t> sort_count;
        std::optional<std::uint64_t> mutation_count;
        std::optional<std::uint64_t> error_count;
        std::optional<std::uint64_t> warning_count;
    };

    struct query_problem {
        std::uint64_t code{};
        std::string message{};
    };

    struct query_meta_data {
        std::string request_id;
        std::string client_context_id;
        std::string status;
        query_metrics metrics;
        std::optional<std::string> signature;
        std::optional<std::string> profile;
        std::optional<std::vector<query_problem>> warnings;
        std::optional<std::vector<query_problem>> errors;
    };

    query_meta_data meta_data{};
    std::optional<std::string> prepared{};
    std::vector<std::string> rows{};
};
} // namespace couchbase::operations

namespace couchbase::operations
{
struct query_response {
    error_context::query ctx;
    query_response_payload payload{};
};

struct query_request {
    using response_type = query_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::query;

    enum class scan_consistency_type { not_bounded, request_plus };

    static const inline service_type type = service_type::query;

    std::string statement;
    std::string client_context_id{ uuid::to_string(uuid::random()) };

    bool adhoc{ true };
    bool metrics{ false };
    bool readonly{ false };
    bool flex_index{ false };

    std::optional<std::uint64_t> max_parallelism{};
    std::optional<std::uint64_t> scan_cap{};
    std::optional<std::uint64_t> scan_wait{};
    std::optional<std::uint64_t> pipeline_batch{};
    std::optional<std::uint64_t> pipeline_cap{};
    std::optional<scan_consistency_type> scan_consistency{};
    std::vector<mutation_token> mutation_state{};
    std::chrono::milliseconds timeout{ timeout_defaults::query_timeout };
    std::optional<std::string> bucket_name{};
    std::optional<std::string> scope_name{};
    std::optional<std::string> scope_qualifier{};

    enum class profile_mode {
        off,
        phases,
        timings,
    };
    profile_mode profile{ profile_mode::off };

    std::map<std::string, couchbase::json_string> raw{};
    std::vector<couchbase::json_string> positional_parameters{};
    std::map<std::string, couchbase::json_string> named_parameters{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context);

    [[nodiscard]] query_response make_response(error_context::query&& ctx, const encoded_response_type& encoded);

    std::optional<http_context> ctx_{};
    bool extract_encoded_plan_{ false };
    std::string body_str{};
};

} // namespace couchbase::operations
