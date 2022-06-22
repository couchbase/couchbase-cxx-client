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

#include <couchbase/analytics_scan_consistency.hxx>
#include <couchbase/error_context/analytics.hxx>
#include <couchbase/io/http_context.hxx>
#include <couchbase/io/http_message.hxx>
#include <couchbase/json_string.hxx>
#include <couchbase/platform/uuid.h>
#include <couchbase/timeout_defaults.hxx>

namespace couchbase::operations
{
struct analytics_response {
    struct analytics_metrics {
        std::chrono::nanoseconds elapsed_time{};
        std::chrono::nanoseconds execution_time{};
        std::uint64_t result_count{};
        std::uint64_t result_size{};
        std::uint64_t error_count{};
        std::uint64_t processed_objects{};
        std::uint64_t warning_count{};
    };

    struct analytics_problem {
        std::uint64_t code;
        std::string message;
    };

    struct analytics_meta_data {
        std::string request_id;
        std::string client_context_id;
        std::string status;
        analytics_metrics metrics{};
        std::optional<std::string> signature{};
        std::vector<analytics_problem> errors{};
        std::vector<analytics_problem> warnings{};
    };

    error_context::analytics ctx;
    analytics_meta_data meta{};
    std::vector<std::string> rows{};
};

struct analytics_request {
    using response_type = analytics_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::analytics;

    static const inline service_type type = service_type::analytics;

    std::string statement;

    bool readonly{ false };
    bool priority{ false };
    std::optional<std::string> bucket_name{};
    std::optional<std::string> scope_name{};
    std::optional<std::string> scope_qualifier{};

    std::optional<couchbase::analytics_scan_consistency> scan_consistency{};

    std::map<std::string, couchbase::json_string> raw{};
    std::vector<couchbase::json_string> positional_parameters{};
    std::map<std::string, couchbase::json_string> named_parameters{};
    std::optional<std::function<utils::json::stream_control(std::string)>> row_callback{};
    std::optional<std::string> client_context_id{};
    std::optional<std::chrono::milliseconds> timeout{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context);
    [[nodiscard]] analytics_response make_response(error_context::analytics&& ctx, const encoded_response_type& encoded) const;

    std::string body_str{};
};

} // namespace couchbase::operations
