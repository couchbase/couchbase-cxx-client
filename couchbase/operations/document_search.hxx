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

#include <couchbase/error_context/search.hxx>
#include <couchbase/io/http_context.hxx>
#include <couchbase/io/http_message.hxx>
#include <couchbase/json_string.hxx>
#include <couchbase/mutation_token.hxx>
#include <couchbase/platform/uuid.h>
#include <couchbase/search_highlight_style.hxx>
#include <couchbase/search_scan_consistency.hxx>
#include <couchbase/timeout_defaults.hxx>

#include <map>
#include <variant>
#include <vector>

namespace couchbase::operations
{
struct search_response {
    struct search_metrics {
        std::chrono::nanoseconds took{};
        std::uint64_t total_rows{};
        double max_score{};
        std::uint64_t success_partition_count{};
        std::uint64_t error_partition_count{};
    };

    struct search_meta_data {
        std::string client_context_id;
        search_metrics metrics{};
        std::map<std::string, std::string> errors;
    };

    struct search_location {
        std::string field;
        std::string term;
        std::uint64_t position;
        std::uint64_t start_offset;
        std::uint64_t end_offset;
        std::optional<std::vector<std::uint64_t>> array_positions{};
    };

    struct search_row {
        std::string index;
        std::string id;
        double score;
        std::vector<search_location> locations{};
        std::map<std::string, std::vector<std::string>> fragments{};
        std::string fields{};
        std::string explanation{};
    };

    struct search_facet {
        struct term_facet {
            std::string term{};
            std::uint64_t count{};
        };

        struct date_range_facet {
            std::string name{};
            std::uint64_t count{};
            std::optional<std::string> start{};
            std::optional<std::string> end{};
        };

        struct numeric_range_facet {
            std::string name{};
            std::uint64_t count{};
            std::variant<std::monostate, std::uint64_t, double> min{};
            std::variant<std::monostate, std::uint64_t, double> max{};
        };

        std::string name;
        std::string field;
        std::uint64_t total;
        std::uint64_t missing;
        std::uint64_t other;
        std::vector<term_facet> terms{};
        std::vector<date_range_facet> date_ranges{};
        std::vector<numeric_range_facet> numeric_ranges{};
    };

    error_context::search ctx;
    std::string status{};
    search_meta_data meta{};
    std::string error{};
    std::vector<search_row> rows{};
    std::vector<search_facet> facets{};
};

struct search_request {
    using response_type = search_response;
    using encoded_request_type = io::http_request;
    using encoded_response_type = io::http_response;
    using error_context_type = error_context::search;

    static const inline service_type type = service_type::search;

    std::string index_name;
    couchbase::json_string query;

    std::optional<std::uint32_t> limit{};
    std::optional<std::uint32_t> skip{};
    bool explain{ false };
    bool disable_scoring{ false };
    /**
     * UNCOMMITTED: If set to true, will include the vector of search_location in rows.
     */
    bool include_locations{ false };

    std::optional<couchbase::search_highlight_style> highlight_style{};
    std::vector<std::string> highlight_fields{};
    std::vector<std::string> fields{};
    std::optional<std::string> scope_name{};
    std::vector<std::string> collections{};

    std::optional<couchbase::search_scan_consistency> scan_consistency{};
    std::vector<mutation_token> mutation_state{};

    std::vector<std::string> sort_specs{};

    std::map<std::string, std::string> facets{};

    std::map<std::string, couchbase::json_string> raw{};
    std::optional<std::function<utils::json::stream_control(std::string)>> row_callback{};
    std::optional<std::string> client_context_id{};
    std::optional<std::chrono::milliseconds> timeout{};

    [[nodiscard]] std::error_code encode_to(encoded_request_type& encoded, http_context& context);

    [[nodiscard]] search_response make_response(error_context::search&& ctx, const encoded_response_type& encoded) const;

    std::string body_str{};
};

} // namespace couchbase::operations
