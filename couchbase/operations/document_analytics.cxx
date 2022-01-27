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

#include <couchbase/errors.hxx>
#include <couchbase/logger/logger.hxx>
#include <couchbase/operations/document_analytics.hxx>
#include <couchbase/utils/duration_parser.hxx>
#include <couchbase/utils/json.hxx>

#include <gsl/assert>

namespace couchbase::operations
{
std::error_code
analytics_request::encode_to(analytics_request::encoded_request_type& encoded, http_context& context)
{
    tao::json::value body{ { "statement", statement },
                           { "client_context_id", encoded.client_context_id },
                           { "timeout", fmt::format("{}ms", timeout.count()) } };
    if (positional_parameters.empty()) {
        for (const auto& [name, value] : named_parameters) {
            Expects(name.empty() == false);
            std::string key = name;
            if (key[0] != '$') {
                key.insert(key.begin(), '$');
            }
            body[key] = utils::json::parse(value);
        }
    } else {
        std::vector<tao::json::value> parameters;
        for (const auto& value : positional_parameters) {
            parameters.emplace_back(utils::json::parse(value));
        }
        body["args"] = std::move(parameters);
    }
    if (readonly) {
        body["readonly"] = true;
    }
    if (scan_consistency) {
        switch (scan_consistency.value()) {
            case scan_consistency_type::not_bounded:
                body["scan_consistency"] = "not_bounded";
                break;
            case scan_consistency_type::request_plus:
                body["scan_consistency"] = "request_plus";
                break;
        }
    }
    if (scope_qualifier) {
        body["query_context"] = scope_qualifier;
    } else if (scope_name && bucket_name) {
        body["query_context"] = fmt::format("default:`{}`.`{}`", *bucket_name, *scope_name);
    }
    for (const auto& [name, value] : raw) {
        body[name] = utils::json::parse(value);
    }
    encoded.type = type;
    encoded.headers["content-type"] = "application/json";
    if (priority) {
        encoded.headers["analytics-priority"] = "-1";
    }
    encoded.method = "POST";
    encoded.path = "/query/service";
    body_str = utils::json::generate(body);
    encoded.body = body_str;
    if (context.options.show_queries) {
        LOG_INFO("ANALYTICS: client_context_id=\"{}\", {}", encoded.client_context_id, utils::json::generate(body["statement"]));
    } else {
        LOG_DEBUG("ANALYTICS: client_context_id=\"{}\", {}", encoded.client_context_id, utils::json::generate(body["statement"]));
    }
    if (row_callback) {
        encoded.streaming.emplace(couchbase::io::streaming_settings{
          "/results/^",
          4,
          std::move(row_callback.value()),
        });
    }
    return {};
}

analytics_response
analytics_request::make_response(error_context::analytics&& ctx, const encoded_response_type& encoded) const
{
    analytics_response response{ std::move(ctx) };
    response.ctx.statement = statement;
    response.ctx.parameters = body_str;
    if (!response.ctx.ec) {
        tao::json::value payload;
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        response.meta.request_id = payload.at("requestID").get_string();
        response.meta.client_context_id = payload.at("clientContextID").get_string();
        if (response.ctx.client_context_id != response.meta.client_context_id) {
            LOG_WARNING(R"(unexpected clientContextID returned by service: "{}", expected "{}")",
                        response.meta.client_context_id,
                        response.ctx.client_context_id);
        }
        response.meta.status = payload.at("status").get_string();

        if (const auto* s = payload.find("signature"); s != nullptr) {
            response.meta.signature = couchbase::utils::json::generate(*s);
        }

        const tao::json::value& metrics = payload.at("metrics");
        response.meta.metrics.result_count = metrics.at("resultCount").get_unsigned();
        response.meta.metrics.result_size = metrics.at("resultSize").get_unsigned();
        response.meta.metrics.elapsed_time = utils::parse_duration(metrics.at("elapsedTime").get_string());
        response.meta.metrics.execution_time = utils::parse_duration(metrics.at("executionTime").get_string());
        response.meta.metrics.processed_objects = metrics.at("processedObjects").get_unsigned();
        response.meta.metrics.error_count = metrics.optional<std::uint64_t>("errorCount").value_or(0);
        response.meta.metrics.warning_count = metrics.optional<std::uint64_t>("warningCount").value_or(0);

        if (const auto* e = payload.find("errors"); e != nullptr) {
            for (const auto& err : e->get_array()) {
                couchbase::operations::analytics_response::analytics_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                response.meta.errors.emplace_back(problem);
            }
        }

        if (const auto* w = payload.find("warnings"); w != nullptr) {
            for (const auto& warn : w->get_array()) {
                couchbase::operations::analytics_response::analytics_problem problem;
                problem.code = warn.at("code").get_unsigned();
                problem.message = warn.at("msg").get_string();
                response.meta.warnings.emplace_back(problem);
            }
        }

        if (const auto* r = payload.find("results"); r != nullptr) {
            response.rows.reserve(r->get_array().size());
            for (const auto& row : r->get_array()) {
                response.rows.emplace_back(couchbase::utils::json::generate(row));
            }
        }

        if (response.meta.status != "success") {
            bool server_timeout = false;
            bool job_queue_is_full = false;
            bool dataset_not_found = false;
            bool dataverse_not_found = false;
            bool dataset_exists = false;
            bool dataverse_exists = false;
            bool link_not_found = false;
            bool compilation_failure = false;

            for (const auto& error : response.meta.errors) {
                switch (error.code) {
                    case 21002: /* Request timed out and will be cancelled */
                        server_timeout = true;
                        break;
                    case 23007: /* Job queue is full with [string] jobs */
                        job_queue_is_full = true;
                        break;
                    case 24044: /* Cannot find dataset [string] because there is no dataverse declared, nor an alias with name [string]!
                                 */
                    case 24045: /* Cannot find dataset [string] in dataverse [string] nor an alias with name [string]! */
                    case 24025: /* Cannot find dataset with name [string] in dataverse [string] */
                        dataset_not_found = true;
                        break;
                    case 24034: /* Cannot find dataverse with name [string] */
                        dataverse_not_found = true;
                        break;
                    case 24040: /* A dataset with name [string] already exists in dataverse [string] */
                        dataset_exists = true;
                        break;
                    case 24039: /* A dataverse with this name [string] already exists. */
                        dataverse_exists = true;
                        break;
                    case 24006: /* Link [string] does not exist | Link [string] does not exist */
                        link_not_found = true;
                        break;
                    default:
                        if (error.code >= 24000 && error.code < 25000) {
                            compilation_failure = true;
                        }
                }
            }
            if (compilation_failure) {
                response.ctx.ec = error::analytics_errc::compilation_failure;
            } else if (link_not_found) {
                response.ctx.ec = error::analytics_errc::link_not_found;
            } else if (dataset_not_found) {
                response.ctx.ec = error::analytics_errc::dataset_not_found;
            } else if (dataverse_not_found) {
                response.ctx.ec = error::analytics_errc::dataverse_not_found;
            } else if (server_timeout) {
                response.ctx.ec = error::common_errc::unambiguous_timeout;
            } else if (dataset_exists) {
                response.ctx.ec = error::analytics_errc::dataset_exists;
            } else if (dataverse_exists) {
                response.ctx.ec = error::analytics_errc::dataverse_exists;
            } else if (job_queue_is_full) {
                response.ctx.ec = error::analytics_errc::job_queue_full;
            } else {
                response.ctx.ec = error::common_errc::internal_server_failure;
            }
        }
    }
    return response;
}
} // namespace couchbase::operations
