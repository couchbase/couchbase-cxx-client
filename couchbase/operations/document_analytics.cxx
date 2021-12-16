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

#include <gsl/assert>

#include <couchbase/logger/logger.hxx>

#include <couchbase/operations/document_analytics.hxx>

#include <couchbase/errors.hxx>

#include <couchbase/utils/json.hxx>

namespace tao::json
{
template<>
struct traits<couchbase::operations::analytics_response_payload> {
    template<template<typename...> class Traits>
    static couchbase::operations::analytics_response_payload as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::analytics_response_payload result;
        result.meta_data.request_id = v.at("requestID").get_string();
        result.meta_data.client_context_id = v.at("clientContextID").get_string();
        result.meta_data.status = v.at("status").get_string();

        if (const auto* s = v.find("signature"); s != nullptr) {
            result.meta_data.signature = couchbase::utils::json::generate(*s);
        }

        if (const auto* p = v.find("profile"); p != nullptr) {
            result.meta_data.profile = couchbase::utils::json::generate(*p);
        }

        if (const auto* m = v.find("metrics"); m != nullptr) {
            result.meta_data.metrics.result_count = m->at("resultCount").get_unsigned();
            result.meta_data.metrics.result_size = m->at("resultSize").get_unsigned();
            result.meta_data.metrics.elapsed_time = m->at("elapsedTime").get_string();
            result.meta_data.metrics.execution_time = m->at("executionTime").get_string();
            result.meta_data.metrics.sort_count = m->template optional<std::uint64_t>("sortCount");
            result.meta_data.metrics.mutation_count = m->template optional<std::uint64_t>("mutationCount");
            result.meta_data.metrics.error_count = m->template optional<std::uint64_t>("errorCount");
            result.meta_data.metrics.warning_count = m->template optional<std::uint64_t>("warningCount");
        }

        if (const auto* e = v.find("errors"); e != nullptr) {
            std::vector<couchbase::operations::analytics_response_payload::analytics_problem> problems{};
            for (auto& err : e->get_array()) {
                couchbase::operations::analytics_response_payload::analytics_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.errors.emplace(problems);
        }

        if (const auto* w = v.find("warnings"); w != nullptr) {
            std::vector<couchbase::operations::analytics_response_payload::analytics_problem> problems{};
            for (auto& warn : w->get_array()) {
                couchbase::operations::analytics_response_payload::analytics_problem problem;
                problem.code = warn.at("code").get_unsigned();
                problem.message = warn.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.warnings.emplace(problems);
        }

        if (const auto* r = v.find("results"); r != nullptr) {
            result.rows.reserve(result.meta_data.metrics.result_count);
            for (auto& row : r->get_array()) {
                result.rows.emplace_back(couchbase::utils::json::generate(row));
            }
        }

        return result;
    }
};
} // namespace tao::json

namespace couchbase::operations
{
std::error_code
analytics_request::encode_to(analytics_request::encoded_request_type& encoded, http_context& context)
{
    tao::json::value body{ { "statement", statement },
                           { "client_context_id", client_context_id },
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
        LOG_INFO("ANALYTICS: {}", utils::json::generate(body["statement"]));
    } else {
        LOG_DEBUG("ANALYTICS: {}", utils::json::generate(body["statement"]));
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
        try {
            response.payload = utils::json::parse(encoded.body).as<analytics_response_payload>();
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        Expects(response.payload.meta_data.client_context_id == client_context_id);
        if (response.payload.meta_data.status != "success") {
            bool server_timeout = false;
            bool job_queue_is_full = false;
            bool dataset_not_found = false;
            bool dataverse_not_found = false;
            bool dataset_exists = false;
            bool dataverse_exists = false;
            bool link_not_found = false;
            bool compilation_failure = false;

            if (response.payload.meta_data.errors) {
                for (const auto& error : *response.payload.meta_data.errors) {
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
