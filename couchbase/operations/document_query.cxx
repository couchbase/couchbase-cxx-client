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

#include <couchbase/operations/document_query.hxx>

#include <couchbase/errors.hxx>

#include <couchbase/logger/logger.hxx>

#include <couchbase/utils/json.hxx>

namespace tao::json
{
template<>
struct traits<couchbase::operations::query_response_payload> {
    template<template<typename...> class Traits>
    static couchbase::operations::query_response_payload as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::query_response_payload result;
        result.meta_data.request_id = v.at("requestID").get_string();

        if (const auto* i = v.find("clientContextID"); i != nullptr) {
            result.meta_data.client_context_id = i->get_string();
        }
        result.meta_data.status = v.at("status").get_string();
        if (const auto* s = v.find("signature"); s != nullptr) {
            result.meta_data.signature = couchbase::utils::json::generate(*s);
        }
        if (const auto* c = v.find("prepared"); c != nullptr) {
            result.prepared = c->get_string();
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
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& err : e->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
                problem.code = err.at("code").get_unsigned();
                problem.message = err.at("msg").get_string();
                problems.emplace_back(problem);
            }
            result.meta_data.errors.emplace(problems);
        }

        if (const auto* w = v.find("warnings"); w != nullptr) {
            std::vector<couchbase::operations::query_response_payload::query_problem> problems{};
            for (auto& warn : w->get_array()) {
                couchbase::operations::query_response_payload::query_problem problem;
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
query_request::encode_to(query_request::encoded_request_type& encoded, http_context& context)
{
    ctx_.emplace(context);
    tao::json::value body{};
    if (adhoc) {
        body["statement"] = statement;
    } else {
        auto entry = ctx_->cache.get(statement);
        if (entry) {
            body["prepared"] = entry->name;
            if (entry->plan) {
                body["encoded_plan"] = entry->plan.value();
            }
        } else {
            body["statement"] = "PREPARE " + statement;
            if (context.config.supports_enhanced_prepared_statements()) {
                body["auto_execute"] = true;
            } else {
                extract_encoded_plan_ = true;
            }
        }
    }
    body["client_context_id"] = client_context_id;
    body["timeout"] =
      fmt::format("{}ms", ((timeout > std::chrono::milliseconds(5'000)) ? (timeout - std::chrono::milliseconds(500)) : timeout).count());
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
    switch (profile) {
        case profile_mode::phases:
            body["profile"] = "phases";
            break;
        case profile_mode::timings:
            body["profile"] = "timings";
            break;
        case profile_mode::off:
            break;
    }
    if (max_parallelism) {
        body["max_parallelism"] = std::to_string(max_parallelism.value());
    }
    if (pipeline_cap) {
        body["pipeline_cap"] = std::to_string(pipeline_cap.value());
    }
    if (pipeline_batch) {
        body["pipeline_batch"] = std::to_string(pipeline_batch.value());
    }
    if (scan_cap) {
        body["scan_cap"] = std::to_string(scan_cap.value());
    }
    if (!metrics) {
        body["metrics"] = false;
    }
    if (readonly) {
        body["readonly"] = true;
    }
    if (flex_index) {
        body["use_fts"] = true;
    }
    bool check_scan_wait = false;
    if (scan_consistency) {
        switch (scan_consistency.value()) {
            case scan_consistency_type::not_bounded:
                body["scan_consistency"] = "not_bounded";
                break;
            case scan_consistency_type::request_plus:
                check_scan_wait = true;
                body["scan_consistency"] = "request_plus";
                break;
        }
    } else if (!mutation_state.empty()) {
        check_scan_wait = true;
        body["scan_consistency"] = "at_plus";
        tao::json::value scan_vectors = tao::json::empty_object;
        for (const auto& token : mutation_state) {
            auto* bucket = scan_vectors.find(token.bucket_name);
            if (bucket == nullptr) {
                scan_vectors[token.bucket_name] = tao::json::empty_object;
                bucket = scan_vectors.find(token.bucket_name);
            }
            auto& bucket_obj = bucket->get_object();
            bucket_obj[std::to_string(token.partition_id)] =
              std::vector<tao::json::value>{ token.sequence_number, std::to_string(token.partition_uuid) };
        }
        body["scan_vectors"] = scan_vectors;
    }
    if (check_scan_wait && scan_wait) {
        body["scan_wait"] = fmt::format("{}ms", scan_wait.value());
    }
    if (scope_qualifier) {
        body["query_context"] = scope_qualifier;
    } else if (scope_name) {
        if (bucket_name) {
            body["query_context"] = fmt::format("default:`{}`.`{}`", *bucket_name, *scope_name);
        }
    }
    for (const auto& [name, value] : raw) {
        body[name] = utils::json::parse(value);
    }
    encoded.type = type;
    encoded.headers["connection"] = "keep-alive";
    encoded.headers["content-type"] = "application/json";
    encoded.method = "POST";
    encoded.path = "/query/service";
    body_str = utils::json::generate(body);
    encoded.body = body_str;

    tao::json::value stmt = body["statement"];
    tao::json::value prep = body["prepared"];
    if (!stmt.is_string()) {
        stmt = statement;
    }
    if (!prep.is_string()) {
        prep = false;
    }
    if (ctx_->options.show_queries) {
        LOG_INFO(
          "QUERY: client_context_id=\"{}\", prep={}, {}", client_context_id, utils::json::generate(prep), utils::json::generate(stmt));
    } else {
        LOG_DEBUG(
          "QUERY: client_context_id=\"{}\", prep={}, {}", client_context_id, utils::json::generate(prep), utils::json::generate(stmt));
    }
    return {};
}

query_response
query_request::make_response(error_context::query&& ctx, const encoded_response_type& encoded)
{
    query_response response{ std::move(ctx) };
    response.ctx.statement = statement;
    response.ctx.parameters = body_str;
    if (!response.ctx.ec) {
        try {
            response.payload = utils::json::parse(encoded.body).as<query_response_payload>();
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        Expects(response.payload.meta_data.client_context_id.empty() || response.payload.meta_data.client_context_id == client_context_id);
        if (response.payload.meta_data.status == "success") {
            if (response.payload.prepared) {
                ctx_->cache.put(statement, response.payload.prepared.value());
            } else if (extract_encoded_plan_) {
                extract_encoded_plan_ = false;
                if (response.payload.rows.size() == 1) {
                    tao::json::value row{};
                    try {
                        row = utils::json::parse(response.payload.rows[0]);
                    } catch (const tao::pegtl::parse_error&) {
                        response.ctx.ec = error::common_errc::parsing_failure;
                        return response;
                    }
                    auto* plan = row.find("encoded_plan");
                    auto* name = row.find("name");
                    if (plan != nullptr && name != nullptr) {
                        ctx_->cache.put(statement, name->get_string(), plan->get_string());
                        throw couchbase::priv::retry_http_request{};
                    }
                    response.ctx.ec = error::query_errc::prepared_statement_failure;

                } else {
                    response.ctx.ec = error::query_errc::prepared_statement_failure;
                }
            }
        } else {
            bool prepared_statement_failure = false;
            bool index_not_found = false;
            bool index_failure = false;
            bool planning_failure = false;
            bool syntax_error = false;
            bool server_timeout = false;
            bool invalid_argument = false;
            bool cas_mismatch = false;
            bool dml_failure = false;
            bool authentication_failure = false;
            bool rate_limited = false;
            bool quota_limited = false;

            if (response.payload.meta_data.errors) {
                for (const auto& error : *response.payload.meta_data.errors) {
                    switch (error.code) {
                        case 1065: /* IKey: "service.io.request.unrecognized_parameter" */
                            invalid_argument = true;
                            break;
                        case 1080: /* IKey: "timeout" */
                            server_timeout = true;
                            break;
                        case 3000: /* IKey: "parse.syntax_error" */
                            syntax_error = true;
                            break;
                        case 4040: /* IKey: "plan.build_prepared.no_such_name" */
                        case 4050: /* IKey: "plan.build_prepared.unrecognized_prepared" */
                        case 4060: /* IKey: "plan.build_prepared.no_such_name" */
                        case 4070: /* IKey: "plan.build_prepared.decoding" */
                        case 4080: /* IKey: "plan.build_prepared.name_encoded_plan_mismatch" */
                        case 4090: /* IKey: "plan.build_prepared.name_not_in_encoded_plan" */
                            prepared_statement_failure = true;
                            break;
                        case 12009: /* IKey: "datastore.couchbase.DML_error" */
                            if (error.message.find("CAS mismatch") != std::string::npos) {
                                cas_mismatch = true;
                            } else {
                                dml_failure = true;
                            }
                            break;

                        case 1191: /* ICode: E_SERVICE_USER_REQUEST_EXCEEDED, IKey: "service.requests.exceeded" */
                        case 1192: /* ICode: E_SERVICE_USER_REQUEST_RATE_EXCEEDED, IKey: "service.request.rate.exceeded" */
                        case 1193: /* ICode: E_SERVICE_USER_REQUEST_SIZE_EXCEEDED, IKey: "service.request.size.exceeded" */
                        case 1194: /* ICode: E_SERVICE_USER_RESULT_SIZE_EXCEEDED, IKey: "service.result.size.exceeded" */
                            rate_limited = true;
                            break;

                        case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
                        case 12016: /* IKey: "datastore.couchbase.index_not_found" */
                            index_not_found = true;
                            break;
                        case 13014: /* IKey: "datastore.couchbase.insufficient_credentials" */
                            authentication_failure = true;
                            break;
                        default:
                            if ((error.code >= 12000 && error.code < 13000) || (error.code >= 14000 && error.code < 15000)) {
                                index_failure = true;
                            } else if (error.code >= 4000 && error.code < 5000) {
                                planning_failure = true;
                            } else if (error.code == 5000 &&
                                       error.message.find("Limit for number of indexes that can be created per scope has been reached") !=
                                         std::string::npos) {
                                quota_limited = true;
                            }
                            break;
                    }
                }
            }
            if (syntax_error) {
                response.ctx.ec = error::common_errc::parsing_failure;
            } else if (invalid_argument) {
                response.ctx.ec = error::common_errc::invalid_argument;
            } else if (server_timeout) {
                response.ctx.ec = error::common_errc::unambiguous_timeout;
            } else if (prepared_statement_failure) {
                response.ctx.ec = error::query_errc::prepared_statement_failure;
            } else if (index_failure) {
                response.ctx.ec = error::query_errc::index_failure;
            } else if (planning_failure) {
                response.ctx.ec = error::query_errc::planning_failure;
            } else if (index_not_found) {
                response.ctx.ec = error::common_errc::index_not_found;
            } else if (cas_mismatch) {
                response.ctx.ec = error::common_errc::cas_mismatch;
            } else if (dml_failure) {
                response.ctx.ec = error::query_errc::dml_failure;
            } else if (authentication_failure) {
                response.ctx.ec = error::common_errc::authentication_failure;
            } else if (rate_limited) {
                response.ctx.ec = error::common_errc::rate_limited;
            } else if (quota_limited) {
                response.ctx.ec = error::common_errc::quota_limited;
            } else {
                LOG_TRACE("Unexpected error returned by query engine: client_context_id=\"{}\", body={}",
                          response.ctx.client_context_id,
                          encoded.body);
                response.ctx.ec = error::common_errc::internal_server_failure;
            }
        }
    }
    return response;
}
} // namespace couchbase::operations
