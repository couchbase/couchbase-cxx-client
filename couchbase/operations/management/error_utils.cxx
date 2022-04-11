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

#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{

std::error_code
extract_common_error_code(std::uint32_t status_code, const std::string& response_body)
{
    if (status_code == 429) {
        if (response_body.find("Limit(s) exceeded") != std::string::npos) {
            return error::common_errc::rate_limited;
        }
        if (response_body.find("Maximum number of collections has been reached for scope") != std::string::npos) {
            return error::common_errc::quota_limited;
        }
    }
    return error::common_errc::internal_server_failure;
}

std::optional<std::error_code>
extract_common_query_error_code(std::uint64_t code, const std::string& message)
{
    switch (code) {
        case 1191: /* ICode: E_SERVICE_USER_REQUEST_EXCEEDED, IKey: "service.requests.exceeded" */
        case 1192: /* ICode: E_SERVICE_USER_REQUEST_RATE_EXCEEDED, IKey: "service.request.rate.exceeded" */
        case 1193: /* ICode: E_SERVICE_USER_REQUEST_SIZE_EXCEEDED, IKey: "service.request.size.exceeded" */
        case 1194: /* ICode: E_SERVICE_USER_RESULT_SIZE_EXCEEDED, IKey: "service.result.size.exceeded" */
            return error::common_errc::rate_limited;

        case 5000:
            if (message.find("Limit for number of indexes that can be created per scope has been reached") != std::string::npos) {
                return error::common_errc::quota_limited;
            }
            break;

        default:
            break;
    }

    return {};
}

std::pair<std::error_code, eventing_problem>
extract_eventing_error_code(const tao::json::value& response)
{
    if (!response.is_object()) {
        return {};
    }
    if (const auto& name = response.find("name"); name != nullptr && name->is_string()) {
        eventing_problem problem{
            response.at("code").get_unsigned(),
            name->get_string(),
            response.at("description").get_string(),
        };
        if (problem.name == "ERR_APP_NOT_FOUND_TS") {
            return { error::management_errc::eventing_function_not_found, problem };
        }
        if (problem.name == "ERR_APP_NOT_DEPLOYED") {
            if (const auto* runtime_info = response.find("runtime_info"); runtime_info != nullptr && runtime_info->is_object()) {
                if (const auto* info = runtime_info->find("info");
                    info != nullptr && info->is_string() && info->get_string().find("already in paused state") != std::string::npos) {
                    return { error::management_errc::eventing_function_paused, problem };
                }
            }
            return { error::management_errc::eventing_function_not_deployed, problem };
        }
        if (problem.name == "ERR_HANDLER_COMPILATION") {
            return { error::management_errc::eventing_function_compilation_failure, problem };
        }
        if (problem.name == "ERR_COLLECTION_MISSING") {
            return { error::common_errc::collection_not_found, problem };
        }
        if (problem.name == "ERR_SRC_MB_SAME") {
            return { error::management_errc::eventing_function_identical_keyspace, problem };
        }
        if (problem.name == "ERR_APP_NOT_BOOTSTRAPPED") {
            return { error::management_errc::eventing_function_not_bootstrapped, problem };
        }
        if (problem.name == "ERR_APP_NOT_UNDEPLOYED" || problem.name == "ERR_APP_ALREADY_DEPLOYED") {
            return { error::management_errc::eventing_function_deployed, problem };
        }
        if (problem.name == "ERR_APP_PAUSED") {
            return { error::management_errc::eventing_function_paused, problem };
        }
        if (problem.name == "ERR_BUCKET_MISSING") {
            return { error::common_errc::bucket_not_found, problem };
        }
        if (problem.name == "ERR_INVALID_CONFIG" || problem.name == "ERR_INTER_FUNCTION_RECURSION") {
            return { error::common_errc::invalid_argument, problem };
        }
        return { error::common_errc::internal_server_failure, problem };
    }
    return {};
}

std::optional<std::error_code>
translate_query_error_code(std::uint64_t error, const std::string& message, std::uint64_t reason)
{
    switch (error) {
        case 5000: /* IKey: "Internal Error" */
            if (message.find(" already exists") != std::string::npos) {
                return error::common_errc::index_exists;
            }
            if (message.find("not found.") != std::string::npos) {
                return error::common_errc::index_not_found;
            }
            if (message.find("Bucket Not Found") != std::string::npos) {
                return error::common_errc::bucket_not_found;
            }
            break;

        case 12003: /* IKey: "datastore.couchbase.keyspace_not_found" */
            return error::common_errc::bucket_not_found;

        case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
        case 12016: /* IKey: "datastore.couchbase.index_not_found" */
            return error::common_errc::index_not_found;

        case 4300: /* IKey: "plan.new_index_already_exists" */
            return error::common_errc::index_exists;

        case 1065: /* IKey: "service.io.request.unrecognized_parameter" */
            return error::common_errc::invalid_argument;

        case 1080: /* IKey: "timeout" */
            return error::common_errc::unambiguous_timeout;

        case 3000: /* IKey: "parse.syntax_error" */
            return error::common_errc::parsing_failure;

        case 4040: /* IKey: "plan.build_prepared.no_such_name" */
        case 4050: /* IKey: "plan.build_prepared.unrecognized_prepared" */
        case 4060: /* IKey: "plan.build_prepared.no_such_name" */
        case 4070: /* IKey: "plan.build_prepared.decoding" */
        case 4080: /* IKey: "plan.build_prepared.name_encoded_plan_mismatch" */
        case 4090: /* IKey: "plan.build_prepared.name_not_in_encoded_plan" */
            return error::query_errc::prepared_statement_failure;

        case 12009: /* IKey: "datastore.couchbase.DML_error" */
            if (message.find("CAS mismatch") != std::string::npos) {
                return error::common_errc::cas_mismatch;
            } else {
                switch (reason) {
                    case 12033:
                        return error::common_errc::cas_mismatch;
                    case 17014:
                        return error::key_value_errc::document_not_found;
                    case 17012:
                        return error::key_value_errc::document_exists;
                    default:
                        return error::query_errc::dml_failure;
                }
            }

        case 13014: /* IKey: "datastore.couchbase.insufficient_credentials" */
            return error::common_errc::authentication_failure;

        default:
            if ((error >= 12000 && error < 13000) || (error >= 14000 && error < 15000)) {
                return error::query_errc::index_failure;
            } else if (error >= 4000 && error < 5000) {
                return error::query_errc::planning_failure;
            }
            break;
    }
    return extract_common_query_error_code(error, message);
}

std::optional<std::error_code>
translate_analytics_error_code(std::uint64_t error, const std::string& /* message */)
{
    switch (error) {
        case 0:
            return {};

        case 21002: /* Request timed out and will be cancelled */
            return error::common_errc::unambiguous_timeout;

        case 24006: /* Link [string] does not exist | Link [string] does not exist */
            return error::analytics_errc::link_not_found;

        case 23007: /* Job queue is full with [string] jobs */
            return error::analytics_errc::job_queue_full;

        case 24044: /* Cannot find dataset [string] because there is no dataverse declared, nor an alias with name [string]! */
        case 24045: /* Cannot find dataset [string] in dataverse [string] nor an alias with name [string]! */
        case 24025: /* Cannot find dataset with name [string] in dataverse [string] */
            return error::analytics_errc::dataset_not_found;

        case 24034: /* Cannot find dataverse with name [string] */
            return error::analytics_errc::dataverse_not_found;

        case 24039: /* A dataverse with this name [string] already exists. */
            return error::analytics_errc::dataverse_exists;

        case 24040: /* A dataset with name [string] already exists in dataverse [string] */
            return error::analytics_errc::dataset_exists;

        case 24047: /* Cannot find index with name [string] */
            return error::common_errc::index_not_found;

        case 24048: /* An index with this name [string] already exists */
            return error::common_errc::index_exists;

        case 24055: /* Link [string] already exists */
            return error::analytics_errc::link_exists;

        default:
            if (error >= 24000 && error < 25000) {
                return error::analytics_errc::compilation_failure;
            }
            break;
    }
    return error::common_errc::internal_server_failure;
}

std::optional<std::error_code>
translate_search_error_code(std::uint32_t status_code, const std::string& response_body)
{
    if (status_code == 400 || status_code == 500) {
        if (response_body.find("no indexName:") != std::string::npos) {
            return error::common_errc::index_not_found;
        }
        tao::json::value payload{};
        try {
            payload = utils::json::parse(response_body);
        } catch (const tao::pegtl::parse_error&) {
            return error::common_errc::parsing_failure;
        }

        const auto& error = payload.at("error").get_string();
        if (error.find("index not found") != std::string::npos) {
            return error::common_errc::index_not_found;
        }
        if (error.find("index with the same name already exists") != std::string::npos) {
            return error::common_errc::index_exists;
        }
        if (error.find("no planPIndexes for indexName") != std::string::npos) {
            return error::search_errc::index_not_ready;
        }
        if (error.find("num_fts_indexes (active + pending)") != std::string::npos) {
            return error::common_errc::quota_limited;
        }
    }
    return {};
}
} // namespace couchbase::operations::management
