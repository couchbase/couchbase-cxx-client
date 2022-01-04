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

std::pair<std::error_code, eventing::problem>
extract_eventing_error_code(const tao::json::value& response)
{
    if (!response.is_object()) {
        return {};
    }
    if (const auto& name = response.find("name"); name != nullptr && name->is_string()) {
        eventing::problem problem{
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

} // namespace couchbase::operations::management
