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

#include "search_index_control_query.hxx"

#include "core/utils/json.hxx"
#include "error_utils.hxx"

namespace couchbase::core::operations::management
{
std::error_code
search_index_control_query_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    if (index_name.empty()) {
        return errc::common::invalid_argument;
    }
    encoded.method = "POST";
    encoded.path = fmt::format("/api/index/{}/queryControl/{}", index_name, allow ? "allow" : "disallow");
    return {};
}

search_index_control_query_response
search_index_control_query_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    search_index_control_query_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = errc::common::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            if (response.status == "ok") {
                return response;
            }
        } else if (encoded.status_code == 400) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = errc::common::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ctx.ec = errc::common::index_not_found;
                return response;
            }
        }
        response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
    }
    return response;
}
} // namespace couchbase::core::operations::management
