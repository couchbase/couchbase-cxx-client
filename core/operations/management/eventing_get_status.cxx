/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include "eventing_get_status.hxx"
#include "core/management/eventing_status_json.hxx"
#include "core/utils/json.hxx"
#include "error_utils.hxx"

namespace couchbase::core::operations::management
{
std::error_code
eventing_get_status_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = "/api/v1/status";
    return {};
}

eventing_get_status_response
eventing_get_status_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    eventing_get_status_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = errc::common::parsing_failure;
            return response;
        }
        auto [ec, problem] = extract_eventing_error_code(payload);
        if (ec) {
            response.ctx.ec = ec;
            response.error.emplace(problem);
            return response;
        }
        response.status = payload.as<couchbase::core::management::eventing::status>();
    }
    return response;
}
} // namespace couchbase::core::operations::management
