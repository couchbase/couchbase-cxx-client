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

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/operations/management/eventing_deploy_function.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
eventing_deploy_function_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "POST";
    encoded.path = fmt::format("/api/v1/functions/{}/deploy", name);
    return {};
}

eventing_deploy_function_response
eventing_deploy_function_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    eventing_deploy_function_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.body.data().empty()) {
            return response;
        }
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        auto [ec, problem] = extract_eventing_error_code(payload);
        if (ec) {
            response.ctx.ec = ec;
            response.error.emplace(problem);
        }
    }
    return response;
}
} // namespace couchbase::operations::management
