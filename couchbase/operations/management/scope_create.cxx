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

#include <couchbase/operations/management/scope_create.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>
#include <couchbase/utils/url_codec.hxx>

#include <regex>

namespace couchbase::operations::management
{
std::error_code
scope_create_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "POST";
    encoded.path = fmt::format("/pools/default/buckets/{}/scopes", bucket_name);
    encoded.headers["content-type"] = "application/x-www-form-urlencoded";
    encoded.body = fmt::format("name={}", utils::string_codec::form_encode(scope_name));
    return {};
}

scope_create_response
scope_create_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    scope_create_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 400: {
                std::regex scope_exists("Scope with name .+ already exists");
                if (std::regex_search(encoded.body.data(), scope_exists)) {
                    response.ctx.ec = error::management_errc::scope_exists;
                } else if (encoded.body.data().find("Not allowed on this version of cluster") != std::string::npos) {
                    response.ctx.ec = error::common_errc::feature_not_available;
                } else {
                    response.ctx.ec = error::common_errc::invalid_argument;
                }
            } break;
            case 404:
                response.ctx.ec = error::common_errc::bucket_not_found;
                break;
            case 200: {
                tao::json::value payload{};
                try {
                    payload = utils::json::parse(encoded.body.data());
                } catch (const tao::pegtl::parse_error&) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                response.uid = std::stoull(payload.at("uid").get_string(), nullptr, 16);
            } break;
            default:
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
                break;
        }
    }
    return response;
}
} // namespace couchbase::operations::management
