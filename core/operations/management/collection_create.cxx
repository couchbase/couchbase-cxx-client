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

#include "collection_create.hxx"

#include "core/utils/json.hxx"
#include "core/utils/url_codec.hxx"
#include "error_utils.hxx"

#include <regex>

namespace couchbase::core::operations::management
{
std::error_code
collection_create_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "POST";
    encoded.path = fmt::format("/pools/default/buckets/{}/scopes/{}/collections", bucket_name, scope_name);
    encoded.headers["content-type"] = "application/x-www-form-urlencoded";
    encoded.body = fmt::format("name={}", utils::string_codec::form_encode(collection_name));
    if (max_expiry > 0) {
        encoded.body.append(fmt::format("&maxTTL={}", max_expiry));
    }
    return {};
}

collection_create_response
collection_create_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    collection_create_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 400: {
                std::regex collection_exists("Collection with name .+ already exists");
                if (std::regex_search(encoded.body.data(), collection_exists)) {
                    response.ctx.ec = errc::management::collection_exists;
                } else if (encoded.body.data().find("Not allowed on this version of cluster") != std::string::npos) {
                    response.ctx.ec = errc::common::feature_not_available;
                } else {
                    response.ctx.ec = errc::common::invalid_argument;
                }
            } break;
            case 404: {
                std::regex scope_not_found("Scope with name .+ is not found");
                if (std::regex_search(encoded.body.data(), scope_not_found)) {
                    response.ctx.ec = errc::common::scope_not_found;
                } else {
                    response.ctx.ec = errc::common::bucket_not_found;
                }
            } break;
            case 200: {
                tao::json::value payload{};
                try {
                    payload = utils::json::parse(encoded.body.data());
                } catch (const tao::pegtl::parse_error&) {
                    response.ctx.ec = errc::common::parsing_failure;
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
} // namespace couchbase::core::operations::management
