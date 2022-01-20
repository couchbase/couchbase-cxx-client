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

#include <couchbase/operations/management/scope_get_all.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/topology/collections_manifest_json.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
scope_get_all_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = fmt::format("/pools/default/buckets/{}/scopes", bucket_name);
    return {};
}

scope_get_all_response
scope_get_all_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    scope_get_all_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 400:
                response.ctx.ec = error::common_errc::unsupported_operation;
                break;
            case 404:
                response.ctx.ec = error::common_errc::bucket_not_found;
                break;
            case 200:
                try {
                    response.manifest = utils::json::parse(encoded.body.data()).as<topology::collections_manifest>();
                } catch (const tao::pegtl::parse_error&) {
                    response.ctx.ec = error::common_errc::parsing_failure;
                    return response;
                }
                break;
            default:
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
                break;
        }
    }
    return response;
}
} // namespace couchbase::operations::management
