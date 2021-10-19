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

#include <couchbase/operations/management/bucket_get_all.hxx>

#include <couchbase/errors.hxx>

#include <couchbase/utils/json.hxx>
#include <couchbase/operations/management/bucket_settings_json.hxx>

namespace couchbase::operations::management
{
std::error_code
bucket_get_all_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = "/pools/default/buckets";
    return {};
}

bucket_get_all_response
bucket_get_all_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    bucket_get_all_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body);
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        const auto& entries = payload.get_array();
        response.buckets.reserve(entries.size());
        for (const auto& entry : entries) {
            response.buckets.emplace_back(entry.as<bucket_settings>());
        }
    }
    return response;
}
} // namespace couchbase::operations::management
