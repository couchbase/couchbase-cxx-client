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

#include <couchbase/operations/management/group_get_all.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/management/rbac_json.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
group_get_all_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = fmt::format("/settings/rbac/groups");
    encoded.headers["content-type"] = "application/x-www-form-urlencoded";
    return {};
}

group_get_all_response
group_get_all_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    group_get_all_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code != 200) {
            response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            return response;
        }
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        for (const auto& entry : payload.get_array()) {
            response.groups.emplace_back(entry.as<couchbase::management::rbac::group>());
        }
    }
    return response;
}
} // namespace couchbase::operations::management
