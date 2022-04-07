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

#include <couchbase/operations/management/view_index_get.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
view_index_get_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = fmt::format("/{}/_design/{}{}", bucket_name, ns == design_document_namespace::development ? "dev_" : "", document_name);
    return {};
}

view_index_get_response
view_index_get_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    view_index_get_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            response.document.name = document_name;
            response.document.ns = ns;

            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            const auto* views = payload.find("views");
            if (views != nullptr && views->is_object()) {
                for (const auto& [name, view_entry] : views->get_object()) {
                    couchbase::management::views::design_document::view view;
                    view.name = name;
                    if (view_entry.is_object()) {
                        if (const auto* map = view_entry.find("map"); map != nullptr && map->is_string()) {
                            view.map = map->get_string();
                        }
                        if (const auto* reduce = view_entry.find("reduce"); reduce != nullptr && reduce->is_string()) {
                            view.reduce = reduce->get_string();
                        }
                    }
                    response.document.views[view.name] = view;
                }
            }
        } else if (encoded.status_code == 404) {
            response.ctx.ec = error::view_errc::design_document_not_found;
        } else {
            response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
        }
    }
    return response;
}
} // namespace couchbase::operations::management
