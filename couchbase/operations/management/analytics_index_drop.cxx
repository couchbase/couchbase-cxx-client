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

#include <couchbase/operations/management/analytics_index_drop.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>
#include <couchbase/utils/name_codec.hxx>

namespace couchbase::operations::management
{
std::error_code
analytics_index_drop_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    std::string if_exists_clause = ignore_if_does_not_exist ? "IF EXISTS" : "";

    tao::json::value body{
        { "statement",
          fmt::format(
            "DROP INDEX {}.`{}`.`{}` {}", utils::analytics::uncompound_name(dataverse_name), dataset_name, index_name, if_exists_clause) },
    };
    encoded.headers["content-type"] = "application/json";
    encoded.method = "POST";
    encoded.path = "/analytics/service";
    encoded.body = utils::json::generate(body);
    return {};
}

analytics_index_drop_response
analytics_index_drop_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    analytics_index_drop_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        response.status = payload.optional<std::string>("status").value_or("unknown");

        if (response.status != "success") {
            bool index_does_not_exist = false;
            bool dataset_not_found = false;

            if (auto* errors = payload.find("errors"); errors != nullptr && errors->is_array()) {
                for (const auto& error : errors->get_array()) {
                    analytics_problem err{
                        error.at("code").as<std::uint32_t>(),
                        error.at("msg").get_string(),
                    };
                    switch (err.code) {
                        case 24047: /* Cannot find index with name [string] */
                            index_does_not_exist = true;
                            break;
                        case 24025: /* Cannot find dataset with name [string] in dataverse [string] */
                            dataset_not_found = true;
                            break;
                    }
                    response.errors.emplace_back(err);
                }
            }
            if (index_does_not_exist) {
                response.ctx.ec = error::common_errc::index_not_found;
            } else if (dataset_not_found) {
                response.ctx.ec = error::analytics_errc::dataset_not_found;
            } else {
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            }
        }
    }
    return response;
}
} // namespace couchbase::operations::management
