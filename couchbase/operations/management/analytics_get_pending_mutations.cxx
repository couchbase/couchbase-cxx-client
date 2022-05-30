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

#include <couchbase/operations/management/analytics_get_pending_mutations.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
analytics_get_pending_mutations_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = "/analytics/node/agg/stats/remaining";
    return {};
}

analytics_get_pending_mutations_response
analytics_get_pending_mutations_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    analytics_get_pending_mutations_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        if (encoded.status_code == 200) {
            if (payload.is_object()) {
                for (const auto& [dataverse, entry] : payload.get_object()) {
                    for (const auto& [dataset, counter] : entry.get_object()) {
                        response.stats.try_emplace(fmt::format("{}.{}", dataverse, dataset), counter.as<std::int64_t>());
                    }
                }
            }
            return response;
        }
        response.status = payload.optional<std::string>("status").value_or("unknown");
        if (auto* errors = payload.find("errors"); errors != nullptr && errors->is_array()) {
            for (const auto& error : errors->get_array()) {
                analytics_problem err{
                    error.at("code").as<std::uint32_t>(),
                    error.at("msg").get_string(),
                };
                response.errors.emplace_back(err);
            }
        }
        response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
    }
    return response;
}
} // namespace couchbase::operations::management
