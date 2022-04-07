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

#include <couchbase/operations/management/query_index_build_deferred.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
query_index_build_deferred_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    if ((scope_name.empty() && !collection_name.empty()) || (!scope_name.empty() && collection_name.empty())) {
        return error::common_errc::invalid_argument;
    }
    std::string statement;
    if (!scope_name.empty() && !collection_name.empty()) {
        statement = fmt::format(
          R"(BUILD INDEX ON `{}`.`{}`.`{}` ((SELECT RAW name FROM system:indexes WHERE bucket_id = "{}" AND scope_id = "{}" AND keyspace_id = "{}" AND state = "deferred")))",
          bucket_name,
          scope_name,
          collection_name,
          bucket_name,
          scope_name,
          collection_name);
    } else {
        statement = fmt::format(
          R"(BUILD INDEX ON `{}` ((SELECT RAW name FROM system:indexes WHERE keyspace_id = "{}" AND bucket_id IS MISSING AND state = "deferred")))",
          bucket_name,
          bucket_name);
    }
    encoded.headers["content-type"] = "application/json";
    tao::json::value body{ { "statement", statement }, { "client_context_id", encoded.client_context_id } };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_build_deferred_response
query_index_build_deferred_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_build_deferred_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = error::common_errc::parsing_failure;
            return response;
        }
        response.status = payload.at("status").get_string();
        if (response.status != "success") {
            std::optional<std::error_code> common_ec{};
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_build_deferred_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                response.errors.emplace_back(error);
                common_ec = management::extract_common_query_error_code(error.code, error.message);
            }

            if (common_ec) {
                response.ctx.ec = common_ec.value();
            } else {
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            }
        }
    }
    return response;
}
} // namespace couchbase::operations::management
