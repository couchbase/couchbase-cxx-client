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

#include "query_index_get_all_deferred.hxx"

#include "core/utils/json.hxx"
#include "error_utils.hxx"

namespace couchbase::core::operations::management
{
std::error_code
query_index_get_all_deferred_request::encode_to(encoded_request_type& encoded, couchbase::core::http_context& /* context */) const
{
    std::string where;
    if (collection_name.empty()) {
        where = "(keyspace_id = $bucket_name AND bucket_id IS MISSING)";
    } else {
        where = "(bucket_id = $bucket_name AND scope_id = $scope_name AND keyspace_id = $collection_name)";
    }

    std::string query_context = fmt::format("{}:`{}`", namespace_id, bucket_name);
    if (!scope_name.empty()) {
        query_context += ".`" + scope_name + "`";
    } else {
        query_context += fmt::format(".`{}`", couchbase::scope::default_name);
    }

    std::string statement = "SELECT RAW name FROM system:indexes"
                            " WHERE " +
                            where +
                            " AND state = \"deferred\" AND `using` = \"gsi\""
                            " ORDER BY is_primary DESC, name ASC";

    encoded.headers["content-type"] = "application/json";
    tao::json::value body{ { "statement", statement },
                           { "client_context_id", encoded.client_context_id },
                           { "$bucket_name", bucket_name },
                           { "$scope_name", scope_name },
                           { "$collection_name", collection_name },
                           { "query_context", query_context } };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_get_all_deferred_response
query_index_get_all_deferred_request::make_response(couchbase::core::error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_get_all_deferred_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code != 200) {
            response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            return response;
        }
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = errc::common::parsing_failure;
            return response;
        }
        response.status = payload.at("status").get_string();
        if (response.status != "success") {
            return response;
        }
        for (const auto& entry : payload.at("results").get_array()) {
            response.index_names.emplace_back(entry.get_string());
        }
    }
    return response;
}
} // namespace couchbase::core::operations::management
