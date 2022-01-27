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

#include <couchbase/operations/management/query_index_get_all.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
query_index_get_all_request::encode_to(encoded_request_type& encoded, couchbase::http_context& /* context */) const
{
    encoded.headers["content-type"] = "application/json";
    tao::json::value body{
        { "statement",
          fmt::format(
            R"(SELECT idx.* FROM system:indexes AS idx WHERE ((keyspace_id = "{}" AND bucket_id IS MISSING) OR (bucket_id = "{}")) AND `using`="gsi" ORDER BY is_primary DESC, name ASC)",
            bucket_name,
            bucket_name) },
        { "client_context_id", encoded.client_context_id }
    };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_get_all_response
query_index_get_all_request::make_response(couchbase::error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_get_all_response response{ std::move(ctx) };
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
        response.status = payload.at("status").get_string();
        if (response.status != "success") {
            return response;
        }
        for (const auto& entry : payload.at("results").get_array()) {
            query_index_get_all_response::query_index index;
            index.id = entry.at("id").get_string();
            index.datastore_id = entry.at("datastore_id").get_string();
            index.namespace_id = entry.at("namespace_id").get_string();
            index.keyspace_id = entry.at("keyspace_id").get_string();
            index.type = entry.at("using").get_string();
            index.name = entry.at("name").get_string();
            index.state = entry.at("state").get_string();
            if (const auto* prop = entry.find("bucket_id")) {
                index.bucket_id = prop->get_string();
            }
            if (const auto* prop = entry.find("scope_id")) {
                index.scope_id = prop->get_string();
            }
            if (const auto* prop = entry.find("is_primary")) {
                index.is_primary = prop->get_boolean();
            }
            if (const auto* prop = entry.find("condition")) {
                index.condition = prop->get_string();
            }
            if (const auto* prop = entry.find("partition")) {
                index.partition = prop->get_string();
            }
            for (const auto& key : entry.at("index_key").get_array()) {
                index.index_key.emplace_back(key.get_string());
            }
            response.indexes.emplace_back(index);
        }
    }
    return response;
}
} // namespace couchbase::operations::management
