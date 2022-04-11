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

#include <couchbase/operations/management/search_index_upsert.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/management/search_index_json.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
search_index_upsert_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    if (index.name.empty()) {
        return error::common_errc::invalid_argument;
    }
    encoded.method = "PUT";
    encoded.headers["cache-control"] = "no-cache";
    encoded.headers["content-type"] = "application/json";
    encoded.path = fmt::format("/api/index/{}", index.name);
    tao::json::value body{
        { "name", index.name },
        { "type", index.type },
        { "sourceType", index.source_type },
    };
    if (!index.uuid.empty()) {
        body["uuid"] = index.uuid;
    }
    if (!index.params_json.empty()) {
        body["params"] = utils::json::parse(index.params_json);
    }
    if (!index.source_name.empty()) {
        body["sourceName"] = index.source_name;
    }
    if (!index.source_uuid.empty()) {
        body["sourceUUID"] = index.source_uuid;
    }
    if (!index.source_params_json.empty()) {
        body["sourceParams"] = utils::json::parse(index.source_params_json);
    }
    if (!index.plan_params_json.empty()) {
        body["planParams"] = utils::json::parse(index.plan_params_json);
    }
    encoded.body = utils::json::generate(body);
    return {};
}

search_index_upsert_response
search_index_upsert_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    search_index_upsert_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        if (encoded.status_code == 200) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            if (response.status == "ok") {
                return response;
            }
        } else if (encoded.status_code == 400) {
            tao::json::value payload{};
            try {
                payload = utils::json::parse(encoded.body.data());
            } catch (const tao::pegtl::parse_error&) {
                response.ctx.ec = error::common_errc::parsing_failure;
                return response;
            }
            response.status = payload.at("status").get_string();
            response.error = payload.at("error").get_string();
            if (response.error.find("index not found") != std::string::npos) {
                response.ctx.ec = error::common_errc::index_not_found;
                return response;
            }
            if (response.error.find("index with the same name already exists") != std::string::npos) {
                response.ctx.ec = error::common_errc::index_exists;
                return response;
            }
            if (response.error.find("num_fts_indexes (active + pending)") != std::string::npos) {
                response.ctx.ec = error::common_errc::quota_limited;
                return response;
            }
        }
        response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
    }
    return response;
}
} // namespace couchbase::operations::management
