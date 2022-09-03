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

#include "query_index_build.hxx"

#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "error_utils.hxx"

namespace couchbase::core::operations::management
{

template<typename Range>
std::string
quote_and_join_strings(const Range& values, const std::string& sep)
{
    std::stringstream stream;
    auto sentinel = std::end(values);
    if (auto it = std::begin(values); it != sentinel) {
        stream << '`' << *it << '`';
        ++it;
        while (it != sentinel) {
            stream << sep << '`' << *it << '`';
            ++it;
        }
    }
    return stream.str();
}

std::error_code
query_index_build_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    if ((scope_name.empty() && !collection_name.empty()) || (!scope_name.empty() && collection_name.empty()) || index_names.empty()) {
        return errc::common::invalid_argument;
    }
    std::string statement;
    if (!scope_name.empty() && !collection_name.empty()) {
        statement = fmt::format(
          R"(BUILD INDEX ON `{}`.`{}`.`{}` ({}))", bucket_name, scope_name, collection_name, quote_and_join_strings(index_names, ","));
    } else {
        statement = fmt::format(R"(BUILD INDEX ON `{}` ({}))", bucket_name, quote_and_join_strings(index_names, ","));
    }
    encoded.headers["content-type"] = "application/json";
    tao::json::value body{ { "statement", statement }, { "client_context_id", encoded.client_context_id } };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_build_response
query_index_build_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_build_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        tao::json::value payload{};
        try {
            payload = utils::json::parse(encoded.body.data());
        } catch (const tao::pegtl::parse_error&) {
            response.ctx.ec = errc::common::parsing_failure;
            return response;
        }
        response.status = payload.at("status").get_string();
        if (response.status != "success") {
            std::optional<std::error_code> common_ec{};
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_build_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                response.errors.emplace_back(error);
                common_ec = management::extract_common_query_error_code(error.code, error.message);
            }

            response.ctx.ec = common_ec.value_or(extract_common_error_code(encoded.status_code, encoded.body.data()));
        }
    }
    return response;
}
} // namespace couchbase::core::operations::management
