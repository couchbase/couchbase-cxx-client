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

#include <couchbase/operations/management/query_index_drop.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/operations/management/error_utils.hxx>
#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
query_index_drop_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    if ((scope_name.empty() && !collection_name.empty()) || (!scope_name.empty() && collection_name.empty())) {
        return error::common_errc::invalid_argument;
    }
    encoded.headers["content-type"] = "application/json";
    std::string keyspace = fmt::format("`{}`", bucket_name);
    if (!scope_name.empty()) {
        keyspace += ".`" + scope_name + "`";
    }
    if (!collection_name.empty()) {
        keyspace += ".`" + collection_name + "`";
    }

    std::string drop_index_stmt;
    if (!scope_name.empty() || !collection_name.empty()) {
        drop_index_stmt = fmt::format(R"(DROP INDEX `{}` ON {} USING GSI)", index_name, keyspace);
    } else {
        drop_index_stmt = fmt::format(R"(DROP INDEX {}.`{}` USING GSI)", keyspace, index_name);
    }
    tao::json::value body{ { "statement", is_primary ? fmt::format(R"(DROP PRIMARY INDEX ON {} USING GSI)", keyspace) : drop_index_stmt },
                           { "client_context_id", encoded.client_context_id } };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_drop_response
query_index_drop_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_drop_response response{ std::move(ctx) };
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
            bool bucket_not_found = false;
            bool index_not_found = false;
            bool collection_not_found = false;
            bool scope_not_found = false;
            std::optional<std::error_code> common_ec{};
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_drop_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                switch (error.code) {
                    case 5000: /* IKey: "Internal Error" */
                        if (error.message.find("not found.") != std::string::npos) {
                            index_not_found = true;
                        }
                        break;

                    case 12003: /* IKey: "datastore.couchbase.keyspace_not_found" */
                        if (error.message.find("missing_collection") != std::string::npos) {
                            collection_not_found = true;
                        } else {
                            bucket_not_found = true;
                        }
                        break;

                    case 12021:
                        scope_not_found = true;
                        break;

                    case 12004: /* IKey: "datastore.couchbase.primary_idx_not_found" */
                    case 12016: /* IKey: "datastore.couchbase.index_not_found" */
                        index_not_found = true;
                        break;

                    default:
                        common_ec = management::extract_common_query_error_code(error.code, error.message);
                        break;
                }
                response.errors.emplace_back(error);
            }
            if (index_not_found) {
                if (!ignore_if_does_not_exist) {
                    response.ctx.ec = error::common_errc::index_not_found;
                }
            } else if (bucket_not_found) {
                response.ctx.ec = error::common_errc::bucket_not_found;
            } else if (collection_not_found) {
                response.ctx.ec = error::common_errc::collection_not_found;
            } else if (scope_not_found) {
                response.ctx.ec = error::common_errc::scope_not_found;
            } else if (common_ec) {
                response.ctx.ec = common_ec.value();
            } else if (!response.errors.empty()) {
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            }
        }
    }
    return response;
}
} // namespace couchbase::operations::management
