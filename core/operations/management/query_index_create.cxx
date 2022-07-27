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

#include "query_index_create.hxx"

#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "error_utils.hxx"

namespace couchbase::core::operations::management
{
std::error_code
query_index_create_request::encode_to(encoded_request_type& encoded, http_context&) const
{
    if ((scope_name.empty() && !collection_name.empty()) || (!scope_name.empty() && collection_name.empty())) {
        return errc::common::invalid_argument;
    }
    encoded.headers["content-type"] = "application/json";
    tao::json::value with{};
    if (deferred) {
        with["defer_build"] = *deferred;
    }
    if (num_replicas) {
        with["num_replica"] = *num_replicas; /* no 's' in key name */
    }
    std::string where_clause{};
    if (condition) {
        where_clause = fmt::format("WHERE {}", *condition);
    }
    std::string with_clause{};
    if (with) {
        with_clause = fmt::format("WITH {}", utils::json::generate(with));
    }
    std::string keyspace = fmt::format("{}:`{}`", namespace_id, bucket_name);
    if (!scope_name.empty()) {
        keyspace += ".`" + scope_name + "`";
    }
    if (!collection_name.empty()) {
        keyspace += ".`" + collection_name + "`";
    }
    tao::json::value body{ { "statement",
                             is_primary ? fmt::format(R"(CREATE PRIMARY INDEX {} ON {} USING GSI {})",
                                                      index_name.empty() ? "" : fmt::format("`{}`", index_name),
                                                      keyspace,
                                                      with_clause)
                                        : fmt::format(R"(CREATE INDEX `{}` ON {}({}) {} USING GSI {})",
                                                      index_name,
                                                      keyspace,
                                                      utils::join_strings(fields, ", "),
                                                      where_clause,
                                                      with_clause) },
                           { "client_context_id", encoded.client_context_id } };
    encoded.method = "POST";
    encoded.path = "/query/service";
    encoded.body = utils::json::generate(body);
    return {};
}

query_index_create_response
query_index_create_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    query_index_create_response response{ std::move(ctx) };
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
            bool index_already_exists = false;
            bool bucket_not_found = false;
            bool collection_not_found = false;
            bool scope_not_found = false;
            std::optional<std::error_code> common_ec{};
            for (const auto& entry : payload.at("errors").get_array()) {
                query_index_create_response::query_problem error;
                error.code = entry.at("code").get_unsigned();
                error.message = entry.at("msg").get_string();
                switch (error.code) {
                    case 5000: /* IKey: "Internal Error" */
                        if (error.message.find(" already exists") != std::string::npos) {
                            index_already_exists = true;
                        }
                        if (error.message.find("Bucket Not Found") != std::string::npos) {
                            bucket_not_found = true;
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

                    case 4300: /* IKey: "plan.new_index_already_exists" */
                        index_already_exists = true;
                        break;

                    default:
                        common_ec = management::extract_common_query_error_code(error.code, error.message);
                        break;
                }
                response.errors.emplace_back(error);
            }
            if (index_already_exists) {
                if (!ignore_if_exists) {
                    response.ctx.ec = errc::common::index_exists;
                }
            } else if (bucket_not_found) {
                response.ctx.ec = errc::common::bucket_not_found;
            } else if (collection_not_found) {
                response.ctx.ec = errc::common::collection_not_found;
            } else if (scope_not_found) {
                response.ctx.ec = errc::common::scope_not_found;
            } else if (common_ec) {
                response.ctx.ec = common_ec.value();
            } else if (!response.errors.empty()) {
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
            }
        }
    }
    return response;
}
} // namespace couchbase::core::operations::management
