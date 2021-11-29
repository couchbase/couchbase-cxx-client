/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include <couchbase/operations/management/cluster_describe.hxx>

#include <couchbase/errors.hxx>

#include <couchbase/utils/json.hxx>

namespace couchbase::operations::management
{
std::error_code
cluster_describe_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = "/pools/default";
    return {};
}

cluster_describe_response
cluster_describe_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    cluster_describe_response response{ std::move(ctx) };
    if (!response.ctx.ec && encoded.status_code != 200) {
        response.ctx.ec = error::common_errc::internal_server_failure;
    }
    if (response.ctx.ec) {
        return response;
    }

    auto payload = utils::json::parse(encoded.body);

    if (auto* nodes = payload.find("nodes"); nodes != nullptr && nodes->is_array()) {
        for (auto& node : nodes->get_array()) {
            cluster_info::node entry{};
            entry.hostname = node.at("hostname").get_string();
            entry.otp_node = node.at("otpNode").get_string();
            entry.uuid = node.at("nodeUUID").get_string();
            entry.version = node.at("version").get_string();
            entry.os = node.at("os").get_string();
            entry.status = node.at("status").get_string();
            response.info.nodes.emplace_back(entry);
        }
    }
    if (auto* buckets = payload.find("bucketNames"); buckets != nullptr && buckets->is_array()) {
        for (auto& node : buckets->get_array()) {
            cluster_info::bucket entry{};
            entry.name = node.at("bucketName").get_string();
            entry.uuid = node.at("uuid").get_string();
            response.info.buckets.emplace_back(entry);
        }
    }

    return response;
}
} // namespace couchbase::operations::management
