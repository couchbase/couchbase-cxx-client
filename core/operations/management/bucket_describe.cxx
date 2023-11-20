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

#include "bucket_describe.hxx"

#include "core/utils/json.hxx"
#include "error_utils.hxx"

#include <fmt/core.h>

namespace couchbase::core::operations::management
{
std::error_code
bucket_describe_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "GET";
    encoded.path = fmt::format("/pools/default/b/{}", name);
    return {};
}

std::string
normalize_capability(const std::string& capability)
{
    std::string normalized;
    normalized.reserve(capability.size());
    for (auto ch : capability) {
        if (ch != '_') {
            normalized.push_back(static_cast<char>(std::tolower(ch)));
        }
    }
    return normalized;
}

bucket_describe_response
bucket_describe_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    bucket_describe_response response{ std::move(ctx) };
    if (!response.ctx.ec && encoded.status_code != 200) {
        response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
    }
    if (response.ctx.ec) {
        return response;
    }

    auto payload = utils::json::parse(encoded.body.data());

    response.info.name = payload.at("name").get_string();
    response.info.uuid = payload.at("uuid").get_string();
    if (const auto* nodes_ext = payload.find("nodesExt"); nodes_ext != nullptr && nodes_ext->is_array()) {
        response.info.number_of_nodes = nodes_ext->get_array().size();
    }
    if (const auto* vbs_map = payload.find("vBucketServerMap"); vbs_map != nullptr && vbs_map->is_object()) {
        if (const auto* num_replicas = vbs_map->find("numReplicas"); num_replicas != nullptr && num_replicas->is_number()) {
            response.info.number_of_replicas = num_replicas->get_unsigned();
        }
    }
    if (const auto* storage_backend = payload.find("storageBackend"); storage_backend != nullptr && storage_backend->is_string()) {
        if (const auto& str = storage_backend->get_string(); str == "couchstore") {
            response.info.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::couchstore;
        } else if (str == "magma") {
            response.info.storage_backend = couchbase::core::management::cluster::bucket_storage_backend::magma;
        }
    }

    if (const auto* bucket_caps = payload.find("bucketCapabilities"); bucket_caps != nullptr && bucket_caps->is_array()) {
        for (const auto& cap : bucket_caps->get_array()) {
            if (cap.is_string()) {
                response.info.bucket_capabilities.emplace_back(normalize_capability(cap.get_string()));
            }
        }
    }

    return response;
}

auto
bucket_describe_response::bucket_info::has_capability(const std::string& capability) const -> bool
{
    for (const auto& cap : bucket_capabilities) {
        if (cap == normalize_capability(capability)) {
            return true;
        }
    }
    return false;
}
} // namespace couchbase::core::operations::management
