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

#include "bucket_update.hxx"

#include "core/management/bucket_settings.hxx"
#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"
#include "error_utils.hxx"

#include <couchbase/durability_level.hxx>

namespace couchbase::core::operations::management
{
std::error_code
bucket_update_request::encode_to(encoded_request_type& encoded, http_context& /* context */) const
{
    encoded.method = "POST";
    encoded.path = fmt::format("/pools/default/buckets/{}", bucket.name);

    encoded.headers["content-type"] = "application/x-www-form-urlencoded";
    encoded.body.append(fmt::format("&ramQuotaMB={}", bucket.ram_quota_mb));
    encoded.body.append(fmt::format("&replicaNumber={}", bucket.num_replicas));
    if (bucket.max_expiry > 0) {
        encoded.body.append(fmt::format("&maxTTL={}", bucket.max_expiry));
    }
    encoded.body.append(fmt::format("&replicaIndex={}", bucket.replica_indexes ? "1" : "0"));
    encoded.body.append(fmt::format("&flushEnabled={}", bucket.flush_enabled ? "1" : "0"));
    switch (bucket.eviction_policy) {
        case couchbase::core::management::cluster::bucket_eviction_policy::full:
            encoded.body.append("&evictionPolicy=fullEviction");
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::value_only:
            encoded.body.append("&evictionPolicy=valueOnly");
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::no_eviction:
            encoded.body.append("&evictionPolicy=noEviction");
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::not_recently_used:
            encoded.body.append("&evictionPolicy=nruEviction");
            break;
        case couchbase::core::management::cluster::bucket_eviction_policy::unknown:
            break;
    }
    switch (bucket.compression_mode) {
        case couchbase::core::management::cluster::bucket_compression::off:
            encoded.body.append("&compressionMode=off");
            break;
        case couchbase::core::management::cluster::bucket_compression::active:
            encoded.body.append("&compressionMode=active");
            break;
        case couchbase::core::management::cluster::bucket_compression::passive:
            encoded.body.append("&compressionMode=passive");
            break;
        case couchbase::core::management::cluster::bucket_compression::unknown:
            break;
    }
    if (bucket.minimum_durability_level) {
        switch (bucket.minimum_durability_level.value()) {
            case durability_level::none:
                encoded.body.append("&durabilityMinLevel=none");
                break;
            case durability_level::majority:
                encoded.body.append("&durabilityMinLevel=majority");
                break;
            case durability_level::majority_and_persist_to_active:
                encoded.body.append("&durabilityMinLevel=majorityAndPersistActive");
                break;
            case durability_level::persist_to_majority:
                encoded.body.append("&durabilityMinLevel=persistToMajority");
                break;
        }
    }
    return {};
}

bucket_update_response
bucket_update_request::make_response(error_context::http&& ctx, const encoded_response_type& encoded) const
{
    bucket_update_response response{ std::move(ctx) };
    if (!response.ctx.ec) {
        switch (encoded.status_code) {
            case 404:
                response.ctx.ec = errc::common::bucket_not_found;
                break;
            case 400: {
                tao::json::value payload{};
                try {
                    payload = utils::json::parse(encoded.body.data());
                } catch (const tao::pegtl::parse_error&) {
                    response.ctx.ec = errc::common::parsing_failure;
                    return response;
                }
                response.ctx.ec = errc::common::invalid_argument;
                auto* errors = payload.find("errors");
                if (errors != nullptr) {
                    std::vector<std::string> error_list{};
                    for (const auto& [field, message] : errors->get_object()) {
                        error_list.emplace_back(message.get_string());
                    }
                    if (!error_list.empty()) {
                        response.error_message = utils::join_strings(error_list, ". ");
                    }
                }
            } break;
            case 200:
            case 202:
                break;
            default:
                response.ctx.ec = extract_common_error_code(encoded.status_code, encoded.body.data());
                break;
        }
    }
    return response;
}
} // namespace couchbase::core::operations::management
