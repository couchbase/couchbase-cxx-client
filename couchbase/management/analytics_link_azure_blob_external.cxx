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

#include <couchbase/management/analytics_link_azure_blob_external.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/utils/url_codec.hxx>

#include <algorithm>

namespace couchbase::management::analytics
{
std::error_code
azure_blob_external_link::validate() const
{
    if (dataverse.empty() || link_name.empty()) {
        return error::common_errc::invalid_argument;
    }
    if (connection_string.has_value() || (account_name.has_value() && (account_key.has_value() || shared_access_signature.has_value()))) {
        return {};
    }
    return error::common_errc::invalid_argument;
}

std::string
azure_blob_external_link::encode() const
{
    std::map<std::string, std::string> values{
        { "type", "azureblob" },
    };
    if (std::count(dataverse.begin(), dataverse.end(), '/') == 0) {
        values["dataverse"] = dataverse;
        values["name"] = link_name;
    }
    if (connection_string) {
        values["connectionString"] = connection_string.value();
    } else if (account_name) {
        values["accountName"] = account_name.value();
        if (account_key) {
            values["accountKey"] = account_key.value();
        } else if (shared_access_signature) {
            values["sharedAccessSignature"] = shared_access_signature.value();
        }
    }
    if (blob_endpoint) {
        values["blobEndpoint"] = blob_endpoint.value();
    }
    if (endpoint_suffix) {
        values["endpointSuffix"] = endpoint_suffix.value();
    }
    return utils::string_codec::v2::form_encode(values);
}
} // namespace couchbase::management::analytics
