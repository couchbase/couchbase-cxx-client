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

#include <couchbase/management/analytics_link_couchbase_remote.hxx>

#include <couchbase/errors.hxx>
#include <couchbase/utils/url_codec.hxx>

#include <algorithm>

namespace couchbase::management::analytics
{
std::error_code
couchbase_remote_link::validate() const
{
    if (dataverse.empty() || link_name.empty() || hostname.empty()) {
        return error::common_errc::invalid_argument;
    }
    switch (encryption.level) {
        case couchbase_link_encryption_level::none:
        case couchbase_link_encryption_level::half:
            if (/* username and password must be provided */ username.has_value() && password.has_value() &&
                /* and client certificate and key must be empty */
                (!encryption.client_certificate.has_value() && !encryption.client_key.has_value())) {

                return {};
            }
            return error::common_errc::invalid_argument;

        case couchbase_link_encryption_level::full:
            if (/* certificate must be provided and */ encryption.certificate.has_value() &&
                (/* either username/password must be set */ (username.has_value() && password.has_value() &&
                                                             !encryption.client_certificate.has_value() &&
                                                             !encryption.client_key.has_value()) ||
                 /* or client certificate/key must be set */ (!username.has_value() && !password.has_value() &&
                                                              encryption.client_certificate.has_value() &&
                                                              encryption.client_key.has_value()))) {
                return {};
            }
            return error::common_errc::invalid_argument;
    }
    return {};
}

std::string
couchbase_remote_link::encode() const
{
    std::map<std::string, std::string> values{
        { "type", "couchbase" },
        { "hostname", hostname },
        { "encryption", to_string(encryption.level) },
    };
    if (std::count(dataverse.begin(), dataverse.end(), '/') == 0) {
        values["dataverse"] = dataverse;
        values["name"] = link_name;
    }
    if (username) {
        values["username"] = username.value();
    }
    if (password) {
        values["password"] = password.value();
    }
    if (encryption.certificate) {
        values["certificate"] = encryption.certificate.value();
    }
    if (encryption.client_certificate) {
        values["clientCertificate"] = encryption.client_certificate.value();
    }
    if (encryption.client_key) {
        values["clientKey"] = encryption.client_key.value();
    }
    return utils::string_codec::v2::form_encode(values);
}

std::string
to_string(couchbase_link_encryption_level level)
{
    switch (level) {
        case couchbase_link_encryption_level::none:
            return "none";

        case couchbase_link_encryption_level::half:
            return "half";

        case couchbase_link_encryption_level::full:
            return "full";
    }
    return "none";
}
} // namespace couchbase::management::analytics
