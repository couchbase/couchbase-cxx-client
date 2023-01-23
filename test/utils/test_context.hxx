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

#pragma once

#include "server_version.hxx"

#include "core/origin.hxx"

#include <string>

namespace test::utils
{
struct test_context {
    std::string connection_string{ "couchbase://127.0.0.1" };
    std::string username{ "Administrator" };
    std::string password{ "password" };
    std::string certificate_path{};
    std::string key_path{};
    std::string bucket{ "default" };
    server_version version{ 6, 6, 0 };
    deployment_type deployment{ deployment_type::on_prem };
    server_config_profile profile{ server_config_profile::unknown };
    std::optional<std::string> dns_nameserver{};
    std::optional<std::uint16_t> dns_port{};
    std::size_t number_of_io_threads{ 1 };
    std::string other_bucket{ "secBucket" };

    [[nodiscard]] couchbase::core::cluster_credentials build_auth() const
    {
        couchbase::core::cluster_credentials auth{};
        if (certificate_path.empty()) {
            auth.username = username;
            auth.password = password;
        } else {
            auth.certificate_path = certificate_path;
            auth.key_path = key_path;
        }
        return auth;
    }

    static test_context load_from_environment();
};

} // namespace test::utils
