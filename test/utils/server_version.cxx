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

#include "server_version.hxx"

#include <regex>

namespace test::utils
{
server_version
server_version::parse(const std::string& str, const deployment_type deployment)
{
    std::regex version_regex(R"((\d+).(\d+).(\d+)(-(\d+))?(-(.+))?)");
    std::smatch version_match{};
    server_version ver{};
    ver.deployment = deployment;
    if (std::regex_match(str, version_match, version_regex) && version_match.ready()) {
        ver.major = std::stoul(version_match[1]);
        ver.minor = std::stoul(version_match[2]);
        if (version_match.length(3) > 0) {
            ver.micro = std::stoul(version_match[3]);
            if (version_match.length(5) > 0) {
                ver.build = std::stoul(version_match[5]);
                if (version_match.length(7) > 0) {
                    if (version_match[7] == "enterprise") {
                        ver.edition = server_edition::enterprise;
                    } else if (version_match[7] == "community") {
                        ver.edition = server_edition::community;
                    }
                }
            }
        }
    } else {
        ver.major = 6;
        ver.minor = 6;
        ver.micro = 0;
    }
    return ver;
}
} // namespace test::utils
