/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "version.hxx"

#include "utils.hxx"

#include <core/meta/version.hxx>

#include <tao/json.hpp>

namespace cbc
{
static constexpr auto* USAGE =
  R"(Display version information.

Usage:
  cbc version [options]
  cbc version (-h|--help)

Options:
  -h --help  Show this screen.
  --json     Dump version and build info in JSON format.
)";

void
cbc::version::execute(const std::vector<std::string>& argv)
{
    try {
        auto options = cbc::parse_options(USAGE, argv);
        if (options["--help"].asBool()) {
            fmt::print(stdout, USAGE);
            return;
        }
        if (options["--json"].asBool()) {
            tao::json::value info;
            for (const auto& [name, value] : couchbase::core::meta::sdk_build_info()) {
                if (name == "version_major" || name == "version_minor" || name == "version_patch" || name == "version_build" ||
                    name == "mozilla_ca_bundle_size") {
                    info[name] = std::stoi(value);
                } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl" ||
                           name == "mozilla_ca_bundle_embedded") {
                    info[name] = value == "true";
                } else {
                    info[name] = value;
                }
            }
            fmt::print(stdout, "{}\n", tao::json::to_string(info, 2));
            return;
        }
        fmt::print(stdout, "Version: {}\n", couchbase::core::meta::sdk_semver());
        auto info = couchbase::core::meta::sdk_build_info();
        fmt::print(stdout, "Build date: {}\n", info["build_timestamp"]);
        fmt::print(stdout, "Build type: {}\n", info["cmake_build_type"]);
        fmt::print(stdout, "Platform: {}, {}\n", info["platform"], info["cpu"]);
        fmt::print(stdout, "C compiler: {}\n", info["cc"]);
        fmt::print(stdout, "C++ compiler: {}\n", info["cxx"]);
        fmt::print(stdout, "CMake: {}\n", info["cmake_version"]);
        fmt::print(stdout, "ASIO: {}\n", info["asio"]);
        fmt::print(stdout, "Snappy: {}\n", info["snappy"]);
        fmt::print(stdout, "OpenSSL:\n");
        fmt::print(stdout, "  headers: {}\n", info["openssl_headers"]);
        fmt::print(stdout, "  runtime: {}\n", info["openssl_runtime"]);
        fmt::print(stdout, "  default certificate directory: {}\n", info["openssl_default_cert_dir"]);
        fmt::print(stdout, "  default certificate file: {}\n", info["openssl_default_cert_file"]);
    } catch (const docopt::DocoptArgumentError& e) {
        fmt::print(stderr, "Error: {}\n", e.what());
    }
}
} // namespace cbc
