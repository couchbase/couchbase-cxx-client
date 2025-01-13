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

#include <core/meta/version.hxx>

#include <spdlog/fmt/bundled/format.h>
#include <tao/json.hpp>

namespace
{
class version_app : public CLI::App
{
public:
  version_app()
    : CLI::App("Display version information.", "version")
  {
    add_flag("--json", "Dump version and build info in JSON format.");
  }

  void execute() const
  {
    if (count("--json") > 0) {
      tao::json::value info;
      for (const auto& [name, value] : couchbase::core::meta::sdk_build_info()) {
        if (name == "version_major" || name == "version_minor" || name == "version_patch" ||
            name == "version_build" || name == "mozilla_ca_bundle_size") {
          info[name] = std::stoi(value);
        } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl" ||
                   name == "static_target" || name == "static_boringssl" || name == "columnar" ||
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
  }
};
} // namespace

namespace cbc
{
auto
make_version_command() -> std::shared_ptr<CLI::App>
{
  return std::make_shared<version_app>();
}

auto
execute_version_command(const CLI::App* app) -> int
{
  if (const auto* version = dynamic_cast<const version_app*>(app); version != nullptr) {
    version->execute();
    return 0;
  }
  return 1;
}
} // namespace cbc
