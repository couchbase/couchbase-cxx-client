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

#include <spdlog/fmt/bundled/core.h>

#include <asio/version.hpp>
#include <http_parser.h>
#include <openssl/crypto.h>
#include <snappy-stubs-public.h>
#include <spdlog/version.h>

#include <couchbase/meta/version.hxx>

#include <couchbase/build_version.hxx>
#include <couchbase/build_info.hxx>

#include <couchbase/utils/json.hxx>

namespace couchbase::meta
{
std::map<std::string, std::string>
sdk_build_info()
{
    std::map<std::string, std::string> info{};
    info["build_timestamp"] = COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP;
    info["revision"] = COUCHBASE_CXX_CLIENT_GIT_REVISION;
    info["version_major"] = std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MAJOR);
    info["version_minor"] = std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MINOR);
    info["version_patch"] = std::to_string(COUCHBASE_CXX_CLIENT_VERSION_PATCH);
    info["version_build"] = std::to_string(COUCHBASE_CXX_CLIENT_VERSION_BUILD);
    info["version"] = std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MAJOR) + "." + std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MINOR) + "." +
                      std::to_string(COUCHBASE_CXX_CLIENT_VERSION_PATCH);
#if COUCHBASE_CXX_CLIENT_VERSION_BUILD > 0
    info["version"] += "." + std::to_string(COUCHBASE_CXX_CLIENT_VERSION_BUILD);
    info["snapshot"] = "true";
#else
    info["snapshot"] = "false";
#endif
    info["platform"] = COUCHBASE_CXX_CLIENT_SYSTEM;
    info["cpu"] = COUCHBASE_CXX_CLIENT_SYSTEM_PROCESSOR;
    info["cc"] = COUCHBASE_CXX_CLIENT_C_COMPILER;
    info["cxx"] = COUCHBASE_CXX_CLIENT_CXX_COMPILER;
    info["cmake_version"] = CMAKE_VERSION;
    info["cmake_build_type"] = CMAKE_BUILD_TYPE;
    info["compile_definitions"] = COUCHBASE_CXX_CLIENT_COMPILE_DEFINITIONS;
    info["compile_features"] = COUCHBASE_CXX_CLIENT_COMPILE_FEATURES;
    info["compile_flags"] = COUCHBASE_CXX_CLIENT_COMPILE_FLAGS;
    info["compile_options"] = COUCHBASE_CXX_CLIENT_COMPILE_OPTIONS;
    info["link_depends"] = COUCHBASE_CXX_CLIENT_LINK_DEPENDS;
    info["link_flags"] = COUCHBASE_CXX_CLIENT_LINK_FLAGS;
    info["link_libraries"] = COUCHBASE_CXX_CLIENT_LINK_LIBRARIES;
    info["link_options"] = COUCHBASE_CXX_CLIENT_LINK_OPTIONS;
    info["static_stdlib"] =
#if defined(STATIC_STDLIB)
      "true"
#else
      "false"
#endif
      ;
    info["post_linked_openssl"] = COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL;
    info["static_openssl"] =
#if defined(STATIC_OPENSSL)
      "true"
#else
      "false"
#endif
      ;
    info["spdlog"] = fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR, SPDLOG_VER_PATCH);
    info["fmt"] = fmt::format("{}.{}.{}", FMT_VERSION / 10'000, FMT_VERSION / 100 % 1000, FMT_VERSION % 100);
    info["asio"] = fmt::format("{}.{}.{}", ASIO_VERSION / 100'000, ASIO_VERSION / 100 % 1000, ASIO_VERSION % 100);
    info["snappy"] = fmt::format("{}.{}.{}", SNAPPY_MAJOR, SNAPPY_MINOR, SNAPPY_PATCHLEVEL);
    info["http_parser"] = fmt::format("{}.{}.{}", HTTP_PARSER_VERSION_MAJOR, HTTP_PARSER_VERSION_MINOR, HTTP_PARSER_VERSION_PATCH);
    info["openssl_headers"] = OPENSSL_VERSION_TEXT;
#if defined(OPENSSL_VERSION)
    info["openssl_runtime"] = OpenSSL_version(OPENSSL_VERSION);
#elif defined(SSLEAY_VERSION)
    info["openssl_runtime"] = SSLeay_version(SSLEAY_VERSION);
#endif

    return info;
}

std::string
sdk_build_info_json()
{
    tao::json::value info;
    for (const auto& [name, value] : sdk_build_info()) {
        if (name == "version_major" || name == "version_minor" || name == "version_patch" || name == "version_build") {
            info[name] = std::stoi(value);
        } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl") {
            info[name] = value == "true";
        } else {
            info[name] = value;
        }
    }
    return utils::json::generate(info);
}

std::string
sdk_build_info_short()
{
    return fmt::format(R"(rev="{}", compiler="{}", system="{}", date="{}")",
                       COUCHBASE_CXX_CLIENT_GIT_REVISION,
                       COUCHBASE_CXX_CLIENT_CXX_COMPILER,
                       COUCHBASE_CXX_CLIENT_SYSTEM,
                       COUCHBASE_CXX_CLIENT_BUILD_TIMESTAMP);
}

const std::string&
sdk_id()
{
    static const std::string identifier{ std::string("cxx/") + std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MAJOR) + "." +
                                         std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MINOR) + "." +
                                         std::to_string(COUCHBASE_CXX_CLIENT_VERSION_PATCH) + "/" + COUCHBASE_CXX_CLIENT_GIT_REVISION };
    return identifier;
}

const std::string&
os()
{
    static const std::string system{ COUCHBASE_CXX_CLIENT_SYSTEM };
    return system;
}
} // namespace couchbase::meta
