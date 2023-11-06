/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "core/mozilla_ca_bundle.hxx"
#include "core/transactions/forward_compat.hxx"
#include "core/utils/join_strings.hxx"
#include "core/utils/json.hxx"

#include <couchbase/build_config.hxx>
#include <couchbase/build_info.hxx>
#include <couchbase/build_version.hxx>

#include <asio/version.hpp>
#include <fmt/core.h>
#include <hdr/hdr_histogram_version.h>
#include <llhttp.h>
#include <openssl/crypto.h>
#include <openssl/x509.h>
#include <snappy-stubs-public.h>
#include <spdlog/version.h>

#include <regex>

namespace couchbase::core::meta
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
    info["semver"] = sdk_semver();
    auto txns_forward_compat = core::transactions::forward_compat_supported{};
    info["txns_forward_compat_protocol_version"] =
      fmt::format("{}.{}", txns_forward_compat.protocol_major, txns_forward_compat.protocol_minor);
    info["txns_forward_compat_extensions"] = utils::join_strings(txns_forward_compat.extensions, ",");
    info["platform"] = COUCHBASE_CXX_CLIENT_SYSTEM;
    info["platform_name"] = COUCHBASE_CXX_CLIENT_SYSTEM_NAME;
    info["platform_version"] = COUCHBASE_CXX_CLIENT_SYSTEM_VERSION;
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
#if defined(COUCHBASE_CXX_CLIENT_STATIC_STDLIB)
      "true"
#else
      "false"
#endif
      ;
    info["post_linked_openssl"] = COUCHBASE_CXX_CLIENT_POST_LINKED_OPENSSL;
    info["static_openssl"] =
#if defined(COUCHBASE_CXX_CLIENT_STATIC_OPENSSL)
      "true"
#else
      "false"
#endif
      ;
    info["static_boringssl"] =
#if defined(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
      "true"
#else
      "false"
#endif
      ;
#if defined(COUCHBASE_CXX_CLIENT_BORINGSSL_SHA)
    info["boringssl_sha"] = COUCHBASE_CXX_CLIENT_BORINGSSL_SHA;
#endif
    info["spdlog"] = fmt::format("{}.{}.{}", SPDLOG_VER_MAJOR, SPDLOG_VER_MINOR, SPDLOG_VER_PATCH);
    info["fmt"] = fmt::format("{}.{}.{}", FMT_VERSION / 10'000, FMT_VERSION / 100 % 1000, FMT_VERSION % 100);
    info["asio"] = fmt::format("{}.{}.{}", ASIO_VERSION / 100'000, ASIO_VERSION / 100 % 1000, ASIO_VERSION % 100);
    info["snappy"] = fmt::format("{}.{}.{}", SNAPPY_MAJOR, SNAPPY_MINOR, SNAPPY_PATCHLEVEL);
    info["llhttp"] = fmt::format("{}.{}.{}", LLHTTP_VERSION_MAJOR, LLHTTP_VERSION_MINOR, LLHTTP_VERSION_PATCH);
    info["hdr_histogram_c"] = HDR_HISTOGRAM_VERSION;
    info["openssl_headers"] = OPENSSL_VERSION_TEXT;
#if defined(OPENSSL_VERSION)
    info["openssl_runtime"] = OpenSSL_version(OPENSSL_VERSION);
#elif defined(SSLEAY_VERSION)
    info["openssl_runtime"] = SSLeay_version(SSLEAY_VERSION);
#endif
#if defined(OPENSSL_INFO_CONFIG_DIR)
    info["openssl_config_dir"] = OPENSSL_info(OPENSSL_INFO_CONFIG_DIR);
#elif defined(OPENSSL_DIR)
    if (std::string config_dir(OpenSSL_version(OPENSSL_DIR)); !config_dir.empty()) {
        if (auto quote = config_dir.find('"'); quote != std::string::npos && quote + 2 < config_dir.size()) {
            info["openssl_config_dir"] = config_dir.substr(quote + 1, config_dir.size() - quote - 2);
        } else {
            info["openssl_config_dir"] = config_dir;
        }
    }
#endif

#if defined(COUCHBASE_CXX_CLIENT_EMBED_MOZILLA_CA_BUNDLE)
    info["mozilla_ca_bundle_embedded"] = "true";
    info["mozilla_ca_bundle_sha256"] = COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_SHA256;
    info["mozilla_ca_bundle_date"] = COUCHBASE_CXX_CLIENT_MOZILLA_CA_BUNDLE_DATE;
#else
    info["mozilla_ca_bundle_embedded"] = "false";
#endif
    info["mozilla_ca_bundle_size"] = std::to_string(default_ca::mozilla_ca_certs().size());
    info["openssl_default_cert_dir"] = X509_get_default_cert_dir();
    info["openssl_default_cert_file"] = X509_get_default_cert_file();
    info["openssl_default_cert_dir_env"] = X509_get_default_cert_dir_env();
    info["openssl_default_cert_file_env"] = X509_get_default_cert_file_env();
    info["openssl_ssl_interface_include_directories"] = OPENSSL_SSL_INTERFACE_INCLUDE_DIRECTORIES;
    info["openssl_ssl_interface_link_libraries"] = OPENSSL_SSL_INTERFACE_LINK_LIBRARIES;
    info["openssl_ssl_imported_location"] = OPENSSL_SSL_IMPORTED_LOCATION;
    info["openssl_crypto_interface_imported_location"] = OPENSSL_CRYPTO_IMPORTED_LOCATION;
    info["openssl_crypto_interface_include_directories"] = OPENSSL_CRYPTO_INTERFACE_INCLUDE_DIRECTORIES;
    info["openssl_crypto_interface_link_libraries"] = OPENSSL_CRYPTO_INTERFACE_LINK_LIBRARIES;
    info["openssl_pkg_config_interface_include_directories"] = OPENSSL_PKG_CONFIG_INTERFACE_INCLUDE_DIRECTORIES;
    info["openssl_pkg_config_interface_link_libraries"] = OPENSSL_PKG_CONFIG_INTERFACE_LINK_LIBRARIES;
    info["__cplusplus"] = fmt::format("{}", __cplusplus);
#if defined(_MSC_VER)
    info["_MSC_VER"] = fmt::format("{}", _MSC_VER);
#endif
#if defined(__GLIBC__)
    info["libc"] = fmt::format("glibc {}.{}", __GLIBC__, __GLIBC_MINOR__);
#endif

    return info;
}

std::string
sdk_build_info_json()
{
    tao::json::value info;
    for (const auto& [name, value] : sdk_build_info()) {
        if (name == "version_major" || name == "version_minor" || name == "version_patch" || name == "version_build" ||
            name == "mozilla_ca_bundle_size") {
            info[name] = std::stoi(value);
        } else if (name == "snapshot" || name == "static_stdlib" || name == "static_openssl" || name == "mozilla_ca_bundle_embedded") {
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
    static const std::string identifier{ sdk_version() + ";" + COUCHBASE_CXX_CLIENT_SYSTEM_NAME + "/" +
                                         COUCHBASE_CXX_CLIENT_SYSTEM_PROCESSOR };
    return identifier;
}

std::string
parse_git_describe_output(const std::string& git_describe_output)
{
    if (git_describe_output.empty() || git_describe_output == "unknown") {
        return "";
    }

    static const std::regex version_regex(R"(^(\d+(?:\.\d+){2})(?:-(\w+(?:\.\w+)*))?(-(\d+)-g(\w+))?$)");
    std::smatch match;
    if (std::regex_match(git_describe_output, match, version_regex)) {
        auto version_core = match[1].str();
        auto pre_release = match[2].str();
        auto number_of_commits{ 0 };
        if (match[4].matched) {
            number_of_commits = std::stoi(match[4].str());
        }
        if (auto build = match[5].str(); !build.empty() && number_of_commits > 0) {
            if (pre_release.empty()) {
                return fmt::format("{}+{}.{}", version_core, number_of_commits, build);
            }
            return fmt::format("{}-{}+{}.{}", version_core, pre_release, number_of_commits, build);
        }
        if (pre_release.empty()) {
            return fmt::format("{}", version_core);
        }
        return fmt::format("{}-{}", version_core, pre_release);
    }

    return "";
}

const std::string&
sdk_semver()
{
    static const std::string simple_version{ std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MAJOR) + "." +
                                             std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MINOR) + "." +
                                             std::to_string(COUCHBASE_CXX_CLIENT_VERSION_PATCH) + "+" +
                                             std::string(COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT) };
    static const std::string git_describe_output{ COUCHBASE_CXX_CLIENT_GIT_DESCRIBE };
    static const std::string semantic_version = parse_git_describe_output(git_describe_output);
    if (semantic_version.empty()) {
        return simple_version;
    }
    return semantic_version;
}

const std::string&
sdk_version()
{
    static const std::string version{ sdk_version_short() + "/" + COUCHBASE_CXX_CLIENT_GIT_REVISION_SHORT };
    return version;
}

const std::string&
sdk_version_short()
{
    static const std::string version{ std::string("cxx/") + std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MAJOR) + "." +
                                      std::to_string(COUCHBASE_CXX_CLIENT_VERSION_MINOR) + "." +
                                      std::to_string(COUCHBASE_CXX_CLIENT_VERSION_PATCH) };
    return version;
}

const std::string&
os()
{
    static const std::string system{ COUCHBASE_CXX_CLIENT_SYSTEM };
    return system;
}

constexpr const char* ssl_lib_id =
#if defined(COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL)
  "bssl"
#else
  "ssl"
#endif
  ;

std::string
user_agent_for_http(const std::string& client_id, const std::string& session_id, const std::string& extra)
{
    auto user_agent = fmt::format("{};{}/0x{:x};client/{};session/{};{}",
                                  couchbase::core::meta::sdk_id(),
                                  ssl_lib_id,
                                  OpenSSL_version_num(),
                                  client_id,
                                  session_id,
                                  couchbase::core::meta::os());
    if (!extra.empty()) {
        user_agent.append(";").append(extra);
    }
    for (auto& ch : user_agent) {
        if (ch == '\n' || ch == '\r') {
            ch = ' ';
        }
    }
    return user_agent;
}

std::string
user_agent_for_mcbp(const std::string& client_id, const std::string& session_id, const std::string& extra, std::size_t max_length)
{
    tao::json::value user_agent{
        { "i", fmt::format("{}/{}", client_id, session_id) },
    };
    const std::string core_id = fmt::format("{};{}/0x{:x}", couchbase::core::meta::sdk_id(), ssl_lib_id, OpenSSL_version_num());
    std::string sdk_id = core_id;
    if (!extra.empty()) {
        sdk_id.append(";").append(extra);
    }
    if (max_length > 0) {
        auto current_length = utils::json::generate(user_agent).size();
        auto allowed_length = max_length - current_length;
        auto sdk_id_length = utils::json::generate(tao::json::value{ { "a", sdk_id } }).size() -
                             1 /* object adds "{}" (braces), but eventually we only need "," (comma) */;
        if (sdk_id_length > allowed_length) {
            auto escaped_characters = sdk_id_length - sdk_id.size();
            if (escaped_characters >= allowed_length) {
                /* user-provided string is too weird, lets just fall back to just core */
                sdk_id = core_id;
            } else {
                sdk_id.erase(allowed_length - escaped_characters);
            }
        }
    }
    user_agent["a"] = sdk_id;
    return utils::json::generate(user_agent);
}
} // namespace couchbase::core::meta
