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

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>

#include <iphlpapi.h>
#include <winsock2.h>

#include "core/utils/join_strings.hxx"
#endif

#include "core/logger/logger.hxx"
#include "dns_config.hxx"
#include <mutex>

namespace couchbase::core::io::dns
{

#ifdef _WIN32
// Reference:  https://learn.microsoft.com/en-us/windows/win32/api/_iphlp/
std::string
load_resolv_conf()
{
    FIXED_INFO* fixed_info;
    ULONG buf;
    DWORD ret;

    fixed_info = (FIXED_INFO*)malloc(sizeof(FIXED_INFO));
    if (fixed_info == NULL) {
        CB_LOG_WARNING("Error allocating memory needed to call GetNetworkParams");
    }
    buf = sizeof(FIXED_INFO);

    // Make an initial call to GetAdaptersInfo to get
    // the necessary size into the ulOutBufLen variable
    if (GetNetworkParams(fixed_info, &buf) == ERROR_BUFFER_OVERFLOW) {
        free(fixed_info);
        fixed_info = (FIXED_INFO*)malloc(buf);
        if (fixed_info == NULL) {
            CB_LOG_WARNING("Error allocating memory needed to call GetNetworkParams");
        }
    }

    auto dns_servers = std::vector<std::string>{};
    ret = GetNetworkParams(fixed_info, &buf);
    if (ret == NO_ERROR) {
        if (fixed_info) {
            auto dns_ip = std::string{ fixed_info->DnsServerList.IpAddress.String };
            if (!dns_ip.empty()) {
                dns_servers.emplace_back(dns_ip);
            }

            auto ip_addr = fixed_info->DnsServerList.Next;
            while (ip_addr) {
                dns_ip = std::string{ ip_addr->IpAddress.String };
                if (!dns_ip.empty()) {
                    dns_servers.emplace_back(dns_ip);
                }
                ip_addr = ip_addr->Next;
            }
        }
    } else {
        CB_LOG_WARNING("GetNetworkParams failed with error: {}", ret);
    }

    if (fixed_info)
        free(fixed_info);

    if (dns_servers.size() > 0) {
        CB_LOG_DEBUG(
          "Found DNS Servers: [{}], using nameserver: {}", couchbase::core::utils::join_strings(dns_servers, ", "), dns_servers[0]);
        return dns_servers[0];
    }
    CB_LOG_WARNING("Unable to find DNS nameserver");
    return {};
}
#else
static constexpr auto default_resolv_conf_path = "/etc/resolv.conf";

std::string
load_resolv_conf(const char* conf_path)
{
    if (std::error_code ec{}; std::filesystem::exists(conf_path, ec) && !ec) {
        std::ifstream conf(conf_path);
        while (conf.good()) {
            std::string line;
            std::getline(conf, line);
            if (line.empty()) {
                continue;
            }
            std::size_t offset = 0;
            while (line[offset] == ' ') {
                ++offset;
            }
            if (line[offset] == '#') {
                continue;
            }
            std::size_t space = line.find(' ', offset);
            if (space == std::string::npos || space == offset || line.size() < space + 2) {
                continue;
            }
            if (std::string keyword = line.substr(offset, space); keyword != "nameserver") {
                continue;
            }
            offset = space + 1;
            space = line.find(' ', offset);
            auto nameserver = line.substr(offset, space);
            CB_LOG_DEBUG("Using nameserver: {}", nameserver);
            return nameserver;
        }
    }
    return {};
}
#endif
static std::once_flag system_config_initialized_flag;

const dns_config&
dns_config::system_config()
{
    static dns_config instance{};

    std::call_once(system_config_initialized_flag, []() {
#ifdef _WIN32
        auto nameserver = load_resolv_conf();
#else
        auto nameserver = load_resolv_conf(default_resolv_conf_path);
#endif
        std::error_code ec;
        asio::ip::address::from_string(nameserver, ec);
        if (ec) {
            nameserver = default_nameserver;
        }
        instance.nameserver_ = nameserver;
    });

    return instance;
}
} // namespace couchbase::core::io::dns
