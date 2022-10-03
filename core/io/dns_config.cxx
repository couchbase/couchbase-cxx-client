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

#include "dns_config.hxx"

#include <mutex>

namespace couchbase::core::io::dns
{
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
            return line.substr(offset, space);
        }
    }
    return {};
}

static std::once_flag system_config_initialized_flag;

const dns_config&
dns_config::system_config()
{
    static dns_config instance{};

    std::call_once(system_config_initialized_flag, []() {
        auto nameserver = load_resolv_conf(default_resolv_conf_path);
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
