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

#include "core/timeout_defaults.hxx"

#include <asio/ip/address.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

namespace couchbase::core::io::dns
{
class dns_config
{
  public:
    static constexpr auto default_nameserver = "8.8.8.8";
    static constexpr std::uint16_t default_port = 53;

    dns_config() = default;

    dns_config(const std::string& nameserver, std::uint16_t port, std::chrono::milliseconds timeout = timeout_defaults::dns_srv_timeout)
      : nameserver_{ nameserver }
      , port_{ port }
      , timeout_{ timeout }
    {
    }

    [[nodiscard]] std::uint16_t port() const
    {
        return port_;
    }

    [[nodiscard]] const std::string& nameserver() const
    {
        return nameserver_;
    }

    [[nodiscard]] std::chrono::milliseconds timeout() const
    {
        return timeout_;
    }

    static const dns_config& system_config();

  private:
    std::string nameserver_{ default_nameserver };
    std::uint16_t port_{ default_port };
    std::chrono::milliseconds timeout_{ timeout_defaults::dns_srv_timeout };
};
} // namespace couchbase::core::io::dns
