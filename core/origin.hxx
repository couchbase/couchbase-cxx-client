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

#include "cluster_options.hxx"

#include <string>
#include <utility>
#include <vector>

namespace couchbase::core
{
namespace utils
{
struct connection_string;
} // namespace utils

struct cluster_credentials {
    std::string username{};
    std::string password{};
    std::string certificate_path{};
    std::string key_path{};
    std::optional<std::vector<std::string>> allowed_sasl_mechanisms{};

    [[nodiscard]] bool uses_certificate() const;
};

namespace topology
{
struct configuration;
}

struct origin {
    using node_entry = std::pair<std::string, std::string>;
    using node_list = std::vector<node_entry>;

    origin() = default;
    ~origin() = default;

    origin(origin&& other) = default;
    origin(const origin& other);
    origin(const origin& other, const topology::configuration& config);
    origin(cluster_credentials auth, const std::string& hostname, std::uint16_t port, cluster_options options);
    origin(cluster_credentials auth, const std::string& hostname, const std::string& port, cluster_options options);
    origin(cluster_credentials auth, const utils::connection_string& connstr);
    origin& operator=(origin&& other) = default;
    origin& operator=(const origin& other);

    [[nodiscard]] const std::string& username() const;
    [[nodiscard]] const std::string& password() const;
    [[nodiscard]] const std::string& certificate_path() const;
    [[nodiscard]] const std::string& key_path() const;

    [[nodiscard]] std::vector<std::string> get_hostnames() const;
    [[nodiscard]] std::vector<std::string> get_nodes() const;

    void set_nodes(node_list nodes);
    void set_nodes_from_config(const topology::configuration& config);

    [[nodiscard]] std::pair<std::string, std::string> next_address();

    [[nodiscard]] bool exhausted() const;

    void restart();

    [[nodiscard]] const couchbase::core::cluster_options& options() const;
    [[nodiscard]] couchbase::core::cluster_options& options();
    [[nodiscard]] const couchbase::core::cluster_credentials& credentials() const;
    [[nodiscard]] auto to_json() const -> std::string;

  private:
    couchbase::core::cluster_options options_{};
    cluster_credentials credentials_{};
    node_list nodes_{};
    node_list::iterator next_node_{};
    bool exhausted_{ false };
};

} // namespace couchbase::core
