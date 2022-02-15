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

#include <couchbase/platform/uuid.h>
#include <couchbase/service_type.hxx>
#include <couchbase/topology/capabilities.hxx>
#include <couchbase/utils/crc32.hxx>

#include <fmt/core.h>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace couchbase::topology
{
struct configuration {
    enum class node_locator_type {
        unknown,
        vbucket,
        ketama,
    };

    struct port_map {
        std::optional<std::uint16_t> key_value{};
        std::optional<std::uint16_t> management{};
        std::optional<std::uint16_t> analytics{};
        std::optional<std::uint16_t> search{};
        std::optional<std::uint16_t> views{};
        std::optional<std::uint16_t> query{};
        std::optional<std::uint16_t> eventing{};
    };

    struct alternate_address {
        std::string name{};
        std::string hostname{};
        port_map services_plain{};
        port_map services_tls{};
    };

    struct node {
        bool this_node{ false };
        size_t index{};
        std::string hostname{};
        port_map services_plain{};
        port_map services_tls{};
        std::map<std::string, alternate_address> alt{};

        [[nodiscard]] std::uint16_t port_or(service_type type, bool is_tls, std::uint16_t default_value) const;

        [[nodiscard]] std::uint16_t port_or(const std::string& network, service_type type, bool is_tls, std::uint16_t default_value) const;

        [[nodiscard]] const std::string& hostname_for(const std::string& network) const;
    };

    [[nodiscard]] std::string select_network(const std::string& bootstrap_hostname) const;

    using vbucket_map = typename std::vector<std::vector<std::int16_t>>;

    std::optional<std::int64_t> epoch{};
    std::optional<std::int64_t> rev{};
    couchbase::uuid::uuid_t id{};
    std::optional<std::uint32_t> num_replicas{};
    std::vector<node> nodes{};
    std::optional<std::string> uuid{};
    std::optional<std::string> bucket{};
    std::optional<vbucket_map> vbmap{};
    std::optional<std::uint64_t> collections_manifest_uid{};
    std::set<bucket_capability> bucket_capabilities{};
    std::set<cluster_capability> cluster_capabilities{};
    node_locator_type node_locator{ node_locator_type::unknown };

    bool operator==(const configuration& other) const
    {
        return epoch == other.epoch && rev == other.rev;
    }

    bool operator<(const configuration& other) const
    {
        return epoch < other.epoch || (epoch == other.epoch && rev < other.rev);
    }

    bool operator>(const configuration& other) const
    {
        return other < *this;
    }

    [[nodiscard]] std::string rev_str() const;

    [[nodiscard]] bool supports_enhanced_prepared_statements() const
    {
        return cluster_capabilities.find(cluster_capability::n1ql_enhanced_prepared_statements) != cluster_capabilities.end();
    }

    [[nodiscard]] std::size_t index_for_this_node() const;
    [[nodiscard]] bool has_node_with_hostname(const std::string& hostname) const;

    std::pair<std::uint16_t, std::int16_t> map_key(const std::string& key);
};

configuration
make_blank_configuration(const std::string& hostname, std::uint16_t plain_port, std::uint16_t tls_port);
} // namespace couchbase::topology
