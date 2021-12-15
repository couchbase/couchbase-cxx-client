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

#include <map>
#include <optional>
#include <set>
#include <vector>
#include <string>

#include <spdlog/fmt/fmt.h>

#include <couchbase/topology/capabilities.hxx>
#include <couchbase/platform/uuid.h>
#include <couchbase/service_type.hxx>
#include <couchbase/utils/crc32.hxx>
#include <couchbase/utils/join_strings.hxx>

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
        return epoch < other.epoch && rev < other.rev;
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

    std::pair<std::uint16_t, std::int16_t> map_key(const std::string& key);
};

configuration
make_blank_configuration(const std::string& hostname, std::uint16_t plain_port, std::uint16_t tls_port);
} // namespace couchbase::topology

template<>
struct fmt::formatter<couchbase::topology::configuration::node> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::topology::configuration::node& node, FormatContext& ctx)
    {
        std::vector<std::string> plain;
        if (node.services_plain.key_value) {
            plain.push_back(fmt::format("kv={}", *node.services_plain.key_value));
        }
        if (node.services_plain.management) {
            plain.push_back(fmt::format("mgmt={}", *node.services_plain.management));
        }
        if (node.services_plain.analytics) {
            plain.push_back(fmt::format("cbas={}", *node.services_plain.analytics));
        }
        if (node.services_plain.search) {
            plain.push_back(fmt::format("fts={}", *node.services_plain.search));
        }
        if (node.services_plain.query) {
            plain.push_back(fmt::format("n1ql={}", *node.services_plain.query));
        }
        if (node.services_plain.views) {
            plain.push_back(fmt::format("capi={}", *node.services_plain.views));
        }
        std::vector<std::string> tls;
        if (node.services_tls.key_value) {
            tls.push_back(fmt::format("kv={}", *node.services_tls.key_value));
        }
        if (node.services_tls.management) {
            tls.push_back(fmt::format("mgmt={}", *node.services_tls.management));
        }
        if (node.services_tls.analytics) {
            tls.push_back(fmt::format("cbas={}", *node.services_tls.analytics));
        }
        if (node.services_tls.search) {
            tls.push_back(fmt::format("fts={}", *node.services_tls.search));
        }
        if (node.services_tls.query) {
            tls.push_back(fmt::format("n1ql={}", *node.services_tls.query));
        }
        if (node.services_tls.views) {
            tls.push_back(fmt::format("capi={}", *node.services_tls.views));
        }
        std::vector<std::string> alternate_addresses{};
        if (!node.alt.empty()) {
            alternate_addresses.reserve(node.alt.size());
            for (const auto& entry : node.alt) {
                std::string network = fmt::format(R"(name="{}", host="{}")", entry.second.name, entry.second.hostname);
                {
                    std::vector<std::string> ports;
                    if (entry.second.services_plain.key_value) {
                        ports.push_back(fmt::format("kv={}", *entry.second.services_plain.key_value));
                    }
                    if (entry.second.services_plain.management) {
                        ports.push_back(fmt::format("mgmt={}", *entry.second.services_plain.management));
                    }
                    if (entry.second.services_plain.analytics) {
                        ports.push_back(fmt::format("cbas={}", *entry.second.services_plain.analytics));
                    }
                    if (entry.second.services_plain.search) {
                        ports.push_back(fmt::format("fts={}", *entry.second.services_plain.search));
                    }
                    if (entry.second.services_plain.query) {
                        ports.push_back(fmt::format("n1ql={}", *entry.second.services_plain.query));
                    }
                    if (entry.second.services_plain.views) {
                        ports.push_back(fmt::format("capi={}", *entry.second.services_plain.views));
                    }
                    if (!ports.empty()) {
                        network += fmt::format(", plain=({})", couchbase::utils::join_strings(ports, ","));
                    }
                }
                {
                    std::vector<std::string> ports;
                    if (entry.second.services_tls.key_value) {
                        ports.push_back(fmt::format("kv={}", *entry.second.services_tls.key_value));
                    }
                    if (entry.second.services_tls.management) {
                        ports.push_back(fmt::format("mgmt={}", *entry.second.services_tls.management));
                    }
                    if (entry.second.services_tls.analytics) {
                        ports.push_back(fmt::format("cbas={}", *entry.second.services_tls.analytics));
                    }
                    if (entry.second.services_tls.search) {
                        ports.push_back(fmt::format("fts={}", *entry.second.services_tls.search));
                    }
                    if (entry.second.services_tls.query) {
                        ports.push_back(fmt::format("n1ql={}", *entry.second.services_tls.query));
                    }
                    if (entry.second.services_tls.views) {
                        ports.push_back(fmt::format("capi={}", *entry.second.services_tls.views));
                    }
                    if (!ports.empty()) {
                        network += fmt::format(", tls=({})", couchbase::utils::join_strings(ports, ","));
                    }
                }
                alternate_addresses.emplace_back(network);
            }
        }
        format_to(ctx.out(),
                  R"(#<node:{} hostname="{}", plain=({}), tls=({}), alt=[{}]>)",
                  node.index,
                  node.hostname,
                  couchbase::utils::join_strings(plain, ", "),
                  couchbase::utils::join_strings(tls, ", "),
                  couchbase::utils::join_strings(alternate_addresses, ", "));
        return formatter<std::string>::format("", ctx);
    }
};

template<>
struct fmt::formatter<couchbase::topology::configuration> : formatter<std::string> {
    template<typename FormatContext>
    auto format(const couchbase::topology::configuration& config, FormatContext& ctx)
    {
        format_to(ctx.out(),
                  R"(#<config:{} rev={}{}{}{}{}, nodes({})=[{}], bucket_caps=[{}], cluster_caps=[{}]>)",
                  couchbase::uuid::to_string(config.id),
                  config.rev_str(),
                  config.uuid ? fmt::format(", uuid={}", *config.uuid) : "",
                  config.bucket ? fmt::format(", bucket={}", *config.bucket) : "",
                  config.num_replicas ? fmt::format(", replicas={}", *config.num_replicas) : "",
                  config.vbmap.has_value() ? fmt::format(", partitions={}", config.vbmap->size()) : "",
                  config.nodes.size(),
                  fmt::join(config.nodes, ", "),
                  fmt::join(config.bucket_capabilities, ", "),
                  fmt::join(config.cluster_capabilities, ", "));
        return formatter<std::string>::format("", ctx);
    }
};
