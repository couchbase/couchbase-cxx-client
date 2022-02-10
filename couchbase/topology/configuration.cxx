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

#include <couchbase/topology/configuration.hxx>

#include <couchbase/logger/logger.hxx>
#include <couchbase/service_type_fmt.hxx>

#include <algorithm>
#include <stdexcept>

namespace couchbase::topology
{
std::uint16_t
configuration::node::port_or(service_type type, bool is_tls, std::uint16_t default_value) const
{
    if (is_tls) {
        switch (type) {
            case service_type::query:
                return services_tls.query.value_or(default_value);

            case service_type::analytics:
                return services_tls.analytics.value_or(default_value);

            case service_type::search:
                return services_tls.search.value_or(default_value);

            case service_type::view:
                return services_tls.views.value_or(default_value);

            case service_type::management:
                return services_tls.management.value_or(default_value);

            case service_type::key_value:
                return services_tls.key_value.value_or(default_value);

            case service_type::eventing:
                return services_tls.eventing.value_or(default_value);
        }
    }
    switch (type) {
        case service_type::query:
            return services_plain.query.value_or(default_value);

        case service_type::analytics:
            return services_plain.analytics.value_or(default_value);

        case service_type::search:
            return services_plain.search.value_or(default_value);

        case service_type::view:
            return services_plain.views.value_or(default_value);

        case service_type::management:
            return services_plain.management.value_or(default_value);

        case service_type::key_value:
            return services_plain.key_value.value_or(default_value);

        case service_type::eventing:
            return services_plain.eventing.value_or(default_value);
    }
    return default_value;
}

const std::string&
configuration::node::hostname_for(const std::string& network) const
{
    if (network == "default") {
        return hostname;
    }
    const auto& address = alt.find(network);
    if (address == alt.end()) {
        LOG_WARNING(R"(requested network "{}" is not found, fallback to "default" host)", network);
        return hostname;
    }
    return address->second.hostname;
}

std::uint16_t
configuration::node::port_or(const std::string& network, service_type type, bool is_tls, std::uint16_t default_value) const
{
    if (network == "default") {
        return port_or(type, is_tls, default_value);
    }
    const auto& address = alt.find(network);
    if (address == alt.end()) {
        LOG_WARNING(R"(requested network "{}" is not found, fallback to "default" port of {} service)", network, type);
        return port_or(type, is_tls, default_value);
    }
    if (is_tls) {
        switch (type) {
            case service_type::query:
                return address->second.services_tls.query.value_or(default_value);

            case service_type::analytics:
                return address->second.services_tls.analytics.value_or(default_value);

            case service_type::search:
                return address->second.services_tls.search.value_or(default_value);

            case service_type::view:
                return address->second.services_tls.views.value_or(default_value);

            case service_type::management:
                return address->second.services_tls.management.value_or(default_value);

            case service_type::key_value:
                return address->second.services_tls.key_value.value_or(default_value);

            case service_type::eventing:
                return address->second.services_tls.eventing.value_or(default_value);
        }
    }
    switch (type) {
        case service_type::query:
            return address->second.services_plain.query.value_or(default_value);

        case service_type::analytics:
            return address->second.services_plain.analytics.value_or(default_value);

        case service_type::search:
            return address->second.services_plain.search.value_or(default_value);

        case service_type::view:
            return address->second.services_plain.views.value_or(default_value);

        case service_type::management:
            return address->second.services_plain.management.value_or(default_value);

        case service_type::key_value:
            return address->second.services_plain.key_value.value_or(default_value);

        case service_type::eventing:
            return address->second.services_plain.eventing.value_or(default_value);
    }
    return default_value;
}

bool
configuration::has_node_with_hostname(const std::string& hostname) const
{
    return std::any_of(nodes.begin(), nodes.end(), [&hostname](const auto& n) { return n.hostname == hostname; });
}

std::string
configuration::select_network(const std::string& bootstrap_hostname) const
{
    for (const auto& n : nodes) {
        if (n.this_node) {
            if (n.hostname == bootstrap_hostname) {
                return "default";
            }
            for (const auto& [network, address] : n.alt) {
                if (address.hostname == bootstrap_hostname) {
                    return network;
                }
            }
        }
    }
    return "default";
}

std::string
configuration::rev_str() const
{
    if (epoch) {
        return fmt::format("{}:{}", epoch.value(), rev.value_or(0));
    }
    return rev ? fmt::format("{}", *rev) : "(none)";
}

std::size_t
configuration::index_for_this_node() const
{
    for (const auto& n : nodes) {
        if (n.this_node) {
            return n.index;
        }
    }
    throw std::runtime_error("no nodes marked as this_node");
}

std::pair<std::uint16_t, std::int16_t>
configuration::map_key(const std::string& key)
{
    if (!vbmap.has_value()) {
        throw std::runtime_error("cannot map key: partition map is not available");
    }
    uint32_t crc = utils::hash_crc32(key.data(), key.size());
    auto vbucket = uint16_t(crc % vbmap->size());
    return { vbucket, vbmap->at(vbucket)[0] };
}

configuration
make_blank_configuration(const std::string& hostname, std::uint16_t plain_port, std::uint16_t tls_port)
{
    configuration result;
    result.id = couchbase::uuid::random();
    result.epoch = 0;
    result.rev = 0;
    result.nodes.resize(1);
    result.nodes[0].hostname = hostname;
    result.nodes[0].this_node = true;
    result.nodes[0].services_plain.key_value = plain_port;
    result.nodes[0].services_tls.key_value = tls_port;
    return result;
}
} // namespace couchbase::topology
