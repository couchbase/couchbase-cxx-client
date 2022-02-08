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

#include <couchbase/protocol/cmd_get_cluster_config.hxx>

#include <couchbase/logger/logger.hxx>
#include <couchbase/topology/configuration_json.hxx>
#include <couchbase/utils/json.hxx>

#include <gsl/assert>

namespace couchbase::protocol
{
topology::configuration
parse_config(const std::string& input, std::string_view endpoint_address, uint16_t endpoint_port)
{
    auto config = utils::json::parse(input).as<topology::configuration>();
    for (auto& node : config.nodes) {
        if (node.hostname == "$HOST") {
            node.hostname = endpoint_address;
        }
    }

    // workaround for servers which don't specify this_node
    {
        bool has_this_node = false;
        for (auto& node : config.nodes) {
            if (node.this_node) {
                has_this_node = true;
                break;
            }
        }

        if (!has_this_node) {
            for (auto& node : config.nodes) {
                auto kv_port = node.port_or(couchbase::service_type::key_value, false, 0);
                auto kv_tls_port = node.port_or(couchbase::service_type::key_value, true, 0);
                if (node.hostname == endpoint_address && (kv_port == endpoint_port || kv_tls_port == endpoint_port)) {
                    node.this_node = true;
                    break;
                }
            }
        }
    }

    return config;
}

bool
get_cluster_config_response_body::parse(protocol::status status,
                                        const header_buffer& header,
                                        std::uint8_t framing_extras_size,
                                        std::uint16_t key_size,
                                        std::uint8_t extras_size,
                                        const std::vector<uint8_t>& body,
                                        const cmd_info& info)
{
    Expects(header[1] == static_cast<uint8_t>(opcode));
    if (status == protocol::status::success) {
        std::vector<uint8_t>::difference_type offset = framing_extras_size + key_size + extras_size;
        try {
            config_ = parse_config(std::string(body.begin() + offset, body.end()), info.endpoint_address, info.endpoint_port);
        } catch (const tao::pegtl::parse_error& e) {
            LOG_DEBUG("unable to parse cluster configuration as JSON: {}, {}", e.message(), std::string(body.begin(), body.end()));
        }
        return true;
    }
    return false;
}
} // namespace couchbase::protocol
