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

#include <couchbase/io/ip_protocol.hxx>
#include <couchbase/metrics/logging_meter_options.hxx>
#include <couchbase/service_type.hxx>
#include <couchbase/timeout_defaults.hxx>
#include <couchbase/tracing/threshold_logging_options.hxx>

#include <chrono>
#include <string>

namespace couchbase
{
enum class tls_verify_mode {
    none,
    peer,
};

struct cluster_options {
    std::chrono::milliseconds bootstrap_timeout = timeout_defaults::bootstrap_timeout;
    std::chrono::milliseconds resolve_timeout = timeout_defaults::resolve_timeout;
    std::chrono::milliseconds connect_timeout = timeout_defaults::connect_timeout;
    std::chrono::milliseconds key_value_timeout = timeout_defaults::key_value_timeout;
    std::chrono::milliseconds key_value_durable_timeout = timeout_defaults::key_value_durable_timeout;
    std::chrono::milliseconds view_timeout = timeout_defaults::view_timeout;
    std::chrono::milliseconds query_timeout = timeout_defaults::query_timeout;
    std::chrono::milliseconds analytics_timeout = timeout_defaults::analytics_timeout;
    std::chrono::milliseconds search_timeout = timeout_defaults::search_timeout;
    std::chrono::milliseconds management_timeout = timeout_defaults::management_timeout;
    std::chrono::milliseconds dns_srv_timeout = timeout_defaults::dns_srv_timeout;

    bool enable_tls{ false };
    std::string trust_certificate{};
    bool enable_mutation_tokens{ true };
    bool enable_tcp_keep_alive{ true };
    io::ip_protocol use_ip_protocol{ io::ip_protocol::any };
    bool enable_dns_srv{ true };
    bool show_queries{ false };
    bool enable_unordered_execution{ true };
    bool enable_clustermap_notification{ true };
    bool enable_compression{ true };
    bool enable_tracing{ true };
    bool enable_metrics{ true };
    std::string network{ "auto" };
    tracing::threshold_logging_options tracing_options{};
    metrics::logging_meter_options metrics_options{};
    tls_verify_mode tls_verify{ tls_verify_mode::peer };

    std::chrono::milliseconds tcp_keep_alive_interval = timeout_defaults::tcp_keep_alive_interval;
    std::chrono::milliseconds config_poll_interval = timeout_defaults::config_poll_interval;
    std::chrono::milliseconds config_poll_floor = timeout_defaults::config_poll_floor;
    std::chrono::milliseconds config_idle_redial_timeout = timeout_defaults::config_idle_redial_timeout;

    std::size_t max_http_connections{ 0 };
    std::chrono::milliseconds idle_http_connection_timeout = timeout_defaults::idle_http_connection_timeout;
    std::string user_agent_extra{};

    [[nodiscard]] std::chrono::milliseconds default_timeout_for(service_type type) const;
};

} // namespace couchbase
