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

#include "core/transactions.hxx"

#include "core/cluster.hxx"

#include <couchbase/cluster.hxx>

namespace couchbase
{

auto
cluster::transactions() -> std::shared_ptr<couchbase::transactions::transactions>
{
    // TODO: add mutex for thread safety.
    if (!transactions_) {
        // TODO: fill in the cluster config, add an optional transactions_config, use it here.
        transactions_ = std::make_shared<couchbase::core::transactions::transactions>(core_, core_->origin().second.options().transactions);
    }
    return transactions_;
}

auto
cluster::close() -> void
{
    if (transactions_) {
        // blocks until cleanup is finished
        transactions_->close();
    }
    transactions_.reset();
    return core::impl::initiate_cluster_close(core_);
}

namespace core::impl
{

static auto
options_to_origin(const std::string& connection_string, const couchbase::cluster_options& options) -> core::origin
{
    auto opts = options.build();

    couchbase::core::cluster_credentials auth;
    auth.username = std::move(opts.username);
    auth.password = std::move(opts.password);
    auth.certificate_path = std::move(opts.certificate_path);
    auth.key_path = std::move(opts.key_path);
    auth.allowed_sasl_mechanisms = std::move(opts.allowed_sasl_mechanisms);

    core::cluster_options user_options;

    if (opts.default_retry_strategy != nullptr) {
        user_options.default_retry_strategy_ = std::move(opts.default_retry_strategy);
    }
    user_options.bootstrap_timeout = opts.timeouts.bootstrap_timeout;
    user_options.resolve_timeout = opts.timeouts.resolve_timeout;
    user_options.connect_timeout = opts.timeouts.connect_timeout;
    user_options.key_value_timeout = opts.timeouts.key_value_timeout;
    user_options.key_value_durable_timeout = opts.timeouts.key_value_durable_timeout;
    user_options.view_timeout = opts.timeouts.view_timeout;
    user_options.query_timeout = opts.timeouts.query_timeout;
    user_options.analytics_timeout = opts.timeouts.analytics_timeout;
    user_options.search_timeout = opts.timeouts.search_timeout;
    user_options.management_timeout = opts.timeouts.management_timeout;

    user_options.enable_tls = opts.security.enabled;
    if (opts.security.enabled) {
        if (opts.security.trust_certificate.has_value()) {
            user_options.trust_certificate = opts.security.trust_certificate.value();
        }
        switch (opts.security.tls_verify) {
            case couchbase::tls_verify_mode::none:
                user_options.tls_verify = core::tls_verify_mode::none;
                break;
            case couchbase::tls_verify_mode::peer:
                user_options.tls_verify = core::tls_verify_mode::peer;
                break;
        }
        user_options.disable_mozilla_ca_certificates = opts.security.disable_mozilla_ca_certificates;
    }

    if (opts.dns.nameserver) {
        user_options.dns_config =
          io::dns::dns_config(opts.dns.nameserver.value(), opts.dns.port.value_or(io::dns::dns_config::default_port), opts.dns.timeout);
    }
    user_options.enable_clustermap_notification = opts.behavior.enable_clustermap_notification;
    user_options.show_queries = opts.behavior.show_queries;
    user_options.dump_configuration = opts.behavior.dump_configuration;
    user_options.enable_mutation_tokens = opts.behavior.enable_mutation_tokens;
    user_options.enable_unordered_execution = opts.behavior.enable_unordered_execution;
    user_options.user_agent_extra = opts.behavior.user_agent_extra;

    user_options.enable_tcp_keep_alive = opts.network.enable_tcp_keep_alive;
    user_options.tcp_keep_alive_interval = opts.network.tcp_keep_alive_interval;
    user_options.config_poll_interval = opts.network.config_poll_interval;
    user_options.idle_http_connection_timeout = opts.network.idle_http_connection_timeout;
    if (opts.network.max_http_connections) {
        user_options.max_http_connections = opts.network.max_http_connections.value();
    }
    if (!opts.network.network.empty()) {
        user_options.network = opts.network.network;
    }
    switch (opts.network.ip_protocol) {
        case ip_protocol::any:
            user_options.use_ip_protocol = core::io::ip_protocol::any;
            break;
        case ip_protocol::force_ipv4:
            user_options.use_ip_protocol = core::io::ip_protocol::force_ipv4;
            break;
        case ip_protocol::force_ipv6:
            user_options.use_ip_protocol = core::io::ip_protocol::force_ipv6;
            break;
    }

    user_options.enable_compression = opts.compression.enabled;

    user_options.enable_metrics = opts.metrics.enabled;
    if (opts.metrics.enabled) {
        user_options.meter = opts.metrics.meter;
        user_options.metrics_options.emit_interval = opts.metrics.emit_interval;
    }

    user_options.enable_tracing = opts.tracing.enabled;
    if (opts.tracing.enabled) {
        user_options.tracer = opts.tracing.tracer;
        user_options.tracing_options.orphaned_emit_interval = opts.tracing.orphaned_emit_interval;
        user_options.tracing_options.orphaned_sample_size = opts.tracing.orphaned_sample_size;

        user_options.tracing_options.threshold_emit_interval = opts.tracing.threshold_emit_interval;
        user_options.tracing_options.threshold_sample_size = opts.tracing.threshold_sample_size;
        user_options.tracing_options.key_value_threshold = opts.tracing.key_value_threshold;
        user_options.tracing_options.query_threshold = opts.tracing.query_threshold;
        user_options.tracing_options.view_threshold = opts.tracing.view_threshold;
        user_options.tracing_options.search_threshold = opts.tracing.search_threshold;
        user_options.tracing_options.analytics_threshold = opts.tracing.analytics_threshold;
        user_options.tracing_options.management_threshold = opts.tracing.management_threshold;
        user_options.tracing_options.eventing_threshold = opts.tracing.eventing_threshold;
    }
    user_options.transactions = opts.transactions;
    // connection string might override some user options
    return { auth, couchbase::core::utils::parse_connection_string(connection_string, user_options) };
}

void
initiate_cluster_connect(asio::io_service& io,
                         const std::string& connection_string,
                         const couchbase::cluster_options& options,
                         cluster_connect_handler&& handler)
{
    auto core = couchbase::core::cluster::create(io);
    auto origin = options_to_origin(connection_string, options);
    core->open(origin, [core, handler = std::move(handler)](std::error_code ec) mutable {
        if (ec) {
            return handler({}, ec);
        }
        auto c = couchbase::cluster(core);
        // create txns as we want to start cleanup immediately if configured with metadata_collection
        [[maybe_unused]] std::shared_ptr<couchbase::transactions::transactions> t = c.transactions();
        handler(std::move(c), {});
    });
}

void
initiate_cluster_close(std::shared_ptr<couchbase::core::cluster> core)
{
    if (core == nullptr) {
        return;
    }
    core->close([]() { /* do nothing */ });
}
} // namespace core::impl
} // namespace couchbase
