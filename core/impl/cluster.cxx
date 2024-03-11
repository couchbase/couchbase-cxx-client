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

#include "core/cluster.hxx"

#include "analytics.hxx"
#include "core/agent_group.hxx"
#include "core/transactions.hxx"
#include "core/utils/connection_string.hxx"
#include "diagnostics.hxx"
#include "internal_search_error_context.hxx"
#include "internal_search_meta_data.hxx"
#include "internal_search_result.hxx"
#include "internal_search_row.hxx"
#include "internal_search_row_location.hxx"
#include "internal_search_row_locations.hxx"
#include "query.hxx"
#include "search.hxx"

#include <couchbase/bucket.hxx>
#include <couchbase/cluster.hxx>

namespace couchbase
{
namespace
{
auto
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
        if (opts.security.trust_certificate_value.has_value()) {
            user_options.trust_certificate_value = opts.security.trust_certificate_value.value();
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
        user_options.tls_disable_deprecated_protocols = opts.security.disable_deprecated_protocols;
        user_options.tls_disable_v1_2 = opts.security.disable_tls_v1_2;
    }

    if (opts.dns.nameserver) {
        user_options.dns_config = core::io::dns::dns_config(
          opts.dns.nameserver.value(), opts.dns.port.value_or(core::io::dns::dns_config::default_port), opts.dns.timeout);
    }
    user_options.enable_clustermap_notification = opts.behavior.enable_clustermap_notification;
    user_options.show_queries = opts.behavior.show_queries;
    user_options.dump_configuration = opts.behavior.dump_configuration;
    user_options.enable_mutation_tokens = opts.behavior.enable_mutation_tokens;
    user_options.enable_unordered_execution = opts.behavior.enable_unordered_execution;
    user_options.user_agent_extra = opts.behavior.user_agent_extra;
    user_options.network = opts.behavior.network;

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

} // namespace

class cluster_impl : public std::enable_shared_from_this<cluster_impl>
{
  public:
    explicit cluster_impl(couchbase::core::cluster core)
      : core_{ std::move(core) }
    {
    }

    explicit cluster_impl(couchbase::core::cluster core, std::shared_ptr<couchbase::core::transactions::transactions> transactions)
      : core_{ std::move(core) }
      , transactions_{ transactions }
    {
    }

    void initialize_transactions(std::function<void(std::error_code)>&& handler)
    {
        return core::transactions::transactions::create(
          core_,
          core_.origin().second.options().transactions,
          [self = shared_from_this(), handler = std::move(handler)](auto ec, auto txns) mutable {
              if (ec) {
                  return handler(ec);
              }

              self->transactions_ = txns;
              handler({});
          });
    }

    void query(std::string statement, query_options::built options, query_handler&& handler) const
    {
        return core_.execute(
          core::impl::build_query_request(std::move(statement), {}, std::move(options)),
          [handler = std::move(handler)](auto resp) { return handler(core::impl::build_context(resp), core::impl::build_result(resp)); });
    }

    void analytics_query(std::string statement, analytics_options::built options, analytics_handler&& handler) const
    {
        return core_.execute(
          core::impl::build_analytics_request(std::move(statement), std::move(options), {}, {}),
          [handler = std::move(handler)](auto resp) { return handler(core::impl::build_context(resp), core::impl::build_result(resp)); });
    }

    void search_query(std::string index_name,
                      const class search_query& query,
                      const search_options::built& options,
                      search_handler&& handler) const
    {
        return core_.execute(core::impl::build_search_request(std::move(index_name), query, options, {}, {}),
                             [handler = std::move(handler)](auto resp) mutable {
                                 return handler(search_error_context{ internal_search_error_context{ resp } },
                                                search_result{ internal_search_result{ resp } });
                             });
    }

    void ping(const ping_options::built& options, ping_handler&& handler) const
    {
        return core_.ping(options.report_id,
                          {},
                          core::impl::to_core_service_types(options.service_types),
                          options.timeout,
                          [handler = std::move(handler)](auto resp) mutable { return handler(core::impl::build_result(resp)); });
    };

    void diagnostics(const diagnostics_options::built& options, diagnostics_handler&& handler) const
    {
        return core_.diagnostics(options.report_id,
                                 [handler = std::move(handler)](auto resp) mutable { return handler(core::impl::build_result(resp)); });
    }

    void search(std::string index_name,
                couchbase::search_request request,
                const search_options::built& options,
                search_handler&& handler) const
    {
        return core_.execute(core::impl::build_search_request(std::move(index_name), std::move(request), options, {}, {}),
                             [handler = std::move(handler)](auto resp) mutable {
                                 return handler(search_error_context{ internal_search_error_context{ resp } },
                                                search_result{ internal_search_result{ resp } });
                             });
    }

    void notify_fork(fork_event event)
    {
        if (transactions_) {
            transactions_->notify_fork(event);
        }
    }

    void close(core::utils::movable_function<void()> handler)
    {
        if (transactions_) {
            // blocks until cleanup is finished
            transactions_->close();
        }
        transactions_.reset();
        return core_.close(std::move(handler));
    }

    [[nodiscard]] auto core() const -> const core::cluster&
    {
        return core_;
    }

    [[nodiscard]] auto transactions() const -> std::shared_ptr<couchbase::core::transactions::transactions>
    {
        return transactions_;
    }

  private:
    couchbase::core::cluster core_;
    std::shared_ptr<couchbase::core::transactions::transactions> transactions_;
};

/*
 * This function exists only for usage in the unit tests, and might be removed at any moment.
 * Avoid using it unless it is absolutely necessary.
 */
auto
extract_core_cluster(const couchbase::cluster& cluster) -> const core::cluster&
{
    static_assert(alignof(couchbase::cluster) == alignof(std::shared_ptr<cluster_impl>),
                  "expected alignment of couchbase::cluster and std::shared_ptr<cluster_impl> to match");
    static_assert(sizeof(couchbase::cluster) == sizeof(std::shared_ptr<cluster_impl>),
                  "expected size of couchbase::cluster and std::shared_ptr<cluster_impl> to match");
    return reinterpret_cast<const std::shared_ptr<cluster_impl>*>(&cluster)->get()->core();
}

cluster::cluster(core::cluster core)
  : impl_{ std::make_shared<cluster_impl>(std::move(core)) }
{
}

cluster::cluster(core::cluster core, std::shared_ptr<core::transactions::transactions> transactions)
  : impl_{ std::make_shared<cluster_impl>(std::move(core), transactions) }
{
}

void
cluster::query(std::string statement, const query_options& options, query_handler&& handler) const
{
    return impl_->query(std::move(statement), options.build(), std::move(handler));
}

auto
cluster::query(std::string statement, const query_options& options) const -> std::future<std::pair<query_error_context, query_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<query_error_context, query_result>>>();
    auto future = barrier->get_future();
    query(std::move(statement), options, [barrier](auto ctx, auto result) { barrier->set_value({ std::move(ctx), std::move(result) }); });
    return future;
}

void
cluster::analytics_query(std::string statement, const analytics_options& options, analytics_handler&& handler) const
{
    impl_->analytics_query(std::move(statement), options.build(), std::move(handler));
}

auto
cluster::analytics_query(std::string statement, const analytics_options& options) const
  -> std::future<std::pair<analytics_error_context, analytics_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<analytics_error_context, analytics_result>>>();
    auto future = barrier->get_future();
    analytics_query(std::move(statement), options, [barrier](auto ctx, auto result) {
        barrier->set_value({ std::move(ctx), std::move(result) });
    });
    return future;
}

void
cluster::search_query(std::string index_name,
                      const class search_query& query,
                      const search_options& options,
                      search_handler&& handler) const
{
    return impl_->search_query(std::move(index_name), query, options.build(), std::move(handler));
}

auto
cluster::search_query(std::string index_name, const class search_query& query, const search_options& options) const
  -> std::future<std::pair<search_error_context, search_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<search_error_context, search_result>>>();
    search_query(std::move(index_name), query, options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

void
cluster::ping(const couchbase::ping_options& options, couchbase::ping_handler&& handler) const
{
    return impl_->ping(options.build(), std::move(handler));
}

auto
cluster::ping(const couchbase::ping_options& options) const -> std::future<ping_result>
{
    auto barrier = std::make_shared<std::promise<ping_result>>();
    ping(options, [barrier](auto result) mutable { barrier->set_value(std::move(result)); });
    return barrier->get_future();
}

void
cluster::diagnostics(const couchbase::diagnostics_options& options, couchbase::diagnostics_handler&& handler) const
{
    return impl_->diagnostics(options.build(), std::move(handler));
}

auto
cluster::diagnostics(const couchbase::diagnostics_options& options) const -> std::future<diagnostics_result>
{
    auto barrier = std::make_shared<std::promise<diagnostics_result>>();
    diagnostics(options, [barrier](auto result) mutable { barrier->set_value(std::move(result)); });
    return barrier->get_future();
}

void
cluster::search(std::string index_name, search_request request, const search_options& options, search_handler&& handler) const
{
    return impl_->search(std::move(index_name), std::move(request), options.build(), std::move(handler));
}

auto
cluster::search(std::string index_name, search_request request, const search_options& options) const
  -> std::future<std::pair<search_error_context, search_result>>
{
    auto barrier = std::make_shared<std::promise<std::pair<search_error_context, search_result>>>();
    search(std::move(index_name), std::move(request), options, [barrier](auto ctx, auto result) mutable {
        barrier->set_value(std::make_pair(std::move(ctx), std::move(result)));
    });
    return barrier->get_future();
}

auto
cluster::connect(asio::io_context& io, const std::string& connection_string, const cluster_options& options)
  -> std::future<std::pair<cluster, std::error_code>>
{
    auto barrier = std::make_shared<std::promise<std::pair<cluster, std::error_code>>>();
    connect(io, connection_string, options, [barrier](auto c, auto ec) mutable { barrier->set_value({ std::move(c), ec }); });
    return barrier->get_future();
}

void
cluster::connect(asio::io_context& io,
                 const std::string& connection_string,
                 const cluster_options& options,
                 cluster_connect_handler&& handler)
{
    auto core = couchbase::core::cluster(io);
    auto origin = options_to_origin(connection_string, options);
    return core.open(origin, [core, handler = std::move(handler)](std::error_code ec) mutable {
        if (ec) {
            return handler({}, ec);
        }
        auto cluster = couchbase::cluster(std::move(core));
        return cluster.impl_->initialize_transactions([cluster, handler = std::move(handler)](std::error_code ec) mutable {
            if (ec) {
                return cluster.impl_->close([ec, handler = std::move(handler)]() mutable { return handler({}, ec); });
            }
            return handler(cluster, ec);
        });
    });
}

auto
cluster::notify_fork(fork_event event) -> void
{
    if (!impl_) {
        return;
    }
    return impl_->notify_fork(event);
}

auto
cluster::close() const -> void
{
    if (!impl_) {
        return;
    }
    return impl_->close([] { /* do nothing */ });
}

auto
cluster::query_indexes() const -> query_index_manager
{
    return query_index_manager{ impl_->core() };
}

auto
cluster::analytics_indexes() const -> analytics_index_manager
{
    return analytics_index_manager{ impl_->core() };
}

auto
cluster::bucket(std::string_view bucket_name) const -> couchbase::bucket
{
    return { impl_->core(), bucket_name };
}

auto
cluster::transactions() const -> std::shared_ptr<couchbase::transactions::transactions>
{
    return impl_->transactions();
}

auto
cluster::buckets() const -> bucket_manager
{
    return bucket_manager{ impl_->core() };
}

auto
cluster::search_indexes() const -> search_index_manager
{
    return search_index_manager{ impl_->core() };
}

namespace core
{
auto
get_core_cluster(couchbase::cluster public_api_cluster) -> core::cluster
{
    auto* impl_ptr = reinterpret_cast<std::shared_ptr<couchbase::cluster_impl>*>(&public_api_cluster);
    return (*impl_ptr)->core();
}

auto
make_agent_group(couchbase::cluster public_api_cluster) -> core::agent_group
{
    auto core_cluster = get_core_cluster(std::move(public_api_cluster));
    return { core_cluster.io_context(), core::agent_group_config{ core::core_sdk_shim{ core_cluster } } };
}

} // namespace core
} // namespace couchbase
