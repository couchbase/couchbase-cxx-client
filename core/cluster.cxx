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

#include "couchbase/build_config.hxx"

#include "cluster.hxx"

#include "bucket.hxx"
#include "capella_ca.hxx"
#include "core/impl/get_replica.hxx"
#include "core/impl/lookup_in_replica.hxx"
#include "core/impl/observe_seqno.hxx"
#include "core/io/http_command.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/management/analytics_link.hxx"
#include "core/mcbp/completion_token.hxx"
#include "core/mcbp/queue_request.hxx"
#include "core/metrics/logging_meter.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations.hxx"
#include "core/operations/management/analytics.hxx"
#include "core/operations/management/bucket.hxx"
#include "core/operations/management/bucket_describe.hxx"
#include "core/operations/management/cluster_describe.hxx"
#include "core/operations/management/cluster_developer_preview_enable.hxx"
#include "core/operations/management/collections.hxx"
#include "core/operations/management/eventing.hxx"
#include "core/operations/management/freeform.hxx"
#include "core/operations/management/query.hxx"
#include "core/operations/management/search.hxx"
#include "core/operations/management/user.hxx"
#include "core/operations/management/view.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"
#include "core/utils/join_strings.hxx"
#include "crud_component.hxx"
#include "dispatcher.hxx"
#include "impl/dns_srv_tracker.hxx"
#include "mozilla_ca_bundle.hxx"
#include "ping_collector.hxx"
#include "ping_reporter.hxx"

#include <asio/ssl.hpp>
#include <fstream>
#include <memory>
#include <thread>

namespace couchbase::core
{
class ping_collector_impl
  : public std::enable_shared_from_this<ping_collector_impl>
  , public diag::ping_reporter
  , public diag::ping_collector
{
    diag::ping_result res_;
    utils::movable_function<void(diag::ping_result)> handler_;
    std::atomic_int expected_{ 0 };
    std::mutex mutex_{};

  public:
    ping_collector_impl(std::string report_id, utils::movable_function<void(diag::ping_result)>&& handler)
      : res_{ std::move(report_id), meta::sdk_id() }
      , handler_(std::move(handler))
    {
    }

    ~ping_collector_impl() override
    {
        invoke_handler();
    }

    [[nodiscard]] diag::ping_result& result()
    {
        return res_;
    }

    void report(diag::endpoint_ping_info&& info) override
    {
        const std::scoped_lock lock(mutex_);
        res_.services[info.type].emplace_back(std::move(info));
        if (--expected_ == 0) {
            invoke_handler();
        }
    }

    auto build_reporter() -> std::shared_ptr<diag::ping_reporter> override
    {
        ++expected_;
        return shared_from_this();
    }

    void invoke_handler()
    {
        if (handler_ != nullptr) {
            handler_(std::move(res_));
            handler_ = nullptr;
        }
    }
};

class cluster_impl : public std::enable_shared_from_this<cluster_impl>
{
  public:
    explicit cluster_impl(asio::io_context& ctx)
      : ctx_(ctx)
      , work_(asio::make_work_guard(ctx_))
      , session_manager_(std::make_shared<io::http_session_manager>(id_, ctx_, tls_))
    {
    }

    auto io_context() -> asio::io_context&
    {
        return ctx_;
    }

    void configure_tls_options(bool has_capella_host)
    {
        asio::ssl::context::options tls_options =
          asio::ssl::context::default_workarounds | // various bug workarounds that should be rather harmless
          asio::ssl::context::no_sslv2 |            // published: 1995, deprecated: 2011
          asio::ssl::context::no_sslv3;             // published: 1996, deprecated: 2015
        if (origin_.options().tls_disable_deprecated_protocols) {
            tls_options |= asio::ssl::context::no_tlsv1 |  // published: 1999, deprecated: 2021
                           asio::ssl::context::no_tlsv1_1; // published: 2006, deprecated: 2021
        }
        if (origin_.options().tls_disable_v1_2 || has_capella_host) {
            tls_options |= asio::ssl::context::no_tlsv1_2; // published: 2008, still in use
        }
        tls_.set_options(tls_options);
        switch (origin_.options().tls_verify) {
            case tls_verify_mode::none:
                tls_.set_verify_mode(asio::ssl::verify_none);
                break;

            case tls_verify_mode::peer:
                tls_.set_verify_mode(asio::ssl::verify_peer);
                break;
        }

#ifdef COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE
        SSL_CTX_set_keylog_callback(tls_.native_handle(), [](const SSL* /* ssl */, const char* line) {
            std::ofstream keylog(COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE, std::ios::out | std::ios::app | std::ios::binary);
            keylog << std::string_view(line) << std::endl;
        });
        CB_LOG_CRITICAL(
          "COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE was set to \"{}\" during build, all TLS keys will be logged for network analysis "
          "(https://wiki.wireshark.org/TLS). DO NOT USE THIS BUILD IN PRODUCTION",
          COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE);
#endif
    }

    void open(couchbase::core::origin origin, utils::movable_function<void(std::error_code)>&& handler)
    {
        if (stopped_) {
            return handler(errc::network::cluster_closed);
        }
        if (origin.get_nodes().empty()) {
            stopped_ = true;
            work_.reset();
            return handler(errc::common::invalid_argument);
        }

        origin_ = std::move(origin);
        CB_LOG_DEBUG(R"(open cluster, id: "{}", core version: "{}", {})", id_, couchbase::core::meta::sdk_semver(), origin_.to_json());
        // ignore the enable_tracing flag if a tracer was passed in
        if (nullptr != origin_.options().tracer) {
            tracer_ = origin_.options().tracer;
        } else {
            if (origin_.options().enable_tracing) {
                tracer_ = std::make_shared<tracing::threshold_logging_tracer>(ctx_, origin_.options().tracing_options);
            } else {
                tracer_ = std::make_shared<tracing::noop_tracer>();
            }
        }
        tracer_->start();
        // ignore the metrics options if a meter was passed in.
        if (nullptr != origin_.options().meter) {
            meter_ = origin_.options().meter;
        } else {
            if (origin_.options().enable_metrics) {
                meter_ = std::make_shared<metrics::logging_meter>(ctx_, origin_.options().metrics_options);
            } else {
                meter_ = std::make_shared<metrics::noop_meter>();
            }
        }
        meter_->start();
        session_manager_->set_tracer(tracer_);
        if (origin_.options().enable_dns_srv) {
            std::string hostname;
            std::string port;
            std::tie(hostname, port) = origin_.next_address();
            dns_srv_tracker_ =
              std::make_shared<impl::dns_srv_tracker>(ctx_, hostname, origin_.options().dns_config, origin_.options().enable_tls);
            return asio::post(asio::bind_executor(
              ctx_, [self = shared_from_this(), hostname = std::move(hostname), handler = std::move(handler)]() mutable {
                  return self->dns_srv_tracker_->get_srv_nodes([self, hostname = std::move(hostname), handler = std::move(handler)](
                                                                 origin::node_list nodes, std::error_code ec) mutable {
                      if (ec) {
                          return self->close([ec, handler = std::move(handler)]() mutable { handler(ec); });
                      }
                      if (!nodes.empty()) {
                          self->origin_.set_nodes(std::move(nodes));
                          CB_LOG_INFO("replace list of bootstrap nodes with addresses from DNS SRV of \"{}\": [{}]",
                                      hostname,
                                      utils::join_strings(self->origin_.get_nodes(), ", "));
                      }
                      return self->do_open(std::move(handler));
                  });
              }));
        }
        do_open(std::move(handler));
    }

    void open_bucket(const std::string& bucket_name, utils::movable_function<void(std::error_code)>&& handler)
    {
        if (stopped_) {
            return handler(errc::network::cluster_closed);
        }
        std::shared_ptr<bucket> b{};
        {
            std::scoped_lock lock(buckets_mutex_);
            auto ptr = buckets_.find(bucket_name);
            if (ptr == buckets_.end()) {
                std::vector<protocol::hello_feature> known_features;
                if (session_ && session_->has_config()) {
                    known_features = session_->supported_features();
                }
                b = std::make_shared<bucket>(id_, ctx_, tls_, tracer_, meter_, bucket_name, origin_, known_features, dns_srv_tracker_);
                buckets_.try_emplace(bucket_name, b);
            }
        }
        if (b == nullptr) {
            return handler({});
        }

        b->on_configuration_update(session_manager_);
        b->bootstrap([self = shared_from_this(), bucket_name, handler = std::move(handler)](std::error_code ec,
                                                                                            const topology::configuration& config) mutable {
            if (ec) {
                std::scoped_lock lock(self->buckets_mutex_);
                self->buckets_.erase(bucket_name);
            } else if (self->session_ && !self->session_->supports_gcccp()) {
                self->session_manager_->set_configuration(config, self->origin_.options());
            }
            handler(ec);
        });
    }

    void close_bucket(const std::string& bucket_name, utils::movable_function<void(std::error_code)>&& handler)
    {
        if (stopped_) {
            return handler(errc::network::cluster_closed);
        }
        std::shared_ptr<bucket> b{};
        {
            std::scoped_lock lock(buckets_mutex_);

            if (auto ptr = buckets_.find(bucket_name); ptr != buckets_.end()) {
                b = std::move(ptr->second);
                buckets_.erase(ptr);
            }
        }
        if (b != nullptr) {
            b->close();
        }
        return handler({});
    }

    std::pair<std::error_code, couchbase::core::origin> origin() const
    {
        if (stopped_) {
            return { errc::network::cluster_closed, {} };
        }
        return { {}, origin_ };
    }

    template<class Request,
             class Handler,
             typename std::enable_if_t<!std::is_same_v<typename Request::encoded_request_type, io::http_request>, int> = 0>
    void execute(Request request, Handler&& handler)
    {
        using response_type = typename Request::encoded_response_type;
        if (stopped_) {
            return handler(request.make_response(make_key_value_error_context(errc::network::cluster_closed, request.id), response_type{}));
        }
        if (auto bucket = find_bucket_by_name(request.id.bucket()); bucket != nullptr) {
            return bucket->execute(std::move(request), std::forward<Handler>(handler));
        }
        if (request.id.bucket().empty()) {
            return handler(
              request.make_response(make_key_value_error_context(errc::common::bucket_not_found, request.id), response_type{}));
        }
        auto bucket_name = request.id.bucket();
        return open_bucket(
          bucket_name,
          [self = shared_from_this(), request = std::move(request), handler = std::forward<Handler>(handler)](std::error_code ec) mutable {
              if (ec) {
                  return handler(request.make_response(make_key_value_error_context(ec, request.id), response_type{}));
              }
              return self->execute(std::move(request), std::forward<Handler>(handler));
          });
    }

    template<class Request,
             class Handler,
             typename std::enable_if_t<std::is_same_v<typename Request::encoded_request_type, io::http_request>, int> = 0>
    void execute(Request request, Handler&& handler)
    {
        using response_type = typename Request::encoded_response_type;
        if (stopped_) {
            return handler(request.make_response({ errc::network::cluster_closed }, response_type{}));
        }
        if constexpr (operations::is_compound_operation_v<Request>) {
            return request.execute(shared_from_this(), std::forward<Handler>(handler));
        } else {
            return session_manager_->execute(std::move(request), std::forward<Handler>(handler), origin_.credentials());
        }
    }

    std::shared_ptr<bucket> find_bucket_by_name(const std::string& name)
    {
        std::scoped_lock lock(buckets_mutex_);

        auto bucket = buckets_.find(name);
        if (bucket == buckets_.end()) {
            return {};
        }
        return bucket->second;
    }

    void for_each_bucket(utils::movable_function<void(std::shared_ptr<bucket>)> handler)
    {
        std::vector<std::shared_ptr<bucket>> buckets{};
        {
            std::scoped_lock lock(buckets_mutex_);
            buckets.reserve(buckets_.size());
            for (const auto& [name, bucket] : buckets_) {
                buckets.push_back(bucket);
            }
        }
        for (auto bucket : buckets) {
            handler(bucket);
        }
    }

    void do_open(utils::movable_function<void(std::error_code)> handler)
    {
        // Warn users if idle_http_connection_timeout is too close to server idle timeouts
        if (origin_.options().idle_http_connection_timeout > std::chrono::milliseconds(4'500)) {
            CB_LOG_INFO("[{}]: The SDK may produce trivial warnings due to the idle HTTP connection timeout being set above the idle"
                        "timeout of various services",
                        id_);
        }

        // Warn users if they attempt to use Capella without TLS being enabled.
        bool has_capella_host = false;
        {
            bool has_non_capella_host = false;
            static std::string suffix = "cloud.couchbase.com";
            for (const auto& node : origin_.get_hostnames()) {
                if (auto pos = node.find(suffix); pos != std::string::npos && pos + suffix.size() == node.size()) {
                    has_capella_host = true;
                } else {
                    has_non_capella_host = true;
                }
            }

            if (has_capella_host && !origin_.options().enable_tls) {
                CB_LOG_WARNING("[{}]: TLS is required when connecting to Couchbase Capella. Please enable TLS by prefixing "
                               "the connection string with \"couchbases://\" (note the final 's').",
                               id_);
            }

            if (origin_.options().enable_tls                   /* TLS is enabled */
                && origin_.options().trust_certificate.empty() /* No CA certificate (or other SDK-specific trust source) is specified */
                && origin_.options().trust_certificate_value.empty()     /* and certificate value has not been specified */
                && origin_.options().tls_verify != tls_verify_mode::none /* The user did not disable all TLS verification */
                && has_non_capella_host /* The connection string has a hostname that does NOT end in ".cloud.couchbase.com" */) {
                CB_LOG_WARNING("[{}] When TLS is enabled, the cluster options must specify certificate(s) to trust or ensure that they are "
                               "available in system CA store. (Unless connecting to cloud.couchbase.com.)",
                               id_);
            }
        }

        if (origin_.options().enable_tls) {
            configure_tls_options(has_capella_host);

            if (origin_.options().trust_certificate.empty() &&
                origin_.options().trust_certificate_value.empty()) { // trust certificate is not explicitly specified
                CB_LOG_DEBUG(R"([{}]: use default CA for TLS verify)", id_);
                std::error_code ec{};

                // load system certificates
                tls_.set_default_verify_paths(ec);
                if (ec) {
                    CB_LOG_WARNING(R"([{}]: failed to load system CAs: {})", id_, ec.message());
                }

                // add the Capella Root CA in addition to system CAs
                tls_.add_certificate_authority(
                  asio::const_buffer(couchbase::core::default_ca::capellaCaCert, strlen(couchbase::core::default_ca::capellaCaCert)), ec);
                if (ec) {
                    CB_LOG_WARNING("[{}]: unable to load default CAs: {}", id_, ec.message());
                    // we don't consider this fatal and try to continue without it
                }

                if (const auto certificates = default_ca::mozilla_ca_certs();
                    !origin_.options().disable_mozilla_ca_certificates && !certificates.empty()) {
                    CB_LOG_DEBUG("[{}]: loading {} CA certificates from Mozilla bundle. Update date: \"{}\", SHA256: \"{}\"",
                                 id_,
                                 certificates.size(),
                                 default_ca::mozilla_ca_certs_date(),
                                 default_ca::mozilla_ca_certs_sha256());
                    for (const auto& cert : certificates) {
                        tls_.add_certificate_authority(asio::const_buffer(cert.body.data(), cert.body.size()), ec);
                        if (ec) {
                            CB_LOG_WARNING("[{}]: unable to load CA \"{}\" from Mozilla bundle: {}", id_, cert.authority, ec.message());
                        }
                    }
                }
            } else { // trust certificate is explicitly specified
                std::error_code ec{};
                // load only the explicit certificate
                // system and default capella certificates are not loaded
                if (!origin_.options().trust_certificate_value.empty()) {
                    CB_LOG_DEBUG(R"([{}]: use TLS certificate passed through via options object)", id_);
                    tls_.add_certificate_authority(asio::const_buffer(origin_.options().trust_certificate_value.data(),
                                                                      origin_.options().trust_certificate_value.size()),
                                                   ec);
                    if (ec) {
                        CB_LOG_WARNING("[{}]: unable to load CA passed via options object: {}", id_, ec.message());
                    }
                }
                if (!origin_.options().trust_certificate.empty()) {
                    CB_LOG_DEBUG(R"([{}]: use TLS verify file: "{}")", id_, origin_.options().trust_certificate);
                    tls_.load_verify_file(origin_.options().trust_certificate, ec);
                    if (ec) {
                        CB_LOG_ERROR("[{}]: unable to load verify file \"{}\": {}", id_, origin_.options().trust_certificate, ec.message());
                        return close([ec, handler = std::move(handler)]() mutable { return handler(ec); });
                    }
                }
            }
            if (origin_.credentials().uses_certificate()) {
                std::error_code ec{};
                CB_LOG_DEBUG(R"([{}]: use TLS certificate chain: "{}")", id_, origin_.certificate_path());
                tls_.use_certificate_chain_file(origin_.certificate_path(), ec);
                if (ec) {
                    CB_LOG_ERROR("[{}]: unable to load certificate chain \"{}\": {}", id_, origin_.certificate_path(), ec.message());
                    return close([ec, handler = std::move(handler)]() mutable { return handler(ec); });
                }
                CB_LOG_DEBUG(R"([{}]: use TLS private key: "{}")", id_, origin_.key_path());
                tls_.use_private_key_file(origin_.key_path(), asio::ssl::context::file_format::pem, ec);
                if (ec) {
                    CB_LOG_ERROR("[{}]: unable to load private key \"{}\": {}", id_, origin_.key_path(), ec.message());
                    return close([ec, handler = std::move(handler)]() mutable { return handler(ec); });
                }
            }
            session_ = io::mcbp_session(id_, ctx_, tls_, origin_, dns_srv_tracker_);
        } else {
            session_ = io::mcbp_session(id_, ctx_, origin_, dns_srv_tracker_);
        }
        session_->bootstrap([self = shared_from_this(), handler = std::move(handler)](std::error_code ec,
                                                                                      const topology::configuration& config) mutable {
            if (!ec) {
                if (self->origin_.options().network == "auto") {
                    self->origin_.options().network = config.select_network(self->session_->bootstrap_hostname());
                    if (self->origin_.options().network == "default") {
                        CB_LOG_DEBUG(R"({} detected network is "{}")", self->session_->log_prefix(), self->origin_.options().network);
                    } else {
                        CB_LOG_INFO(R"({} detected network is "{}")", self->session_->log_prefix(), self->origin_.options().network);
                    }
                }
                if (self->origin_.options().network != "default") {
                    origin::node_list nodes;
                    nodes.reserve(config.nodes.size());
                    for (const auto& address : config.nodes) {
                        auto port =
                          address.port_or(self->origin_.options().network, service_type::key_value, self->origin_.options().enable_tls, 0);
                        if (port == 0) {
                            continue;
                        }
                        origin::node_entry node;
                        node.first = address.hostname_for(self->origin_.options().network);
                        node.second = std::to_string(port);
                        nodes.emplace_back(node);
                    }
                    self->origin_.set_nodes(nodes);
                    CB_LOG_INFO("replace list of bootstrap nodes with addresses of alternative network \"{}\": [{}]",
                                self->origin_.options().network,
                                utils::join_strings(self->origin_.get_nodes(), ","));
                }
                self->session_manager_->set_configuration(config, self->origin_.options());
                self->session_->on_configuration_update(self->session_manager_);
                self->session_->on_stop([self]() {
                    if (self->session_) {
                        self->session_.reset();
                    }
                });
            }
            if (ec) {
                return self->close([ec, handler = std::move(handler)]() mutable { handler(ec); });
            }
            handler(ec);
        });
    }

    void with_bucket_configuration(const std::string& bucket_name,
                                   utils::movable_function<void(std::error_code, topology::configuration)>&& handler)
    {
        if (stopped_) {
            return handler(errc::network::cluster_closed, {});
        }
        if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
            return bucket->with_configuration(std::move(handler));
        }
        return handler(errc::common::bucket_not_found, {});
    }

    void ping(std::optional<std::string> report_id,
              std::optional<std::string> bucket_name,
              std::set<service_type> services,
              std::optional<std::chrono::milliseconds> timeout,
              utils::movable_function<void(diag::ping_result)> handler)
    {
        if (!report_id) {
            report_id = std::make_optional(uuid::to_string(uuid::random()));
        }
        if (stopped_) {
            return handler({ report_id.value(), meta::sdk_id() });
        }
        if (services.empty()) {
            services = {
                service_type::key_value, service_type::view,       service_type::query,    service_type::search,
                service_type::analytics, service_type::management, service_type::eventing,
            };
        }
        asio::post(asio::bind_executor(
          ctx_, [cluster = shared_from_this(), report_id, bucket_name, services, timeout, handler = std::move(handler)]() mutable {
              auto collector = std::make_shared<ping_collector_impl>(report_id.value(), std::move(handler));
              if (bucket_name) {
                  if (services.find(service_type::key_value) != services.end()) {
                      if (auto bucket = cluster->find_bucket_by_name(bucket_name.value()); bucket) {
                          return bucket->ping(collector, timeout);
                      }
                      cluster->open_bucket(bucket_name.value(), [collector, cluster, bucket_name, timeout](std::error_code ec) {
                          if (!ec) {
                              if (auto bucket = cluster->find_bucket_by_name(bucket_name.value()); bucket) {
                                  return bucket->ping(collector, timeout);
                              }
                          }
                      });
                  }
              } else {
                  if (services.find(service_type::key_value) != services.end()) {
                      if (cluster->session_) {
                          cluster->session_->ping(collector->build_reporter(), timeout);
                      }
                      cluster->for_each_bucket([&collector, &timeout](auto bucket) { bucket->ping(collector, timeout); });
                  }
                  cluster->session_manager_->ping(services, timeout, collector, cluster->origin_.credentials());
              }
          }));
    }

    void diagnostics(std::optional<std::string> report_id, utils::movable_function<void(diag::diagnostics_result)>&& handler)
    {
        if (!report_id) {
            report_id = std::make_optional(uuid::to_string(uuid::random()));
        }
        if (stopped_) {
            return handler({ report_id.value(), couchbase::core::meta::sdk_id() });
        }
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), report_id, handler = std::move(handler)]() mutable {
            diag::diagnostics_result res{ report_id.value(), couchbase::core::meta::sdk_id() };
            if (self->session_) {
                res.services[service_type::key_value].emplace_back(self->session_->diag_info());
            }
            self->for_each_bucket([&res](const auto& bucket) { bucket->export_diag_info(res); });
            self->session_manager_->export_diag_info(res);
            handler(std::move(res));
        }));
    }

    void close(utils::movable_function<void()>&& handler)
    {
        if (stopped_) {
            return handler();
        }
        stopped_ = true;
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), handler = std::move(handler)]() mutable {
            if (self->session_) {
                self->session_->stop(retry_reason::do_not_retry);
                self->session_.reset();
            }
            self->for_each_bucket([](auto bucket) { bucket->close(); });
            self->session_manager_->close();
            handler();
            self->work_.reset();
            if (self->tracer_) {
                self->tracer_->stop();
            }
            self->tracer_.reset();
            if (self->meter_) {
                self->meter_->stop();
            }
            self->meter_.reset();
        }));
    }

    auto direct_dispatch(const std::string& bucket_name, std::shared_ptr<couchbase::core::mcbp::queue_request> req) -> std::error_code
    {
        if (stopped_) {
            return errc::network::cluster_closed;
        }
        if (bucket_name.empty()) {
            return errc::common::invalid_argument;
        }
        if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
            return bucket->direct_dispatch(std::move(req));
        }

        open_bucket(bucket_name, [self = shared_from_this(), req = std::move(req), bucket_name](std::error_code ec) mutable {
            if (ec) {
                return req->cancel(ec);
            }
            self->direct_dispatch(bucket_name, std::move(req));
        });
        return {};
    }

    auto direct_re_queue(const std::string& bucket_name, std::shared_ptr<mcbp::queue_request> req, bool is_retry) -> std::error_code
    {
        if (stopped_) {
            return errc::network::cluster_closed;
        }
        if (bucket_name.empty()) {
            return errc::common::invalid_argument;
        }
        if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
            return bucket->direct_re_queue(std::move(req), is_retry);
        }

        open_bucket(bucket_name, [self = shared_from_this(), bucket_name, req = std::move(req), is_retry](std::error_code ec) mutable {
            if (ec) {
                return req->cancel(ec);
            }
            self->direct_re_queue(bucket_name, std::move(req), is_retry);
        });
        return {};
    }

  private:
    std::string id_{ uuid::to_string(uuid::random()) };
    asio::io_context& ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    asio::ssl::context tls_{ asio::ssl::context::tls_client };
    std::shared_ptr<io::http_session_manager> session_manager_;
    std::optional<io::mcbp_session> session_{};
    std::shared_ptr<impl::dns_srv_tracker> dns_srv_tracker_{};
    std::mutex buckets_mutex_{};
    std::map<std::string, std::shared_ptr<bucket>> buckets_{};
    couchbase::core::origin origin_{};
    std::shared_ptr<couchbase::tracing::request_tracer> tracer_{ nullptr };
    std::shared_ptr<couchbase::metrics::meter> meter_{ nullptr };
    std::atomic_bool stopped_{ false };
};

cluster::cluster(asio::io_context& ctx)
  : impl_{ std::make_shared<cluster_impl>(ctx) }
{
}

auto
cluster::direct_dispatch(const std::string& bucket_name, std::shared_ptr<couchbase::core::mcbp::queue_request> req) const -> std::error_code
{
    if (impl_) {
        return impl_->direct_dispatch(bucket_name, std::move(req));
    }
    return errc::network::cluster_closed;
}

auto
cluster::direct_re_queue(const std::string& bucket_name, std::shared_ptr<mcbp::queue_request> req, bool is_retry) const -> std::error_code
{
    if (impl_) {
        return impl_->direct_re_queue(bucket_name, std::move(req), is_retry);
    }
    return errc::network::cluster_closed;
}

void
cluster::close(utils::movable_function<void()>&& handler) const
{
    if (impl_) {
        impl_->close(std::move(handler));
    }
}

void
cluster::open_bucket(const std::string& bucket_name, utils::movable_function<void(std::error_code)>&& handler) const
{
    if (impl_) {
        impl_->open_bucket(bucket_name, std::move(handler));
    }
}

void
cluster::open(couchbase::core::origin origin, utils::movable_function<void(std::error_code)>&& handler) const
{
    if (impl_) {
        impl_->open(std::move(origin), std::move(handler));
    }
}

void
cluster::diagnostics(std::optional<std::string> report_id, utils::movable_function<void(diag::diagnostics_result)>&& handler) const
{
    if (impl_) {
        impl_->diagnostics(std::move(report_id), std::move(handler));
    }
}

void
cluster::ping(std::optional<std::string> report_id,
              std::optional<std::string> bucket_name,
              std::set<service_type> services,
              std::optional<std::chrono::milliseconds> timeout,
              utils::movable_function<void(diag::ping_result)>&& handler) const
{
    if (impl_) {
        impl_->ping(std::move(report_id), std::move(bucket_name), std::move(services), std::move(timeout), std::move(handler));
    }
}

void
cluster::with_bucket_configuration(const std::string& bucket_name,
                                   utils::movable_function<void(std::error_code, topology::configuration)>&& handler) const
{
    if (impl_) {
        impl_->with_bucket_configuration(bucket_name, std::move(handler));
    }
}

void
cluster::close_bucket(const std::string& bucket_name, utils::movable_function<void(std::error_code)>&& handler) const
{
    if (impl_) {
        impl_->close_bucket(bucket_name, std::move(handler));
    }
}

std::pair<std::error_code, couchbase::core::origin>
cluster::origin() const
{
    if (impl_) {
        return impl_->origin();
    }
    return { errc::network::cluster_closed, {} };
}

auto
cluster::io_context() const -> asio::io_context&
{
    return impl_->io_context();
}

void
cluster::execute(operations::append_request request, utils::movable_function<void(operations::append_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::decrement_request request, utils::movable_function<void(operations::decrement_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::exists_request request, utils::movable_function<void(operations::exists_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::get_request request, utils::movable_function<void(operations::get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::get_all_replicas_request request,
                 utils::movable_function<void(operations::get_all_replicas_response)>&& handler) const
{
    return request.execute(impl_, std::move(handler));
}

void
cluster::execute(operations::get_and_lock_request request, utils::movable_function<void(operations::get_and_lock_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::get_and_touch_request request,
                 utils::movable_function<void(operations::get_and_touch_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::get_any_replica_request request,
                 utils::movable_function<void(operations::get_any_replica_response)>&& handler) const
{
    return request.execute(impl_, std::move(handler));
}

void
cluster::execute(operations::get_projected_request request,
                 utils::movable_function<void(operations::get_projected_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::increment_request request, utils::movable_function<void(operations::increment_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::insert_request request, utils::movable_function<void(operations::insert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::lookup_in_request request, utils::movable_function<void(operations::lookup_in_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::lookup_in_any_replica_request request,
                 utils::movable_function<void(operations::lookup_in_any_replica_response)>&& handler) const
{
    return request.execute(impl_, std::move(handler));
}

void
cluster::execute(operations::lookup_in_all_replicas_request request,
                 utils::movable_function<void(operations::lookup_in_all_replicas_response)>&& handler) const
{
    return request.execute(impl_, std::move(handler));
}

void
cluster::execute(operations::mutate_in_request request, utils::movable_function<void(operations::mutate_in_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::prepend_request request, utils::movable_function<void(operations::prepend_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::query_request request, utils::movable_function<void(operations::query_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::remove_request request, utils::movable_function<void(operations::remove_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::replace_request request, utils::movable_function<void(operations::replace_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::search_request request, utils::movable_function<void(operations::search_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::touch_request request, utils::movable_function<void(operations::touch_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::unlock_request request, utils::movable_function<void(operations::unlock_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::upsert_request request, utils::movable_function<void(operations::upsert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::document_view_request request,
                 utils::movable_function<void(operations::document_view_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::http_noop_request request, utils::movable_function<void(operations::http_noop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_dataset_create_request request,
                 utils::movable_function<void(operations::management::analytics_dataset_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_dataset_drop_request request,
                 utils::movable_function<void(operations::management::analytics_dataset_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_dataset_get_all_request request,
                 utils::movable_function<void(operations::management::analytics_dataset_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_dataverse_create_request request,
                 utils::movable_function<void(operations::management::analytics_dataverse_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_dataverse_drop_request request,
                 utils::movable_function<void(operations::management::analytics_dataverse_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_get_pending_mutations_request request,
                 utils::movable_function<void(operations::management::analytics_get_pending_mutations_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_index_create_request request,
                 utils::movable_function<void(operations::management::analytics_index_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_index_drop_request request,
                 utils::movable_function<void(operations::management::analytics_index_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_index_get_all_request request,
                 utils::movable_function<void(operations::management::analytics_index_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_connect_request request,
                 utils::movable_function<void(operations::management::analytics_link_connect_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_disconnect_request request,
                 utils::movable_function<void(operations::management::analytics_link_disconnect_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_drop_request request,
                 utils::movable_function<void(operations::management::analytics_link_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_get_all_request request,
                 utils::movable_function<void(operations::management::analytics_link_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_create_request request,
                 utils::movable_function<void(operations::management::bucket_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_drop_request request,
                 utils::movable_function<void(operations::management::bucket_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_flush_request request,
                 utils::movable_function<void(operations::management::bucket_flush_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_get_request request,
                 utils::movable_function<void(operations::management::bucket_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_get_all_request request,
                 utils::movable_function<void(operations::management::bucket_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_update_request request,
                 utils::movable_function<void(operations::management::bucket_update_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::collection_create_request request,
                 utils::movable_function<void(operations::management::collection_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::collection_update_request request,
                 utils::movable_function<void(operations::management::collection_update_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::collection_drop_request request,
                 utils::movable_function<void(operations::management::collection_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::collections_manifest_get_request request,
                 utils::movable_function<void(operations::management::collections_manifest_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::scope_create_request request,
                 utils::movable_function<void(operations::management::scope_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::scope_drop_request request,
                 utils::movable_function<void(operations::management::scope_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::scope_get_all_request request,
                 utils::movable_function<void(operations::management::scope_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_deploy_function_request request,
                 utils::movable_function<void(operations::management::eventing_deploy_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_drop_function_request request,
                 utils::movable_function<void(operations::management::eventing_drop_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_get_all_functions_request request,
                 utils::movable_function<void(operations::management::eventing_get_all_functions_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_get_function_request request,
                 utils::movable_function<void(operations::management::eventing_get_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_get_status_request request,
                 utils::movable_function<void(operations::management::eventing_get_status_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_pause_function_request request,
                 utils::movable_function<void(operations::management::eventing_pause_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_resume_function_request request,
                 utils::movable_function<void(operations::management::eventing_resume_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_undeploy_function_request request,
                 utils::movable_function<void(operations::management::eventing_undeploy_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::eventing_upsert_function_request request,
                 utils::movable_function<void(operations::management::eventing_upsert_function_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::view_index_drop_request request,
                 utils::movable_function<void(operations::management::view_index_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::view_index_get_request request,
                 utils::movable_function<void(operations::management::view_index_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::view_index_get_all_request request,
                 utils::movable_function<void(operations::management::view_index_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::view_index_upsert_request request,
                 utils::movable_function<void(operations::management::view_index_upsert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::change_password_request request,
                 utils::movable_function<void(operations::management::change_password_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::group_drop_request request,
                 utils::movable_function<void(operations::management::group_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::group_get_request request,
                 utils::movable_function<void(operations::management::group_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::group_get_all_request request,
                 utils::movable_function<void(operations::management::group_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::group_upsert_request request,
                 utils::movable_function<void(operations::management::group_upsert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::role_get_all_request request,
                 utils::movable_function<void(operations::management::role_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::user_drop_request request,
                 utils::movable_function<void(operations::management::user_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::user_get_request request,
                 utils::movable_function<void(operations::management::user_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::user_get_all_request request,
                 utils::movable_function<void(operations::management::user_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::user_upsert_request request,
                 utils::movable_function<void(operations::management::user_upsert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_get_stats_request request,
                 utils::movable_function<void(operations::management::search_get_stats_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_analyze_document_request request,
                 utils::movable_function<void(operations::management::search_index_analyze_document_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_control_ingest_request request,
                 utils::movable_function<void(operations::management::search_index_control_ingest_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_control_plan_freeze_request request,
                 utils::movable_function<void(operations::management::search_index_control_plan_freeze_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_control_query_request request,
                 utils::movable_function<void(operations::management::search_index_control_query_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_drop_request request,
                 utils::movable_function<void(operations::management::search_index_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_get_request request,
                 utils::movable_function<void(operations::management::search_index_get_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_get_all_request request,
                 utils::movable_function<void(operations::management::search_index_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_get_documents_count_request request,
                 utils::movable_function<void(operations::management::search_index_get_documents_count_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_get_stats_request request,
                 utils::movable_function<void(operations::management::search_index_get_stats_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::search_index_upsert_request request,
                 utils::movable_function<void(operations::management::search_index_upsert_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_build_request request,
                 utils::movable_function<void(operations::management::query_index_build_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_build_deferred_request request,
                 utils::movable_function<void(operations::management::query_index_build_deferred_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_create_request request,
                 utils::movable_function<void(operations::management::query_index_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_drop_request request,
                 utils::movable_function<void(operations::management::query_index_drop_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_get_all_request request,
                 utils::movable_function<void(operations::management::query_index_get_all_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::query_index_get_all_deferred_request request,
                 utils::movable_function<void(operations::management::query_index_get_all_deferred_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::bucket_describe_request request,
                 utils::movable_function<void(operations::management::bucket_describe_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::cluster_describe_request request,
                 utils::movable_function<void(operations::management::cluster_describe_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::cluster_developer_preview_enable_request request,
                 utils::movable_function<void(operations::management::cluster_developer_preview_enable_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::freeform_request request,
                 utils::movable_function<void(operations::management::freeform_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(impl::get_replica_request request, utils::movable_function<void(impl::get_replica_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(impl::observe_seqno_request request, utils::movable_function<void(impl::observe_seqno_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_replace_request<management::analytics::azure_blob_external_link> request,
                 utils::movable_function<void(operations::management::analytics_link_replace_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_replace_request<management::analytics::couchbase_remote_link> request,
                 utils::movable_function<void(operations::management::analytics_link_replace_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_replace_request<management::analytics::s3_external_link> request,
                 utils::movable_function<void(operations::management::analytics_link_replace_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_create_request<management::analytics::azure_blob_external_link> request,
                 utils::movable_function<void(operations::management::analytics_link_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_create_request<management::analytics::couchbase_remote_link> request,
                 utils::movable_function<void(operations::management::analytics_link_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::management::analytics_link_create_request<management::analytics::s3_external_link> request,
                 utils::movable_function<void(operations::management::analytics_link_create_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::analytics_request request, utils::movable_function<void(operations::analytics_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

void
cluster::execute(operations::upsert_request_with_legacy_durability request,
                 utils::movable_function<void(operations::upsert_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::insert_request_with_legacy_durability request,
                 utils::movable_function<void(operations::insert_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::append_request_with_legacy_durability request,
                 utils::movable_function<void(operations::append_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::prepend_request_with_legacy_durability request,
                 utils::movable_function<void(operations::prepend_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::replace_request_with_legacy_durability request,
                 utils::movable_function<void(operations::replace_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::mutate_in_request_with_legacy_durability request,
                 utils::movable_function<void(operations::mutate_in_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::remove_request_with_legacy_durability request,
                 utils::movable_function<void(operations::remove_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::increment_request_with_legacy_durability request,
                 utils::movable_function<void(operations::increment_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(operations::decrement_request_with_legacy_durability request,
                 utils::movable_function<void(operations::decrement_response)>&& handler) const
{
    return request.execute(*this, std::move(handler));
}

void
cluster::execute(impl::lookup_in_replica_request request, utils::movable_function<void(impl::lookup_in_replica_response)>&& handler) const
{
    return impl_->execute(std::move(request), std::move(handler));
}

auto
cluster::to_string() const -> std::string
{
    return fmt::format(
      R"(#<cluster:{} impl={}>)", static_cast<const void*>(this), impl_ ? static_cast<const void*>(impl_.get()) : "(none)");
}
} // namespace couchbase::core
