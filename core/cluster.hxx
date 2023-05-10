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

#include "bucket.hxx"
#include "capella_ca.hxx"
#include "core/io/http_command.hxx"
#include "core/io/http_session_manager.hxx"
#include "core/io/mcbp_command.hxx"
#include "core/io/mcbp_session.hxx"
#include "core/metrics/logging_meter.hxx"
#include "core/metrics/noop_meter.hxx"
#include "core/operations/management/bucket_create.hxx"
#include "core/tracing/noop_tracer.hxx"
#include "core/tracing/threshold_logging_tracer.hxx"
#include "core/utils/join_strings.hxx"
#include "crud_component.hxx"
#include "diagnostics.hxx"
#include "dispatcher.hxx"
#include "impl/dns_srv_tracker.hxx"
#include "mozilla_ca_bundle.hxx"
#include "operations.hxx"
#include "origin.hxx"

#include <asio/ssl.hpp>
#include <fstream>
#include <thread>
#include <utility>

namespace couchbase::core
{
class crud_component;

class cluster : public std::enable_shared_from_this<cluster>
{
  public:
    [[nodiscard]] auto io_context() -> asio::io_context&
    {
        return ctx_;
    }
    [[nodiscard]] static std::shared_ptr<cluster> create(asio::io_context& ctx)
    {
        return std::shared_ptr<cluster>(new cluster(ctx));
    }

    [[nodiscard]] std::pair<std::error_code, couchbase::core::origin> origin() const
    {
        if (stopped_) {
            return { errc::network::cluster_closed, {} };
        }
        return { {}, origin_ };
    }

    template<typename Handler>
    void open(couchbase::core::origin origin, Handler&& handler)
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
            auto [hostname, _] = origin_.next_address();
            dns_srv_tracker_ =
              std::make_shared<impl::dns_srv_tracker>(ctx_, hostname, origin_.options().dns_config, origin_.options().enable_tls);
            return asio::post(asio::bind_executor(
              ctx_, [self = shared_from_this(), hostname = std::move(hostname), handler = std::forward<Handler>(handler)]() mutable {
                  return self->dns_srv_tracker_->get_srv_nodes(
                    [self, hostname = std::move(hostname), handler = std::forward<Handler>(handler)](origin::node_list nodes,
                                                                                                     std::error_code ec) mutable {
                        if (ec) {
                            return self->close([ec, handler = std::forward<Handler>(handler)]() mutable { handler(ec); });
                        }
                        if (!nodes.empty()) {
                            self->origin_.set_nodes(std::move(nodes));
                            CB_LOG_INFO("replace list of bootstrap nodes with addresses from DNS SRV of \"{}\": [{}]",
                                        hostname,
                                        utils::join_strings(self->origin_.get_nodes(), ", "));
                        }
                        return self->do_open(std::forward<Handler>(handler));
                    });
              }));
        }
        do_open(std::forward<Handler>(handler));
    }

    template<typename Handler>
    void close(Handler&& handler)
    {
        if (stopped_) {
            return handler();
        }
        stopped_ = true;
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
            if (self->session_) {
                self->session_->stop(retry_reason::do_not_retry);
                self->session_.reset();
            }
            self->for_each_bucket([](auto& bucket) { bucket->close(); });
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

    template<typename Handler>
    void open_bucket(const std::string& bucket_name, Handler&& handler)
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
        b->bootstrap([self = shared_from_this(), bucket_name, handler = std::forward<Handler>(handler)](
                       std::error_code ec, const topology::configuration& config) mutable {
            if (ec) {
                std::scoped_lock lock(self->buckets_mutex_);
                self->buckets_.erase(bucket_name);
            } else if (self->session_ && !self->session_->supports_gcccp()) {
                self->session_manager_->set_configuration(config, self->origin_.options());
            }
            handler(ec);
        });
    }

    template<typename Handler>
    void close_bucket(const std::string& bucket_name, Handler&& handler)
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

    template<class Handler>
    void with_bucket_configuration(const std::string& bucket_name, Handler&& handler)
    {
        if (stopped_) {
            return handler(errc::network::cluster_closed, {});
        }
        if (auto bucket = find_bucket_by_name(bucket_name); bucket != nullptr) {
            return bucket->with_configuration(std::forward<Handler>(handler));
        }
        return handler(errc::common::bucket_not_found, {});
    }

    template<class Request,
             class Handler,
             typename std::enable_if_t<!std::is_same_v<typename Request::encoded_request_type, io::http_request>, int> = 0>
    void execute(Request request, Handler&& handler)
    {
        if constexpr (operations::is_compound_operation_v<Request>) {
            return request.execute(shared_from_this(), std::forward<Handler>(handler));
        } else {
            using response_type = typename Request::encoded_response_type;
            if (stopped_) {
                return handler(
                  request.make_response(make_key_value_error_context(errc::network::cluster_closed, request.id), response_type{}));
            }
            if (auto bucket = find_bucket_by_name(request.id.bucket()); bucket != nullptr) {
                return bucket->execute(std::move(request), std::forward<Handler>(handler));
            }
            if (request.id.bucket().empty()) {
                return handler(
                  request.make_response(make_key_value_error_context(errc::common::bucket_not_found, request.id), response_type{}));
            }
            auto bucket_name = request.id.bucket();
            return open_bucket(bucket_name,
                               [self = shared_from_this(), request = std::move(request), handler = std::forward<Handler>(handler)](
                                 std::error_code ec) mutable {
                                   if (ec) {
                                       return handler(request.make_response(make_key_value_error_context(ec, request.id), response_type{}));
                                   }
                                   return self->execute(std::move(request), std::forward<Handler>(handler));
                               });
        }
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

    template<typename Handler>
    void diagnostics(std::optional<std::string> report_id, Handler&& handler)
    {
        if (!report_id) {
            report_id = std::make_optional(uuid::to_string(uuid::random()));
        }
        if (stopped_) {
            return handler({ report_id.value(), couchbase::core::meta::sdk_id() });
        }
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), report_id, handler = std::forward<Handler>(handler)]() mutable {
            diag::diagnostics_result res{ report_id.value(), couchbase::core::meta::sdk_id() };
            if (self->session_) {
                res.services[service_type::key_value].emplace_back(self->session_->diag_info());
            }
            self->for_each_bucket([&res](const auto& bucket) { bucket->export_diag_info(res); });
            self->session_manager_->export_diag_info(res);
            handler(std::move(res));
        }));
    }

    template<typename Handler>
    void ping(std::optional<std::string> report_id,
              std::optional<std::string> bucket_name,
              std::set<service_type> services,
              Handler&& handler)
    {
        do_ping(report_id, bucket_name, services, std::forward<Handler>(handler));
    }

    auto direct_dispatch(const std::string& bucket_name, std::shared_ptr<couchbase::core::mcbp::queue_request> req) -> std::error_code;

    auto direct_re_queue(const std::string& bucket_name, std::shared_ptr<mcbp::queue_request> req, bool is_retry) -> std::error_code;

  private:
    explicit cluster(asio::io_context& ctx)
      : ctx_(ctx)
      , work_(asio::make_work_guard(ctx_))
      , session_manager_(std::make_shared<io::http_session_manager>(id_, ctx_, tls_))
    {
    }

    std::shared_ptr<bucket> find_bucket_by_name(const std::string& name);

    void do_ping(std::optional<std::string> report_id,
                 std::optional<std::string> bucket_name,
                 std::set<service_type> services,
                 utils::movable_function<void(diag::ping_result)> handler);

    template<typename Handler>
    void for_each_bucket(Handler handler)
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

    template<typename Handler>
    void do_open(Handler&& handler)
    {
        // Warn users if they attempt to use Capella without TLS being enabled.
        {
            bool has_capella_host = false;
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
                && origin_.options().tls_verify != tls_verify_mode::none /* The user did not disable all TLS verification */
                && has_non_capella_host /* The connection string has a hostname that does NOT end in ".cloud.couchbase.com" */) {
                CB_LOG_WARNING("[{}] When TLS is enabled, the cluster options must specify certificate(s) to trust or ensure that they are "
                               "available in system CA store. (Unless connecting to cloud.couchbase.com.)",
                               id_);
            }
        }

        if (origin_.options().enable_tls) {
            tls_.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3);
            switch (origin_.options().tls_verify) {
                case tls_verify_mode::none:
                    tls_.set_verify_mode(asio::ssl::verify_none);
                    break;

                case tls_verify_mode::peer:
                    tls_.set_verify_mode(asio::ssl::verify_peer);
                    break;
            }
            if (origin_.options().trust_certificate.empty()) { // trust certificate is not explicitly specified
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
                CB_LOG_DEBUG(R"([{}]: use TLS verify file: "{}")", id_, origin_.options().trust_certificate);
                tls_.load_verify_file(origin_.options().trust_certificate, ec);
                if (ec) {
                    CB_LOG_ERROR("[{}]: unable to load verify file \"{}\": {}", id_, origin_.options().trust_certificate, ec.message());
                    return close([ec, handler = std::forward<Handler>(handler)]() mutable { return handler(ec); });
                }
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
            if (origin_.credentials().uses_certificate()) {
                std::error_code ec{};
                CB_LOG_DEBUG(R"([{}]: use TLS certificate chain: "{}")", id_, origin_.certificate_path());
                tls_.use_certificate_chain_file(origin_.certificate_path(), ec);
                if (ec) {
                    CB_LOG_ERROR("[{}]: unable to load certificate chain \"{}\": {}", id_, origin_.certificate_path(), ec.message());
                    return close([ec, handler = std::forward<Handler>(handler)]() mutable { return handler(ec); });
                }
                CB_LOG_DEBUG(R"([{}]: use TLS private key: "{}")", id_, origin_.key_path());
                tls_.use_private_key_file(origin_.key_path(), asio::ssl::context::file_format::pem, ec);
                if (ec) {
                    CB_LOG_ERROR("[{}]: unable to load private key \"{}\": {}", id_, origin_.key_path(), ec.message());
                    return close([ec, handler = std::forward<Handler>(handler)]() mutable { return handler(ec); });
                }
            }

            session_ = io::mcbp_session(id_, ctx_, tls_, origin_, dns_srv_tracker_);
        } else {
            session_ = io::mcbp_session(id_, ctx_, origin_, dns_srv_tracker_);
        }
        session_->bootstrap([self = shared_from_this(),
                             handler = std::forward<Handler>(handler)](std::error_code ec, const topology::configuration& config) mutable {
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
                return self->close([ec, handler = std::forward<Handler>(handler)]() mutable { handler(ec); });
            }
            handler(ec);
        });
    }

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
} // namespace couchbase::core
