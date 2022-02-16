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

#include <couchbase/bucket.hxx>
#include <couchbase/diagnostics.hxx>
#include <couchbase/io/dns_client.hxx>
#include <couchbase/io/http_command.hxx>
#include <couchbase/io/http_session_manager.hxx>
#include <couchbase/io/mcbp_command.hxx>
#include <couchbase/io/mcbp_session.hxx>
#include <couchbase/metrics/logging_meter.hxx>
#include <couchbase/metrics/noop_meter.hxx>
#include <couchbase/operations.hxx>
#include <couchbase/operations/management/bucket_create.hxx>
#include <couchbase/origin.hxx>
#include <couchbase/tracing/noop_tracer.hxx>
#include <couchbase/tracing/threshold_logging_tracer.hxx>
#include <couchbase/utils/join_strings.hxx>

#include <asio/ssl.hpp>
#include <fstream>
#include <thread>
#include <utility>

namespace couchbase
{
class cluster : public std::enable_shared_from_this<cluster>
{
  public:
    [[nodiscard]] static std::shared_ptr<cluster> create(asio::io_context& ctx)
    {
        return std::shared_ptr<cluster>(new cluster(ctx));
    }

    template<typename Handler>
    void open(const couchbase::origin& origin, Handler&& handler)
    {
        if (stopped_) {
            return handler(error::network_errc::cluster_closed);
        }
        if (origin.get_nodes().empty()) {
            stopped_ = true;
            work_.reset();
            return handler(error::common_errc::invalid_argument);
        }

        origin_ = origin;
        if (origin_.options().enable_tracing) {
            tracer_ = std::make_shared<tracing::threshold_logging_tracer>(ctx_, origin.options().tracing_options);
        } else {
            tracer_ = std::make_shared<tracing::noop_tracer>();
        }
        if (origin_.options().enable_metrics) {
            meter_ = std::make_shared<metrics::logging_meter>(ctx_, origin.options().metrics_options);
        } else {
            meter_ = std::make_shared<metrics::noop_meter>();
        }
        session_manager_->set_tracer(tracer_);
        if (origin_.options().enable_dns_srv) {
            return asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), handler = std::forward<Handler>(handler)]() mutable {
                return self->do_dns_srv(std::forward<Handler>(handler));
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
                self->session_->stop(io::retry_reason::do_not_retry);
            }
            self->for_each_bucket([](auto& bucket) { bucket->close(); });
            self->session_manager_->close();
            handler();
            self->work_.reset();
            self->tracer_.reset();
            self->meter_.reset();
        }));
    }

    template<typename Handler>
    void open_bucket(const std::string& bucket_name, Handler&& handler)
    {
        if (stopped_) {
            return handler(error::network_errc::cluster_closed);
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
                b = std::make_shared<bucket>(id_, ctx_, tls_, tracer_, meter_, bucket_name, origin_, known_features);
                buckets_.try_emplace(bucket_name, b);
            }
        }
        if (b == nullptr) {
            return handler({});
        }

        b->bootstrap([self = shared_from_this(), bucket_name, handler = std::forward<Handler>(handler)](
                       std::error_code ec, const topology::configuration& config) mutable {
            if (ec) {
                std::scoped_lock lock(self->buckets_mutex_);
                self->buckets_.erase(bucket_name);
            } else if (!self->session_->supports_gcccp()) {
                self->session_manager_->set_configuration(config, self->origin_.options());
            }
            handler(ec);
        });
        b->on_configuration_update([self = shared_from_this()](topology::configuration new_config) {
            self->session_manager_->update_configuration(std::move(new_config));
        });
    }

    template<typename Handler>
    void close_bucket(const std::string& bucket_name, Handler&& handler)
    {
        if (stopped_) {
            return handler(error::network_errc::cluster_closed);
        }
        std::shared_ptr<bucket> b{};
        {
            std::scoped_lock lock(buckets_mutex_);
            auto ptr = buckets_.find(bucket_name);
            if (ptr != buckets_.end()) {
                b = ptr->second;
                buckets_.erase(ptr);
            }
        }
        if (b != nullptr) {
            b->close();
        }
        return handler({});
    }

    template<class Request,
             class Handler,
             typename std::enable_if_t<!std::is_same_v<typename Request::encoded_request_type, io::http_request>, int> = 0>
    void execute(Request request, Handler&& handler)
    {
        using response_type = typename Request::encoded_response_type;
        if (stopped_) {
            return handler(request.make_response({ request.id, error::network_errc::cluster_closed }, response_type{}));
        }
        if (auto bucket = find_bucket_by_name(request.id.bucket()); bucket != nullptr) {
            return bucket->execute(request, std::forward<Handler>(handler));
        }
        return handler(request.make_response({ request.id, error::common_errc::bucket_not_found }, response_type{}));
    }

    template<class Request,
             class Handler,
             typename std::enable_if_t<std::is_same_v<typename Request::encoded_request_type, io::http_request>, int> = 0>
    void execute(Request request, Handler&& handler)
    {
        using response_type = typename Request::encoded_response_type;
        if (stopped_) {
            return handler(request.make_response({ error::network_errc::cluster_closed }, response_type{}));
        }
        return session_manager_->execute(request, std::forward<Handler>(handler), origin_.credentials());
    }

    template<typename Handler>
    void diagnostics(std::optional<std::string> report_id, Handler&& handler)
    {
        if (!report_id) {
            report_id = std::make_optional(uuid::to_string(uuid::random()));
        }
        if (stopped_) {
            return handler({ report_id.value(), couchbase::meta::sdk_id() });
        }
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this(), report_id, handler = std::forward<Handler>(handler)]() mutable {
            diag::diagnostics_result res{ report_id.value(), couchbase::meta::sdk_id() };
            if (self->session_) {
                res.services[service_type::key_value].emplace_back(self->session_->diag_info());
            }
            self->for_each_bucket([&res](auto& bucket) { bucket->export_diag_info(res); });
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

  private:
    explicit cluster(asio::io_context& ctx)
      : ctx_(ctx)
      , work_(asio::make_work_guard(ctx_))
      , session_manager_(std::make_shared<io::http_session_manager>(id_, ctx_, tls_))
      , dns_client_(ctx_)
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
    void do_dns_srv(Handler&& handler)
    {
        std::string hostname;
        std::string service;
        std::tie(hostname, service) = origin_.next_address();
        service = origin_.options().enable_tls ? "_couchbases" : "_couchbase";
        dns_client_.query_srv(
          hostname,
          service,
          [hostname, self = shared_from_this(), handler = std::forward<Handler>(handler)](
            couchbase::io::dns::dns_client::dns_srv_response&& resp) mutable {
              if (resp.ec) {
                  LOG_WARNING("failed to fetch DNS SRV records for \"{}\" ({}), assuming that cluster is listening this address",
                              hostname,
                              resp.ec.message());
              } else if (resp.targets.empty()) {
                  LOG_WARNING("DNS SRV query returned 0 records for \"{}\", assuming that cluster is listening this address", hostname);
              } else {
                  origin::node_list nodes;
                  nodes.reserve(resp.targets.size());
                  for (const auto& address : resp.targets) {
                      origin::node_entry node;
                      node.first = address.hostname;
                      node.second = std::to_string(address.port);
                      nodes.emplace_back(node);
                  }
                  self->origin_.set_nodes(nodes);
                  LOG_INFO("replace list of bootstrap nodes with addresses from DNS SRV of \"{}\": [{}]",
                           hostname,
                           utils::join_strings(self->origin_.get_nodes(), ", "));
              }
              return self->do_open(std::forward<Handler>(handler));
          });
    }

    template<typename Handler>
    void do_open(Handler&& handler)
    {
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
            if (!origin_.options().trust_certificate.empty()) {
                std::error_code ec{};
                LOG_DEBUG(R"([{}]: use TLS verify file: "{}")", id_, origin_.options().trust_certificate);
                tls_.load_verify_file(origin_.options().trust_certificate, ec);
                if (ec) {
                    LOG_ERROR("[{}]: unable to load verify file \"{}\": {}", id_, origin_.options().trust_certificate, ec.message());
                    return handler(ec);
                }
            }
#ifdef COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE
            SSL_CTX_set_keylog_callback(tls_.native_handle(), [](const SSL* /* ssl */, const char* line) {
                std::ofstream keylog(COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE, std::ios::out | std::ios::app | std::ios::binary);
                keylog << std::string_view(line) << std::endl;
            });
            LOG_CRITICAL(
              "COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE was set to \"{}\" during build, all TLS keys will be logged for network analysis "
              "(https://wiki.wireshark.org/TLS). DO NOT USE THIS BUILD IN PRODUCTION",
              COUCHBASE_CXX_CLIENT_TLS_KEY_LOG_FILE);
#endif
            if (origin_.credentials().uses_certificate()) {
                std::error_code ec{};
                LOG_DEBUG(R"([{}]: use TLS certificate chain: "{}")", id_, origin_.certificate_path());
                tls_.use_certificate_chain_file(origin_.certificate_path(), ec);
                if (ec) {
                    LOG_ERROR("[{}]: unable to load certificate chain \"{}\": {}", id_, origin_.certificate_path(), ec.message());
                    return handler(ec);
                }
                LOG_DEBUG(R"([{}]: use TLS private key: "{}")", id_, origin_.key_path());
                tls_.use_private_key_file(origin_.key_path(), asio::ssl::context::file_format::pem, ec);
                if (ec) {
                    LOG_ERROR("[{}]: unable to load private key \"{}\": {}", id_, origin_.key_path(), ec.message());
                    return handler(ec);
                }
            }
            session_ = std::make_shared<io::mcbp_session>(id_, ctx_, tls_, origin_);
        } else {
            session_ = std::make_shared<io::mcbp_session>(id_, ctx_, origin_);
        }
        session_->bootstrap([self = shared_from_this(),
                             handler = std::forward<Handler>(handler)](std::error_code ec, const topology::configuration& config) mutable {
            if (!ec) {
                if (self->origin_.options().network == "auto") {
                    self->origin_.options().network = config.select_network(self->session_->bootstrap_hostname());
                    if (self->origin_.options().network == "default") {
                        LOG_DEBUG(R"({} detected network is "{}")", self->session_->log_prefix(), self->origin_.options().network);
                    } else {
                        LOG_INFO(R"({} detected network is "{}")", self->session_->log_prefix(), self->origin_.options().network);
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
                    LOG_INFO("replace list of bootstrap nodes with addresses of alternative network \"{}\": [{}]",
                             self->origin_.options().network,
                             utils::join_strings(self->origin_.get_nodes(), ","));
                }
                self->session_manager_->set_configuration(config, self->origin_.options());
                self->session_->on_configuration_update([manager = self->session_manager_](topology::configuration new_config) {
                    manager->update_configuration(std::move(new_config));
                });
            }
            handler(ec);
        });
    }

    std::string id_{ uuid::to_string(uuid::random()) };
    asio::io_context& ctx_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    asio::ssl::context tls_{ asio::ssl::context::tls_client };
    std::shared_ptr<io::http_session_manager> session_manager_;
    io::dns::dns_config& dns_config_{ io::dns::dns_config::get() };
    couchbase::io::dns::dns_client dns_client_;
    std::shared_ptr<io::mcbp_session> session_{};
    std::mutex buckets_mutex_{};
    std::map<std::string, std::shared_ptr<bucket>> buckets_{};
    couchbase::origin origin_{};
    std::shared_ptr<tracing::request_tracer> tracer_{ nullptr };
    std::shared_ptr<metrics::meter> meter_{ nullptr };
    std::atomic_bool stopped_{ false };
};
} // namespace couchbase
