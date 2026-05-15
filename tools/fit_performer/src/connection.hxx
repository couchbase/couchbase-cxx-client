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

#pragma once

#include "hooks.hxx"
#include "observability/span_owner.hxx"
#include "performer.pb.h"
#include "utils.hxx"

#include <core/cluster.hxx>
#include <core/operations.hxx>
#include <core/transactions.hxx>
#include <core/transactions/internal/utils.hxx>

#include <couchbase/cluster.hxx>
#include <couchbase/metrics/meter.hxx>
#include <couchbase/tracing/request_tracer.hxx>
#include <couchbase/transactions/transactions_config.hxx>

#include <asio/bind_executor.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#endif

#include <filesystem>
#include <fstream>
#include <memory>

static const size_t NUM_IO_THREADS = 1;
static const std::string SCHEME = "couchbase://";

namespace fit_cxx
{
using work_guard_type = asio::executor_work_guard<asio::io_context::executor_type>;

class TxnLatch
{
private:
  std::string name_;
  uint32_t value_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;

public:
  TxnLatch(const std::string name, uint32_t initial_value)
    : name_(name)
    , value_(initial_value)
  {
  }

  TxnLatch(const TxnLatch& other)
  {
    name_ = other.name_;
    std::unique_lock<std::mutex> lock(other.mutex_);
    value_ = other.value_;
  }

  const std::string& name() const
  {
    return name_;
  }

  void set()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (value_ > 0) {
      --value_;
    } else {
      spdlog::error("cannot set latch, {} is already at 0", name_);
    }
    if (value_ == 0) {
      cv_.notify_all();
    }
  }

  void wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (value_ == 0) {
      spdlog::trace("latch {} already at 0, returning immediately", name_);
      return;
    }
    spdlog::trace("waiting for latch {}", name_);
    cv_.wait(lock, [&]() -> bool {
      return value_ == 0;
    });
    spdlog::trace("wait for latch {} done, returning now", name_);
  }
};

class Connection : public std::enable_shared_from_this<Connection>
{
private:
  asio::io_context ctx_{ ASIO_CONCURRENCY_HINT_SAFE };
  work_guard_type guard_{ asio::make_work_guard(ctx_) };
  std::shared_ptr<couchbase::cluster> cluster_;
  std::list<std::thread> io_threads_{};
  std::string bucket_;
  std::string cluster_address_;
  std::list<TxnLatch> latches_;
  std::function<void(const std::string&)> latch_cb_;
  std::atomic<bool> should_read_{ false };
  fit_cxx::test_hook_pair hook_pair_;
  std::vector<TxnSvcHook> hook_vector_;
  std::mutex mutex_;

  // We have to keep the pointers to the providers, otherwise once they get destroyed they will shut
  // down all tracers/meters created by them
#ifdef COUCHBASE_CXX_CLIENT_BUILD_OPENTELEMETRY
  std::unique_ptr<opentelemetry::sdk::trace::TracerProvider> otel_tracer_provider_{};
  std::unique_ptr<opentelemetry::sdk::metrics::MeterProvider> otel_meter_provider_{};
#endif

  std::shared_ptr<couchbase::tracing::request_tracer> tracer_{};
  std::shared_ptr<couchbase::metrics::meter> meter_{};

  std::vector<std::filesystem::path> temp_files_{};

public:
  explicit Connection(const ::protocol::shared::ClusterConnectionCreateRequest* req)
    : cluster_address_(req->cluster_hostname())
  {
    // If the FIT_CXX_NUM_IO_THREADS env variable is set use it to determine the number of IO
    // threads, otherwise use the default
    std::size_t io_thread_count = NUM_IO_THREADS;
    char* io_thread_count_env;
    io_thread_count_env = std::getenv("FIT_CXX_NUM_IO_THREADS");
    if (io_thread_count_env != nullptr) {
      io_thread_count = static_cast<std::size_t>(std::stoi(io_thread_count_env));
    }

    // A numIOThreads tunable can also be given through the driver which has precedence over the
    // environment variable
    auto io_thread_count_tunable_name = "numIOThreads";
    if (req->tunables().contains(io_thread_count_tunable_name)) {
      io_thread_count =
        static_cast<std::size_t>(std::stoi(req->tunables().at(io_thread_count_tunable_name)));
    }

    spdlog::info("Using {} IO thread(s)", io_thread_count);

    for (std::size_t i = 0; i < io_thread_count; i++) {
      io_threads_.emplace_back([this]() {
        ctx_.run();
      });
    }

    auto options = [&]() -> couchbase::cluster_options {
      if (!req->has_authenticator()) {
        // If authenticator is not set, the protobuf request _must_ populate the username & password
        // fields
        return couchbase::cluster_options{ req->cluster_username(), req->cluster_password() };
      }

      switch (req->authenticator().authenticator_case()) {
        case protocol::shared::Authenticator::AuthenticatorCase::kPasswordAuth: {
          return couchbase::cluster_options{
            couchbase::password_authenticator{
              req->authenticator().password_auth().username(),
              req->authenticator().password_auth().password(),
            },
          };
        }
        case protocol::shared::Authenticator::AuthenticatorCase::kCertificateAuth: {
          auto cert_auth =
            create_certificate_authenticator(req->authenticator().certificate_auth());
          return couchbase::cluster_options{ std::move(cert_auth) };
        }
#ifdef COUCHBASE_CXX_CLIENT_HAS_JWT_AUTHENTICATOR
        case protocol::shared::Authenticator::AuthenticatorCase::kJwtAuth: {
          return couchbase::cluster_options{ couchbase::jwt_authenticator{
            req->authenticator().jwt_auth().jwt(),
          } };
        }
#endif
        default:
          throw performer_exception::invalid_argument("Unknown authenticator type");
      }
    }();

    if (req->has_cluster_config()) {
      if (req->cluster_config().has_transactions_config()) {
        TxnSvcUtils::update_common_options(req->cluster_config().transactions_config(),
                                           options.transactions(),
                                           nullptr,
                                           hook_vector_);
        auto& conf = req->cluster_config().transactions_config();
        if (conf.has_query_config()) {
          auto& query_conf = conf.query_config();
          if (query_conf.has_scan_consistency()) {
            options.transactions().query_config().scan_consistency(
              query_conf.scan_consistency() == protocol::shared::ScanConsistency::NOT_BOUNDED
                ? couchbase::query_scan_consistency::not_bounded
                : couchbase::query_scan_consistency::request_plus);
          }
        }
        if (conf.has_cleanup_config()) {
          if (conf.cleanup_config().has_cleanup_client_attempts()) {
            options.transactions().cleanup_config().cleanup_client_attempts(
              conf.cleanup_config().cleanup_client_attempts());
          } else {
            options.transactions().cleanup_config().cleanup_client_attempts(true);
          }
          if (conf.cleanup_config().has_cleanup_lost_attempts()) {
            options.transactions().cleanup_config().cleanup_lost_attempts(
              conf.cleanup_config().cleanup_lost_attempts());
          } else {
            options.transactions().cleanup_config().cleanup_lost_attempts(true);
          }
          if (conf.cleanup_config().has_cleanup_window_millis()) {
            options.transactions().cleanup_config().cleanup_window(
              std::chrono::milliseconds(conf.cleanup_config().cleanup_window_millis()));
          }
          for (int i = 0; i < conf.cleanup_config().cleanup_collection_size(); i++) {
            auto& coll = conf.cleanup_config().cleanup_collection(i);
            options.transactions().cleanup_config().add_collection(
              { coll.bucket_name(), coll.scope_name(), coll.collection_name() });
          }
        }
      }

      if (req->cluster_config().has_cert_path()) {
        options.security().trust_certificate(req->cluster_config().cert_path());
      }
      if (req->cluster_config().has_cert()) {
        options.security().trust_certificate_value(req->cluster_config().cert());
      }
      if (req->cluster_config().has_insecure() && req->cluster_config().insecure()) {
        options.security().tls_verify(couchbase::tls_verify_mode::none);
      }
      if (req->cluster_config().has_preferred_server_group()) {
        options.network().preferred_server_group(req->cluster_config().preferred_server_group());
      }
      if (req->cluster_config().has_observability_config()) {
        configure_observability(req->cluster_config().observability_config(), options);
      }
#ifdef COUCHBASE_CXX_CLIENT_SUPPORTS_APP_TELEMETRY
      if (req->cluster_config().has_enable_app_telemetry()) {
        options.application_telemetry().enable(req->cluster_config().enable_app_telemetry());
      }
      if (req->cluster_config().has_app_telemetry_endpoint()) {
        options.application_telemetry().override_endpoint(
          req->cluster_config().app_telemetry_endpoint());
      }
      if (req->cluster_config().has_app_telemetry_backoff_secs()) {
        options.application_telemetry().backoff_interval(
          std::chrono::seconds(req->cluster_config().app_telemetry_backoff_secs()));
      }
      if (req->cluster_config().has_app_telemetry_ping_timeout_secs()) {
        options.application_telemetry().ping_timeout(
          std::chrono::seconds(req->cluster_config().app_telemetry_ping_timeout_secs()));
      }
      if (req->cluster_config().has_app_telemetry_ping_interval_secs()) {
        options.application_telemetry().ping_interval(
          std::chrono::seconds(req->cluster_config().app_telemetry_ping_interval_secs()));
      }
#endif
    }
    std::string address = cluster_address_;
    // Workaround: SDK should work with connection strings without a scheme - TODO: remove this once
    // fixed
    if (address.find("://") == std::string::npos) {
      address.insert(0, SCHEME);
    }
    spdlog::info("Using connection string `{}`.", address);
    for (int i = 0; i < 10 && !cluster_; i++) {
      // For SDK versions where the asio context is not exposed, the IO context created in the
      // performer is only used to fire hook actions.
      auto [err, cluster] = couchbase::cluster::connect(address, options).get();
      if (err) {
        spdlog::error(
          "Error #{} opening cluster, will sleep for 5 seconds and try again.  Error is {}",
          i,
          err.ec().message());
        std::this_thread::sleep_for(std::chrono::seconds(5));
      } else {
        cluster_ = std::make_shared<couchbase::cluster>(std::move(cluster));
      }
    }
  }

  ~Connection()
  {
    spdlog::trace("destroying connection...");
    cluster_->close().get();

    spdlog::trace("cluster closed, stopping io...");
    ctx_.stop();

    spdlog::trace("io context stopped, resetting guard...");
    guard_.reset();

    // join io_threads...
    spdlog::trace("waiting on i/o threads...");
    for (auto& t : io_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
    spdlog::trace("connection destroyed");

    clear_temporary_files();
  }

  void configure_observability(const protocol::observability::Config& config,
                               couchbase::cluster_options& cluster_opts);

  template<typename R, typename T>
  R run_with_timeout(T timeout, const std::function<R()>& fn, const std::function<R()>& err)
  {
    asio::steady_timer fn_timer(ctx_);
    auto id_for_logger = couchbase::core::uuid::to_string(couchbase::core::uuid::random());
    auto barrier = std::make_shared<std::promise<R>>();
    auto f = barrier->get_future();
    fn_timer.expires_after(timeout);
    fn_timer.async_wait([&, id_for_logger, barrier, err](std::error_code ec) {
      if (ec) {
        // we were cancelled, do nothing
        spdlog::debug("[{}]timer cancelled {}", id_for_logger, ec.message());
        return;
      }
      spdlog::error("[{}]time out running a transaction!!!", id_for_logger);
      barrier->set_value(err());
    });
    spdlog::debug("[{}]starting txn ", id_for_logger);
    barrier->set_value(fn());
    return f.get();
  }
  void fixup_hooks()
  {
    for (auto& h : hook_vector_) {
      h.update_conn(shared_from_this());
    }
  }
  std::vector<std::byte> convert_content(const std::string& s)
  {
    std::vector<std::byte> bytes;
    bytes.reserve(s.size());

    std::transform(std::begin(s), std::end(s), std::back_inserter(bytes), [](char c) {
      return std::byte(c);
    });
    return bytes;
  }

  void upsert_doc(couchbase::core::document_id id,
                  const std::string& content,
                  couchbase::core::utils::movable_function<void()>&& cb)
  {
    return asio::post(asio::bind_executor(ctx_, [this, id, content, cb = std::move(cb)]() mutable {
      couchbase::core::operations::upsert_request req{
        id,
        couchbase::core::utils::to_binary(content),
      };
      return core().execute(
        req, [id, cb = std::move(cb)](couchbase::core::operations::upsert_response resp) mutable {
          if (resp.ctx.ec()) {
            spdlog::info("upserting doc id {} got error {}", id.key(), resp.ctx.ec().message());
          } else {
            spdlog::info("upserted doc id {} and cas {}", id.key(), resp.cas);
          }
          return cb();
        });
    }));
  }

  void remove_doc(couchbase::core::document_id id,
                  couchbase::core::utils::movable_function<void()>&& cb)
  {
    return asio::post(
      asio::bind_executor(ctx_, [this, id = std::move(id), cb = std::move(cb)]() mutable {
        couchbase::core::operations::remove_request req{ id };
        return core().execute(
          req, [id, cb = std::move(cb)](couchbase::core::operations::remove_response resp) mutable {
            if (resp.ctx.ec()) {
              spdlog::info("removed doc id {} and cas {}", id.key(), resp.cas);
            } else {
              spdlog::info("removing doc id {} got error {}", id.key(), resp.ctx.ec().message());
            }
            return cb();
          });
      }));
  }

  void upsert_doc(couchbase::core::document_id id, const std::string& content)
  {
    auto json_content = couchbase::core::utils::json::parse(content);
    auto f = std::async(std::launch::async, [this, id, json_content] {
      try {
        auto [err, doc] = cluster_->bucket(id.bucket())
                            .scope(id.scope())
                            .collection(id.collection())
                            .upsert(id.key(), json_content, {})
                            .get();
        if (!err.ec()) {
          std::cout << "upserted doc id " << id.key() << " and cas " << doc.cas().value()
                    << std::endl;
        } else {
          std::cout << "upserted doc " << id.key() << " got error " << err.ec().message()
                    << std::endl;
        }
      } catch (std::exception& e) {
        std::cout << "got error " << e.what() << std::endl;
      }
    });
    bool working = true;
    while (working) {
      auto status = f.wait_for(std::chrono::milliseconds(0));
      working = (status != std::future_status::ready);
      std::cout << "waiting...";
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "upsert doc complete";
  }

  void remove_doc(couchbase::core::document_id id)
  {
    auto f = std::async(std::launch::async, [this, id] {
      try {
        auto [err, res] = cluster_->bucket(id.bucket())
                            .scope(id.scope())
                            .collection(id.collection())
                            .remove(id.key(), {})
                            .get();
        if (!err.ec()) {
          std::cout << "removed doc id " << id.key() << std::endl;
        } else {
          std::cout << "removed doc id " << id.key() << " got error " << err.ec().message()
                    << std::endl;
        }
      } catch (std::exception& e) {
        std::cout << "got error " << e.what() << std::endl;
      }
    });
    bool working = true;
    while (working) {
      auto status = f.wait_for(std::chrono::milliseconds(0));
      working = (status != std::future_status::ready);
      std::cout << "waiting...";
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "removed doc complete";
  }

  std::shared_ptr<couchbase::cluster> cluster()
  {
    return cluster_;
  }

  const std::string& cluster_address()
  {
    return cluster_address_;
  }

  asio::io_context& ctx()
  {
    return ctx_;
  }

  couchbase::core::cluster core()
  {
    return couchbase::core::get_core_cluster(*cluster_);
  }

  bool should_read()
  {
    return should_read_.load();
  }

  void should_read(bool should_read)
  {
    should_read_ = should_read;
  }

  void add_latch(TxnLatch& latch)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    latches_.push_back(latch);
  }

  TxnLatch& get_latch(const std::string& name)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    auto it = std::find_if(latches_.begin(), latches_.end(), [&](const TxnLatch& latch) {
      return latch.name() == name;
    });
    if (it == latches_.end()) {
      throw performer_exception::internal(std::string("unknown latch name ") + name);
    }
    return *it;
  }

  void set_latch_callback(std::function<void(const std::string&)> cb)
  {
    // store the callback, to add to any future latches
    const std::scoped_lock<std::mutex> lock(mutex_);
    latch_cb_ = cb;
  }

  void call_latch_callback(const std::string& name)
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (latch_cb_) {
      latch_cb_(name);
    }
  }

  void clear_latches()
  {
    const std::scoped_lock<std::mutex> lock(mutex_);
    should_read_ = false;
    latches_.clear();
  }

  auto create_certificate_authenticator(
    const protocol::shared::Authenticator_CertificateAuthenticator& auth)
    -> couchbase::certificate_authenticator;
  auto create_temporary_file(const std::string& id, const std::string& content)
    -> std::filesystem::path;
  void clear_temporary_files();

  auto tracer() -> const std::shared_ptr<couchbase::tracing::request_tracer>&;
};
} // namespace fit_cxx
