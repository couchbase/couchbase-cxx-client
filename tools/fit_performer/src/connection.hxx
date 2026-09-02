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
#include <core/platform/uuid.h>
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

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static const size_t NUM_IO_THREADS = 1;
// inline constexpr const char* avoids the per-translation-unit dynamic initialization (and separate
// instance) that a header-scope `static const std::string` would incur.
inline constexpr const char* SCHEME = "couchbase://";

namespace fit_cxx
{
using work_guard_type = asio::executor_work_guard<asio::io_context::executor_type>;

// Parse a thread-count value coming from external input (the FIT_CXX_NUM_IO_THREADS env variable or
// the numIOThreads driver tunable). std::stoi throws on empty/non-numeric/out-of-range input, which
// would otherwise crash the performer at startup, so fall back to the current value and warn.
inline auto
parse_io_thread_count(const std::string& value, std::size_t fallback) -> std::size_t
{
  try {
    const int parsed = std::stoi(value);
    if (parsed < 1) {
      // A non-positive count must not be cast to size_t: a negative value would wrap to an enormous
      // number and spawn a huge number of IO threads (resource exhaustion). Fall back instead.
      spdlog::warn("ignoring non-positive IO thread count \"{}\"; using {}", value, fallback);
      return fallback;
    }
    return static_cast<std::size_t>(parsed);
  } catch (const std::exception& e) {
    spdlog::warn(
      "ignoring invalid IO thread count \"{}\": {}; using {}", value, e.what(), fallback);
    return fallback;
  }
}

class Connection : public std::enable_shared_from_this<Connection>
{
private:
  asio::io_context ctx_{ ASIO_CONCURRENCY_HINT_SAFE };
  work_guard_type guard_{ asio::make_work_guard(ctx_) };
  std::shared_ptr<couchbase::cluster> cluster_;
  std::list<std::thread> io_threads_{};
  std::string bucket_;
  std::string cluster_address_;
  fit_cxx::test_hook_pair hook_pair_;
  std::list<TxnSvcHook> hook_vector_;

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
      io_thread_count = parse_io_thread_count(io_thread_count_env, io_thread_count);
    }

    // A numIOThreads tunable can also be given through the driver which has precedence over the
    // environment variable
    auto io_thread_count_tunable_name = "numIOThreads";
    if (req->tunables().contains(io_thread_count_tunable_name)) {
      io_thread_count =
        parse_io_thread_count(req->tunables().at(io_thread_count_tunable_name), io_thread_count);
    }

    // At least one IO thread is required, otherwise ctx_.run() never executes and timeouts/hooks
    // that depend on the io_context would hang.
    if (io_thread_count < 1) {
      io_thread_count = 1;
    }

    spdlog::info("Using {} IO thread(s)", io_thread_count);

    for (std::size_t i = 0; i < io_thread_count; i++) {
      io_threads_.emplace_back([this]() {
        ctx_.run();
      });
    }

    // From here on the IO threads are running and (for certificate auth) private-key temp files may
    // be written. If construction fails past this point the destructor will NOT run, so on any
    // exception clean up here (join the IO threads -- a joinable std::thread destructor calls
    // std::terminate -- and remove the temp files) before rethrowing.
    try {
      auto options = [&]() -> couchbase::cluster_options {
        if (!req->has_authenticator()) {
          // If authenticator is not set, the protobuf request _must_ populate the username &
          // password fields
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
      // Workaround: SDK should work with connection strings without a scheme - TODO: remove this
      // once fixed
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
      if (!cluster_) {
        // Fail fast: the rest of the class (including the destructor) unconditionally dereferences
        // cluster_, so a Connection must never be constructed without one.
        throw std::runtime_error("failed to connect to cluster at " + address +
                                 " after multiple attempts");
      }
    } catch (...) {
      ctx_.stop();
      guard_.reset();
      for (auto& t : io_threads_) {
        if (t.joinable()) {
          t.join();
        }
      }
      clear_temporary_files();
      throw;
    }
  }

  ~Connection()
  {
    // A destructor must never let an exception escape (that calls std::terminate). close().get()
    // can throw (e.g. broken_promise) so it is guarded separately, and the io_context teardown runs
    // regardless: a joinable std::thread destructor also calls std::terminate, so the threads must
    // be joined even if closing the cluster failed.
    spdlog::trace("destroying connection...");
    try {
      cluster_->close().get();
    } catch (const std::exception& e) {
      spdlog::warn("error closing cluster during teardown: {}", e.what());
    } catch (...) {
      spdlog::warn("unknown error closing cluster during teardown");
    }

    spdlog::trace("cluster closed, stopping io...");
    ctx_.stop();

    spdlog::trace("io context stopped, resetting guard...");
    guard_.reset();

    // join io_threads...
    spdlog::trace("waiting on i/o threads...");
    for (auto& t : io_threads_) {
      if (t.joinable()) {
        try {
          t.join();
        } catch (const std::exception& e) {
          spdlog::warn("error joining io thread during teardown: {}", e.what());
        }
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
    // The timer fires on an io thread while fn() runs on this thread; both used to call
    // set_value() unconditionally, which throws std::future_error on the second call. Guard the
    // promise so it is satisfied exactly once. fn() is expected to observe the timeout
    // cooperatively (err() flips a flag that fn() polls), so it still runs to completion here.
    auto resolved = std::make_shared<std::atomic_bool>(false);
    auto f = barrier->get_future();
    fn_timer.expires_after(timeout);
    fn_timer.async_wait([id_for_logger, barrier, err, resolved](std::error_code ec) {
      if (ec) {
        // we were cancelled, do nothing
        spdlog::debug("[{}]timer cancelled {}", id_for_logger, ec.message());
        return;
      }
      if (!resolved->exchange(true)) {
        spdlog::error("[{}]time out running a transaction!!!", id_for_logger);
        barrier->set_value(err());
      }
    });
    spdlog::debug("[{}]starting txn ", id_for_logger);
    // Never let an exception from fn() unwind through the (noexcept) gRPC handler that called us:
    // that is undefined behaviour and can terminate the server process. Convert it to the error
    // result instead, exactly as a timeout would. If the timer already resolved the barrier, the
    // exception is simply swallowed and the timeout result is returned.
    try {
      auto result = fn();
      // Cancel the timer on the success path too (the catch paths already do): otherwise its
      // callback stays queued and runs later only to lose the `resolved` race - pointless work that
      // keeps the io_context busier than needed under heavy transaction load.
      fn_timer.cancel();
      if (!resolved->exchange(true)) {
        barrier->set_value(std::move(result));
      }
    } catch (const std::exception& e) {
      fn_timer.cancel();
      if (!resolved->exchange(true)) {
        spdlog::error("[{}]exception running a transaction ({}); returning error result",
                      id_for_logger,
                      e.what());
        barrier->set_value(err());
      }
    } catch (...) {
      fn_timer.cancel();
      if (!resolved->exchange(true)) {
        spdlog::error("[{}]unknown exception running a transaction; returning error result",
                      id_for_logger);
        barrier->set_value(err());
      }
    }
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
      // Cast through unsigned char so high-bit bytes are not sign-extended on platforms where char
      // is signed (which would corrupt the byte value).
      return std::byte(static_cast<unsigned char>(c));
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
              spdlog::info("removing doc id {} got error {}", id.key(), resp.ctx.ec().message());
            } else {
              spdlog::info("removed doc id {} and cas {}", id.key(), resp.cas);
            }
            return cb();
          });
      }));
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

  auto create_certificate_authenticator(
    const protocol::shared::Authenticator_CertificateAuthenticator& auth)
    -> couchbase::certificate_authenticator;
  auto create_temporary_file(const std::string& id, const std::string& content)
    -> std::filesystem::path;
  void clear_temporary_files();

  auto tracer() -> const std::shared_ptr<couchbase::tracing::request_tracer>&;
};
} // namespace fit_cxx
